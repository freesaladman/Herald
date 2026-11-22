//! Hook runner: subscribes to the bus and runs matching hooks.

use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Semaphore, broadcast, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;

use crate::bus::Bus;
use crate::config::{Config, HookAction, HookConfig, Paths};
use crate::event::Event;
use crate::hooks::{CompiledHookFilter, expand, lookup};
use crate::process::kill_tree;
use crate::store::{HookRun, Store};

const MAX_CONCURRENT: usize = 4;
const STDERR_TAIL_BYTES: usize = 4096;
const STDIN_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const STDERR_GRACE: Duration = Duration::from_secs(2);
const EVENT_FILE_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);
const DEFAULT_TIMEOUT_SECS: u64 = 60;
const SNOOZE_KEY: &str = "snoozed_until_utc";
const SHUTDOWN_GRACE: Duration = Duration::from_secs(8);
const DEBOUNCE_PRUNE_AT: usize = 1024;
type Recorder = Arc<dyn Fn(HookRun) -> BoxFuture<'static, ()> + Send + Sync>;
type RunKey = (String, String);
type CompiledHooks = Vec<(HookConfig, CompiledHookFilter)>;

/// Owns the task that runs hooks; `shutdown` stops it and every run in flight.
pub struct HookRunner {
    task: JoinHandle<()>,
    stop: oneshot::Sender<()>,
}

impl HookRunner {
    /// Subscribes to the bus before returning, so no event published after this call is missed.
    pub fn start(
        bus: &Bus,
        store: Arc<dyn Store>,
        paths: Paths,
        config: watch::Receiver<Arc<Config>>,
    ) -> Self {
        let rx = bus.subscribe();
        let recorder: Recorder = {
            let store = store.clone();
            Arc::new(move |run| {
                let store = store.clone();
                async move {
                    if let Err(e) = store.record_hook_run(run).await {
                        tracing::error!(error = %e, "could not record hook run");
                    }
                }
                .boxed()
            })
        };
        let shared = Arc::new(Shared::new(store, paths, recorder));
        let (stop, stop_rx) = oneshot::channel();
        let task = tokio::spawn(run_loop(shared, rx, config, stop_rx));
        Self { task, stop }
    }

    /// Lets the loop end its runs (their guards kill the child trees); aborts if that stalls.
    pub async fn shutdown(self) {
        let Self { mut task, stop } = self;
        let _ = stop.send(());
        if tokio::time::timeout(SHUTDOWN_GRACE, &mut task)
            .await
            .is_err()
        {
            tracing::warn!("hook runner did not stop in time; aborting");
            task.abort();
            let _ = task.await;
        }
    }
}

fn compile_hooks(config: &Config) -> CompiledHooks {
    config
        .hooks
        .iter()
        .map(|h| (h.clone(), CompiledHookFilter::new(&h.on, &h.r#match)))
        .collect()
}

async fn run_loop(
    shared: Arc<Shared>,
    mut rx: broadcast::Receiver<Arc<Event>>,
    mut config: watch::Receiver<Arc<Config>>,
    mut stop: oneshot::Receiver<()>,
) {
    let mut hooks = compile_hooks(&config.borrow_and_update());
    let mut runs: JoinSet<()> = JoinSet::new();
    let dir = shared.tmp_dir();
    if let Err(e) = tokio::task::spawn_blocking(move || clean_old_event_files(&dir)).await {
        tracing::warn!(error = %e, "event file cleanup failed");
    }
    loop {
        tokio::select! {
            _ = &mut stop => break,
            received = rx.recv() => match received {
                Ok(event) => {
                    // Read before dispatch, which must stay sync to claim debounce slots atomically.
                    let snoozed = hooks.iter().any(|(h, _)| h.respect_snooze) && shared.snoozed().await;
                    let dispatched = std::panic::catch_unwind(AssertUnwindSafe(|| {
                        shared.dispatch(&hooks, &event, snoozed)
                    }));
                    let Ok(jobs) = dispatched else {
                        tracing::error!(event = %event.id, "hook dispatch panicked");
                        continue;
                    };
                    for job in jobs {
                        let shared = shared.clone();
                        let name = job.hook.name.clone();
                        runs.spawn(async move {
                            if AssertUnwindSafe(shared.execute(job)).catch_unwind().await.is_err() {
                                tracing::error!(hook = %name, "hook run panicked");
                            }
                        });
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::warn!(skipped, "hook runner lagged behind the bus");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            },
            changed = config.changed() => {
                if changed.is_err() {
                    break;
                }
                hooks = compile_hooks(&config.borrow_and_update());
            }
            Some(_) = runs.join_next() => {}
        }
    }
    // Aborting and awaiting the runs drops their kill guards, which end the child trees.
    runs.shutdown().await;
}

fn clean_old_event_files(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let old = entry
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok())
            .is_some_and(|age| age > EVENT_FILE_MAX_AGE);
        if old {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

#[derive(Default)]
struct RunState {
    /// Per (hook, subject): the time before which the hook may not run again.
    debounce_until: HashMap<RunKey, Instant>,
    running: HashSet<RunKey>,
}

struct Shared {
    store: Arc<dyn Store>,
    paths: Paths,
    record: Recorder,
    permits: Semaphore,
    state: Mutex<RunState>,
}

/// Releases the re-entrancy slot when the run ends or is aborted.
struct RunGuard {
    shared: Arc<Shared>,
    key: RunKey,
}

impl Drop for RunGuard {
    fn drop(&mut self) {
        self.shared.state().running.remove(&self.key);
    }
}

/// While armed, dropping it (run finished early, or aborted at shutdown) kills the child's
/// process tree. It also deletes the run's event file.
struct KillGuard {
    pid: Option<u32>,
    event_file: PathBuf,
}

impl KillGuard {
    fn disarm(&mut self) {
        self.pid = None;
    }
}

impl Drop for KillGuard {
    fn drop(&mut self) {
        if let Some(pid) = self.pid {
            kill_tree(pid);
        }
        let _ = std::fs::remove_file(&self.event_file);
    }
}

struct Job {
    hook: HookConfig,
    event: Arc<Event>,
    json: Value,
    _guard: Option<RunGuard>,
}

impl Shared {
    fn new(store: Arc<dyn Store>, paths: Paths, record: Recorder) -> Self {
        Self {
            store,
            paths,
            record,
            permits: Semaphore::new(MAX_CONCURRENT),
            state: Mutex::new(RunState::default()),
        }
    }

    /// A panic while the lock was held must not stop every later hook.
    fn state(&self) -> MutexGuard<'_, RunState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn tmp_dir(&self) -> PathBuf {
        self.paths.home.join("tmp")
    }

    async fn snoozed(&self) -> bool {
        let until = match self.store.get_ui_state(SNOOZE_KEY).await {
            Ok(Some(v)) => v,
            Ok(None) => return false,
            Err(e) => {
                tracing::warn!(error = %e, "could not read snooze state");
                return false;
            }
        };
        DateTime::parse_from_rfc3339(&until).is_ok_and(|t| t.with_timezone(&Utc) > Utc::now())
    }

    /// Applies the gates in order and returns the runs to start. Debounce and
    /// re-entrancy slots are claimed here so two events in a row cannot both pass.
    /// `snoozed` is read by the caller beforehand.
    fn dispatch(
        self: &Arc<Self>,
        hooks: &CompiledHooks,
        event: &Arc<Event>,
        snoozed: bool,
    ) -> Vec<Job> {
        let json = match serde_json::to_value(event.as_ref()) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(error = %e, event = %event.id, "could not serialise event");
                return Vec::new();
            }
        };
        let mut jobs = Vec::new();
        for (hook, filter) in hooks {
            let name = hook.name.as_str();
            let skip = |reason: &str| {
                tracing::debug!(hook = name, event = %event.id, reason, "hook skipped");
            };
            if !filter.matches(&event.event_type, &json) {
                continue;
            }
            if event.catch_up && !hook.catch_up {
                skip("catch-up event");
                continue;
            }
            if event.test && !hook.include_test {
                skip("test event");
                continue;
            }
            if hook.respect_snooze && snoozed {
                skip("snoozed");
                continue;
            }
            if hook.action == Some(HookAction::OpenUrl) && event.url.is_none() {
                skip("no url");
                continue;
            }
            let key = (hook.name.clone(), event.subject_key.clone());
            let mut state = self.state();
            let now = Instant::now();
            if hook.debounce_secs > 0
                && state
                    .debounce_until
                    .get(&key)
                    .is_some_and(|deadline| now < *deadline)
            {
                skip("debounced");
                continue;
            }
            if !hook.detached && state.running.contains(&key) {
                skip("already running");
                continue;
            }
            if hook.debounce_secs > 0 {
                if state.debounce_until.len() > DEBOUNCE_PRUNE_AT {
                    state.debounce_until.retain(|_, d| *d > now);
                }
                let window = Duration::from_secs(hook.debounce_secs);
                state.debounce_until.insert(key.clone(), now + window);
            }
            let guard = if hook.detached {
                None
            } else {
                state.running.insert(key.clone());
                Some(RunGuard {
                    shared: self.clone(),
                    key,
                })
            };
            drop(state);
            tracing::debug!(hook = name, event = %event.id, "hook running");
            jobs.push(Job {
                hook: hook.clone(),
                event: event.clone(),
                json: json.clone(),
                _guard: guard,
            });
        }
        jobs
    }

    async fn execute(&self, job: Job) {
        match job.hook.action {
            Some(HookAction::OpenUrl) => self.open_url(&job).await,
            None => self.run_command(&job).await,
        }
    }

    async fn finish(&self, job: &Job, started: DateTime<Utc>, outcome: Outcome) {
        (self.record)(HookRun {
            hook: job.hook.name.clone(),
            event_id: job.event.id.clone(),
            started_utc: started,
            finished_utc: outcome.finished.then(Utc::now),
            exit_code: outcome.exit_code,
            error: outcome.error,
            stderr_tail: outcome.stderr_tail,
        })
        .await;
    }

    async fn open_url(&self, job: &Job) {
        let started = Utc::now();
        let Some(raw) = job.event.url.as_deref() else {
            return;
        };
        let error = match check_open_url(raw) {
            Ok(url) => {
                let opened =
                    tokio::task::spawn_blocking(move || open::that_detached(url.as_str())).await;
                match opened {
                    Ok(Ok(())) => None,
                    Ok(Err(e)) => Some(format!("could not open url: {e}")),
                    Err(e) => Some(format!("could not open url: {e}")),
                }
            }
            Err(e) => Some(e),
        };
        self.finish(job, started, Outcome::done(None, error, None))
            .await;
    }

    async fn fail(&self, job: &Job, started: DateTime<Utc>, error: String) {
        tracing::warn!(hook = %job.hook.name, event = %job.event.id, %error, "hook not run");
        self.finish(job, started, Outcome::done(None, Some(error), None))
            .await;
    }

    async fn run_command(&self, job: &Job) {
        let started = Utc::now();
        let spec = match build_spec(&job.hook, &job.json, &job.event) {
            Ok(s) => s,
            Err(e) => return self.fail(job, started, e).await,
        };
        let detached = job.hook.detached;
        let _permit = if detached {
            None
        } else {
            self.permits.acquire().await.ok()
        };
        let event_file = match self.write_event_file(job) {
            Ok(p) => p,
            Err(e) => {
                return self
                    .fail(job, started, format!("could not write event file: {e}"))
                    .await;
            }
        };
        let started = Utc::now();
        let mut cmd = tokio::process::Command::new(&spec.argv[0]);
        cmd.args(&spec.argv[1..])
            .envs(base_env(&job.event, &event_file))
            .envs(spec.env.iter().map(|(k, v)| (k, v)))
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(if detached {
                Stdio::null()
            } else {
                Stdio::piped()
            })
            .kill_on_drop(!detached);
        if let Some(cwd) = &spec.cwd {
            cmd.current_dir(cwd);
        }
        #[cfg(windows)]
        {
            const CREATE_NO_WINDOW: u32 = 0x0800_0000;
            const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
            cmd.creation_flags(if detached {
                CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW
            } else {
                CREATE_NO_WINDOW
            });
        }
        #[cfg(unix)]
        cmd.process_group(0);
        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                let _ = std::fs::remove_file(&event_file);
                return self
                    .fail(
                        job,
                        started,
                        format!("could not start {}: {e}", spec.argv[0]),
                    )
                    .await;
            }
        };
        let body = job.json.to_string().into_bytes();
        let stdin = child.stdin.take();
        if !detached {
            crate::process::assign_to_kill_on_exit_job(&child);
        }
        if detached {
            tracing::info!(hook = %job.hook.name, pid = ?child.id(), "detached hook started");
            self.finish(job, started, Outcome::started()).await;
            // The handle is dropped without killing; the child outlives this task.
            write_stdin(stdin, &body).await;
            return;
        }
        let mut guard = KillGuard {
            pid: child.id(),
            event_file,
        };
        let timeout = Duration::from_secs(match job.hook.timeout_secs {
            0 => DEFAULT_TIMEOUT_SECS,
            n => n,
        });
        let stderr = child.stderr.take();
        let (done_tx, done_rx) = oneshot::channel();
        let wait = async {
            write_stdin(stdin, &body).await;
            let outcome = match tokio::time::timeout(timeout, child.wait()).await {
                Ok(Ok(status)) => (status.code(), None),
                Ok(Err(e)) => (None, Some(format!("wait failed: {e}"))),
                Err(_) => {
                    if let Some(pid) = child.id() {
                        let _ = tokio::task::spawn_blocking(move || kill_tree(pid)).await;
                    }
                    let _ = child.kill().await;
                    (
                        None,
                        Some(format!("timed out after {}s", timeout.as_secs())),
                    )
                }
            };
            let _ = done_tx.send(());
            outcome
        };
        let (tail, (exit_code, error)) = tokio::join!(read_tail(stderr, done_rx), wait);
        guard.disarm();
        drop(guard);
        tracing::debug!(hook = %job.hook.name, ?exit_code, "hook finished");
        self.finish(job, started, Outcome::done(exit_code, error, tail))
            .await;
    }

    /// Named per hook as well as per event so two hooks on one event do not delete each
    /// other's file.
    fn write_event_file(&self, job: &Job) -> std::io::Result<PathBuf> {
        let dir = self.tmp_dir();
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(event_file_name(&job.event.id, &job.hook.name));
        std::fs::write(&path, job.json.to_string())?;
        Ok(path)
    }
}

/// `{event}.{hook}.{random}.json`, at most 95 chars; the random part
/// keeps repeated runs of one hook on one event from sharing a file.
fn event_file_name(event_id: &str, hook_name: &str) -> String {
    let random = uuid::Uuid::new_v4().simple().to_string();
    format!(
        "{}.{}.{}.json",
        sanitise(event_id, 40),
        sanitise(hook_name, 40),
        &random[..8]
    )
}

fn base_env(e: &Event, event_file: &Path) -> Vec<(&'static str, String)> {
    let mut env = vec![
        ("HERALD_EVENT_TYPE", e.event_type.clone()),
        ("HERALD_EVENT_ID", e.id.clone()),
        ("HERALD_EVENT_FILE", event_file.display().to_string()),
        ("HERALD_SUBJECT", e.subject_key.clone()),
        ("HERALD_CONNECTION", e.connection.id.clone()),
        ("HERALD_PROVIDER", e.connection.provider.clone()),
    ];
    let optional = [
        ("HERALD_URL", e.url.clone()),
        ("HERALD_PR_ID", e.pr.as_ref().map(|p| p.id.to_string())),
        ("HERALD_REPO", e.repo.as_ref().map(|r| r.name.clone())),
        (
            "HERALD_BRANCH",
            e.branch.as_ref().map(|b| b.short_name.clone()),
        ),
        (
            "HERALD_LOCAL_PATH",
            e.local
                .as_ref()
                .and_then(|l| l.worktree_path.clone().or_else(|| l.repo_path.clone())),
        ),
    ];
    env.extend(optional.into_iter().filter_map(|(k, v)| v.map(|v| (k, v))));
    env
}

struct Outcome {
    finished: bool,
    exit_code: Option<i32>,
    error: Option<String>,
    stderr_tail: Option<String>,
}

impl Outcome {
    fn done(exit_code: Option<i32>, error: Option<String>, stderr_tail: Option<String>) -> Self {
        Self {
            finished: true,
            exit_code,
            error,
            stderr_tail,
        }
    }

    fn started() -> Self {
        Self {
            finished: false,
            exit_code: None,
            error: None,
            stderr_tail: None,
        }
    }
}

struct CommandSpec {
    argv: Vec<String>,
    cwd: Option<String>,
    env: Vec<(String, String)>,
}

fn build_spec(hook: &HookConfig, json: &Value, event: &Event) -> Result<CommandSpec, String> {
    let template = hook
        .run
        .as_ref()
        .and_then(|r| r.for_current_os())
        .filter(|a| !a.is_empty())
        .ok_or("no command configured for this OS")?;
    let ex = |s: &str| expand(s, json).map_err(|e| format!("missing placeholder {{{}}}", e.0));
    let argv: Vec<String> = template.iter().map(|a| ex(a)).collect::<Result<_, _>>()?;
    let env: Vec<(String, String)> = hook
        .env
        .iter()
        .map(|(k, v)| Ok((k.clone(), ex(v)?)))
        .collect::<Result<_, String>>()?;
    check_shell_safety(template, json, &env, event)?;
    Ok(CommandSpec {
        argv,
        cwd: hook.cwd.as_deref().map(ex).transpose()?,
        env,
    })
}

/// Placeholder paths in a template, in order; `{{` and `}}` are literal braces.
pub(crate) fn placeholder_paths(template: &str) -> Vec<String> {
    let mut paths = Vec::new();
    let mut chars = template.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '{' | '}' if chars.peek() == Some(&c) => {
                chars.next();
            }
            '{' => {
                let path: String = chars.by_ref().take_while(|&c| c != '}').collect();
                paths.push(path.trim().to_string());
            }
            _ => {}
        }
    }
    paths
}

const SHELLS: &[&str] = &[
    "cmd",
    "sh",
    "bash",
    "zsh",
    "dash",
    "ksh",
    "mksh",
    "ash",
    "fish",
    "busybox",
    "pwsh",
    "powershell",
    "wsl",
    "csh",
    "tcsh",
    "nu",
    "ssh",
    "su",
    "runuser",
];

#[derive(Clone, Copy, PartialEq)]
enum ShellFamily {
    Sh,
    Windows,
    Cmd,
}

/// Lower-cased file name without a directory or a `.exe`/`.com`/`.bat`/`.cmd` extension.
fn program_stem(arg: &str) -> String {
    let name = arg.rsplit(['/', '\\']).next().unwrap_or(arg).to_lowercase();
    for ext in [".exe", ".com", ".bat", ".cmd"] {
        if let Some(stem) = name.strip_suffix(ext) {
            return stem.to_string();
        }
    }
    name
}

fn shell_family(stem: &str) -> ShellFamily {
    match stem {
        "cmd" => ShellFamily::Cmd,
        "pwsh" | "powershell" => ShellFamily::Windows,
        _ => ShellFamily::Sh,
    }
}

/// Allowlist for a value that ends up inside a shell command line. Windows paths need
/// the backslash; nothing else that any shell treats as syntax is accepted, spaces included.
fn value_allowed(value: &str, family: ShellFamily) -> bool {
    let extra: &[char] = if family == ShellFamily::Sh {
        &[]
    } else {
        &['\\']
    };
    value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "._/@:+-".contains(c) || extra.contains(&c))
}

/// Shells whose positional parameters (`$0`, `$1`, ...) after a `-c` command string are
/// plain data, not code.
const POSITIONAL_SHELLS: &[&str] = &["sh", "bash", "zsh", "dash", "ksh", "mksh", "ash"];

/// Programs that run their arguments as a command. Their options are not parsed, so
/// `check_shell_safety` refuses any placeholder from the first one up to the first shell.
const WRAPPERS: &[&str] = &[
    "env", "sudo", "doas", "timeout", "nice", "nohup", "setsid", "stdbuf", "xargs", "watch",
    "flock", "script", "chroot", "runas", "start", "time", "exec", "command",
];

/// `-c`, optionally clustered with flags that take no value (`-ce`, `-ec`, `-cex`).
fn is_c_cluster(arg: &str) -> bool {
    arg.strip_prefix('-')
        .is_some_and(|f| f.contains('c') && f.chars().all(|c| "ceuxvf".contains(c)))
}

/// Index in `after` (the elements following an sh-family shell) of the first positional
/// parameter. Positionals exist only when the element directly after the shell is a `-c`
/// cluster and the command string after it does not start with `-` or `+` (the shell would
/// read that as an option, e.g. `bash -c +o posix ...`). Any other shape is ambiguous, so
/// nothing counts as positional.
fn sh_positional_start(after: &[String]) -> Option<usize> {
    if !is_c_cluster(after.first()?) {
        return None;
    }
    if after.get(1)?.starts_with(['-', '+']) {
        return None;
    }
    Some(2)
}

/// Whether cmd would expand variable `name` in `text` (already lower-cased): `%NAME%`,
/// `%NAME:...%`, `!NAME!` or `!NAME:...!`. Matches the whole token, so `T` does not hit `%TEMP%`.
fn cmd_references(text: &str, name: &str) -> bool {
    let name = name.to_lowercase();
    ["%", "!"]
        .iter()
        .any(|d| text.contains(&format!("{d}{name}{d}")) || text.contains(&format!("{d}{name}:")))
}

/// Whether `arg` is, or contains as a whole word, the cmd program (`cmd`, `CMD.EXE`, a path to it).
fn mentions_cmd(arg: &str) -> bool {
    arg.split(|c: char| !(c.is_alphanumeric() || "._-/\\".contains(c)))
        .any(|word| program_stem(word) == "cmd")
}

/// A shell re-parses its arguments, and std does not escape for it, so a run is in a
/// shell context when any argument is a shell (this also covers wrappers such as `env`
/// or `sudo`). The first shell decides: from there on a placeholder is allowed only as a
/// positional parameter of an sh-family shell given a `-c` command string
/// (`sh -c 'echo "$1"' _ {x}`). Everything else is refused, because it is command text
/// the shell would parse: the command string and options of an sh-family shell, any
/// argument after cmd, PowerShell, wsl, ssh, su, csh, fish, nu or busybox, and scripts
/// run by an sh-family shell without `-c`. Values reach those through HERALD_* env vars
/// or the event file.
///
/// An allowed positional placeholder must still hold only allowlisted characters. Shells
/// nest (`cmd /c bash -c ...`), so the strictest allowlist among every shell in the argv
/// applies: the sh family (no backslash, no space) beats cmd and PowerShell. Non-shell
/// programs get argv directly and are not restricted. The check is on template
/// placeholders, not the final args: the template itself may legitimately contain `&&`.
///
/// cmd also expands `%NAME%` (and `!NAME!` under delayed expansion) after parsing, so
/// when any shell is cmd, including a `cmd` word inside a single string argument, every
/// hook env entry and every HERALD_* base env entry whose name the arguments reference
/// must hold an allowlisted value (HERALD_EVENT_FILE is not known yet and is
/// app-generated). The check is name-driven: no `%` pairing is parsed, and env names are
/// case-insensitive on Windows, so each entry is checked on its own. sh and PowerShell
/// read env values as variables, not as code, except for names the shell itself executes
/// (`BASH_ENV`, `ENV`, `PS4`, `PROMPT_COMMAND`, ...); config validation refuses a
/// placeholder in those.
///
/// Command wrappers (`env`, `sudo`, `timeout`, ...) run their arguments as a command and
/// their options are not parsed, so no placeholder is allowed from the first wrapper up to
/// the first shell (or the end of the argv when there is none).
fn check_shell_safety(
    template: &[String],
    json: &Value,
    hook_env: &[(String, String)],
    event: &Event,
) -> Result<(), String> {
    let shell_at = template
        .iter()
        .position(|a| SHELLS.contains(&program_stem(a).as_str()));
    if let Some(wrapper_at) = template
        .iter()
        .position(|a| WRAPPERS.contains(&program_stem(a).as_str()))
    {
        let end = shell_at.unwrap_or(template.len());
        let covered = template.get(wrapper_at..end).unwrap_or_default();
        if let Some(path) = covered
            .iter()
            .find_map(|a| placeholder_paths(a).into_iter().next())
        {
            return Err(format!(
                "placeholder not allowed after a command wrapper ({}): {path}",
                program_stem(&template[wrapper_at])
            ));
        }
    }
    let Some(shell_at) = shell_at else {
        return Ok(());
    };
    let args = &template[shell_at + 1..];
    let first = program_stem(&template[shell_at]);
    let mut stem = first.clone();
    let mut family = shell_family(&stem);
    let mut has_cmd = family == ShellFamily::Cmd;
    for arg in args {
        let arg_stem = program_stem(arg);
        if SHELLS.contains(&arg_stem.as_str()) {
            let nested = shell_family(&arg_stem);
            has_cmd |= nested == ShellFamily::Cmd;
            if nested == ShellFamily::Sh && family != ShellFamily::Sh {
                family = ShellFamily::Sh;
                stem = arg_stem;
            }
        } else {
            has_cmd |= mentions_cmd(arg);
        }
    }
    let positional_from = if POSITIONAL_SHELLS.contains(&first.as_str()) {
        sh_positional_start(args)
    } else {
        None
    };
    for (i, arg) in args.iter().enumerate() {
        let is_positional = positional_from.is_some_and(|from| i >= from);
        for path in placeholder_paths(arg) {
            if !is_positional {
                return Err(format!(
                    "placeholder not allowed in a shell command: {path}; pass it via HERALD_* env vars, the event file, or as an sh positional argument"
                ));
            }
            if lookup(&path, json).is_some_and(|v| !value_allowed(&v, family)) {
                return Err(format!("unsafe value for {stem}: {path}"));
            }
        }
    }
    if has_cmd {
        let base = base_env(event, Path::new(""));
        let text = args.join(" ").to_lowercase();
        let hook_env = hook_env.iter().map(|(k, v)| (k.as_str(), v.as_str()));
        let base = base.iter().map(|(k, v)| (*k, v.as_str()));
        for (name, value) in hook_env.chain(base) {
            if cmd_references(&text, name) && !value_allowed(value, family) {
                return Err(format!("unsafe value for {stem}: %{name}%"));
            }
        }
    }
    Ok(())
}

fn sanitise(s: &str, max: usize) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .take(max)
        .collect()
}

/// Only http and https are opened; anything else (file, UNC, javascript, custom schemes) is refused.
fn check_open_url(raw: &str) -> Result<url::Url, String> {
    let url =
        url::Url::parse(raw).map_err(|_| "event url is not a valid absolute url".to_string())?;
    match url.scheme() {
        "http" | "https" => Ok(url),
        other => Err(format!("refusing to open {other} url")),
    }
}

async fn write_stdin(stdin: Option<tokio::process::ChildStdin>, body: &[u8]) {
    let Some(mut stdin) = stdin else { return };
    // A child that exits without reading gives a broken pipe; that is fine.
    let _ = tokio::time::timeout(STDIN_WRITE_TIMEOUT, stdin.write_all(body)).await;
    drop(stdin);
}

/// Keeps the last 4 KB of stderr. Stops shortly after the child is done, in case a
/// grandchild keeps the pipe open.
async fn read_tail(
    stderr: Option<tokio::process::ChildStderr>,
    done: oneshot::Receiver<()>,
) -> Option<String> {
    let mut stderr = stderr?;
    let grace = async {
        let _ = done.await;
        tokio::time::sleep(STDERR_GRACE).await;
    };
    tokio::pin!(grace);
    let mut tail: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        tokio::select! {
            read = stderr.read(&mut chunk) => match read {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    tail.extend_from_slice(&chunk[..n]);
                    if tail.len() > STDERR_TAIL_BYTES {
                        tail.drain(..tail.len() - STDERR_TAIL_BYTES);
                    }
                }
            },
            () = &mut grace => break,
        }
    }
    (!tail.is_empty()).then(|| String::from_utf8_lossy(&tail).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::classify::Classifier;
    use crate::config::{MatchValue, RunSpec};
    use crate::event::{ConnectionRef, PrRef, PrRole};
    use crate::local_repos::LocalIndex;
    use crate::store::SqliteStore;

    struct Fixture {
        shared: Arc<Shared>,
        runs: Arc<Mutex<Vec<HookRun>>>,
        dir: tempfile::TempDir,
    }

    fn paths_in(dir: &Path) -> Paths {
        Paths {
            home: dir.join("home"),
            runtime: dir.join("runtime"),
        }
    }

    fn fixture() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let runs = Arc::new(Mutex::new(Vec::new()));
        let sink = runs.clone();
        let recorder: Recorder = Arc::new(move |r| {
            sink.lock().unwrap().push(r);
            async {}.boxed()
        });
        let store = Arc::new(SqliteStore::open_in_memory().unwrap());
        Fixture {
            shared: Arc::new(Shared::new(store, paths_in(dir.path()), recorder)),
            runs,
            dir,
        }
    }

    impl Fixture {
        fn runs(&self) -> Vec<HookRun> {
            self.runs.lock().unwrap().clone()
        }

        async fn run_all(&self, hooks: &CompiledHooks, event: Event) {
            for job in self.shared.dispatch(hooks, &Arc::new(event), false) {
                self.shared.execute(job).await;
            }
        }

        async fn run_one(&self, hook: HookConfig) -> HookRun {
            self.run_all(&compiled(vec![hook]), event("pr.created"))
                .await;
            let mut runs = self.runs();
            assert_eq!(runs.len(), 1);
            runs.remove(0)
        }
    }

    fn event(event_type: &str) -> Event {
        let now = Utc::now();
        let mut e = Event::new(
            ConnectionRef {
                id: "c1".into(),
                provider: "fake".into(),
            },
            event_type,
            now,
            now,
            "title",
            "c1:pr:r1:7",
        );
        e.id = "c1:pr:r1:7:created".into();
        e.pr = Some(PrRef {
            id: 7,
            title: "t".into(),
            source_branch: "refs/heads/x".into(),
            target_branch: "refs/heads/main".into(),
            is_draft: false,
            role: PrRole::Author,
        });
        e
    }

    fn hook(name: &str, on: &str) -> HookConfig {
        HookConfig {
            name: name.into(),
            on: vec![on.into()],
            run: Some(RunSpec {
                default: Some(vec!["unused".into()]),
                ..RunSpec::default()
            }),
            ..HookConfig::default()
        }
    }

    /// A hook that runs `windows` argv on Windows and `sh -c unix` elsewhere. Windows
    /// parts are split so cmd sees no quoted argument.
    fn shell_hook(name: &str, windows: &[&str], unix: &str) -> HookConfig {
        let argv: Vec<String> = if cfg!(windows) {
            ["cmd", "/C"]
                .iter()
                .chain(windows)
                .map(|s| s.to_string())
                .collect()
        } else {
            vec!["sh".into(), "-c".into(), unix.into()]
        };
        let mut h = hook(name, "pr.*");
        h.run = Some(RunSpec {
            default: Some(argv),
            ..RunSpec::default()
        });
        h
    }

    fn compiled(hooks: Vec<HookConfig>) -> CompiledHooks {
        hooks
            .into_iter()
            .map(|h| {
                let f = CompiledHookFilter::new(&h.on, &h.r#match);
                (h, f)
            })
            .collect()
    }

    fn names(jobs: &[Job]) -> Vec<&str> {
        jobs.iter().map(|j| j.hook.name.as_str()).collect()
    }

    #[test]
    fn gates_apply_on_type_and_match_table() {
        let fx = fixture();
        let mut author = hook("author", "pr.*");
        author
            .r#match
            .insert("pr.role".into(), MatchValue::One("author".into()));
        let mut reviewer = hook("reviewer", "pr.*");
        reviewer
            .r#match
            .insert("pr.role".into(), MatchValue::One("reviewer".into()));
        let hooks = compiled(vec![author, reviewer, hook("run-only", "run.*")]);
        let jobs = fx
            .shared
            .dispatch(&hooks, &Arc::new(event("pr.created")), false);
        assert_eq!(names(&jobs), ["author"]);
    }

    #[test]
    fn catch_up_and_test_events_need_the_hook_opt_in() {
        let fx = fixture();
        let mut opted_in = hook("opted-in", "pr.*");
        opted_in.catch_up = true;
        let mut no_test = hook("no-test", "pr.*");
        no_test.include_test = false;
        let hooks = compiled(vec![hook("plain", "pr.*"), opted_in, no_test]);

        let mut caught_up = event("pr.created");
        caught_up.catch_up = true;
        let jobs = fx.shared.dispatch(&hooks, &Arc::new(caught_up), false);
        assert_eq!(names(&jobs), ["opted-in"]);
        drop(jobs);

        let mut test = event("pr.created");
        test.test = true;
        let jobs = fx.shared.dispatch(&hooks, &Arc::new(test), false);
        assert_eq!(names(&jobs), ["plain", "opted-in"]);
    }

    #[tokio::test]
    async fn snooze_only_stops_hooks_that_respect_it() {
        let fx = fixture();
        let mut respects = hook("respects", "pr.*");
        respects.respect_snooze = true;
        let hooks = compiled(vec![respects, hook("ignores", "pr.*")]);
        let e = Arc::new(event("pr.created"));
        let store = &fx.shared.store;

        let future = (Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
        store.set_ui_state(SNOOZE_KEY, &future).await.unwrap();
        let snoozed = fx.shared.snoozed().await;
        assert!(snoozed);
        assert_eq!(names(&fx.shared.dispatch(&hooks, &e, snoozed)), ["ignores"]);

        let past = (Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
        store.set_ui_state(SNOOZE_KEY, &past).await.unwrap();
        let snoozed = fx.shared.snoozed().await;
        assert!(!snoozed);
        assert_eq!(
            names(&fx.shared.dispatch(&hooks, &e, snoozed)),
            ["respects", "ignores"]
        );
    }

    #[test]
    fn debounce_and_reentrancy_are_keyed_by_hook_and_subject() {
        let fx = fixture();
        let mut debounced = hook("debounced", "pr.*");
        debounced.debounce_secs = 60;
        debounced.detached = true;
        let hooks = compiled(vec![debounced, hook("busy", "pr.*")]);
        let e = Arc::new(event("pr.created"));

        let first = fx.shared.dispatch(&hooks, &e, false);
        assert_eq!(names(&first), ["debounced", "busy"]);
        // "busy" is still running and "debounced" ran just now.
        assert!(fx.shared.dispatch(&hooks, &e, false).is_empty());

        let mut other = event("pr.created");
        other.subject_key = "c1:pr:r1:8".into();
        let jobs = fx.shared.dispatch(&hooks, &Arc::new(other), false);
        assert_eq!(names(&jobs), ["debounced", "busy"]);

        drop(first);
        // The first run ended, so "busy" may run again; "debounced" is inside its window.
        assert_eq!(names(&fx.shared.dispatch(&hooks, &e, false)), ["busy"]);
    }

    #[test]
    fn open_url_without_a_url_is_skipped_before_any_slot_is_claimed() {
        let fx = fixture();
        let mut h = hook("open", "pr.*");
        h.run = None;
        h.action = Some(HookAction::OpenUrl);
        h.debounce_secs = 60;
        let hooks = compiled(vec![h]);
        let mut e = event("pr.created");
        assert!(
            fx.shared
                .dispatch(&hooks, &Arc::new(e.clone()), false)
                .is_empty()
        );
        e.url = Some("https://example.com/pr/7".into());
        assert_eq!(
            names(&fx.shared.dispatch(&hooks, &Arc::new(e), false)),
            ["open"]
        );
    }

    #[test]
    fn only_http_and_https_urls_may_be_opened() {
        assert!(check_open_url("https://example.com/a?b=1").is_ok());
        assert!(check_open_url("http://example.com").is_ok());
        for bad in [
            "file:///C:/Windows/System32/calc.exe",
            r"\\host\share\file.exe",
            "//host/share",
            "javascript:alert(1)",
            "myapp://do/thing",
            "not a url",
            "JavaScript:alert(1)",
            "data:text/html,<script>1</script>",
            "  file:///C:/Windows/System32/calc.exe",
            "\tfile:///etc/passwd",
        ] {
            assert!(check_open_url(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn scheme_case_and_backslashes_normalise_to_http() {
        for ok in [
            "HTTP://example.com/a",
            r"http:\\example.com\a",
            "HtTpS://example.com",
        ] {
            let url = check_open_url(ok).unwrap();
            assert!(matches!(url.scheme(), "http" | "https"), "{ok}");
            assert_eq!(url.host_str(), Some("example.com"), "{ok}");
        }
    }

    #[tokio::test]
    async fn open_url_records_an_error_for_a_refused_scheme() {
        let fx = fixture();
        let mut h = hook("open", "pr.*");
        h.run = None;
        h.action = Some(HookAction::OpenUrl);
        let mut e = event("pr.created");
        e.url = Some("file:///etc/passwd".into());
        fx.run_all(&compiled(vec![h]), e).await;
        let runs = fx.runs();
        assert_eq!(runs.len(), 1);
        assert!(runs[0].error.as_deref().unwrap().contains("refusing"));
    }

    #[tokio::test]
    async fn placeholders_expand_into_argv_and_exit_code_is_recorded() {
        let fx = fixture();
        let h = shell_hook("exit", &["exit", "%HERALD_PR_ID%"], "exit $HERALD_PR_ID");
        let run = fx.run_one(h).await;
        assert_eq!(run.exit_code, Some(7));
        assert_eq!(run.error, None);
        assert!(run.finished_utc.is_some());
    }

    #[tokio::test]
    async fn missing_placeholder_records_an_error_and_does_not_run() {
        let fx = fixture();
        let run = fx
            .run_one(shell_hook("bad", &["echo", "{pr.nope}"], "echo {pr.nope}"))
            .await;
        assert_eq!(run.exit_code, None);
        assert!(run.error.as_deref().unwrap().contains("pr.nope"));
        assert!(!fx.shared.tmp_dir().exists());
    }

    #[tokio::test]
    async fn child_gets_the_event_file_which_is_removed_after_the_run() {
        let fx = fixture();
        let h = shell_hook(
            "show",
            &["type", "%HERALD_EVENT_FILE%", "1>&2"],
            "cat \"$HERALD_EVENT_FILE\" >&2",
        );
        let run = fx.run_one(h).await;
        let tail = run.stderr_tail.unwrap_or_default();
        assert!(tail.contains("c1:pr:r1:7:created"), "{tail}");
        assert_eq!(run.exit_code, Some(0));
        assert_eq!(std::fs::read_dir(fx.shared.tmp_dir()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn stderr_tail_keeps_only_the_last_4kb() {
        let fx = fixture();
        let h = shell_hook(
            "noisy",
            &[
                "for",
                "/L",
                "%i",
                "in",
                "(1,1,600)",
                "do",
                "@echo",
                "0123456789",
                "1>&2",
            ],
            "i=0; while [ $i -lt 600 ]; do echo 0123456789 >&2; i=$((i+1)); done",
        );
        let tail = fx.run_one(h).await.stderr_tail.unwrap();
        assert!(tail.len() <= STDERR_TAIL_BYTES, "{}", tail.len());
        assert!(tail.len() > 3000);
    }

    #[tokio::test]
    async fn timeout_kills_the_child_and_records_an_error() {
        let fx = fixture();
        let mut h = shell_hook("slow", &["ping", "-n", "20", "127.0.0.1"], "exec sleep 19");
        h.timeout_secs = 1;
        let begun = std::time::Instant::now();
        let run = fx.run_one(h).await;
        assert!(begun.elapsed() < Duration::from_secs(10));
        assert!(run.error.as_deref().unwrap().contains("timed out"));
        assert_eq!(run.exit_code, None);
    }

    #[tokio::test]
    async fn detached_run_records_only_a_start() {
        let fx = fixture();
        let mut h = shell_hook("bg", &["exit", "0"], "exit 0");
        h.detached = true;
        let run = fx.run_one(h).await;
        assert!(run.finished_utc.is_none());
        assert_eq!(run.exit_code, None);
        assert_eq!(run.error, None);
    }

    #[tokio::test]
    async fn runner_runs_hooks_for_bus_events_until_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("marker.json");
        let marker_arg = marker.display().to_string();
        let mut h = shell_hook(
            "copy",
            &["copy", "%HERALD_EVENT_FILE%", &marker_arg],
            &format!("cp \"$HERALD_EVENT_FILE\" '{marker_arg}'"),
        );
        h.debounce_secs = 0;
        let mut config = Config::default();
        config.hooks.push(h);
        let store = Arc::new(SqliteStore::open_in_memory().unwrap());
        let classifier = Classifier::new(&config, &Default::default());
        let bus = Bus::new(store.clone(), classifier, LocalIndex::empty());
        let (_tx, rx) = watch::channel(Arc::new(config));
        let runner = HookRunner::start(&bus, store, paths_in(dir.path()), rx);

        bus.publish(event("pr.created")).await.unwrap();
        let mut seen = false;
        for _ in 0..100 {
            if marker.exists() {
                seen = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        runner.shutdown().await;
        assert!(seen, "hook never ran");
    }

    fn full_event() -> Event {
        let mut e = event("pr.created");
        e.url = Some("https://example.com/pr/7".into());
        e.repo = Some(crate::event::RepoRef {
            id: "r1".into(),
            name: "repo".into(),
            namespace: None,
        });
        e.branch = Some(crate::event::BranchRef {
            name: "refs/heads/x".into(),
            short_name: "x".into(),
        });
        e.local = Some(crate::event::LocalPaths {
            repo_path: Some("/repo".into()),
            worktree_path: Some("/wt".into()),
        });
        e
    }

    fn env_map(e: &Event) -> HashMap<&'static str, String> {
        base_env(e, Path::new("/tmp/e.json")).into_iter().collect()
    }

    #[test]
    fn base_env_carries_every_herald_key_and_prefers_the_worktree() {
        let env = env_map(&full_event());
        let expected = [
            ("HERALD_EVENT_TYPE", "pr.created"),
            ("HERALD_EVENT_ID", "c1:pr:r1:7:created"),
            ("HERALD_SUBJECT", "c1:pr:r1:7"),
            ("HERALD_CONNECTION", "c1"),
            ("HERALD_PROVIDER", "fake"),
            ("HERALD_URL", "https://example.com/pr/7"),
            ("HERALD_PR_ID", "7"),
            ("HERALD_REPO", "repo"),
            ("HERALD_BRANCH", "x"),
            ("HERALD_LOCAL_PATH", "/wt"),
        ];
        for (k, v) in expected {
            assert_eq!(env.get(k).map(String::as_str), Some(v), "{k}");
        }
        assert!(env.contains_key("HERALD_EVENT_FILE"));

        let mut repo_only = full_event();
        repo_only.local = Some(crate::event::LocalPaths {
            repo_path: Some("/repo".into()),
            worktree_path: None,
        });
        assert_eq!(env_map(&repo_only)["HERALD_LOCAL_PATH"], "/repo");
    }

    #[test]
    fn base_env_omits_optional_keys_the_event_lacks() {
        let mut e = event("run.failed");
        e.pr = None;
        let env = env_map(&e);
        for k in [
            "HERALD_URL",
            "HERALD_PR_ID",
            "HERALD_REPO",
            "HERALD_BRANCH",
            "HERALD_LOCAL_PATH",
        ] {
            assert!(!env.contains_key(k), "{k}");
        }
        assert_eq!(env.len(), 6);
    }

    #[test]
    fn build_spec_expands_cwd_and_env_and_rejects_a_missing_placeholder() {
        let json = serde_json::to_value(full_event()).unwrap();
        let mut h = hook("h", "pr.*");
        h.cwd = Some("{local.worktreePath}".into());
        h.env.insert("PR".into(), "#{pr.id}".into());
        let spec = build_spec(&h, &json, &full_event()).unwrap();
        assert_eq!(spec.cwd.as_deref(), Some("/wt"));
        assert_eq!(spec.env, [("PR".to_string(), "#7".to_string())]);

        h.env.insert("BAD".into(), "{pr.nope}".into());
        let err = build_spec(&h, &json, &full_event()).err().unwrap();
        assert!(err.contains("pr.nope"), "{err}");
    }

    #[test]
    fn event_file_names_are_capped_and_unique_per_run() {
        let long = "x".repeat(300);
        let a = event_file_name(&long, &long);
        assert!(a.len() <= 95, "{}", a.len());
        assert!(a.ends_with(".json"));
        assert_ne!(a, event_file_name(&long, &long));
        assert!(event_file_name("c1:pr:1", "my hook").starts_with("c1_pr_1.my_hook."));
    }

    fn template(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    fn event_with(branch: &str, title: &str) -> Event {
        let mut e = full_event();
        e.branch.as_mut().unwrap().short_name = branch.into();
        e.pr.as_mut().unwrap().title = title.into();
        e
    }

    fn spec_with(argv: &[&str], env: &[(&str, &str)], e: Event) -> Result<CommandSpec, String> {
        let mut h = hook("h", "pr.*");
        h.run = Some(RunSpec {
            default: Some(template(argv)),
            ..RunSpec::default()
        });
        for (k, v) in env {
            h.env.insert(k.to_string(), v.to_string());
        }
        build_spec(&h, &serde_json::to_value(&e).unwrap(), &e)
    }

    fn spec_for(argv: &[&str]) -> Result<CommandSpec, String> {
        spec_with(argv, &[], event_with("x&echo injected", "$(whoami)"))
    }

    #[test]
    fn cmd_refuses_unsafe_env_references() {
        let e = event_with("a&calc", "t");
        let err = spec_with(&["cmd", "/C", "echo", "%HERALD_BRANCH%"], &[], e)
            .err()
            .unwrap();
        assert_eq!(err, "unsafe value for cmd: %HERALD_BRANCH%");

        let e = event_with("b", "a),else,(calc");
        let err = spec_with(
            &["cmd", "/C", "echo", "%t:~0,5%"],
            &[("T", "{pr.title}")],
            e,
        )
        .err()
        .unwrap();
        assert_eq!(err, "unsafe value for cmd: %T%");
    }

    #[test]
    fn cmd_reference_detection_is_name_driven() {
        let unsafe_branch = || event_with("a&echo X", "t");
        let refused = |argv: &[&str], env: &[(&str, &str)], e: Event| {
            let err = spec_with(argv, env, e)
                .err()
                .unwrap_or_else(|| panic!("{argv:?}"));
            assert!(err.starts_with("unsafe value for cmd: %"), "{err}");
        };
        refused(
            &["cmd", "/C", "echo %PR-TITLE%"],
            &[("PR-TITLE", "{pr.title}")],
            event_with("b", "a&echo X"),
        );
        refused(
            &["cmd", "/C", "echo 100% %HERALD_BRANCH%"],
            &[],
            unsafe_branch(),
        );
        refused(
            &["cmd", "/C", "echo %T%"],
            &[("T", "ok"), ("t", "{pr.title}")],
            event_with("b", "a&echo X"),
        );
        refused(&["cmd", "/C", "echo !HERALD_BRANCH!"], &[], unsafe_branch());
        refused(
            &["cmd", "/C", "echo !herald_branch:~1!"],
            &[],
            unsafe_branch(),
        );

        let safe = || event_with("feature/abc-1", "ok");
        for arg in [
            "echo 100% %HERALD_BRANCH%",
            "echo !HERALD_BRANCH!",
            "echo %herald_branch:~1%",
        ] {
            assert!(spec_with(&["cmd", "/C", arg], &[], safe()).is_ok(), "{arg}");
        }
        assert!(
            spec_with(
                &["cmd", "/C", "echo %PR-TITLE% %T%"],
                &[("PR-TITLE", "{pr.title}"), ("T", "{pr.title}")],
                safe()
            )
            .is_ok()
        );
        assert!(
            spec_with(
                &["cmd", "/C", "echo %TEMP%"],
                &[("T", "{pr.title}")],
                event_with("b", "a&echo X")
            )
            .is_ok()
        );
    }

    #[test]
    fn nested_shells_use_the_strictest_allowlist_and_cmd_checks() {
        let e = event_with("a&calc", "t");
        let err = spec_with(&["pwsh", "-c", "cmd /c echo %HERALD_BRANCH%"], &[], e)
            .err()
            .unwrap();
        assert_eq!(err, "unsafe value for pwsh: %HERALD_BRANCH%");

        let err = spec_with(
            &["pwsh", "-NoProfile", "-Command", "cmd.exe /c echo %X%"],
            &[("X", "{pr.title}")],
            event_with("b", "a&calc"),
        )
        .err()
        .unwrap();
        assert_eq!(err, "unsafe value for pwsh: %X%");
    }

    const NOT_ALLOWED: &str = "placeholder not allowed in a shell command: ";

    #[test]
    fn placeholders_in_a_shell_context_are_refused_unless_an_sh_positional() {
        let e = || event_with("feature/abc-1", "ok");
        let refused: &[&[&str]] = &[
            // Command strings and options of an sh-family shell.
            &["sh", "-c", "echo {pr.title}"],
            &["bash", "-c", "{pr.title}"],
            &["sh", "-c", "'{pr.title}'"],
            &["sh", "-c", "true; {pr.title}"],
            &["sh", "-c", "echo $({pr.title})"],
            &["sh", "-c", "env FOO=bar {pr.title}"],
            &["sh", "-c", "FOO={pr.title} echo hi"],
            &["bash", "-ce", "echo {pr.title}"],
            &["bash", "-c", "-e", "echo {pr.title}"],
            &["bash", "-c", "--", "echo {pr.title}"],
            &["bash", "-c", "-o", "pipefail", "echo {pr.title}"],
            &["bash", "-O", "{pr.title}", "-c", "true", "_", "{pr.id}"],
            &["bash", "-c", "-o", "pipefail", "true", "{pr.id}"],
            &["bash", "-co", "pipefail", "true", "_", "{pr.id}"],
            &["bash", "-c", "-e", "true", "_", "{pr.id}"],
            &["bash", "-c", "+o", "posix", "{pr.id}"],
            &["sh", "-c", "+O", "extglob", "{pr.id}"],
            &["dash", "-c", "+o", "errexit", "{pr.id}"],
            &["bash", "s.sh", "-c", "true", "{pr.id}"],
            &["bash", "--init-file", "-c", "true", "{pr.id}"],
            &["mksh", "-cT", "x", "{pr.id}"],
            &["bash", "-e", "-c", "true", "_", "{pr.id}"],
            &["env", "bash", "-c", "echo {pr.title}"],
            &["/usr/bin/sh", "-c", "echo {pr.title}"],
            &["env", "FOO=1", "sh", "-c", "echo {pr.title}"],
            &["env", "FOO=1", "sh", "-c", "{pr.title}"],
            &["sh", "-c", "sh -c {pr.title}"],
            &["timeout", "5", "sh", "-c", "{pr.title}"],
            // sh family without a command string, and shells that are not sh.
            &["sh", "{pr.title}"],
            &["sh", "script.sh", "{pr.id}"],
            &["bash", "-x", "script.sh", "{pr.id}"],
            &["busybox", "sh", "-c", "true", "_", "{pr.id}"],
            &["csh", "-c", "{pr.title}"],
            &["tcsh", "-c", "{pr.title}"],
            &["fish", "-c", "{pr.title}"],
            &["nu", "-c", "{pr.title}"],
            &["su", "-c", "{pr.title}"],
            &["runuser", "-c", "{pr.title}"],
            &["wsl", "{pr.id}"],
            &["wsl", "echo", "{pr.title}"],
            &["ssh", "host", "{pr.id}"],
            &["ssh", "host", "deploy", "{pr.title}"],
            // cmd: all of it is command text.
            &["cmd", "/c", "echo", "{pr.id}"],
            &["cmd", "/C", "echo {pr.id}"],
            &["cmd", "/c{pr.id}"],
            &["cmd", "/c", "{pr.title}"],
            &["cmd", "/c", "echo", "x", "&", "{pr.id}"],
            &["cmd", "/c", "echo x & {pr.id}"],
            &["cmd", "/c", "^{pr.id}"],
            &["cmd", "/c", ",{pr.id}"],
            &["cmd", "/c", "={pr.id}"],
            &["cmd", "/c", "call", "{pr.id}"],
            &["cmd", "/c", "start", "{pr.id}"],
            &["cmd", "/c", "bash", "-c", "{pr.id}"],
            &["cmd", "/c", "bash", "-c", "true", "_", "{pr.id}"],
            &["CMD.EXE", "/C", "echo", "{pr.title}", "&", "echo", "done"],
            // PowerShell: any flag, all of it.
            &["pwsh", "-File", "s.ps1", "{pr.id}"],
            &["pwsh", "-Command", "{pr.id}"],
            &["pwsh", "-Command", "echo x; {pr.id}"],
            &["pwsh", "-Command", "{{ {pr.id}; }}"],
            &["pwsh", "-EncodedCommand", "{pr.id}"],
            &["pwsh", "-NoProfile", "-c", "{pr.title}"],
            &["pwsh", "{pr.title}"],
            &["pwsh", "-Command", "iex {pr.id}"],
            &["powershell.exe", "echo", "{pr.id}"],
            // Wrappers, redirections and other shapes.
            &["env", "-i", "sh", "-c", "{pr.id}"],
            &["sudo", "-E", "bash", "-c", "{pr.id}"],
            &["sh", "-c", ">/dev/null {pr.id}"],
            &["sh", "-c", "2>&1 {pr.id}"],
            &["sh", "-c", "$'{pr.id}'"],
            &["sh", "-c", "{{ {pr.id}; }}"],
            &["sh", "-c", "eval {pr.id}"],
            &["sh", "-c", "source {pr.id}"],
            &["sh", "-c", ". {pr.id}"],
        ];
        for argv in refused {
            let err = spec_with(argv, &[], e())
                .err()
                .unwrap_or_else(|| panic!("{argv:?}"));
            assert!(err.starts_with(NOT_ALLOWED), "{argv:?}: {err}");
            assert!(err.ends_with("or as an sh positional argument"), "{err}");
        }
        let err = spec_with(&["wsl", "{pr.title}"], &[], e()).err().unwrap();
        assert_eq!(
            err,
            "placeholder not allowed in a shell command: pr.title; pass it via HERALD_* env vars, the event file, or as an sh positional argument"
        );

        let allowed: &[&[&str]] = &[
            &["sh", "-c", r#"echo "$1""#, "_", "{pr.title}"],
            &[
                "env",
                "FOO=1",
                "sh",
                "-c",
                r#"echo "$1""#,
                "_",
                "{pr.title}",
            ],
            &["bash", "-ce", "true", "_", "{branch.shortName}", "{pr.id}"],
            &["dash", "-c", "true", "_", "{pr.id}"],
            &["timeout", "5", "sh", "-c", "true", "_", "{pr.id}"],
            &["sh", "-c", "true", "{pr.id}"],
            &["sh", "-c", "echo {{literal}}; {{x}}"],
            &["sh", "-c", r#"git checkout "$HERALD_BRANCH""#],
            &["pwsh", "-Command", "echo {{x}}"],
            &["git", "checkout", "--", "{branch.shortName}"],
        ];
        for argv in allowed {
            assert!(spec_with(argv, &[], e()).is_ok(), "{argv:?}");
        }
    }

    #[test]
    fn placeholders_after_a_command_wrapper_are_refused() {
        let e = || event_with("feature/abc-1", "ok");
        let refused: &[(&[&str], &str)] = &[
            (&["env", "{pr.id}"], "env"),
            (&["env", "-S", "{pr.id}"], "env"),
            (&["sudo", "{pr.id}"], "sudo"),
            (&["timeout", "5", "{pr.id}"], "timeout"),
            (&["xargs", "{pr.id}"], "xargs"),
            (&["/usr/bin/nice", "-n", "5", "echo", "{pr.id}"], "nice"),
            (&["start", "{pr.id}"], "start"),
            (&["sudo", "git", "checkout", "--", "{pr.id}"], "sudo"),
        ];
        for (argv, wrapper) in refused {
            let err = spec_with(argv, &[], e())
                .err()
                .unwrap_or_else(|| panic!("{argv:?}"));
            assert_eq!(
                err,
                format!("placeholder not allowed after a command wrapper ({wrapper}): pr.id"),
                "{argv:?}"
            );
        }

        let allowed: &[&[&str]] = &[
            &["env", "FOO=1", "sh", "-c", "true", "_", "{pr.id}"],
            &["env", "FOO={{x}}", "sh", "-c", "true"],
            &["git", "checkout", "--", "{branch.shortName}"],
        ];
        for argv in allowed {
            assert!(spec_with(argv, &[], e()).is_ok(), "{argv:?}");
        }
    }

    #[test]
    fn sh_positional_placeholders_still_follow_the_allowlist() {
        let argv = ["sh", "-c", "true", "_", "{pr.title}"];
        for title in ["has space", "a;b", "$(x)", "a\\b"] {
            let err = spec_with(&argv, &[], event_with("b", title))
                .err()
                .unwrap_or_else(|| panic!("{title}"));
            assert_eq!(err, "unsafe value for sh: pr.title");
        }
        assert!(spec_with(&argv, &[], event_with("b", "feature/a-1@x:y+z")).is_ok());
    }

    #[test]
    fn cmd_allows_env_references_with_safe_values() {
        let e = event_with("feature/abc-1", "t");
        assert!(spec_with(&["cmd", "/C", "echo", "%HERALD_BRANCH%"], &[], e).is_ok());
    }

    #[test]
    fn non_shell_programs_get_argv_without_restriction() {
        assert!(spec_for(&["git", "checkout", "{branch.shortName}"]).is_ok());
    }

    #[tokio::test]
    async fn hostile_value_is_never_run_through_a_shell() {
        let fx = fixture();
        let marker = fx.dir.path().join("injected");
        let marker_arg = marker.display().to_string();
        let h = shell_hook(
            "inject",
            &["echo", "{branch.shortName}", ">", &marker_arg],
            &format!("echo {{branch.shortName}} > '{marker_arg}'"),
        );
        let mut e = event("pr.created");
        e.branch = Some(crate::event::BranchRef {
            name: "refs/heads/x".into(),
            short_name: "x&echo injected".into(),
        });
        fx.run_all(&compiled(vec![h]), e).await;
        let runs = fx.runs();
        assert!(
            runs[0]
                .error
                .as_deref()
                .unwrap()
                .contains("placeholder not allowed")
        );
        assert!(!marker.exists());
    }

    #[tokio::test]
    async fn child_reads_the_event_json_on_stdin() {
        let fx = fixture();
        let h = shell_hook("stdin", &["findstr", "^^", "1>&2"], "cat >&2");
        let run = fx.run_one(h).await;
        let tail = run.stderr_tail.unwrap_or_default();
        assert!(tail.contains("c1:pr:r1:7:created"), "{tail}");
    }

    #[test]
    fn debounce_deadline_is_pruned_once_it_has_passed() {
        let fx = fixture();
        let past = Instant::now();
        {
            let mut state = fx.shared.state();
            for i in 0..=DEBOUNCE_PRUNE_AT {
                state
                    .debounce_until
                    .insert((format!("h{i}"), "s".into()), past);
            }
        }
        let mut h = hook("fresh", "pr.*");
        h.debounce_secs = 60;
        let hooks = compiled(vec![h]);
        assert_eq!(
            names(
                &fx.shared
                    .dispatch(&hooks, &Arc::new(event("pr.created")), false)
            ),
            ["fresh"]
        );
        let state = fx.shared.state();
        assert_eq!(state.debounce_until.len(), 1);
        assert!(state.debounce_until.values().all(|d| *d > past));
    }

    #[tokio::test]
    async fn dispatch_survives_a_poisoned_state_lock() {
        let fx = fixture();
        let poisoner = fx.shared.clone();
        let _ = std::thread::spawn(move || {
            let _guard = poisoner.state.lock().unwrap();
            panic!("poison");
        })
        .join();
        let hooks = compiled(vec![hook("a", "pr.*")]);
        let jobs = fx
            .shared
            .dispatch(&hooks, &Arc::new(event("pr.created")), false);
        assert_eq!(names(&jobs), ["a"]);
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn timeout_kills_grandchildren_too() {
        let fx = fixture();
        let marker = fx.dir.path().join("grandchild-done");
        let script = fx.dir.path().join("grandchild.cmd");
        std::fs::write(
            &script,
            format!(
                "@ping -n 4 127.0.0.1 >NUL\r\n@echo done> \"{}\"\r\n",
                marker.display()
            ),
        )
        .unwrap();
        let script_arg = script.display().to_string();
        let mut h = shell_hook(
            "tree",
            &[
                "start",
                "/b",
                &script_arg,
                "&",
                "ping",
                "-n",
                "30",
                "127.0.0.1",
                ">NUL",
            ],
            "",
        );
        h.timeout_secs = 1;
        let run = fx.run_one(h).await;
        assert!(run.error.as_deref().unwrap().contains("timed out"));
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(!marker.exists(), "grandchild survived the timeout");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_kills_grandchildren_too() {
        let fx = fixture();
        let marker = fx.dir.path().join("grandchild-done");
        let script = format!("(sleep 3; echo done > '{}') & sleep 30", marker.display());
        let mut h = shell_hook("tree", &[], &script);
        h.timeout_secs = 1;
        let run = fx.run_one(h).await;
        assert!(run.error.as_deref().unwrap().contains("timed out"));
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(!marker.exists(), "grandchild survived the timeout");
    }

    #[tokio::test]
    async fn aborting_a_run_kills_its_child_and_removes_the_event_file() {
        let fx = fixture();
        let mut h = shell_hook(
            "slow",
            &["ping", "-n", "30", "127.0.0.1", ">NUL"],
            "sleep 30",
        );
        h.timeout_secs = 60;
        let hooks = compiled(vec![h]);
        let jobs = fx
            .shared
            .dispatch(&hooks, &Arc::new(event("pr.created")), false);
        let shared = fx.shared.clone();
        let mut set = tokio::task::JoinSet::new();
        for job in jobs {
            let shared = shared.clone();
            set.spawn(async move { shared.execute(job).await });
        }
        let dir = fx.shared.tmp_dir();
        for _ in 0..100 {
            if dir.exists() && std::fs::read_dir(&dir).unwrap().count() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
        set.shutdown().await;
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
        assert!(fx.runs().is_empty());
    }

    fn copy_hook(name: &str, marker: &Path) -> HookConfig {
        let marker_arg = marker.display().to_string();
        shell_hook(
            name,
            &["copy", "%HERALD_EVENT_FILE%", &marker_arg],
            &format!("cp \"$HERALD_EVENT_FILE\" '{marker_arg}'"),
        )
    }

    #[tokio::test]
    async fn runner_applies_a_new_config_from_the_watch() {
        let dir = tempfile::tempdir().unwrap();
        let old_marker = dir.path().join("old.json");
        let new_marker = dir.path().join("new.json");
        let mut config = Config::default();
        config.hooks.push(copy_hook("old", &old_marker));
        let store = Arc::new(SqliteStore::open_in_memory().unwrap());
        let classifier = Classifier::new(&config, &Default::default());
        let bus = Bus::new(store.clone(), classifier, LocalIndex::empty());
        let (tx, rx) = watch::channel(Arc::new(config.clone()));
        let runner = HookRunner::start(&bus, store, paths_in(dir.path()), rx);

        config.hooks = vec![copy_hook("new", &new_marker)];
        tx.send(Arc::new(config)).unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        bus.publish(event("pr.created")).await.unwrap();
        for _ in 0..100 {
            if new_marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        runner.shutdown().await;
        assert!(new_marker.exists(), "new hook never ran");
        assert!(!old_marker.exists(), "old hook still ran");
    }

    #[test]
    fn old_event_files_are_cleaned_and_recent_ones_kept() {
        let fx = fixture();
        let dir = fx.dir.path().join("files");
        std::fs::create_dir_all(&dir).unwrap();
        let old = dir.join("old.json");
        let fresh = dir.join("fresh.json");
        std::fs::write(&old, "{}").unwrap();
        std::fs::write(&fresh, "{}").unwrap();
        let two_days = std::time::SystemTime::now() - Duration::from_secs(2 * 24 * 3600);
        std::fs::File::options()
            .write(true)
            .open(&old)
            .unwrap()
            .set_modified(two_days)
            .unwrap();
        clean_old_event_files(&dir);
        assert!(!old.exists());
        assert!(fresh.exists());
    }
}
