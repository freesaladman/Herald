//! Authorization header values for Azure DevOps: PAT (Basic) or Azure CLI token (Bearer).

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use chrono::{DateTime, Local, NaiveDateTime, TimeZone, Utc};
use futures_util::future::BoxFuture;
use herald_core::config::ConnectionConfig;
use herald_core::provider::ProviderError;
use herald_core::secrets::{SecretError, SecretStore};
use tokio::sync::Mutex;

const ADO_RESOURCE: &str = "499b84ac-1321-427f-aa17-267ca6975798";
const EXPIRY_MARGIN_MINUTES: i64 = 5;
const STDERR_EXCERPT_CHARS: usize = 200;
const AZ_TIMEOUT: Duration = Duration::from_secs(30);
// Backstop for runners that do not enforce AZ_TIMEOUT themselves; leaves room for the tree kill.
const AZ_BACKSTOP: Duration = Duration::from_secs(40);

/// Failure of the `az` invocation: it could not start or exited non-zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AzFailure {
    pub(crate) stderr: String,
}

/// Runs `az account get-access-token` and returns its stdout.
pub(crate) trait AzRunner: Send + Sync {
    fn run(&self) -> BoxFuture<'_, Result<String, AzFailure>>;
}

struct CliAzRunner;

/// Stands in for stderr when the run is cut off at the deadline.
const AZ_TIMED_OUT: &str = "az timed out";

/// Kills the process tree when dropped while armed, so a caller that drops the run future
/// (cancellation) still takes az's python child down. Runs on a plain thread: the runtime
/// may already be shutting down, where `spawn_blocking` would panic. The guard owns the
/// Child so its OS handle (and with it the PID) outlives the tree kill; a recycled PID
/// could otherwise be killed instead.
struct TreeKill {
    child: Option<tokio::process::Child>,
    armed: bool,
    kill: fn(u32),
}

impl Drop for TreeKill {
    fn drop(&mut self) {
        let child = self.child.take();
        if !self.armed {
            return;
        }
        if let Some(pid) = child.as_ref().and_then(|c| c.id()) {
            let kill = self.kill;
            std::thread::spawn(move || {
                kill(pid);
                drop(child);
            });
        }
    }
}

async fn read_all<R: tokio::io::AsyncRead + Unpin>(pipe: Option<R>) -> Vec<u8> {
    use tokio::io::AsyncReadExt;
    let mut buf = Vec::new();
    if let Some(mut pipe) = pipe {
        let _ = pipe.read_to_end(&mut buf).await;
    }
    buf
}

/// Runs `cmd` to completion or `timeout`. On timeout the tree is killed while the root
/// process still exists (taskkill /T needs it to find the descendants), then the child.
/// The child has no kill_on_drop: dropping it first would kill only the root.
async fn run_command(
    mut cmd: tokio::process::Command,
    timeout: Duration,
    kill: fn(u32),
) -> Result<String, AzFailure> {
    cmd.stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    #[cfg(windows)]
    cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    #[cfg(unix)]
    cmd.process_group(0);

    let child = cmd.spawn().map_err(|e| AzFailure {
        stderr: format!("could not run az: {e}"),
    })?;
    // Covers az's python child if Herald itself dies.
    herald_core::process::assign_to_kill_on_exit_job(&child);
    let mut guard = TreeKill {
        child: Some(child),
        armed: true,
        kill,
    };
    let Some(child) = guard.child.as_mut() else {
        return Err(AzFailure {
            stderr: "could not run az".to_string(),
        });
    };
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    // Pipes are drained concurrently with the wait so a full pipe cannot stall az.
    let io = async { tokio::join!(read_all(stdout), read_all(stderr), child.wait()) };
    let result = tokio::time::timeout(timeout, io).await;
    guard.armed = false;
    match result {
        Ok((out, err, status)) => match status {
            Ok(s) if s.success() => Ok(String::from_utf8_lossy(&out).into_owned()),
            Ok(_) => Err(AzFailure {
                stderr: String::from_utf8_lossy(&err).into_owned(),
            }),
            Err(e) => Err(AzFailure {
                stderr: format!("could not run az: {e}"),
            }),
        },
        Err(_) => {
            if let Some(child) = guard.child.as_mut() {
                if let Some(pid) = child.id() {
                    let _ = tokio::task::spawn_blocking(move || kill(pid)).await;
                }
                let _ = child.kill().await;
            }
            Err(AzFailure {
                stderr: AZ_TIMED_OUT.to_string(),
            })
        }
    }
}

impl AzRunner for CliAzRunner {
    fn run(&self) -> BoxFuture<'_, Result<String, AzFailure>> {
        Box::pin(async {
            // az is az.cmd on Windows, which only cmd can launch.
            #[cfg(windows)]
            let mut cmd = {
                let mut c = tokio::process::Command::new("cmd");
                // /D skips AutoRun (clink, banners) that would corrupt the JSON on stdout.
                c.args(["/D", "/C", "az"]);
                // cmd resolves `az` from the cwd before PATH, so a repo-planted az.cmd
                // would run. Disable that lookup and start in the system root instead.
                c.env("NoDefaultCurrentDirectoryInExePath", "1");
                c.current_dir(
                    std::env::var_os("SystemRoot").unwrap_or_else(|| r"C:\Windows".into()),
                );
                c
            };
            #[cfg(not(windows))]
            let mut cmd = tokio::process::Command::new("az");
            cmd.args([
                "account",
                "get-access-token",
                "--resource",
                ADO_RESOURCE,
                "-o",
                "json",
            ]);
            run_command(cmd, AZ_TIMEOUT, herald_core::process::kill_tree).await
        })
    }
}

type Clock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

struct CachedToken {
    header: String,
    expires_utc: DateTime<Utc>,
    generation: u64,
}

enum Method {
    Pat,
    AzCli,
}

pub struct AdoAuth {
    connection_id: String,
    method: Method,
    secrets: Arc<dyn SecretStore>,
    runner: Arc<dyn AzRunner>,
    clock: Clock,
    cache: Mutex<Option<CachedToken>>,
    // Bumped by invalidate(); a cached token from an older generation is stale.
    generation: AtomicU64,
}

impl fmt::Debug for AdoAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdoAuth")
            .field("connection_id", &self.connection_id)
            .field("method", &self.method())
            .finish_non_exhaustive()
    }
}

impl AdoAuth {
    pub fn from_config(
        cfg: &ConnectionConfig,
        secrets: Arc<dyn SecretStore>,
    ) -> Result<AdoAuth, ProviderError> {
        Self::with_runner(cfg, secrets, Arc::new(CliAzRunner), Arc::new(Utc::now))
    }

    fn with_runner(
        cfg: &ConnectionConfig,
        secrets: Arc<dyn SecretStore>,
        runner: Arc<dyn AzRunner>,
        clock: Clock,
    ) -> Result<AdoAuth, ProviderError> {
        let method = match cfg.auth.method.trim() {
            "" | "pat" => Method::Pat,
            "azcli" => Method::AzCli,
            other => {
                return Err(ProviderError::Config(format!(
                    "unknown auth method '{other}' (expected 'pat' or 'azcli')"
                )));
            }
        };
        Ok(AdoAuth {
            connection_id: cfg.id.clone(),
            method,
            secrets,
            runner,
            clock,
            cache: Mutex::new(None),
            generation: AtomicU64::new(0),
        })
    }

    pub fn method(&self) -> &str {
        match self.method {
            Method::Pat => "pat",
            Method::AzCli => "azcli",
        }
    }

    /// Drops the cached az token so the next call fetches a fresh one.
    pub fn invalidate(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
    }

    pub async fn authorization(&self) -> Result<String, ProviderError> {
        match self.method {
            Method::Pat => self.pat_header(),
            Method::AzCli => self.azcli_header().await,
        }
    }

    fn pat_header(&self) -> Result<String, ProviderError> {
        let pat = self
            .secrets
            .get(&self.connection_id)
            .map_err(|e| {
                let kind = match e {
                    SecretError::Backend(_) => "Backend",
                    SecretError::NoNativeStore => "NoNativeStore",
                };
                ProviderError::Other(format!("secret store error: {kind}"))
            })?
            .filter(|p| !p.is_empty())
            .ok_or(ProviderError::MissingCredentials)?;
        Ok(format!("Basic {}", STANDARD.encode(format!(":{pat}"))))
    }

    async fn azcli_header(&self) -> Result<String, ProviderError> {
        let mut cache = self.cache.lock().await;
        let generation = self.generation.load(Ordering::SeqCst);
        let now = (self.clock)();
        if let Some(c) = cache.as_ref()
            && c.generation == generation
            && now < c.expires_utc
        {
            return Ok(c.header.clone());
        }
        let stdout = tokio::time::timeout(AZ_BACKSTOP, self.runner.run())
            .await
            .map_err(|_| ProviderError::Other(AZ_TIMED_OUT.to_string()))?
            .map_err(classify_failure)?;
        let (token, expires) = parse_token(&stdout)?;
        let header = format!("Bearer {token}");
        *cache = Some(CachedToken {
            header: header.clone(),
            expires_utc: expires - chrono::Duration::minutes(EXPIRY_MARGIN_MINUTES),
            generation,
        });
        Ok(header)
    }
}

fn classify_failure(f: AzFailure) -> ProviderError {
    if f.stderr == AZ_TIMED_OUT {
        return ProviderError::Other(AZ_TIMED_OUT.to_string());
    }
    let lower = f.stderr.to_lowercase();
    if lower.contains("az login") || lower.contains("not logged in") {
        return ProviderError::MissingCredentials;
    }
    let excerpt: String = f.stderr.trim().chars().take(STDERR_EXCERPT_CHARS).collect();
    ProviderError::Other(format!("Azure CLI sign-in failed: {excerpt}"))
}

fn parse_token(stdout: &str) -> Result<(String, DateTime<Utc>), ProviderError> {
    let bad = |what: &str| ProviderError::Other(format!("unexpected az output: {what}"));
    let v: serde_json::Value = serde_json::from_str(stdout).map_err(|_| bad("not valid JSON"))?;
    let token = v
        .get("accessToken")
        .and_then(|t| t.as_str())
        .filter(|t| !t.is_empty())
        .ok_or_else(|| bad("missing accessToken"))?
        .to_string();

    let epoch = v.get("expires_on").and_then(|e| match e {
        serde_json::Value::Number(n) => n.as_i64(),
        serde_json::Value::String(s) => s.trim().parse().ok(),
        _ => None,
    });
    let expires = match epoch {
        Some(secs) => DateTime::from_timestamp(secs, 0),
        None => v
            .get("expiresOn")
            .and_then(|e| e.as_str())
            .and_then(parse_local_expiry),
    }
    .ok_or_else(|| bad("missing or invalid expiry"))?;
    Ok((token, expires))
}

/// `expiresOn` is local time like `2026-11-20 18:00:00.000000`.
fn parse_local_expiry(s: &str) -> Option<DateTime<Utc>> {
    let naive = NaiveDateTime::parse_from_str(s.trim(), "%Y-%m-%d %H:%M:%S%.f").ok()?;
    Local
        .from_local_datetime(&naive)
        .earliest()
        .map(|d| d.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use herald_core::config::AuthConfig;
    use herald_core::secrets::MemorySecretStore;
    use std::collections::VecDeque;
    use std::sync::Mutex as StdMutex;

    fn cfg(method: &str) -> ConnectionConfig {
        ConnectionConfig {
            notify: Default::default(),
            id: "conn-a".into(),
            provider: "ado".into(),
            enabled: true,
            auth: AuthConfig {
                method: method.into(),
            },
            bots: Vec::new(),
            settings: Default::default(),
        }
    }

    struct FakeRunner {
        results: StdMutex<VecDeque<Result<String, AzFailure>>>,
        calls: StdMutex<u32>,
    }

    impl FakeRunner {
        fn new(results: Vec<Result<String, AzFailure>>) -> Arc<Self> {
            Arc::new(Self {
                results: StdMutex::new(results.into()),
                calls: StdMutex::new(0),
            })
        }
        fn calls(&self) -> u32 {
            *self.calls.lock().unwrap()
        }
    }

    impl AzRunner for FakeRunner {
        fn run(&self) -> BoxFuture<'_, Result<String, AzFailure>> {
            *self.calls.lock().unwrap() += 1;
            let r = self.results.lock().unwrap().pop_front().unwrap();
            Box::pin(async move { r })
        }
    }

    fn token_json(token: &str, expires_on: i64) -> String {
        format!(r#"{{"accessToken":"{token}","expires_on":{expires_on},"tokenType":"Bearer"}}"#)
    }

    fn t0() -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000, 0).unwrap()
    }

    fn az_auth(runner: Arc<FakeRunner>, now: Arc<StdMutex<DateTime<Utc>>>) -> AdoAuth {
        AdoAuth::with_runner(
            &cfg("azcli"),
            Arc::new(MemorySecretStore::new()),
            runner,
            Arc::new(move || *now.lock().unwrap()),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn pat_header_is_basic_of_colon_pat() {
        let secrets = Arc::new(MemorySecretStore::new());
        secrets.set("conn-a", "s3cret").unwrap();
        let auth = AdoAuth::from_config(&cfg(""), secrets).unwrap();
        assert_eq!(auth.method(), "pat");
        assert_eq!(
            auth.authorization().await.unwrap(),
            format!("Basic {}", STANDARD.encode(":s3cret"))
        );
    }

    #[tokio::test]
    async fn missing_pat_is_missing_credentials() {
        let auth = AdoAuth::from_config(&cfg("pat"), Arc::new(MemorySecretStore::new())).unwrap();
        assert_eq!(
            auth.authorization().await.unwrap_err(),
            ProviderError::MissingCredentials
        );
    }

    #[test]
    fn unknown_method_is_config_error() {
        let err =
            AdoAuth::from_config(&cfg("oauth"), Arc::new(MemorySecretStore::new())).unwrap_err();
        assert!(matches!(err, ProviderError::Config(_)));
    }

    #[tokio::test]
    async fn azcli_caches_until_expiry_minus_margin_then_refreshes() {
        let now = Arc::new(StdMutex::new(t0()));
        let runner = FakeRunner::new(vec![
            Ok(token_json("tok-1", t0().timestamp() + 3600)),
            Ok(token_json("tok-2", t0().timestamp() + 7200)),
        ]);
        let auth = az_auth(runner.clone(), now.clone());

        assert_eq!(auth.authorization().await.unwrap(), "Bearer tok-1");
        *now.lock().unwrap() = t0() + chrono::Duration::minutes(54);
        assert_eq!(auth.authorization().await.unwrap(), "Bearer tok-1");
        assert_eq!(runner.calls(), 1);

        *now.lock().unwrap() = t0() + chrono::Duration::minutes(56);
        assert_eq!(auth.authorization().await.unwrap(), "Bearer tok-2");
        assert_eq!(runner.calls(), 2);
    }

    #[tokio::test]
    async fn azcli_invalidate_forces_refetch() {
        let runner = FakeRunner::new(vec![
            Ok(token_json("tok-1", t0().timestamp() + 3600)),
            Ok(token_json("tok-2", t0().timestamp() + 3600)),
        ]);
        let auth = az_auth(runner, Arc::new(StdMutex::new(t0())));
        auth.authorization().await.unwrap();
        auth.invalidate();
        assert_eq!(auth.authorization().await.unwrap(), "Bearer tok-2");
    }

    #[tokio::test]
    async fn azcli_parses_local_expires_on_string() {
        let local = (Local::now() + chrono::Duration::hours(1))
            .format("%Y-%m-%d %H:%M:%S%.6f")
            .to_string();
        let json = format!(r#"{{"accessToken":"tok","expiresOn":"{local}"}}"#);
        let runner = FakeRunner::new(vec![Ok(json)]);
        let auth = AdoAuth::with_runner(
            &cfg("azcli"),
            Arc::new(MemorySecretStore::new()),
            runner.clone(),
            Arc::new(Utc::now),
        )
        .unwrap();
        auth.authorization().await.unwrap();
        auth.authorization().await.unwrap();
        assert_eq!(runner.calls(), 1);
    }

    #[tokio::test]
    async fn azcli_failures_are_classified() {
        let runner = FakeRunner::new(vec![
            Err(AzFailure {
                stderr: "ERROR: Please run 'az login' to setup account.".into(),
            }),
            Err(AzFailure {
                stderr: "boom".into(),
            }),
        ]);
        let auth = az_auth(runner, Arc::new(StdMutex::new(t0())));
        assert_eq!(
            auth.authorization().await.unwrap_err(),
            ProviderError::MissingCredentials
        );
        assert_eq!(
            auth.authorization().await.unwrap_err(),
            ProviderError::Other("Azure CLI sign-in failed: boom".into())
        );
    }

    struct FailingStore;
    impl SecretStore for FailingStore {
        fn get(&self, _: &str) -> Result<Option<String>, SecretError> {
            Err(SecretError::Backend("nope".into()))
        }
        fn set(&self, _: &str, _: &str) -> Result<(), SecretError> {
            Ok(())
        }
        fn delete(&self, _: &str) -> Result<(), SecretError> {
            Ok(())
        }
        fn is_native(&self) -> bool {
            false
        }
    }

    #[tokio::test]
    async fn secret_store_error_is_other() {
        let auth = AdoAuth::from_config(&cfg("pat"), Arc::new(FailingStore)).unwrap();
        assert_eq!(
            auth.authorization().await.unwrap_err(),
            ProviderError::Other("secret store error: Backend".into())
        );
    }

    struct HangThenSucceedRunner {
        calls: StdMutex<u32>,
        ok: String,
    }
    impl AzRunner for HangThenSucceedRunner {
        fn run(&self) -> BoxFuture<'_, Result<String, AzFailure>> {
            let first = {
                let mut calls = self.calls.lock().unwrap();
                *calls += 1;
                *calls == 1
            };
            if first {
                Box::pin(std::future::pending())
            } else {
                let ok = self.ok.clone();
                Box::pin(async move { Ok(ok) })
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn azcli_times_out_then_releases_the_cache_lock() {
        let runner = Arc::new(HangThenSucceedRunner {
            calls: StdMutex::new(0),
            ok: token_json("tok-after", Utc::now().timestamp() + 3600),
        });
        let auth = AdoAuth::with_runner(
            &cfg("azcli"),
            Arc::new(MemorySecretStore::new()),
            runner,
            Arc::new(Utc::now),
        )
        .unwrap();
        assert_eq!(
            auth.authorization().await.unwrap_err(),
            ProviderError::Other("az timed out".into())
        );
        assert_eq!(auth.authorization().await.unwrap(), "Bearer tok-after");
    }

    static KILLED_PID: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

    fn record_and_kill(pid: u32) {
        KILLED_PID.store(pid, Ordering::SeqCst);
        herald_core::process::kill_tree(pid);
    }

    fn sleeper() -> tokio::process::Command {
        #[cfg(windows)]
        {
            let mut c = tokio::process::Command::new("cmd");
            c.args(["/D", "/C", "ping -n 30 127.0.0.1 >NUL"]);
            c
        }
        #[cfg(not(windows))]
        {
            let mut c = tokio::process::Command::new("sleep");
            c.arg("30");
            c
        }
    }

    #[tokio::test]
    async fn run_command_timeout_kills_the_tree_and_reports_a_timeout() {
        let started = std::time::Instant::now();
        let err = run_command(sleeper(), Duration::from_millis(300), record_and_kill)
            .await
            .unwrap_err();
        assert_eq!(err.stderr, AZ_TIMED_OUT);
        assert_ne!(
            KILLED_PID.load(Ordering::SeqCst),
            0,
            "tree kill not invoked"
        );
        assert!(started.elapsed() < Duration::from_secs(15));
        assert_eq!(
            classify_failure(err),
            ProviderError::Other("az timed out".into())
        );
    }

    static CANCELLED_PID: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

    fn record_cancel_and_kill(pid: u32) {
        CANCELLED_PID.store(pid, Ordering::SeqCst);
        herald_core::process::kill_tree(pid);
    }

    #[tokio::test]
    async fn run_command_dropped_future_kills_the_tree() {
        let outer = tokio::time::timeout(
            Duration::from_millis(300),
            run_command(sleeper(), Duration::from_secs(30), record_cancel_and_kill),
        )
        .await;
        assert!(outer.is_err(), "run_command finished before the cancel");
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while CANCELLED_PID.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_ne!(
            CANCELLED_PID.load(Ordering::SeqCst),
            0,
            "tree kill not invoked after the future was dropped"
        );
    }

    #[tokio::test]
    async fn run_command_returns_stdout_on_success() {
        #[cfg(windows)]
        let cmd = {
            let mut c = tokio::process::Command::new("cmd");
            c.args(["/D", "/C", "echo hi"]);
            c
        };
        #[cfg(not(windows))]
        let cmd = {
            let mut c = tokio::process::Command::new("echo");
            c.arg("hi");
            c
        };
        let out = run_command(cmd, Duration::from_secs(10), |_| {})
            .await
            .unwrap();
        assert_eq!(out.trim(), "hi");
    }

    #[tokio::test]
    async fn debug_output_has_no_secret_or_token() {
        let secrets = Arc::new(MemorySecretStore::new());
        secrets.set("conn-a", "s3cret").unwrap();
        let auth = AdoAuth::from_config(&cfg("pat"), secrets).unwrap();
        auth.authorization().await.unwrap();
        assert!(!format!("{auth:?}").contains("s3cret"));

        let runner = FakeRunner::new(vec![Ok(token_json("tok-xyz", t0().timestamp() + 3600))]);
        let az = az_auth(runner, Arc::new(StdMutex::new(t0())));
        az.authorization().await.unwrap();
        assert!(!format!("{az:?}").contains("tok-xyz"));
    }
}
