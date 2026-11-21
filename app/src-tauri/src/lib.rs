use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use anyhow::Context;
use herald_core::config::{Config, ConfigError, Paths};
use herald_core::engine::Engine;
use herald_core::provider::ProviderRegistry;
use herald_core::secrets::{KeyringSecretStore, SecretStore};
use herald_core::store::Store;
use herald_provider_ado::AdoProviderFactory;
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tauri::{AppHandle, Manager, RunEvent};
use tauri_plugin_autostart::{MacosLauncher, ManagerExt};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

mod alerts;
mod commands;
mod sound;
mod tray;
mod windows;

const CONFIG_FILE: &str = "config.toml";
const DB_FILE: &str = "herald.db";
const CONFIG_DEBOUNCE: Duration = Duration::from_millis(500);
const LOG_FILES_KEPT: usize = 7;
const DEFAULT_LOG_FILTER: &str =
    "info,herald_core=debug,herald_provider_ado=debug,herald_app_lib=debug";
/// Appended last so `RUST_LOG` can't enable TRACE here: the handshake logs the Authorization header.
const LOG_CLAMP: [&str; 2] = ["tungstenite=debug", "tokio_tungstenite=debug"];
const AUTOSTART_KEY: &str = "autostart_initialized";

/// Config problems and secret-store status shown by the UI. Managed as `Mutex<UiErrors>`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UiErrors {
    pub config: Vec<ConfigError>,
    pub secrets_native: bool,
}

/// Holds the running engine so exit can shut it down; clone the `Arc` for use.
pub struct EngineHandle(Mutex<Option<Arc<Engine>>>);

impl EngineHandle {
    pub fn get(&self) -> Option<Arc<Engine>> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn take(&self) -> Option<Arc<Engine>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).take()
    }
}

/// Set by `request_quit` so the exit handler lets the app close.
#[derive(Default)]
pub struct QuitFlag(AtomicBool);

/// Keeps the config file watcher alive; dropping it stops the reload thread.
#[allow(dead_code)]
struct ConfigWatcher(Mutex<Option<RecommendedWatcher>>);

/// Keeps the log writer thread flushing; dropped on exit to flush buffered lines.
struct LogGuard(Mutex<Option<tracing_appender::non_blocking::WorkerGuard>>);

/// Quits the app for real, bypassing the keep-running-without-windows guard.
pub fn request_quit(app: &AppHandle) {
    app.state::<QuitFlag>().0.store(true, Ordering::SeqCst);
    app.exit(0);
}

fn build_registry() -> Arc<ProviderRegistry> {
    Arc::new(ProviderRegistry::new(vec![Arc::new(AdoProviderFactory)]))
}

/// Load + provider validation; all problems come back together.
fn load_config(path: &Path, registry: &ProviderRegistry) -> Result<Config, Vec<ConfigError>> {
    let config = Config::load(path)?;
    let errors = config.validate_providers(registry);
    if errors.is_empty() {
        Ok(config)
    } else {
        Err(errors)
    }
}

/// Falls back to the template config (no connections) so the app still runs.
fn config_or_fallback(
    path: &Path,
    registry: &ProviderRegistry,
) -> anyhow::Result<(Config, Vec<ConfigError>)> {
    match load_config(path, registry) {
        Ok(config) => Ok((config, Vec::new())),
        Err(errors) => {
            let fallback = Config::from_toml_str(Config::default_toml())
                .map_err(|e| anyhow::anyhow!("built-in default config is invalid: {e:?}"))?;
            Ok((fallback, errors))
        }
    }
}

/// Autostart is enabled once, on the first launch, unless disabled by the environment.
fn should_enable_autostart(initialized: Option<&str>, disabled_by_env: bool) -> bool {
    !disabled_by_env && initialized != Some("true")
}

/// Blocks for the first item, then until `quiet` passes with no further items.
/// Returns false when the channel is closed.
fn wait_debounced<T>(rx: &Receiver<T>, quiet: Duration) -> bool {
    if rx.recv().is_err() {
        return false;
    }
    loop {
        match rx.recv_timeout(quiet) {
            Ok(_) => {}
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => return true,
        }
    }
}

fn set_config_errors(app: &AppHandle, errors: Vec<ConfigError>) {
    let state = app.state::<Mutex<UiErrors>>();
    state.lock().unwrap_or_else(PoisonError::into_inner).config = errors;
}

fn reload_config(app: &AppHandle, path: &Path, registry: &ProviderRegistry) {
    match load_config(path, registry) {
        Ok(config) => {
            if let Some(engine) = app.state::<EngineHandle>().get() {
                engine.reload(config);
            }
            set_config_errors(app, Vec::new());
            tracing::info!("config reloaded");
        }
        Err(errors) => {
            tracing::warn!(count = errors.len(), ?errors, "config reload rejected");
            set_config_errors(app, errors);
        }
    }
}

/// Re-runs the watcher's load, validate and reload path on demand (tray menu).
pub fn reload_config_now(app: &AppHandle) {
    let Some(engine) = app.try_state::<EngineHandle>().and_then(|h| h.get()) else {
        return;
    };
    let path = engine.paths().home.join(CONFIG_FILE);
    reload_config(app, &path, &build_registry());
    commands::emit_ui_state(app);
}

/// Only content changes count: reads emit Access events, which would retrigger a reload forever.
fn is_config_change(event: &notify::Event) -> bool {
    matches!(
        event.kind,
        EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
    ) && event
        .paths
        .iter()
        .any(|p| p.file_name().is_some_and(|n| n == CONFIG_FILE))
}

fn watch_config(
    app: AppHandle,
    home: PathBuf,
    registry: Arc<ProviderRegistry>,
) -> anyhow::Result<RecommendedWatcher> {
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let mut watcher =
        notify::recommended_watcher(move |res: notify::Result<notify::Event>| match res {
            Ok(event) if is_config_change(&event) => {
                let _ = tx.send(());
            }
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "config watcher error"),
        })?;
    // Watch the directory: editors replace the file, which kills a file-level watch.
    watcher.watch(&home, RecursiveMode::NonRecursive)?;
    let path = home.join(CONFIG_FILE);
    std::thread::Builder::new()
        .name("config-watcher".into())
        .spawn(move || {
            while wait_debounced(&rx, CONFIG_DEBOUNCE) {
                reload_config(&app, &path, &registry);
            }
        })?;
    Ok(watcher)
}

fn build_log_filter(env: Option<&str>) -> EnvFilter {
    let mut filter = env
        .and_then(|v| EnvFilter::try_new(v).ok())
        .unwrap_or_else(|| EnvFilter::new(DEFAULT_LOG_FILTER));
    for directive in LOG_CLAMP {
        if let Ok(d) = directive.parse() {
            filter = filter.add_directive(d);
        }
    }
    filter
}

fn init_tracing(home: &Path) -> anyhow::Result<LogGuard> {
    let log_dir = home.join("logs");
    std::fs::create_dir_all(&log_dir).context("create log dir")?;
    let appender = tracing_appender::rolling::Builder::new()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .filename_prefix("herald")
        .filename_suffix("log")
        .max_log_files(LOG_FILES_KEPT)
        .build(&log_dir)
        .context("create log appender")?;
    let (writer, guard) = tracing_appender::non_blocking(appender);
    let filter = build_log_filter(std::env::var("RUST_LOG").ok().as_deref());
    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(writer)
        .with_ansi(false);
    let stderr_layer = cfg!(debug_assertions)
        .then(|| tracing_subscriber::fmt::layer().with_writer(std::io::stderr));
    tracing_subscriber::registry()
        .with(filter)
        .with(file_layer)
        .with(stderr_layer)
        .try_init()
        .context("install tracing subscriber")?;
    Ok(LogGuard(Mutex::new(Some(guard))))
}

fn enable_autostart_once(app: &AppHandle, store: &Store) {
    let disabled = std::env::var("HERALD_NO_AUTOSTART").is_ok_and(|v| !v.is_empty());
    let initialized = store.get_ui_state(AUTOSTART_KEY).ok().flatten();
    if !should_enable_autostart(initialized.as_deref(), disabled) {
        return;
    }
    let ok = match app.autolaunch().enable() {
        Ok(()) => true,
        Err(e) => {
            tracing::warn!(error = %e, "enabling autostart failed");
            false
        }
    };
    if let Err(e) = store.set_ui_state(AUTOSTART_KEY, if ok { "true" } else { "false" }) {
        tracing::warn!(error = %e, "saving autostart state failed");
    }
}

fn bootstrap(app: &AppHandle, paths: &Paths) -> anyhow::Result<()> {
    let registry = build_registry();
    let config_path = paths.home.join(CONFIG_FILE);
    if let Err(e) = Config::ensure_file(&config_path) {
        tracing::warn!(error = %e, "could not write default config");
    }
    let (config, config_errors) = config_or_fallback(&config_path, &registry)?;
    if !config_errors.is_empty() {
        tracing::warn!(?config_errors, "config invalid; running with defaults");
    }

    let store = Arc::new(Store::open(&paths.home.join(DB_FILE)).context("open store")?);
    let secrets = Arc::new(KeyringSecretStore::new());
    let secrets_native = secrets.is_native();
    if !secrets_native {
        tracing::warn!("native secret store unavailable");
    }
    app.manage(Mutex::new(UiErrors {
        config: config_errors,
        secrets_native,
    }));

    let secrets: Arc<dyn SecretStore> = secrets;
    let engine = tauri::async_runtime::block_on(Engine::start(
        config,
        store.clone(),
        registry.clone(),
        secrets,
        paths.clone(),
    ))
    .context("start engine")?;
    app.manage(EngineHandle(Mutex::new(Some(Arc::new(engine)))));

    enable_autostart_once(app, &store);

    let watcher = watch_config(app.clone(), paths.home.clone(), registry)?;
    app.manage(ConfigWatcher(Mutex::new(Some(watcher))));
    Ok(())
}

fn shutdown_engine(app: &AppHandle) {
    let Some(engine) = app.try_state::<EngineHandle>().and_then(|h| h.take()) else {
        return;
    };
    // In-flight commands hold clones; give them a moment to finish.
    for _ in 0..40 {
        if Arc::strong_count(&engine) == 1 {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    match Arc::try_unwrap(engine) {
        Ok(engine) => tauri::async_runtime::block_on(engine.shutdown()),
        Err(_) => tracing::warn!("engine still referenced at exit; dropping without drain"),
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let paths = Paths::from_env();
    #[allow(clippy::expect_used)]
    paths.ensure().expect("create herald directories");
    #[allow(clippy::expect_used)]
    let log_guard = init_tracing(&paths.home).expect("initialise logging");

    #[allow(clippy::expect_used)]
    let app = tauri::Builder::default()
        .plugin(tauri_plugin_single_instance::init(|app, _args, _cwd| {
            windows::show_pane(app);
        }))
        .plugin(tauri_plugin_autostart::init(
            MacosLauncher::LaunchAgent,
            None,
        ))
        .plugin(tauri_plugin_opener::init())
        .manage(QuitFlag::default())
        .manage(log_guard)
        .manage(commands::ToastHover(tokio::sync::watch::channel(false).0))
        .invoke_handler(tauri::generate_handler![
            commands::list_unviewed,
            commands::unviewed_count,
            commands::mark_viewed,
            commands::mark_all_viewed,
            commands::activate_record,
            commands::get_ui_state,
            commands::set_pinned,
            commands::list_connections,
            commands::set_secret,
            commands::clear_secret,
            commands::test_connection,
            commands::toast_clicked,
            commands::toast_hover,
            commands::toast_resize,
            commands::hide_pane,
        ])
        .setup(move |app| {
            bootstrap(app.handle(), &paths)?;
            app.manage(commands::StatusWatcher::spawn(app.handle()));
            windows::register(app.handle());
            tray::setup(app.handle())?;
            app.manage(sound::SoundPlayer::new());
            app.manage(alerts::Alerts::spawn(app.handle()));
            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("error while building tauri application");

    app.run(|app, event| match event {
        RunEvent::ExitRequested { api, .. } => {
            if !app.state::<QuitFlag>().0.load(Ordering::SeqCst) {
                api.prevent_exit();
            }
        }
        RunEvent::Exit => {
            tray::abort(app);
            if let Some(a) = app.try_state::<alerts::Alerts>() {
                a.abort();
            }
            if let Some(w) = app.try_state::<commands::StatusWatcher>() {
                w.abort();
            }
            shutdown_engine(app);
            if let Some(guard) = app.try_state::<LogGuard>() {
                guard
                    .0
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take();
            }
        }
        _ => {}
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::{AccessKind, CreateKind, DataChange, ModifyKind, RemoveKind};
    use std::sync::mpsc;

    fn event(kind: EventKind, file: &str) -> notify::Event {
        notify::Event::new(kind).add_path(PathBuf::from("/home").join(file))
    }

    #[test]
    fn is_config_change_ignores_access_and_other_files() {
        let modify = EventKind::Modify(ModifyKind::Data(DataChange::Any));
        assert!(is_config_change(&event(modify, CONFIG_FILE)));
        assert!(is_config_change(&event(
            EventKind::Create(CreateKind::File),
            CONFIG_FILE
        )));
        assert!(is_config_change(&event(
            EventKind::Remove(RemoveKind::File),
            CONFIG_FILE
        )));
        assert!(!is_config_change(&event(
            EventKind::Access(AccessKind::Any),
            CONFIG_FILE
        )));
        assert!(!is_config_change(&event(modify, "herald.db")));
    }

    #[derive(Clone, Default)]
    struct Buf(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Buf {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn captured(env: Option<&str>) -> String {
        let buf = Buf::default();
        let writer = buf.clone();
        let subscriber = tracing_subscriber::registry()
            .with(build_log_filter(env))
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(move || writer.clone())
                    .with_ansi(false),
            );
        tracing::subscriber::with_default(subscriber, || {
            tracing::trace!(target: "tungstenite::handshake", "ws-trace");
            tracing::debug!(target: "tungstenite::handshake", "ws-debug");
            tracing::trace!(target: "tokio_tungstenite", "tt-trace");
        });
        let bytes = buf.0.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }

    #[test]
    fn build_log_filter_clamps_tungstenite_to_debug() {
        for env in [None, Some("trace"), Some("tungstenite=trace")] {
            let out = captured(env);
            assert!(!out.contains("ws-trace"), "{env:?}: {out}");
            assert!(!out.contains("tt-trace"), "{env:?}: {out}");
        }
        assert!(captured(Some("trace")).contains("ws-debug"));
    }

    #[test]
    fn config_or_fallback_valid_file_has_no_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(CONFIG_FILE);
        Config::ensure_file(&path).unwrap();
        let (config, errors) = config_or_fallback(&path, &build_registry()).unwrap();
        assert!(errors.is_empty());
        assert!(config.connections.is_empty());
    }

    #[test]
    fn config_or_fallback_invalid_file_returns_defaults_and_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(CONFIG_FILE);
        std::fs::write(
            &path,
            "[[connections]]\nid = \"Bad Id\"\nprovider = \"ado\"\n",
        )
        .unwrap();
        let (config, errors) = config_or_fallback(&path, &build_registry()).unwrap();
        assert!(!errors.is_empty());
        assert!(config.connections.is_empty());
    }

    #[test]
    fn load_config_unknown_provider_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(CONFIG_FILE);
        std::fs::write(
            &path,
            "[[connections]]\nid = \"x\"\nprovider = \"nope\"\nauth = { method = \"pat\" }\n",
        )
        .unwrap();
        let errors = load_config(&path, &build_registry()).unwrap_err();
        assert!(!errors.is_empty());
    }

    #[test]
    fn autostart_enabled_only_on_first_launch_and_not_when_disabled() {
        assert!(should_enable_autostart(None, false));
        assert!(should_enable_autostart(Some("false"), false));
        assert!(!should_enable_autostart(Some("true"), false));
        assert!(!should_enable_autostart(None, true));
    }

    #[test]
    fn wait_debounced_collapses_a_burst_into_one_wakeup() {
        let (tx, rx) = mpsc::channel();
        for _ in 0..5 {
            tx.send(()).unwrap();
        }
        assert!(wait_debounced(&rx, Duration::from_millis(30)));
        assert!(rx.try_recv().is_err());
        drop(tx);
        assert!(!wait_debounced(&rx, Duration::from_millis(30)));
    }
}
