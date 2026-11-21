//! Engine: owns the bus, the scheduler and the shared services.

use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::bus::Bus;
use crate::classify::Classifier;
use crate::config::{Config, ConnectionConfig, Paths};
use crate::hooks::HookRunner;
use crate::local_repos::LocalIndex;
use crate::provider::{Connection, Identity, ProviderError, ProviderRegistry, ScopeOption};
use crate::scheduler::{ConnectionStatus, Scheduler};
use crate::secrets::{SecretError, SecretStore};
use crate::store::Store;
use crate::stream::StreamServer;

/// Upper bound for a draft connection test, so a hung provider cannot stall the caller.
const DRAFT_TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const RELOAD_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

/// Serves one in-memory secret under one key; refuses writes and never reads the real store.
struct DraftSecretStore {
    key: String,
    value: Option<String>,
}

impl SecretStore for DraftSecretStore {
    fn get(&self, key: &str) -> Result<Option<String>, SecretError> {
        Ok(if key == self.key {
            self.value.clone()
        } else {
            None
        })
    }

    fn set(&self, _key: &str, _value: &str) -> Result<(), SecretError> {
        Err(SecretError::Backend(
            "draft secret store is read-only".into(),
        ))
    }

    fn delete(&self, _key: &str) -> Result<(), SecretError> {
        Err(SecretError::Backend(
            "draft secret store is read-only".into(),
        ))
    }

    fn is_native(&self) -> bool {
        false
    }
}

async fn draft_timeout<T>(
    fut: impl std::future::Future<Output = Result<T, ProviderError>>,
) -> Result<T, ProviderError> {
    match tokio::time::timeout(DRAFT_TEST_TIMEOUT, fut).await {
        Ok(result) => result,
        Err(_) => Err(ProviderError::Transient("connection test timed out".into())),
    }
}

/// Nothing in start-up can fail today; kept so callers already handle a failing start.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {}

pub struct Engine {
    bus: Arc<Bus>,
    store: Arc<Store>,
    secrets: Arc<dyn SecretStore>,
    registry: Arc<ProviderRegistry>,
    paths: Paths,
    config: watch::Sender<Arc<Config>>,
    scheduler: Arc<Scheduler>,
    index_task: JoinHandle<()>,
    reloads: mpsc::UnboundedSender<Arc<Config>>,
    reload_task: JoinHandle<()>,
    hooks: HookRunner,
    stream: Arc<tokio::sync::Mutex<StreamSlot>>,
    stream_control_task: JoinHandle<()>,
}

/// The running stream server and the `stream.port` it was started for.
struct StreamSlot {
    server: Option<StreamServer>,
    port: u16,
}

/// What the reload worker needs to restart the stream.
struct StreamRestarter {
    slot: Arc<tokio::sync::Mutex<StreamSlot>>,
    bus: Arc<Bus>,
    paths: Paths,
    control: mpsc::UnboundedSender<String>,
}

impl StreamRestarter {
    /// Rebinds when `stream.port` changed, or retries when the stream is down. The old
    /// server stops first so an explicit port can be reused; if the new bind fails the
    /// engine keeps running with the stream stopped.
    async fn apply(&self, port: u16) {
        let mut slot = self.slot.lock().await;
        if slot.server.is_some() && slot.port == port {
            return;
        }
        if let Some(old) = slot.server.take() {
            old.shutdown().await;
        }
        slot.port = port;
        let started = StreamServer::start(
            self.bus.clone(),
            self.paths.clone(),
            port,
            self.control.clone(),
        )
        .await;
        match started {
            Ok(server) => {
                tracing::info!(port = server.port(), "stream server restarted");
                slot.server = Some(server);
            }
            Err(e) => {
                tracing::warn!(error = %e, port, "stream server failed to restart; stream is stopped");
            }
        }
    }
}

/// Applies reloads one at a time, in the order they were requested.
async fn run_reloads(
    scheduler: Arc<Scheduler>,
    stream: StreamRestarter,
    mut applied: Arc<Config>,
    mut rx: mpsc::UnboundedReceiver<Arc<Config>>,
) {
    while let Some(next) = rx.recv().await {
        apply_reload(&scheduler, &applied, &next).await;
        stream.apply(next.stream.port).await;
        applied = next;
    }
}

/// Applies credential changes requested over the stream.
async fn run_stream_control(scheduler: Arc<Scheduler>, mut rx: mpsc::UnboundedReceiver<String>) {
    while let Some(id) = rx.recv().await {
        credentials_changed(&scheduler, &id);
    }
}

fn credentials_changed(scheduler: &Scheduler, connection_id: &str) {
    let Some(state) = scheduler.connection(connection_id) else {
        return;
    };
    if let Some(connection) = &state.connection {
        connection.credentials_changed();
    }
    state.reset_auth();
    state.resume();
}

fn by_id(config: &Config) -> BTreeMap<&str, &ConnectionConfig> {
    config
        .connections
        .iter()
        .map(|c| (c.id.as_str(), c))
        .collect()
}

async fn apply_reload(scheduler: &Scheduler, old: &Config, new: &Config) {
    // One status publish for the whole reload, not one per stop and start.
    let _statuses = scheduler.hold_statuses();
    let (old_by_id, new_by_id) = (by_id(old), by_id(new));
    for id in old_by_id.keys().filter(|id| !new_by_id.contains_key(*id)) {
        scheduler.stop_connection(id).await;
    }
    for (id, cfg) in new_by_id {
        let restart = match old_by_id.get(id) {
            None => true,
            Some(prev) => *prev != cfg || (cfg.enabled && scheduler.needs_rebuild(id)),
        };
        if !restart {
            continue;
        }
        // The old state must be gone before prepare: both would share one id.
        scheduler.stop_connection(id).await;
        if cfg.enabled {
            let prepared = scheduler.prepare(cfg.clone(), false);
            scheduler.launch(prepared);
        }
    }
    scheduler.rebuild_classifier();
}

impl Engine {
    /// Returns without waiting on the provider or the local scan: connections identify
    /// lazily in their tier tasks and the local index is built in the background.
    pub async fn start(
        config: Config,
        store: Arc<Store>,
        registry: Arc<ProviderRegistry>,
        secrets: Arc<dyn SecretStore>,
        paths: Paths,
    ) -> Result<Engine, EngineError> {
        let local = config.local.clone();
        let connections = config.connections.clone();
        let index_registry = registry.clone();
        // Replaced with one carrying the identities as each connection identifies.
        let classifier = Classifier::new(&config, &BTreeMap::new());
        let bus = Arc::new(Bus::new(store.clone(), classifier, LocalIndex::empty()));
        let index_bus = bus.clone();
        let index_task = tokio::spawn(async move {
            let built = tokio::task::spawn_blocking(move || {
                LocalIndex::build(&local, &connections, &index_registry)
            })
            .await;
            match built {
                Ok(index) => index_bus.set_local_index(index),
                Err(e) => tracing::error!(error = %e, "local index build failed"),
            }
        });
        let (config_tx, config_rx) = watch::channel(Arc::new(config));
        let hooks = HookRunner::start(&bus, store.clone(), paths.clone(), config_tx.subscribe());
        let initial = config_rx.borrow().clone();
        let initial_port = initial.stream.port;
        let scheduler = Arc::new(Scheduler::start(
            bus.clone(),
            store.clone(),
            registry.clone(),
            secrets.clone(),
            config_rx,
        ));
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let server = match StreamServer::start(
            bus.clone(),
            paths.clone(),
            initial_port,
            control_tx.clone(),
        )
        .await
        {
            Ok(server) => Some(server),
            Err(e) => {
                tracing::error!(error = %e, "stream server failed to start; running without it");
                None
            }
        };
        let stream = Arc::new(tokio::sync::Mutex::new(StreamSlot {
            server,
            port: initial_port,
        }));
        let stream_control_task = tokio::spawn(run_stream_control(scheduler.clone(), control_rx));
        let restarter = StreamRestarter {
            slot: stream.clone(),
            bus: bus.clone(),
            paths: paths.clone(),
            control: control_tx,
        };
        let (reloads, reload_rx) = mpsc::unbounded_channel();
        let reload_task = tokio::spawn(run_reloads(
            scheduler.clone(),
            restarter,
            initial,
            reload_rx,
        ));
        Ok(Engine {
            bus,
            store,
            secrets,
            registry,
            paths,
            config: config_tx,
            scheduler,
            index_task,
            reloads,
            reload_task,
            hooks,
            stream,
            stream_control_task,
        })
    }

    pub fn bus(&self) -> Arc<Bus> {
        self.bus.clone()
    }

    pub fn store(&self) -> Arc<Store> {
        self.store.clone()
    }

    pub fn secrets(&self) -> Arc<dyn SecretStore> {
        self.secrets.clone()
    }

    pub fn paths(&self) -> &Paths {
        &self.paths
    }

    pub fn config(&self) -> Arc<Config> {
        self.config.borrow().clone()
    }

    pub fn scheduler(&self) -> &Scheduler {
        &self.scheduler
    }

    /// Swaps in a new config: the classifier is rebuilt now, and connections are
    /// diffed by id in the background (removed stop, added start, changed restart).
    /// A restart rebuilds the connection, which re-enables its disabled sources.
    /// A changed `stream.port` restarts the stream server with a new token; clients
    /// re-read `stream.json`. Changed `[polling]` intervals apply to running connections
    /// from their next poll, without a restart.
    pub fn reload(&self, config: Config) {
        let config = Arc::new(config);
        self.config.send_replace(config.clone());
        self.scheduler.rebuild_classifier();
        if self.reloads.send(config).is_err() {
            tracing::error!("reload worker is not running");
        }
    }

    /// Call after the secret changed: the connection drops cached credentials and the
    /// next tick identifies again.
    pub fn credentials_changed(&self, connection_id: &str) {
        credentials_changed(&self.scheduler, connection_id);
    }

    pub fn connections(&self) -> Vec<ConnectionStatus> {
        self.scheduler.statuses()
    }

    /// Don't hold a `borrow()` guard while calling Engine methods: it blocks the sender
    /// and can deadlock. Clone the value out, or use `borrow_and_update` and drop it.
    pub fn status_changes(&self) -> watch::Receiver<Vec<ConnectionStatus>> {
        self.scheduler.status_changes()
    }

    /// Identifies on the live connection without touching scheduler state.
    pub async fn test_connection(&self, connection_id: &str) -> Result<Identity, ProviderError> {
        let state = self
            .scheduler
            .connection(connection_id)
            .ok_or_else(|| ProviderError::Config("unknown connection".to_string()))?;
        let connection = state
            .connection
            .clone()
            .ok_or_else(|| ProviderError::Config("connection not connected".to_string()))?;
        connection.identify().await
    }

    /// Tests an unsaved connection config. Nothing is persisted, published or
    /// registered with the scheduler, and the connection is dropped on return.
    /// `stored_secret_id` names the connection whose stored secret may stand in when
    /// `secret` is None (the id being edited); None uses no stored secret.
    pub async fn test_draft(
        &self,
        cfg: ConnectionConfig,
        secret: Option<String>,
        stored_secret_id: Option<&str>,
    ) -> Result<Identity, ProviderError> {
        let connection = self.draft_connection(&cfg, secret, stored_secret_id)?;
        draft_timeout(connection.identify()).await
    }

    /// Lists the options of a scope field for an unsaved connection config, under the
    /// same rules as `test_draft`.
    pub async fn list_draft_scopes(
        &self,
        cfg: ConnectionConfig,
        secret: Option<String>,
        stored_secret_id: Option<&str>,
        field_key: &str,
    ) -> Result<Vec<ScopeOption>, ProviderError> {
        let connection = self.draft_connection(&cfg, secret, stored_secret_id)?;
        draft_timeout(connection.list_scopes(field_key)).await
    }

    fn draft_connection(
        &self,
        cfg: &ConnectionConfig,
        secret: Option<String>,
        stored_secret_id: Option<&str>,
    ) -> Result<Arc<dyn Connection>, ProviderError> {
        let factory = self
            .registry
            .get(&cfg.provider)
            .ok_or_else(|| ProviderError::Config(format!("unknown provider {}", cfg.provider)))?;
        let secret = match (secret, stored_secret_id) {
            (Some(s), _) => Some(s),
            (None, Some(id)) => self
                .secrets
                .get(id)
                .map_err(|e| ProviderError::Other(format!("secret store: {e}")))?,
            (None, None) => None,
        };
        factory.connect(
            cfg,
            Arc::new(DraftSecretStore {
                key: cfg.id.clone(),
                value: secret,
            }),
        )
    }

    pub fn poll_now(&self) {
        self.scheduler.poll_now();
    }

    /// Stops every task and waits for them to end.
    pub async fn shutdown(mut self) {
        self.hooks.shutdown().await;
        self.index_task.abort();
        let _ = (&mut self.index_task).await;
        // Closing the channel lets the worker finish a reload in progress, so no
        // connection is left half-stopped; abort only if it stalls.
        drop(self.reloads);
        let grace = tokio::time::timeout(RELOAD_GRACE, &mut self.reload_task).await;
        if grace.is_err() {
            tracing::warn!("reload worker did not finish in time; aborting it");
            self.reload_task.abort();
            let _ = (&mut self.reload_task).await;
        }
        // After the reload worker, so a reload cannot start a stream behind this.
        let server = self.stream.lock().await.server.take();
        if let Some(server) = server {
            server.shutdown().await;
        }
        self.stream_control_task.abort();
        let _ = (&mut self.stream_control_task).await;
        match Arc::try_unwrap(self.scheduler) {
            Ok(scheduler) => scheduler.shutdown().await,
            Err(_) => tracing::warn!("scheduler still shared at shutdown; tasks abort on drop"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::event_types;
    use crate::provider::{AuthState, ProviderFactory, Tier};
    use crate::secrets::MemorySecretStore;
    use crate::testing::{FakeProvider, FakeProviderFactory};
    use std::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn start_wires_services_and_shutdown_ends() {
        let fake = FakeProvider::new();
        let mut config = Config::from_toml_str(Config::default_toml()).unwrap();
        config.connections = toml::from_str::<Config>(
            "[[connections]]\nid = \"c1\"\nprovider = \"fake\"\nauth = { method = \"pat\" }\n",
        )
        .unwrap()
        .connections;
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths {
            home: dir.path().join("home"),
            runtime: dir.path().join("runtime"),
        };
        let registry = Arc::new(ProviderRegistry::new(vec![Arc::new(
            FakeProviderFactory::new(fake.clone()),
        )]));
        let engine = Engine::start(
            config,
            Arc::new(Store::open_in_memory().unwrap()),
            registry,
            Arc::new(MemorySecretStore::new()),
            paths.clone(),
        )
        .await
        .unwrap();

        // start returned without yielding to the tier tasks, so nothing has identified yet.
        assert_eq!(fake.identify_calls(), 0);
        assert_eq!(
            engine.scheduler().statuses()[0].auth_state,
            AuthState::Unknown
        );

        assert_eq!(engine.paths(), &paths);
        assert_eq!(engine.config().connections.len(), 1);
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        assert_eq!(fake.identify_calls(), 1);
        let statuses = engine.scheduler().statuses();
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].auth_state, AuthState::Ok);
        assert_eq!(statuses[0].secret_label, "Fake secret");
        engine.shutdown().await;
    }

    fn stream_info(paths: &Paths) -> serde_json::Value {
        let body = std::fs::read_to_string(paths.runtime.join("stream.json")).unwrap();
        serde_json::from_str(&body).unwrap()
    }

    async fn start_with_stream(
        config: Config,
        fake: &FakeProvider,
        dir: &tempfile::TempDir,
    ) -> (Engine, Paths) {
        let paths = Paths {
            home: dir.path().join("home"),
            runtime: dir.path().join("runtime"),
        };
        let engine = Engine::start(
            config,
            Arc::new(Store::open_in_memory().unwrap()),
            Arc::new(ProviderRegistry::new(vec![Arc::new(
                FakeProviderFactory::new(fake.clone()),
            )])),
            Arc::new(MemorySecretStore::new()),
            paths.clone(),
        )
        .await
        .unwrap();
        (engine, paths)
    }

    async fn eventually(mut done: impl FnMut() -> bool) {
        for _ in 0..100 {
            if done() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("condition not met within 5s");
    }

    #[tokio::test]
    async fn stream_file_exists_while_running_and_is_removed_on_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, paths) =
            start_with_stream(config_of(&[("c1", "fake", "1")]), &fake(), &dir).await;
        assert!(stream_info(&paths)["port"].as_u64().unwrap() > 0);
        engine.shutdown().await;
        assert!(!paths.runtime.join("stream.json").exists());
    }

    #[tokio::test]
    async fn stream_credentials_changed_reaches_the_scheduler() {
        use futures_util::SinkExt;
        use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};

        let dir = tempfile::tempdir().unwrap();
        let fake = fake();
        let (engine, paths) =
            start_with_stream(config_of(&[("c1", "fake", "1")]), &fake, &dir).await;
        eventually(|| fake.poll_count("s") >= 1).await;
        let identified = fake.identify_calls();

        let info = stream_info(&paths);
        let mut req = format!("ws://127.0.0.1:{}/events", info["port"])
            .into_client_request()
            .unwrap();
        req.headers_mut().insert(
            "authorization",
            format!("Bearer {}", info["token"].as_str().unwrap())
                .parse()
                .unwrap(),
        );
        let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
        ws.send(Message::text(
            r#"{"type":"credentials-changed","connectionId":"c1"}"#,
        ))
        .await
        .unwrap();

        eventually(|| fake.credentials_changed_calls() == 1 && fake.identify_calls() > identified)
            .await;
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn stream_bind_failure_is_not_fatal() {
        let blocker = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let mut config = config_of(&[("c1", "fake", "1")]);
        config.stream.port = blocker.local_addr().unwrap().port();
        let dir = tempfile::tempdir().unwrap();
        let fake = fake();
        let (engine, paths) = start_with_stream(config, &fake, &dir).await;

        eventually(|| fake.poll_count("s") >= 1).await;
        assert!(!paths.runtime.join("stream.json").exists());
        engine.shutdown().await;
    }

    async fn can_connect(port: u64, token: &str) -> bool {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut req = format!("ws://127.0.0.1:{port}/events")
            .into_client_request()
            .unwrap();
        req.headers_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        tokio_tungstenite::connect_async(req).await.is_ok()
    }

    #[tokio::test]
    async fn reload_with_changed_port_moves_the_stream() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, paths) =
            start_with_stream(config_of(&[("c1", "fake", "1")]), &fake(), &dir).await;
        let old = stream_info(&paths);
        let old_token = old["token"].as_str().unwrap().to_string();

        let free = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let new_port = u64::from(free.local_addr().unwrap().port());
        drop(free);
        let mut config = engine.config().as_ref().clone();
        config.stream.port = new_port as u16;
        engine.reload(config);

        eventually(|| {
            std::fs::read_to_string(paths.runtime.join("stream.json"))
                .ok()
                .and_then(|b| serde_json::from_str::<serde_json::Value>(&b).ok())
                .is_some_and(|v| v["port"].as_u64() == Some(new_port))
        })
        .await;
        let new = stream_info(&paths);
        let new_token = new["token"].as_str().unwrap();
        assert_ne!(new_token, old_token);
        assert!(!can_connect(old["port"].as_u64().unwrap(), &old_token).await);
        assert!(can_connect(new_port, new_token).await);
        assert!(!can_connect(new_port, &old_token).await);
        engine.shutdown().await;
        assert!(!paths.runtime.join("stream.json").exists());
    }

    #[tokio::test]
    async fn reload_with_unchanged_stream_keeps_the_server() {
        let dir = tempfile::tempdir().unwrap();
        let fake = fake();
        let (engine, paths) =
            start_with_stream(config_of(&[("c1", "fake", "1")]), &fake, &dir).await;
        eventually(|| fake.identify_calls() >= 1).await;
        let before = stream_info(&paths);

        // The connection change is applied before the stream check in the same worker pass.
        engine.reload(config_of(&[("c1", "fake", "2")]));
        eventually(|| fake.identify_calls() >= 2).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(stream_info(&paths), before);
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn reload_with_failed_bind_leaves_the_engine_running_without_a_stream() {
        let dir = tempfile::tempdir().unwrap();
        let fake = fake();
        let (engine, paths) =
            start_with_stream(config_of(&[("c1", "fake", "1")]), &fake, &dir).await;
        let blocker = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let mut config = engine.config().as_ref().clone();
        config.stream.port = blocker.local_addr().unwrap().port();
        engine.reload(config);

        eventually(|| !paths.runtime.join("stream.json").exists()).await;
        assert!(engine.stream.lock().await.server.is_none());
        engine.shutdown().await;
    }

    fn t0() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp(1_800_000_000, 0).unwrap()
    }

    async fn pass(s: u64) {
        tokio::time::sleep(Duration::from_secs(s)).await;
    }

    /// `conns` is (id, kind, extra setting); the extra setting makes a config differ.
    fn config_of(conns: &[(&str, &str, &str)]) -> Config {
        let mut config = Config::from_toml_str(Config::default_toml()).unwrap();
        let toml: String = conns
            .iter()
            .map(|(id, kind, extra)| {
                format!(
                    "[[connections]]\nid = \"{id}\"\nprovider = \"{kind}\"\nauth = {{ method = \"pat\" }}\nextra = \"{extra}\"\n"
                )
            })
            .collect();
        config.connections = toml::from_str::<Config>(&toml).unwrap().connections;
        config.polling.fast_secs = 10;
        config.polling.pr_secs = 20;
        config.polling.slow_secs = 1000;
        config
    }

    /// A provider with one fast source "s".
    fn fake() -> FakeProvider {
        let fake = FakeProvider::new();
        fake.add_source("s", Tier::Fast);
        fake.set_server_time(t0());
        fake
    }

    async fn engine_with(fakes: &[(&'static str, &FakeProvider)], config: Config) -> Engine {
        let factories = fakes
            .iter()
            .map(|(kind, f)| {
                Arc::new(FakeProviderFactory::with_kind((*f).clone(), kind))
                    as Arc<dyn ProviderFactory>
            })
            .collect();
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths {
            home: dir.path().join("home"),
            runtime: dir.path().join("runtime"),
        };
        Engine::start(
            config,
            Arc::new(Store::open_in_memory().unwrap()),
            Arc::new(ProviderRegistry::new(factories)),
            Arc::new(MemorySecretStore::new()),
            paths,
        )
        .await
        .unwrap()
    }

    fn ids(engine: &Engine) -> Vec<String> {
        engine.connections().into_iter().map(|s| s.id).collect()
    }

    #[tokio::test(start_paused = true)]
    async fn reload_removes_adds_and_restarts_only_changed_connections() {
        let (a, b, c, d) = (fake(), fake(), fake(), fake());
        let engine = engine_with(
            &[("ka", &a), ("kb", &b), ("kc", &c), ("kd", &d)],
            config_of(&[("c1", "ka", "1"), ("c2", "kb", "1"), ("c4", "kd", "1")]),
        )
        .await;
        pass(25).await;
        let old_c1 = engine.scheduler().connection("c1").unwrap();
        let old_c2 = engine.scheduler().connection("c2").unwrap();
        let (a_polls, b_polls, d_polls) = (a.poll_count("s"), b.poll_count("s"), d.poll_count("s"));
        assert!(a_polls >= 3 && d_polls >= 3);

        engine.reload(config_of(&[
            ("c1", "ka", "1"),
            ("c2", "kb", "2"),
            ("c3", "kc", "1"),
        ]));
        pass(1).await;

        assert_eq!(ids(&engine), ["c1", "c2", "c3"]);
        assert!(Arc::ptr_eq(
            &old_c1,
            &engine.scheduler().connection("c1").unwrap()
        ));
        assert!(!Arc::ptr_eq(
            &old_c2,
            &engine.scheduler().connection("c2").unwrap()
        ));
        // Unchanged: no extra poll or seed. Changed: restarted and polled at once.
        assert_eq!(a.poll_count("s"), a_polls);
        assert_eq!(
            a.poll_calls("s")
                .iter()
                .filter(|p| p.watermark.is_none())
                .count(),
            1
        );
        assert_eq!(b.poll_count("s"), b_polls + 1);
        assert_eq!(c.poll_count("s"), 1);
        assert_eq!(c.poll_calls("s")[0].connection.id, "c3");
        pass(40).await;
        assert_eq!(d.poll_count("s"), d_polls);
        assert!(a.poll_count("s") > a_polls);
        engine.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn reload_stops_a_disabled_connection_and_relaunches_it_when_enabled() {
        let a = fake();
        let engine = engine_with(&[("ka", &a)], config_of(&[("c1", "ka", "1")])).await;
        pass(5).await;
        let mut off = config_of(&[("c1", "ka", "1")]);
        off.connections[0].enabled = false;
        engine.reload(off);
        pass(1).await;
        let polls = a.poll_count("s");
        assert!(engine.connections().is_empty());
        pass(50).await;
        assert_eq!(a.poll_count("s"), polls);

        engine.reload(config_of(&[("c1", "ka", "1")]));
        pass(1).await;
        assert_eq!(a.poll_count("s"), polls + 1);
        engine.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn reload_reenables_a_forbidden_source() {
        let a = fake();
        a.push_poll("s", Err(ProviderError::Forbidden("no".into())));
        let engine = engine_with(&[("ka", &a)], config_of(&[("c1", "ka", "1")])).await;
        pass(50).await;
        assert_eq!(a.poll_count("s"), 1);
        assert_eq!(engine.connections()[0].disabled_sources, ["s"]);

        engine.reload(config_of(&[("c1", "ka", "1")]));
        pass(1).await;
        assert!(engine.connections()[0].disabled_sources.is_empty());
        assert_eq!(a.poll_count("s"), 2);
        pass(20).await;
        assert!(a.poll_count("s") >= 4);
        engine.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn credentials_changed_resumes_polling_and_reidentifies() {
        let a = fake();
        a.push_poll("s", Err(ProviderError::Unauthorized));
        let engine = engine_with(&[("ka", &a)], config_of(&[("c1", "ka", "1")])).await;
        let mut events = engine.bus().subscribe();
        pass(5).await;
        assert_eq!(engine.connections()[0].auth_state, AuthState::Invalid);
        assert_eq!(a.poll_count("s"), 1);
        assert_eq!(a.identify_calls(), 1);
        pass(60).await;
        assert_eq!(a.poll_count("s"), 1);

        a.push_poll("s", Ok(Default::default()));
        a.push_poll("s", Err(ProviderError::Unauthorized));
        engine.credentials_changed("c1");
        pass(1).await;
        assert_eq!(a.credentials_changed_calls(), 1);
        assert_eq!(a.identify_calls(), 2);
        assert_eq!(a.poll_count("s"), 2);
        assert_eq!(engine.connections()[0].auth_state, AuthState::Ok);

        // The next 401 is a new Ok -> Invalid transition: one more auth.failed, then paused.
        pass(10).await;
        assert_eq!(a.poll_count("s"), 3);
        pass(100).await;
        assert_eq!(a.poll_count("s"), 3);
        let mut failed = 0;
        while let Ok(e) = events.try_recv() {
            failed += usize::from(e.event_type == event_types::AUTH_FAILED);
        }
        assert_eq!(failed, 2);
        engine.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn status_changes_fire_on_auth_transitions_and_reload() {
        let a = fake();
        a.push_poll("s", Err(ProviderError::Unauthorized));
        let b = fake();
        let engine = engine_with(&[("ka", &a), ("kb", &b)], config_of(&[("c1", "ka", "1")])).await;
        let mut rx = engine.status_changes();
        rx.mark_unchanged();
        pass(1).await;
        assert!(rx.has_changed().unwrap());
        assert_eq!(rx.borrow_and_update()[0].auth_state, AuthState::Invalid);

        engine.reload(config_of(&[("c1", "ka", "1"), ("c2", "kb", "1")]));
        pass(1).await;
        assert!(rx.has_changed().unwrap());
        let ids: Vec<String> = rx
            .borrow_and_update()
            .iter()
            .map(|s| s.id.clone())
            .collect();
        assert_eq!(ids, ["c1", "c2"]);
        engine.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_connection_identifies_without_changing_state() {
        let a = fake();
        let engine = engine_with(&[("ka", &a)], config_of(&[("c1", "ka", "1")])).await;
        pass(1).await;
        let before = engine.connections();
        let me = engine.test_connection("c1").await.unwrap();
        assert_eq!(me.id, "me");
        assert_eq!(engine.connections(), before);
        assert_eq!(
            engine.test_connection("nope").await,
            Err(ProviderError::Config("unknown connection".to_string()))
        );
        a.set_identify_error(Some(ProviderError::MissingCredentials));
        assert_eq!(
            engine.test_connection("c1").await,
            Err(ProviderError::MissingCredentials)
        );
        engine.shutdown().await;
    }

    /// Delegates to the fake factory but reads the connection's secret at connect time.
    struct SecretProbe {
        inner: FakeProviderFactory,
        seen: Arc<std::sync::Mutex<Vec<Option<String>>>>,
    }

    impl ProviderFactory for SecretProbe {
        fn kind(&self) -> &'static str {
            self.inner.kind()
        }
        fn secret_label(&self) -> &'static str {
            self.inner.secret_label()
        }
        fn display_name(&self) -> &'static str {
            self.inner.display_name()
        }
        fn connection_fields(&self) -> Vec<crate::provider::FieldSpec> {
            self.inner.connection_fields()
        }
        fn auth_methods(&self) -> Vec<crate::provider::AuthMethodSpec> {
            self.inner.auth_methods()
        }
        fn default_bots(&self) -> Vec<String> {
            self.inner.default_bots()
        }
        fn validate(&self, cfg: &ConnectionConfig) -> Vec<crate::config::ConfigError> {
            self.inner.validate(cfg)
        }
        fn connect(
            &self,
            cfg: &ConnectionConfig,
            secrets: Arc<dyn SecretStore>,
        ) -> Result<Arc<dyn crate::provider::Connection>, ProviderError> {
            let secret = secrets.get(&cfg.id).unwrap();
            self.seen.lock().unwrap().push(secret.clone());
            // Only the draft's store is probed: the scheduler's own connections get the real one.
            if cfg.id == "d1" {
                assert!(secrets.set(&cfg.id, "leak").is_err());
                assert!(secrets.delete(&cfg.id).is_err());
            }
            if secret.is_none() {
                return Err(ProviderError::MissingCredentials);
            }
            self.inner.connect(cfg, secrets)
        }
        fn parse_remote(&self, url: &str) -> Option<crate::provider::RemoteRepo> {
            self.inner.parse_remote(url)
        }
        fn sources(&self) -> Vec<crate::provider::SourceInfo> {
            self.inner.sources()
        }
    }

    /// Records every call that reaches it.
    #[derive(Default)]
    struct RecordingStore {
        values: std::sync::Mutex<BTreeMap<String, String>>,
        writes: std::sync::Mutex<Vec<String>>,
    }

    impl SecretStore for RecordingStore {
        fn get(&self, key: &str) -> Result<Option<String>, crate::secrets::SecretError> {
            Ok(self.values.lock().unwrap().get(key).cloned())
        }
        fn set(&self, key: &str, _value: &str) -> Result<(), crate::secrets::SecretError> {
            self.writes.lock().unwrap().push(format!("set {key}"));
            Ok(())
        }
        fn delete(&self, key: &str) -> Result<(), crate::secrets::SecretError> {
            self.writes.lock().unwrap().push(format!("delete {key}"));
            Ok(())
        }
        fn is_native(&self) -> bool {
            false
        }
    }

    struct DraftHarness {
        engine: Engine,
        fake: FakeProvider,
        seen: Arc<std::sync::Mutex<Vec<Option<String>>>>,
        store: Arc<RecordingStore>,
        draft: ConnectionConfig,
    }

    async fn draft_harness(stored: Option<&str>) -> DraftHarness {
        let fake = fake();
        let seen = Arc::new(std::sync::Mutex::default());
        let store = Arc::new(RecordingStore::default());
        if let Some(v) = stored {
            store.values.lock().unwrap().insert("d1".into(), v.into());
        }
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths {
            home: dir.path().join("home"),
            runtime: dir.path().join("runtime"),
        };
        let engine = Engine::start(
            config_of(&[("c1", "ka", "1")]),
            Arc::new(Store::open_in_memory().unwrap()),
            Arc::new(ProviderRegistry::new(vec![Arc::new(SecretProbe {
                inner: FakeProviderFactory::with_kind(fake.clone(), "ka"),
                seen: seen.clone(),
            })])),
            store.clone(),
            paths,
        )
        .await
        .unwrap();
        let draft = config_of(&[("d1", "ka", "x")]).connections.remove(0);
        DraftHarness {
            engine,
            fake,
            seen,
            store,
            draft,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_draft_uses_override_secret_and_writes_nothing() {
        let h = draft_harness(Some("stored")).await;
        pass(1).await;
        h.seen.lock().unwrap().clear();
        let before = h.engine.connections();
        let me = h
            .engine
            .test_draft(h.draft.clone(), Some("typed".into()), None)
            .await
            .unwrap();
        assert_eq!(me.id, "me");
        assert_eq!(*h.seen.lock().unwrap(), [Some("typed".to_string())]);
        assert!(h.store.writes.lock().unwrap().is_empty());
        assert_eq!(h.store.get("d1").unwrap().as_deref(), Some("stored"));
        assert_eq!(h.engine.connections(), before);
        assert_eq!(h.fake.credentials_changed_calls(), 0);
        h.engine.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_draft_without_override_falls_back_to_stored_secret() {
        let h = draft_harness(Some("stored")).await;
        h.seen.lock().unwrap().clear();
        h.engine
            .test_draft(h.draft.clone(), None, Some("d1"))
            .await
            .unwrap();
        assert_eq!(*h.seen.lock().unwrap(), [Some("stored".to_string())]);
        h.engine.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_draft_never_uses_another_connections_stored_secret() {
        let h = draft_harness(Some("stored")).await;
        h.seen.lock().unwrap().clear();
        assert_eq!(
            h.engine.test_draft(h.draft.clone(), None, None).await,
            Err(ProviderError::MissingCredentials)
        );
        assert_eq!(
            h.engine
                .test_draft(h.draft.clone(), None, Some("other"))
                .await,
            Err(ProviderError::MissingCredentials)
        );
        assert_eq!(*h.seen.lock().unwrap(), [None, None]);
        h.engine.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_draft_rename_uses_the_original_ids_stored_secret() {
        let h = draft_harness(Some("stored")).await;
        h.seen.lock().unwrap().clear();
        let mut cfg = h.draft.clone();
        cfg.id = "d1".into();
        h.store
            .values
            .lock()
            .unwrap()
            .insert("old".into(), "old-secret".into());
        h.engine.test_draft(cfg, None, Some("old")).await.unwrap();
        assert_eq!(*h.seen.lock().unwrap(), [Some("old-secret".to_string())]);
        h.engine.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn list_draft_scopes_returns_scripted_options_with_the_secret_rule() {
        let h = draft_harness(Some("stored")).await;
        let opts = vec![ScopeOption {
            id: "a".into(),
            name: "A".into(),
        }];
        h.fake.set_scopes("projects", Ok(opts.clone()));
        h.seen.lock().unwrap().clear();
        let got = h
            .engine
            .list_draft_scopes(h.draft.clone(), None, Some("d1"), "projects")
            .await
            .unwrap();
        assert_eq!(got, opts);
        assert_eq!(*h.seen.lock().unwrap(), [Some("stored".to_string())]);
        assert_eq!(h.fake.scope_calls(), ["projects"]);
        assert!(h.store.writes.lock().unwrap().is_empty());
        assert_eq!(
            h.engine
                .list_draft_scopes(h.draft.clone(), Some("t".into()), None, "other")
                .await,
            Err(ProviderError::Config("not supported".into()))
        );
        assert_eq!(
            h.engine
                .list_draft_scopes(h.draft.clone(), None, None, "projects")
                .await,
            Err(ProviderError::MissingCredentials)
        );
        h.engine.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_draft_without_any_secret_reports_missing_credentials() {
        let h = draft_harness(None).await;
        assert_eq!(
            h.engine.test_draft(h.draft.clone(), None, Some("d1")).await,
            Err(ProviderError::MissingCredentials)
        );
        h.engine.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_draft_unknown_provider_is_a_config_error() {
        let h = draft_harness(None).await;
        let mut cfg = h.draft.clone();
        cfg.provider = "nope".into();
        assert!(matches!(
            h.engine.test_draft(cfg, Some("s".into()), None).await,
            Err(ProviderError::Config(_))
        ));
        h.engine.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn poll_now_polls_every_connection_immediately() {
        let (a, b) = (fake(), fake());
        let engine = engine_with(
            &[("ka", &a), ("kb", &b)],
            config_of(&[("c1", "ka", "1"), ("c2", "kb", "1")]),
        )
        .await;
        pass(3).await;
        assert_eq!((a.poll_count("s"), b.poll_count("s")), (1, 1));
        engine.poll_now();
        pass(1).await;
        assert_eq!((a.poll_count("s"), b.poll_count("s")), (2, 2));
        engine.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_right_after_a_restarting_reload_leaves_no_orphan_tasks() {
        let a = fake();
        let engine = engine_with(&[("ka", &a)], config_of(&[("c1", "ka", "1")])).await;
        pass(5).await;
        engine.reload(config_of(&[("c1", "ka", "2")]));
        engine.shutdown().await;
        let polls = a.poll_count("s");
        pass(100).await;
        assert_eq!(a.poll_count("s"), polls);
    }

    #[tokio::test(start_paused = true)]
    async fn back_to_back_reloads_apply_in_order_and_leave_one_task_set() {
        let a = fake();
        let engine = engine_with(&[("ka", &a)], config_of(&[("c1", "ka", "1")])).await;
        pass(5).await;
        engine.reload(config_of(&[("c1", "ka", "2")]));
        engine.reload(config_of(&[("c1", "ka", "3")]));
        pass(1).await;

        assert_eq!(ids(&engine), ["c1"]);
        let state = engine.scheduler().connection("c1").unwrap();
        assert_eq!(
            state.config.settings.get("extra"),
            Some(&toml::Value::String("3".into()))
        );
        let before = a.poll_count("s");
        pass(100).await;
        let polled = a.poll_count("s") - before;
        assert!((9..=11).contains(&polled), "polled {polled} times");
        engine.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn reload_rebuilds_a_halted_connection_with_unchanged_config() {
        let a = fake();
        a.set_identify_error(Some(ProviderError::Config("bad".into())));
        let engine = engine_with(&[("ka", &a)], config_of(&[("c1", "ka", "1")])).await;
        pass(30).await;
        assert!(engine.scheduler().needs_rebuild("c1"));
        assert_eq!(a.poll_count("s"), 0);

        a.set_identify_error(None);
        engine.reload(config_of(&[("c1", "ka", "1")]));
        pass(1).await;
        assert!(!engine.scheduler().needs_rebuild("c1"));
        assert_eq!(a.poll_count("s"), 1);
        engine.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn credentials_changed_for_an_unknown_id_is_a_no_op() {
        let a = fake();
        let engine = engine_with(&[("ka", &a)], config_of(&[("c1", "ka", "1")])).await;
        pass(1).await;
        let identifies = a.identify_calls();
        engine.credentials_changed("nope");
        pass(1).await;
        assert_eq!(a.credentials_changed_calls(), 0);
        assert_eq!(a.identify_calls(), identifies);
        assert_eq!(ids(&engine), ["c1"]);
        engine.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn stale_unauthorized_after_credentials_changed_is_ignored() {
        let a = fake();
        let engine = engine_with(&[("ka", &a)], config_of(&[("c1", "ka", "1")])).await;
        let mut events = engine.bus().subscribe();
        pass(1).await;
        a.push_poll("s", Err(ProviderError::Unauthorized));
        a.hold_polls("s");
        pass(15).await;
        assert_eq!(a.poll_count("s"), 2);

        engine.credentials_changed("c1");
        a.release_polls("s");
        pass(1).await;

        assert_ne!(engine.connections()[0].auth_state, AuthState::Invalid);
        assert!(engine.scheduler().connection("c1").unwrap().gate_open());
        while let Ok(e) = events.try_recv() {
            assert_ne!(e.event_type, event_types::AUTH_FAILED);
        }
        engine.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn reload_publishes_one_status_update_for_the_whole_reload() {
        let a = fake();
        let engine = engine_with(&[("ka", &a)], config_of(&[("c1", "ka", "1")])).await;
        pass(1).await;
        let mut rx = engine.status_changes();
        rx.mark_unchanged();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observer = {
            let seen = seen.clone();
            tokio::spawn(async move {
                while rx.changed().await.is_ok() {
                    let ids: Vec<String> = rx
                        .borrow_and_update()
                        .iter()
                        .map(|s| s.id.clone())
                        .collect();
                    seen.lock().unwrap().push(ids);
                }
            })
        };
        engine.reload(config_of(&[("c1", "ka", "2")]));
        pass(1).await;
        observer.abort();
        let seen = seen.lock().unwrap().clone();
        assert!(!seen.is_empty());
        assert!(seen.iter().all(|ids| ids == &["c1"]), "{seen:?}");
        engine.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn poll_now_keeps_a_live_auth_pause_but_ends_credentials_idle() {
        let a = fake();
        a.push_poll("s", Err(ProviderError::Unauthorized));
        let engine = engine_with(&[("ka", &a)], config_of(&[("c1", "ka", "1")])).await;
        pass(5).await;
        assert_eq!(engine.connections()[0].auth_state, AuthState::Invalid);
        engine.poll_now();
        pass(1).await;
        assert_eq!(a.poll_count("s"), 1);
        assert!(!engine.scheduler().connection("c1").unwrap().gate_open());
        engine.shutdown().await;

        let b = fake();
        b.set_identify_error(Some(ProviderError::MissingCredentials));
        let engine = engine_with(&[("ka", &b)], config_of(&[("c1", "ka", "1")])).await;
        pass(5).await;
        assert_eq!(engine.connections()[0].auth_state, AuthState::Missing);
        b.set_identify_error(None);
        engine.poll_now();
        pass(1).await;
        assert_eq!(b.poll_count("s"), 1);
        engine.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_connection_tells_unconnected_from_unknown() {
        let a = fake();
        let engine = engine_with(&[("ka", &a)], config_of(&[("c1", "nokind", "1")])).await;
        pass(1).await;
        assert_eq!(
            engine.test_connection("c1").await,
            Err(ProviderError::Config(
                "connection not connected".to_string()
            ))
        );
        assert_eq!(
            engine.test_connection("nope").await,
            Err(ProviderError::Config("unknown connection".to_string()))
        );
        engine.shutdown().await;
    }
}
