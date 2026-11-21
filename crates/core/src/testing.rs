//! Scripted fake provider for engine, scheduler and source tests.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;

use crate::config::{Config, ConfigError, ConnectionConfig};
use crate::event::ConnectionRef;
use crate::provider::{
    Connection, Identity, PollOutcome, ProviderError, ProviderFactory, RemoteRepo, ScopedSnapshots,
    Source, SyncContext, Tier,
};
use crate::secrets::SecretStore;
use crate::store::Store;

type PollResult = Result<PollOutcome, ProviderError>;

struct SourceScript {
    name: &'static str,
    tier: Tier,
    queue: VecDeque<PollResult>,
    calls: Vec<SyncContext>,
    hold: tokio::sync::watch::Sender<bool>,
}

struct State {
    sources: Vec<SourceScript>,
    server_time: DateTime<Utc>,
    server_time_error: Option<ProviderError>,
    server_time_queue: VecDeque<Result<DateTime<Utc>, ProviderError>>,
    server_time_calls: usize,
    identity: Identity,
    identify_error: Option<ProviderError>,
    identify_queue: VecDeque<Result<Identity, ProviderError>>,
    identify_calls: usize,
    credentials_changed_calls: usize,
    validate_errors: Vec<ConfigError>,
    connect_error: Option<ProviderError>,
}

/// Cloneable handle onto the shared script of a fake provider. Every factory,
/// connection and source built from one handle sees the same state.
#[derive(Clone)]
pub struct FakeProvider {
    state: Arc<Mutex<State>>,
}

impl Default for FakeProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeProvider {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                sources: Vec::new(),
                server_time: DateTime::<Utc>::UNIX_EPOCH,
                server_time_error: None,
                server_time_queue: VecDeque::new(),
                server_time_calls: 0,
                identity: Identity {
                    id: "me".into(),
                    display_name: "Me".into(),
                    unique_name: "me@example.test".into(),
                    group_ids: Vec::new(),
                },
                identify_error: None,
                identify_queue: VecDeque::new(),
                identify_calls: 0,
                credentials_changed_calls: 0,
                validate_errors: Vec::new(),
                connect_error: None,
            })),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn with_source<R>(&self, name: &str, f: impl FnOnce(&mut SourceScript) -> R) -> Option<R> {
        self.lock()
            .sources
            .iter_mut()
            .find(|s| s.name == name)
            .map(f)
    }

    /// Registers a source; connections return sources in registration order.
    pub fn add_source(&self, name: &'static str, tier: Tier) {
        self.lock().sources.push(SourceScript {
            name,
            tier,
            queue: VecDeque::new(),
            calls: Vec::new(),
            hold: tokio::sync::watch::channel(false).0,
        });
    }

    /// Queues the result of the next unconsumed poll of `source`. An empty queue
    /// yields `Ok(PollOutcome::default())`. Panics if the source is not registered.
    pub fn push_poll(&self, source: &str, result: PollResult) {
        let found = self.with_source(source, |s| s.queue.push_back(result));
        assert!(found.is_some(), "fake source '{source}' not registered");
    }

    /// Makes polls of `source` wait, after recording their context, until
    /// `release_polls`. Panics if the source is not registered.
    pub fn hold_polls(&self, source: &str) {
        let found = self.with_source(source, |s| s.hold.send_replace(true));
        assert!(found.is_some(), "fake source '{source}' not registered");
    }

    /// Releases held polls of `source` and stops holding new ones.
    pub fn release_polls(&self, source: &str) {
        let found = self.with_source(source, |s| s.hold.send_replace(false));
        assert!(found.is_some(), "fake source '{source}' not registered");
    }

    /// Contexts received by `source`, oldest first.
    pub fn poll_calls(&self, source: &str) -> Vec<SyncContext> {
        self.with_source(source, |s| s.calls.clone())
            .unwrap_or_default()
    }

    pub fn poll_count(&self, source: &str) -> usize {
        self.with_source(source, |s| s.calls.len()).unwrap_or(0)
    }

    /// Sticky server time returned once the one-shot queue is empty.
    pub fn set_server_time(&self, t: DateTime<Utc>) {
        self.lock().server_time = t;
    }

    /// Sticky error returned by `server_time` (None clears it).
    pub fn set_server_time_error(&self, e: Option<ProviderError>) {
        self.lock().server_time_error = e;
    }

    /// One-shot `server_time` result, consumed before the sticky value. The queue
    /// is shared across concurrently polling tiers, so multi-source tests should
    /// prefer `set_server_time` / `set_server_time_error`.
    pub fn push_server_time(&self, r: Result<DateTime<Utc>, ProviderError>) {
        self.lock().server_time_queue.push_back(r);
    }

    pub fn server_time_calls(&self) -> usize {
        self.lock().server_time_calls
    }

    pub fn set_identity(&self, me: Identity) {
        self.lock().identity = me;
    }

    /// Sticky error returned by `identify` (None clears it).
    pub fn set_identify_error(&self, e: Option<ProviderError>) {
        self.lock().identify_error = e;
    }

    /// One-shot `identify` result, consumed before the sticky value.
    pub fn push_identify(&self, r: Result<Identity, ProviderError>) {
        self.lock().identify_queue.push_back(r);
    }

    pub fn identify_calls(&self) -> usize {
        self.lock().identify_calls
    }

    pub fn credentials_changed_calls(&self) -> usize {
        self.lock().credentials_changed_calls
    }

    /// Errors returned by the factory's `validate`.
    pub fn set_validate_errors(&self, errors: Vec<ConfigError>) {
        self.lock().validate_errors = errors;
    }

    /// Makes the factory's `connect` fail (None clears it).
    pub fn set_connect_error(&self, e: Option<ProviderError>) {
        self.lock().connect_error = e;
    }
}

/// Factory of kind "fake" unless built with `with_kind`.
#[derive(Clone)]
pub struct FakeProviderFactory {
    provider: FakeProvider,
    kind: &'static str,
}

impl FakeProviderFactory {
    pub fn new(provider: FakeProvider) -> Self {
        Self::with_kind(provider, "fake")
    }

    /// Lets several independently scripted fakes share one registry.
    pub fn with_kind(provider: FakeProvider, kind: &'static str) -> Self {
        Self { provider, kind }
    }

    pub fn provider(&self) -> &FakeProvider {
        &self.provider
    }
}

impl ProviderFactory for FakeProviderFactory {
    fn kind(&self) -> &'static str {
        self.kind
    }

    fn secret_label(&self) -> &'static str {
        "Fake secret"
    }

    fn validate(&self, _cfg: &ConnectionConfig) -> Vec<ConfigError> {
        self.provider.lock().validate_errors.clone()
    }

    fn connect(
        &self,
        cfg: &ConnectionConfig,
        _secrets: Arc<dyn SecretStore>,
    ) -> Result<Arc<dyn Connection>, ProviderError> {
        if let Some(e) = self.provider.lock().connect_error.clone() {
            return Err(e);
        }
        Ok(Arc::new(FakeConnection {
            provider: self.provider.clone(),
            kind: self.kind,
            id: cfg.id.clone(),
            auth_method: cfg.auth.method.clone(),
        }))
    }

    /// Parses `fake://{host}/{namespace}/{repo}`.
    fn parse_remote(&self, git_remote_url: &str) -> Option<RemoteRepo> {
        let rest = git_remote_url.strip_prefix("fake://")?;
        let parts: Vec<&str> = rest.split('/').collect();
        match parts.as_slice() {
            [host, namespace, repo] => Some(RemoteRepo {
                provider: "fake".into(),
                host_key: (*host).into(),
                namespace: Some((*namespace).into()),
                repo_name: (*repo).into(),
            }),
            _ => None,
        }
    }
}

pub struct FakeConnection {
    provider: FakeProvider,
    kind: &'static str,
    id: String,
    auth_method: String,
}

impl Connection for FakeConnection {
    fn id(&self) -> &str {
        &self.id
    }

    fn provider(&self) -> &'static str {
        self.kind
    }

    fn auth_method(&self) -> &str {
        &self.auth_method
    }

    fn identify(&self) -> BoxFuture<'_, Result<Identity, ProviderError>> {
        let mut s = self.provider.lock();
        s.identify_calls += 1;
        let result = match s.identify_queue.pop_front() {
            Some(r) => r,
            None => match s.identify_error.clone() {
                Some(e) => Err(e),
                None => Ok(s.identity.clone()),
            },
        };
        drop(s);
        Box::pin(async move { result })
    }

    fn server_time(&self) -> BoxFuture<'_, Result<DateTime<Utc>, ProviderError>> {
        let mut s = self.provider.lock();
        s.server_time_calls += 1;
        let result = match s.server_time_queue.pop_front() {
            Some(r) => r,
            None => match s.server_time_error.clone() {
                Some(e) => Err(e),
                None => Ok(s.server_time),
            },
        };
        drop(s);
        Box::pin(async move { result })
    }

    fn sources(&self) -> Vec<Box<dyn Source>> {
        self.provider
            .lock()
            .sources
            .iter()
            .map(|s| {
                Box::new(FakeSource {
                    provider: self.provider.clone(),
                    name: s.name,
                    tier: s.tier,
                }) as Box<dyn Source>
            })
            .collect()
    }

    fn credentials_changed(&self) {
        self.provider.lock().credentials_changed_calls += 1;
    }
}

pub struct FakeSource {
    provider: FakeProvider,
    name: &'static str,
    tier: Tier,
}

impl Source for FakeSource {
    fn name(&self) -> &'static str {
        self.name
    }

    fn tier(&self) -> Tier {
        self.tier
    }

    fn poll<'a>(
        &'a mut self,
        ctx: &'a SyncContext,
    ) -> BoxFuture<'a, Result<PollOutcome, ProviderError>> {
        let mut hold = self.provider.with_source(self.name, |s| {
            s.calls.push(ctx.clone());
            s.hold.subscribe()
        });
        let provider = self.provider.clone();
        let name = self.name;
        Box::pin(async move {
            if let Some(hold) = hold.as_mut() {
                // Err only when the sender is gone, which ends the hold.
                let _ = hold.wait_for(|held| !*held).await;
            }
            provider
                .with_source(name, |s| s.queue.pop_front())
                .flatten()
                .unwrap_or_else(|| Ok(PollOutcome::default()))
        })
    }
}

/// Builds a context over the default config and an in-memory store.
pub fn sync_context(
    connection_config: ConnectionConfig,
    me: Identity,
    server_now: DateTime<Utc>,
    watermark: Option<DateTime<Utc>>,
    catch_up: bool,
) -> (SyncContext, Arc<Store>) {
    let config = Config::from_toml_str(Config::default_toml()).expect("default config template");
    let store = Arc::new(Store::open_in_memory().expect("in-memory store"));
    let ctx = SyncContext {
        connection: ConnectionRef {
            id: connection_config.id.clone(),
            provider: connection_config.provider.clone(),
        },
        me,
        config: Arc::new(config),
        snapshots: ScopedSnapshots::new(store.clone(), &connection_config.id),
        connection_config,
        server_now,
        watermark,
        catch_up,
    };
    (ctx, store)
}
