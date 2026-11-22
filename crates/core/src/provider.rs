//! Provider-neutral traits and types. Concrete providers live in their own crates.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::config::{Config, ConfigError, ConnectionConfig};
use crate::event::{ConnectionRef, Event};
use crate::secrets::SecretStore;
use crate::store::{self, Store};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Identity {
    pub id: String,
    pub display_name: String,
    pub unique_name: String,
    pub group_ids: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthState {
    Unknown,
    Ok,
    Missing,
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tier {
    Fast,
    Pr,
    Slow,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProviderError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("forbidden: {0}")]
    Forbidden(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("throttled, retry after {retry_after:?}")]
    Throttled { retry_after: Duration },
    #[error("missing credentials")]
    MissingCredentials,
    #[error("transient error: {0}")]
    Transient(String),
    #[error("configuration error: {0}")]
    Config(String),
    #[error("{0}")]
    Other(String),
}

impl From<store::StoreError> for ProviderError {
    fn from(e: store::StoreError) -> Self {
        ProviderError::Other(e.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteRepo {
    pub provider: String,
    pub host_key: String,
    pub namespace: Option<String>,
    pub repo_name: String,
}

/// Snapshot access namespaced to one connection ("{connId}:{key}").
#[derive(Clone)]
pub struct ScopedSnapshots {
    store: Arc<dyn Store>,
    prefix: String,
}

impl ScopedSnapshots {
    pub fn new(store: Arc<dyn Store>, connection_id: &str) -> Self {
        Self {
            store,
            prefix: format!("{connection_id}:"),
        }
    }

    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> store::Result<Option<T>> {
        self.store.get_snapshot(&self.scoped(key)).await
    }

    pub async fn put<T: Serialize>(&self, key: &str, value: &T) -> store::Result<()> {
        self.store.put_snapshot(&self.scoped(key), value).await
    }

    fn scoped(&self, key: &str) -> String {
        format!("{}{key}", self.prefix)
    }
}

#[derive(Clone)]
pub struct SyncContext {
    pub connection: ConnectionRef,
    pub me: Identity,
    pub config: Arc<Config>,
    pub connection_config: ConnectionConfig,
    pub snapshots: ScopedSnapshots,
    pub server_now: DateTime<Utc>,
    /// `None` means the source has never synced (seed).
    pub watermark: Option<DateTime<Utc>>,
    pub catch_up: bool,
}

#[derive(Debug, Clone, Default)]
pub struct PollOutcome {
    pub events: Vec<Event>,
}

pub trait Source: Send {
    fn name(&self) -> &'static str;
    fn tier(&self) -> Tier;
    fn poll<'a>(
        &'a mut self,
        ctx: &'a SyncContext,
    ) -> BoxFuture<'a, Result<PollOutcome, ProviderError>>;
}

pub trait Connection: Send + Sync {
    fn id(&self) -> &str;
    fn provider(&self) -> &'static str;
    fn auth_method(&self) -> &str;
    /// Includes group ids.
    fn identify(&self) -> BoxFuture<'_, Result<Identity, ProviderError>>;
    fn server_time(&self) -> BoxFuture<'_, Result<DateTime<Utc>, ProviderError>>;
    fn sources(&self) -> Vec<Box<dyn Source>>;
    fn credentials_changed(&self);
    /// Options for a `FieldKind::Scope` field, listed live from the provider.
    fn list_scopes<'a>(
        &'a self,
        _field_key: &'a str,
    ) -> BoxFuture<'a, Result<Vec<ScopeOption>, ProviderError>> {
        Box::pin(async { Err(ProviderError::Config("not supported".into())) })
    }
}

pub trait ProviderFactory: Send + Sync {
    fn kind(&self) -> &'static str;
    fn secret_label(&self) -> &'static str;
    fn display_name(&self) -> &'static str;
    /// Provider-specific settings only; id, provider, enabled, auth and bots are generic.
    fn connection_fields(&self) -> Vec<FieldSpec>;
    /// Bot globs suggested for a new connection.
    fn default_bots(&self) -> Vec<String>;
    /// The first entry is the default.
    fn auth_methods(&self) -> Vec<AuthMethodSpec>;
    /// Error paths are relative to the connection (e.g. `organization`);
    /// `Config::validate_providers` prefixes them with `connections[i].`.
    fn validate(&self, cfg: &ConnectionConfig) -> Vec<ConfigError>;
    fn connect(
        &self,
        cfg: &ConnectionConfig,
        secrets: Arc<dyn SecretStore>,
    ) -> Result<Arc<dyn Connection>, ProviderError>;
    fn parse_remote(&self, git_remote_url: &str) -> Option<RemoteRepo>;
    /// Every source a connection of this provider polls. `[polling.sources]` keys are
    /// validated against these names.
    fn sources(&self) -> Vec<SourceInfo>;
    /// The `RemoteRepo::host_key` clones of this connection's repos carry, or `None`
    /// when it can't be derived (remotes are then matched without a host check).
    fn host_key(&self, _cfg: &ConnectionConfig) -> Option<String> {
        None
    }
}

/// What a factory declares about one of its sources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceInfo {
    pub name: &'static str,
    pub tier: Tier,
    pub event_types: Vec<&'static str>,
}

const BOTS_HELP: &str =
    "Activity from these accounts won't alert you. Globs on the account's unique or display name.";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderDescriptor {
    pub kind: String,
    pub display_name: String,
    pub fields: Vec<FieldSpec>,
    pub auth_methods: Vec<AuthMethodSpec>,
    pub default_bots: Vec<String>,
    pub bots_help: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeOption {
    /// The value stored in config.
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldSpec {
    pub key: String,
    pub label: String,
    pub kind: FieldKind,
    pub required: bool,
    pub placeholder: Option<String>,
    pub help: Option<String>,
    #[serde(default)]
    pub advanced: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldKind {
    Text,
    List,
    /// A list whose options come from `Connection::list_scopes`; empty means all.
    Scope,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthMethodSpec {
    pub id: String,
    pub label: String,
    pub help: Option<String>,
    pub secret: Option<SecretSpec>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretSpec {
    pub label: String,
    pub help: Option<String>,
}

#[derive(Clone)]
pub struct ProviderRegistry {
    factories: Vec<Arc<dyn ProviderFactory>>,
}

impl ProviderRegistry {
    pub fn new(factories: Vec<Arc<dyn ProviderFactory>>) -> Self {
        Self { factories }
    }

    pub fn get(&self, kind: &str) -> Option<Arc<dyn ProviderFactory>> {
        self.factories.iter().find(|f| f.kind() == kind).cloned()
    }

    pub fn all(&self) -> &[Arc<dyn ProviderFactory>] {
        &self.factories
    }

    pub fn descriptors(&self) -> Vec<ProviderDescriptor> {
        self.factories
            .iter()
            .map(|f| ProviderDescriptor {
                kind: f.kind().into(),
                display_name: f.display_name().into(),
                fields: f.connection_fields(),
                auth_methods: f.auth_methods(),
                default_bots: f.default_bots(),
                bots_help: BOTS_HELP.into(),
            })
            .collect()
    }
}

impl Config {
    /// Provider-specific validation: unknown providers and each factory's own checks.
    pub fn validate_providers(&self, registry: &ProviderRegistry) -> Vec<ConfigError> {
        let mut errors = Vec::new();
        let known: BTreeSet<&'static str> = registry
            .all()
            .iter()
            .flat_map(|f| f.sources())
            .map(|s| s.name)
            .collect();
        for name in self.polling.sources.keys() {
            if !known.contains(name.as_str()) {
                errors.push(ConfigError::new(
                    format!("polling.sources.{name}"),
                    format!(
                        "unknown source; known: {}",
                        known.iter().copied().collect::<Vec<_>>().join(", ")
                    ),
                ));
            }
        }
        for (i, conn) in self.connections.iter().enumerate() {
            if conn.provider.trim().is_empty() {
                continue;
            }
            match registry.get(&conn.provider) {
                None => errors.push(ConfigError::new(
                    format!("connections[{i}].provider"),
                    format!("unknown provider '{}'", conn.provider),
                )),
                Some(factory) => {
                    errors.extend(factory.validate(conn).into_iter().map(|e| {
                        ConfigError::new(format!("connections[{i}].{}", e.path), e.message)
                    }))
                }
            }
        }
        errors
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::SqliteStore;
    use crate::testing::{FakeProvider, FakeProviderFactory, sync_context};
    use chrono::TimeZone;

    #[test]
    fn descriptors_follow_registry_order_and_fake_shape() {
        let a = FakeProviderFactory::with_kind(FakeProvider::new(), "alpha");
        let b = FakeProviderFactory::new(FakeProvider::new());
        let reg = ProviderRegistry::new(vec![Arc::new(a), Arc::new(b)]);
        let d = reg.descriptors();
        assert_eq!(
            d.iter().map(|x| x.kind.as_str()).collect::<Vec<_>>(),
            ["alpha", "fake"]
        );
        assert_eq!(d[1].display_name, "Fake");
        assert_eq!(d[1].fields.len(), 1);
        assert_eq!(d[1].fields[0].key, "host");
        assert_eq!(d[1].fields[0].kind, FieldKind::Text);
        assert!(!d[1].fields[0].required);
        assert!(!d[1].fields[0].advanced);
        assert_eq!(d[1].default_bots, ["fake-bot*"]);
        assert!(d[1].bots_help.contains("Globs"));
        let methods: Vec<_> = d[1]
            .auth_methods
            .iter()
            .map(|m| (m.id.as_str(), m.secret.is_some()))
            .collect();
        assert_eq!(methods, [("token", true), ("none", false)]);
    }

    fn identity() -> Identity {
        Identity {
            id: "me".into(),
            display_name: "Me".into(),
            unique_name: "me@example.test".into(),
            group_ids: vec![],
        }
    }

    fn conn_cfg(id: &str, provider: &str) -> ConnectionConfig {
        ConnectionConfig {
            id: id.into(),
            provider: provider.into(),
            enabled: true,
            auth: Default::default(),
            bots: vec![],
            settings: toml::Table::new(),
        }
    }

    #[tokio::test]
    async fn scoped_snapshots_prefix_keys_per_connection() {
        let store: Arc<dyn Store> = Arc::new(SqliteStore::open_in_memory().unwrap());
        let a = ScopedSnapshots::new(store.clone(), "conn-a");
        let b = ScopedSnapshots::new(store.clone(), "conn-b");
        a.put("k", &1_i32).await.unwrap();
        b.put("k", &2_i32).await.unwrap();
        assert_eq!(a.get::<i32>("k").await.unwrap(), Some(1));
        assert_eq!(b.get::<i32>("k").await.unwrap(), Some(2));
        assert_eq!(
            store.get_snapshot::<i32>("conn-a:k").await.unwrap(),
            Some(1)
        );
        assert_eq!(a.get::<i32>("other").await.unwrap(), None);
    }

    #[test]
    fn validate_providers_flags_unknown_and_surfaces_factory_errors() {
        let provider = FakeProvider::new();
        provider.set_validate_errors(vec![ConfigError::new("settings.x", "bad x")]);
        let registry = ProviderRegistry::new(vec![Arc::new(FakeProviderFactory::new(provider))]);
        let config = Config {
            connections: vec![conn_cfg("conn-a", "fake"), conn_cfg("conn-b", "nope")],
            ..Config::default()
        };
        let errors = config.validate_providers(&registry);
        assert_eq!(
            errors,
            vec![
                ConfigError::new("connections[0].settings.x", "bad x"),
                ConfigError::new("connections[1].provider", "unknown provider 'nope'"),
            ]
        );
    }

    #[test]
    fn validate_providers_flags_unknown_polling_sources_with_the_known_list() {
        let provider = FakeProvider::new();
        provider.add_source("b_src", Tier::Fast);
        provider.add_source("a_src", Tier::Slow);
        let registry = ProviderRegistry::new(vec![Arc::new(FakeProviderFactory::new(provider))]);
        let mut config = Config::default();
        config.polling.sources.insert("a_src".into(), 30);
        config.polling.sources.insert("nope".into(), 30);
        assert_eq!(
            config.validate_providers(&registry),
            vec![ConfigError::new(
                "polling.sources.nope",
                "unknown source; known: a_src, b_src"
            )]
        );
    }

    #[test]
    fn validate_providers_skips_blank_provider() {
        let registry = ProviderRegistry::new(vec![Arc::new(FakeProviderFactory::new(
            FakeProvider::new(),
        ))]);
        let config = Config {
            connections: vec![conn_cfg("conn-a", "  ")],
            ..Config::default()
        };
        assert!(config.validate_providers(&registry).is_empty());
    }

    #[test]
    fn factories_with_different_kinds_have_independent_scripts() {
        let (a, b) = (FakeProvider::new(), FakeProvider::new());
        a.add_source("alpha", Tier::Fast);
        b.add_source("beta", Tier::Slow);
        let registry = ProviderRegistry::new(vec![
            Arc::new(FakeProviderFactory::with_kind(a, "fake-a")),
            Arc::new(FakeProviderFactory::with_kind(b, "fake-b")),
        ]);
        let secrets: Arc<dyn SecretStore> = Arc::new(crate::secrets::MemorySecretStore::default());
        let conn_a = registry
            .get("fake-a")
            .unwrap()
            .connect(&conn_cfg("ca", "fake-a"), secrets.clone())
            .unwrap();
        let conn_b = registry
            .get("fake-b")
            .unwrap()
            .connect(&conn_cfg("cb", "fake-b"), secrets)
            .unwrap();
        assert_eq!(conn_a.provider(), "fake-a");
        assert_eq!(conn_b.provider(), "fake-b");
        assert_eq!(conn_a.sources()[0].name(), "alpha");
        assert_eq!(conn_b.sources()[0].name(), "beta");
        assert!(registry.get("fake").is_none());
    }

    #[tokio::test]
    async fn held_poll_waits_for_release() {
        let provider = FakeProvider::new();
        provider.add_source("alpha", Tier::Fast);
        provider.push_poll("alpha", Err(ProviderError::Unauthorized));
        provider.hold_polls("alpha");
        let cfg = conn_cfg("conn-a", "fake");
        let conn = FakeProviderFactory::new(provider.clone())
            .connect(&cfg, Arc::new(crate::secrets::MemorySecretStore::default()))
            .unwrap();
        let mut source = conn.sources().remove(0);
        let (ctx, _store) = sync_context(cfg, identity(), chrono::Utc::now(), None, false);

        let mut poll = source.poll(&ctx);
        let held = tokio::time::timeout(Duration::from_millis(50), &mut poll).await;
        assert!(held.is_err());
        assert_eq!(provider.poll_count("alpha"), 1);

        provider.release_polls("alpha");
        let result = tokio::time::timeout(Duration::from_secs(5), poll)
            .await
            .unwrap();
        assert_eq!(result.unwrap_err(), ProviderError::Unauthorized);
    }

    #[test]
    fn registry_get_and_all() {
        let registry = ProviderRegistry::new(vec![Arc::new(FakeProviderFactory::new(
            FakeProvider::new(),
        ))]);
        assert_eq!(registry.all().len(), 1);
        assert!(registry.get("fake").is_some());
        assert!(registry.get("other").is_none());
    }

    #[tokio::test]
    async fn fake_source_returns_queued_results_and_records_contexts() {
        let provider = FakeProvider::new();
        provider.add_source("alpha", Tier::Fast);
        provider.push_poll("alpha", Err(ProviderError::Unauthorized));
        provider.push_poll("alpha", Ok(PollOutcome::default()));

        let factory = FakeProviderFactory::new(provider.clone());
        let cfg = conn_cfg("conn-a", "fake");
        let conn = factory
            .connect(&cfg, Arc::new(crate::secrets::MemorySecretStore::default()))
            .unwrap();
        let mut sources = conn.sources();
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].name(), "alpha");

        let now = chrono::Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
        let (seed_ctx, _store) = sync_context(cfg.clone(), identity(), now, None, false);
        let (catch_ctx, _store) = sync_context(cfg, identity(), now, Some(now), true);

        assert_eq!(
            sources[0].poll(&seed_ctx).await.unwrap_err(),
            ProviderError::Unauthorized
        );
        assert!(sources[0].poll(&catch_ctx).await.is_ok());
        // Queue drained: default is an empty success.
        assert!(sources[0].poll(&catch_ctx).await.unwrap().events.is_empty());

        let calls = provider.poll_calls("alpha");
        assert_eq!(provider.poll_count("alpha"), 3);
        assert_eq!(calls[0].watermark, None);
        assert!(!calls[0].catch_up);
        assert_eq!(calls[1].watermark, Some(now));
        assert!(calls[1].catch_up);
    }
}
