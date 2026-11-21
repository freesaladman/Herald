//! Tauri IPC commands and the app -> UI events they trigger.
//!
//! build.rs lists every command in the app manifest, so each window may call only what its
//! capability grants (capabilities/default.json for the pane, toast.json for the toast).

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use chrono::{DateTime, Utc};
use herald_core::config::{Config, ConfigError, ConnectionConfig};
use herald_core::config_edit::{self, ConnectionDraft};
use herald_core::engine::Engine;
use herald_core::event::{Actor, ActorKind, Event, event_types, ids};
use herald_core::provider::{
    Identity, ProviderDescriptor, ProviderError, ProviderRegistry, ScopeOption,
};
use herald_core::scheduler::ConnectionStatus;
use herald_core::secrets::SecretStore;
use serde::Serialize;
use tauri::{AppHandle, Emitter, Manager, State};
use tokio::sync::watch;

use crate::{
    CONFIG_FILE, EngineHandle, UiErrors, build_registry, reload_config_if_changed, windows,
};

const UNVIEWED_LIMIT: u32 = 200;
const PINNED_KEY: &str = "pinned";
const MUTED_KEY: &str = "muted";
const SNOOZE_KEY: &str = "snoozed_until_utc";
pub const EVENT_UNVIEWED_CHANGED: &str = "herald://unviewed-changed";
pub const EVENT_UI_STATE: &str = "herald://ui-state";
const MAX_MARK_VIEWED: usize = 1000;
const CONNECTION_TEST_TIMEOUT: Duration = Duration::from_secs(30);
const STARTING: &str = "Herald is still starting";

/// Whether the pointer is over the toast stack; alerts pause their timers while true.
pub struct ToastHover(pub watch::Sender<bool>);

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UiState {
    pub pinned: bool,
    pub muted: bool,
    pub snoozed_until_utc: Option<String>,
    pub connections: Vec<ConnectionStatus>,
    pub config_errors: Vec<ConfigError>,
    pub secrets_native: bool,
}

fn flag(value: Option<&str>) -> bool {
    value == Some("true")
}

/// An elapsed or unparseable snooze reads as not snoozed.
fn active_snooze(value: Option<&str>, now: DateTime<Utc>) -> Option<String> {
    let until = DateTime::parse_from_rfc3339(value?)
        .ok()?
        .with_timezone(&Utc);
    (until > now).then(|| until.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
}

fn assemble_ui_state(
    pinned: Option<&str>,
    muted: Option<&str>,
    snoozed: Option<&str>,
    now: DateTime<Utc>,
    connections: Vec<ConnectionStatus>,
    errors: &UiErrors,
) -> UiState {
    UiState {
        pinned: flag(pinned),
        muted: flag(muted),
        snoozed_until_utc: active_snooze(snoozed, now),
        connections,
        config_errors: errors.config.clone(),
        secrets_native: errors.secrets_native,
    }
}

/// record.activated for an opened record; hooks (open-url) react to it.
fn build_activated(original: &Event, now: DateTime<Utc>) -> Event {
    let mut e = Event::new(
        original.connection.clone(),
        event_types::RECORD_ACTIVATED,
        now,
        now,
        original.title.clone(),
        original.subject_key.clone(),
    )
    .with_id(ids::ui_activated(&original.id, now));
    e.catch_up = false;
    e.test = original.test;
    e.actor = Actor {
        kind: ActorKind::Me,
        id: String::new(),
        display_name: String::new(),
        unique_name: None,
    };
    e.url.clone_from(&original.url);
    e.repo.clone_from(&original.repo);
    e.pr.clone_from(&original.pr);
    e.branch.clone_from(&original.branch);
    e.run.clone_from(&original.run);
    e.deployment.clone_from(&original.deployment);
    e.local.clone_from(&original.local);
    e.data = serde_json::json!({
        "originalId": original.id,
        "originalType": original.event_type,
    });
    e
}

fn trimmed_secret(secret: &str) -> Result<&str, String> {
    let s = secret.trim();
    if s.is_empty() {
        Err("The secret is empty".into())
    } else {
        Ok(s)
    }
}

#[cfg(test)]
fn provider_message(e: &ProviderError) -> String {
    provider_message_for(e, None)
}

/// `auth_method` shapes the sign-in wording; `None` when the connection is not a draft.
fn provider_message_for(e: &ProviderError, auth_method: Option<&str>) -> String {
    let azcli = auth_method == Some("azcli");
    match e {
        ProviderError::Unauthorized if azcli => {
            "Azure CLI sign-in failed: the token was rejected. Run az login again.".into()
        }
        ProviderError::Unauthorized => {
            "The token was rejected. Check that it hasn't expired and has the listed scopes.".into()
        }
        ProviderError::MissingCredentials if azcli => {
            "Azure CLI sign-in failed: run az login first.".into()
        }
        ProviderError::MissingCredentials => "No credentials are saved for this connection".into(),
        ProviderError::Forbidden(m) | ProviderError::Transient(m) | ProviderError::Config(m) => {
            m.clone()
        }
        ProviderError::NotFound(m) => format!("Not found: {m}"),
        ProviderError::Throttled { .. } => {
            "The server is throttling requests; try again shortly".into()
        }
        ProviderError::Other(m) => m.clone(),
    }
}

fn engine(handle: &EngineHandle) -> Result<Arc<Engine>, String> {
    handle.get().ok_or_else(|| STARTING.to_string())
}

fn ui_state(app: &AppHandle) -> Option<UiState> {
    let engine = app.try_state::<EngineHandle>()?.get()?;
    let store = engine.store();
    let get = |key| store.get_ui_state(key).ok().flatten();
    let errors = app
        .try_state::<Mutex<UiErrors>>()?
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    Some(assemble_ui_state(
        get(PINNED_KEY).as_deref(),
        get(MUTED_KEY).as_deref(),
        get(SNOOZE_KEY).as_deref(),
        Utc::now(),
        engine.connections(),
        &errors,
    ))
}

pub fn emit_unviewed_changed(app: &AppHandle) {
    let Some(engine) = app.try_state::<EngineHandle>().and_then(|h| h.get()) else {
        return;
    };
    match engine.store().unviewed_count() {
        Ok(n) => {
            if let Err(e) = app.emit(EVENT_UNVIEWED_CHANGED, n) {
                tracing::warn!(error = %e, "emit unviewed-changed failed");
            }
        }
        Err(e) => tracing::warn!(error = %e, "unviewed count failed"),
    }
}

pub fn emit_ui_state(app: &AppHandle) {
    if let Some(state) = ui_state(app)
        && let Err(e) = app.emit(EVENT_UI_STATE, state)
    {
        tracing::warn!(error = %e, "emit ui-state failed");
    }
}

/// Owns the status watcher task so exit can abort it.
pub struct StatusWatcher(Mutex<Option<tauri::async_runtime::JoinHandle<()>>>);

impl StatusWatcher {
    /// Re-emits the UI state whenever the engine's connection statuses change.
    pub fn spawn(app: &AppHandle) -> Self {
        let rx = app
            .state::<EngineHandle>()
            .get()
            .map(|e| e.status_changes());
        let handle = rx.map(|mut rx| {
            let app = app.clone();
            tauri::async_runtime::spawn(async move {
                while rx.changed().await.is_ok() {
                    emit_ui_state(&app);
                }
            })
        });
        Self(Mutex::new(handle))
    }

    pub fn abort(&self) {
        if let Some(h) = self.0.lock().unwrap_or_else(PoisonError::into_inner).take() {
            h.abort();
        }
    }
}

#[tauri::command]
pub fn list_unviewed(engine_h: State<'_, EngineHandle>) -> Result<Vec<Event>, String> {
    engine(&engine_h)?
        .store()
        .unviewed(UNVIEWED_LIMIT)
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub fn unviewed_count(engine_h: State<'_, EngineHandle>) -> Result<u32, String> {
    engine(&engine_h)?
        .store()
        .unviewed_count()
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub fn mark_viewed(
    app: AppHandle,
    engine_h: State<'_, EngineHandle>,
    ids: Vec<String>,
) -> Result<(), String> {
    if ids.len() > MAX_MARK_VIEWED {
        return Err("Too many ids".into());
    }
    engine(&engine_h)?
        .store()
        .mark_viewed(&ids)
        .map_err(|e| e.to_string())?;
    emit_unviewed_changed(&app);
    Ok(())
}

#[tauri::command]
pub fn mark_all_viewed(app: AppHandle, engine_h: State<'_, EngineHandle>) -> Result<(), String> {
    engine(&engine_h)?
        .store()
        .mark_all_viewed()
        .map_err(|e| e.to_string())?;
    emit_unviewed_changed(&app);
    Ok(())
}

#[tauri::command]
pub fn activate_record(
    app: AppHandle,
    engine_h: State<'_, EngineHandle>,
    id: String,
) -> Result<(), String> {
    let engine = engine(&engine_h)?;
    let store = engine.store();
    let original = store
        .get_event(&id)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "That record no longer exists".to_string())?;
    engine
        .bus()
        .publish_transient(build_activated(&original, Utc::now()));
    store.mark_viewed(&[id]).map_err(|e| e.to_string())?;
    emit_unviewed_changed(&app);
    Ok(())
}

#[tauri::command]
pub fn get_ui_state(app: AppHandle) -> Result<UiState, String> {
    ui_state(&app).ok_or_else(|| STARTING.to_string())
}

#[tauri::command]
pub fn set_pinned(
    app: AppHandle,
    engine_h: State<'_, EngineHandle>,
    pinned: bool,
) -> Result<(), String> {
    engine(&engine_h)?
        .store()
        .set_ui_state(PINNED_KEY, if pinned { "true" } else { "false" })
        .map_err(|e| e.to_string())?;
    emit_ui_state(&app);
    Ok(())
}

#[tauri::command]
pub fn list_connections(
    engine_h: State<'_, EngineHandle>,
) -> Result<Vec<ConnectionStatus>, String> {
    Ok(engine(&engine_h)?.connections())
}

fn ensure_known_connection<'a>(
    mut configured: impl Iterator<Item = &'a str>,
    id: &str,
) -> Result<(), String> {
    if configured.any(|c| c == id) {
        Ok(())
    } else {
        Err("Unknown connection".into())
    }
}

#[tauri::command]
pub fn set_secret(
    app: AppHandle,
    engine_h: State<'_, EngineHandle>,
    connection_id: String,
    secret: String,
) -> Result<(), String> {
    let secret = trimmed_secret(&secret)?;
    let engine = engine(&engine_h)?;
    ensure_known_connection(
        engine.connections().iter().map(|c| c.id.as_str()),
        &connection_id,
    )?;
    engine
        .secrets()
        .set(&connection_id, secret)
        .map_err(|e| format!("Could not save the secret: {e}"))?;
    engine.credentials_changed(&connection_id);
    emit_ui_state(&app);
    Ok(())
}

#[tauri::command]
pub fn clear_secret(
    app: AppHandle,
    engine_h: State<'_, EngineHandle>,
    connection_id: String,
) -> Result<(), String> {
    let engine = engine(&engine_h)?;
    ensure_known_connection(
        engine.connections().iter().map(|c| c.id.as_str()),
        &connection_id,
    )?;
    engine
        .secrets()
        .delete(&connection_id)
        .map_err(|e| format!("Could not remove the secret: {e}"))?;
    engine.credentials_changed(&connection_id);
    emit_ui_state(&app);
    Ok(())
}

#[tauri::command]
pub async fn test_connection(
    engine_h: State<'_, EngineHandle>,
    connection_id: String,
) -> Result<Identity, String> {
    let engine = engine(&engine_h)?;
    let auth_method = engine
        .connections()
        .into_iter()
        .find(|c| c.id == connection_id)
        .map(|c| c.auth_method);
    tokio::time::timeout(
        CONNECTION_TEST_TIMEOUT,
        engine.test_connection(&connection_id),
    )
    .await
    .map_err(|_| "Connection test timed out".to_string())?
    .map_err(|e| provider_message_for(&e, auth_method.as_deref()))
}

fn format_config_errors(errors: &[ConfigError]) -> String {
    errors
        .iter()
        .map(|e| format!("{}: {}", e.path, e.message))
        .collect::<Vec<_>>()
        .join("\n")
}

/// A blank secret means "keep what is stored".
fn secret_override(secret: Option<&str>) -> Option<&str> {
    secret.map(str::trim).filter(|s| !s.is_empty())
}

fn draft_for(path: &Path, id: &str) -> Result<ConnectionDraft, String> {
    let config = Config::load(path).map_err(|e| format_config_errors(&e))?;
    config
        .connections
        .iter()
        .find(|c| c.id == id)
        .map(ConnectionDraft::from_config)
        .ok_or_else(|| "Unknown connection".to_string())
}

fn restore_secret(secrets: &dyn SecretStore, id: &str, previous: Option<&str>) {
    let result = match previous {
        Some(v) => secrets.set(id, v),
        None => secrets.delete(id),
    };
    if let Err(e) = result {
        tracing::warn!(error = %e, connection = id, "could not restore the previous secret");
    }
}

fn secret_err(e: herald_core::secrets::SecretError) -> String {
    format!("Secret store: {e}")
}

/// Whether `draft`'s auth method takes a secret. None when the provider or method is unknown.
fn auth_takes_secret(registry: &ProviderRegistry, draft: &ConnectionDraft) -> Option<bool> {
    let descriptor = registry
        .descriptors()
        .into_iter()
        .find(|d| d.kind == draft.provider)?;
    let method = descriptor
        .auth_methods
        .into_iter()
        .find(|m| m.id == draft.auth_method)?;
    Some(method.secret.is_some())
}

fn clear_orphaned_secret(secrets: &dyn SecretStore, id: &str) {
    match secrets.get(id) {
        Ok(Some(_)) => {
            if let Err(e) = secrets.delete(id) {
                tracing::warn!(error = %e, connection = id, "could not clear an unused secret");
            }
        }
        Ok(None) => {}
        Err(e) => tracing::warn!(error = %e, connection = id, "could not look up an unused secret"),
    }
}

/// Validates and builds the new config first (an invalid draft touches no secret), then
/// stores the secret, then writes the file; a failed write puts the secret back. When the
/// auth method takes no secret, secrets left under the id (or the pre-rename id) are cleared
/// after the write. Returns whether a secret was stored under `draft.id`.
fn save_connection_with(
    path: &Path,
    registry: &ProviderRegistry,
    secrets: &dyn SecretStore,
    draft: &ConnectionDraft,
    secret: Option<&str>,
    original_id: Option<&str>,
) -> Result<bool, String> {
    let prepared = config_edit::prepare_save(path, draft, original_id, registry)
        .map_err(|e| format_config_errors(&e))?;
    let takes_secret = auth_takes_secret(registry, draft).unwrap_or(true);
    let renamed = original_id.filter(|o| *o != draft.id);
    let value = match (takes_secret, secret_override(secret), renamed) {
        (false, ..) => None,
        (true, Some(s), _) => Some(s.to_owned()),
        (true, None, Some(old)) => secrets.get(old).map_err(secret_err)?,
        (true, None, None) => None,
    };
    let previous = match &value {
        Some(v) => {
            let previous = secrets.get(&draft.id).map_err(secret_err)?;
            secrets.set(&draft.id, v).map_err(secret_err)?;
            Some(previous)
        }
        None => None,
    };
    if let Err(e) = prepared.write() {
        if let Some(previous) = previous {
            restore_secret(secrets, &draft.id, previous.as_deref());
        }
        return Err(format!("Could not write the config file: {e}"));
    }
    if let Some(old) = renamed
        && let Err(e) = secrets.delete(old)
    {
        tracing::warn!(error = %e, connection = old, "could not clear the renamed connection's secret");
    }
    if !takes_secret {
        clear_orphaned_secret(secrets, &draft.id);
    }
    Ok(value.is_some())
}

/// What the engine needs to build a draft connection: the config, the typed secret, and the
/// connection whose stored secret stands in when none was typed.
struct DraftRequest {
    cfg: ConnectionConfig,
    secret: Option<String>,
    stored_secret_id: Option<String>,
}

fn draft_request(
    draft: &ConnectionDraft,
    secret: Option<&str>,
    original_id: Option<String>,
) -> Result<DraftRequest, String> {
    let cfg = draft
        .to_connection_config()
        .map_err(|e| format_config_errors(&[e]))?;
    Ok(DraftRequest {
        cfg,
        secret: secret_override(secret).map(str::to_owned),
        stored_secret_id: original_id,
    })
}

/// The config entry goes first; a secret that can't be cleared is reported after it.
fn remove_connection_with(
    path: &Path,
    registry: &ProviderRegistry,
    secrets: &dyn SecretStore,
    id: &str,
) -> Result<(), String> {
    config_edit::remove_connection(path, id, registry).map_err(|e| format_config_errors(&e))?;
    secrets.delete(id).map_err(|e| {
        format!("The connection was removed, but its secret could not be cleared: {e}")
    })
}

fn config_path(engine: &Engine) -> PathBuf {
    engine.paths().home.join(CONFIG_FILE)
}

#[tauri::command]
pub fn list_providers() -> Vec<ProviderDescriptor> {
    build_registry().descriptors()
}

#[tauri::command]
pub fn get_connection_draft(
    engine_h: State<'_, EngineHandle>,
    id: String,
) -> Result<ConnectionDraft, String> {
    draft_for(&config_path(&*engine(&engine_h)?), &id)
}

#[tauri::command]
pub async fn test_draft_connection(
    engine_h: State<'_, EngineHandle>,
    draft: ConnectionDraft,
    secret: Option<String>,
    original_id: Option<String>,
) -> Result<Identity, String> {
    let engine = engine(&engine_h)?;
    let req = draft_request(&draft, secret.as_deref(), original_id)?;
    tokio::time::timeout(
        CONNECTION_TEST_TIMEOUT,
        engine.test_draft(req.cfg, req.secret, req.stored_secret_id.as_deref()),
    )
    .await
    .map_err(|_| "Connection test timed out".to_string())?
    .map_err(|e| provider_message_for(&e, Some(&draft.auth_method)))
}

#[tauri::command]
pub async fn list_draft_scopes(
    engine_h: State<'_, EngineHandle>,
    draft: ConnectionDraft,
    secret: Option<String>,
    original_id: Option<String>,
    field_key: String,
) -> Result<Vec<ScopeOption>, String> {
    let engine = engine(&engine_h)?;
    let req = draft_request(&draft, secret.as_deref(), original_id)?;
    tokio::time::timeout(
        CONNECTION_TEST_TIMEOUT,
        engine.list_draft_scopes(
            req.cfg,
            req.secret,
            req.stored_secret_id.as_deref(),
            &field_key,
        ),
    )
    .await
    .map_err(|_| "Listing timed out".to_string())?
    .map_err(|e| provider_message_for(&e, Some(&draft.auth_method)))
}

#[tauri::command]
pub fn save_connection(
    app: AppHandle,
    engine_h: State<'_, EngineHandle>,
    draft: ConnectionDraft,
    secret: Option<String>,
    original_id: Option<String>,
) -> Result<(), String> {
    let engine = engine(&engine_h)?;
    let secret_set = save_connection_with(
        &config_path(&engine),
        &build_registry(),
        engine.secrets().as_ref(),
        &draft,
        secret.as_deref(),
        original_id.as_deref(),
    )?;
    // Emits the UI state itself.
    reload_config_if_changed(&app);
    if secret_set {
        engine.credentials_changed(&draft.id);
    }
    Ok(())
}

#[tauri::command]
pub fn remove_connection(
    app: AppHandle,
    engine_h: State<'_, EngineHandle>,
    id: String,
) -> Result<(), String> {
    let engine = engine(&engine_h)?;
    let result = remove_connection_with(
        &config_path(&engine),
        &build_registry(),
        engine.secrets().as_ref(),
        &id,
    );
    reload_config_if_changed(&app);
    emit_ui_state(&app);
    result
}

#[tauri::command]
pub fn toast_clicked(app: AppHandle) {
    windows::show_pane(&app);
}

#[tauri::command]
pub fn toast_hover(hover: State<'_, ToastHover>, hovering: bool) {
    hover.0.send_replace(hovering);
}

#[tauri::command]
pub fn toast_resize(app: AppHandle, height_logical: f64) {
    windows::position_toast(&app, height_logical);
}

#[tauri::command]
pub fn hide_pane(app: AppHandle) {
    windows::hide_pane(&app);
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use herald_core::event::{ConnectionRef, LocalPaths, PrRef, PrRole, RepoRef, RunRef};
    use herald_core::secrets::MemorySecretStore;

    #[test]
    fn ensure_known_connection_rejects_unconfigured_ids() {
        assert_eq!(ensure_known_connection(["a", "b"].into_iter(), "b"), Ok(()));
        assert_eq!(
            ensure_known_connection(["a"].into_iter(), "x"),
            Err("Unknown connection".to_string())
        );
        assert!(ensure_known_connection([].into_iter(), "").is_err());
    }

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap()
    }

    fn original() -> Event {
        let mut e = Event::new(
            ConnectionRef {
                id: "conn-a".into(),
                provider: "ado".into(),
            },
            event_types::PR_CREATED,
            now(),
            now(),
            "PR 1",
            "conn-a:pr:r:1",
        )
        .with_id("conn-a:pr:r:1:created");
        e.url = Some("https://example.test/pr/1".into());
        e.catch_up = true;
        e.test = true;
        e.repo = Some(RepoRef {
            id: "r1".into(),
            name: "repo".into(),
            namespace: Some("proj".into()),
        });
        e.pr = Some(PrRef {
            id: 1,
            title: "PR 1".into(),
            source_branch: "a".into(),
            target_branch: "main".into(),
            is_draft: false,
            role: PrRole::Author,
        });
        e.run = Some(RunRef {
            id: "9".into(),
            definition: "ci".into(),
            source_branch: "a".into(),
            reason: "manual".into(),
            status: "completed".into(),
            result: "failed".into(),
            stage: None,
        });
        e.local = Some(LocalPaths {
            repo_path: Some("/r".into()),
            worktree_path: None,
        });
        e
    }

    #[test]
    fn build_activated_copies_fields_and_marks_me() {
        let o = original();
        let later = now() + chrono::Duration::seconds(7);
        let a = build_activated(&o, later);
        assert_eq!(a.event_type, "record.activated");
        assert_eq!(
            a.id,
            format!("ui:activated:{}:{}", o.id, later.timestamp_millis())
        );
        assert_eq!(a.actor.kind, ActorKind::Me);
        assert!(!a.catch_up);
        assert!(a.test);
        assert_eq!(a.url, o.url);
        assert_eq!(a.subject_key, o.subject_key);
        assert_eq!(a.connection, o.connection);
        assert_eq!(a.local, o.local);
        assert_eq!(a.repo, o.repo);
        assert_eq!(a.pr, o.pr);
        assert_eq!(a.run, o.run);
        assert_eq!(a.data["originalId"], o.id);
        assert_eq!(a.data["originalType"], "pr.created");
    }

    #[test]
    fn assemble_ui_state_reads_flags_and_drops_elapsed_snooze() {
        let errors = UiErrors {
            config: vec![ConfigError {
                path: "p".into(),
                message: "m".into(),
            }],
            secrets_native: true,
        };
        let s = assemble_ui_state(
            Some("true"),
            None,
            Some("2026-01-02T04:00:00Z"),
            now(),
            vec![],
            &errors,
        );
        assert!(s.pinned && !s.muted && s.secrets_native);
        assert_eq!(
            s.snoozed_until_utc.as_deref(),
            Some("2026-01-02T04:00:00.000Z")
        );
        assert_eq!(s.config_errors.len(), 1);
        let past = assemble_ui_state(
            None,
            Some("true"),
            Some("2026-01-01T00:00:00Z"),
            now(),
            vec![],
            &errors,
        );
        assert!(past.muted);
        assert_eq!(past.snoozed_until_utc, None);
        let json = serde_json::to_value(&past).unwrap();
        assert!(json.get("snoozedUntilUtc").is_some() && json.get("secretsNative").is_some());
    }

    #[test]
    fn trimmed_secret_trims_and_rejects_blank() {
        assert_eq!(trimmed_secret("  abc\n"), Ok("abc"));
        assert!(trimmed_secret("   ").is_err());
    }

    #[test]
    fn provider_message_is_readable() {
        assert!(
            provider_message(&ProviderError::Unauthorized).starts_with("The token was rejected.")
        );
        assert!(
            provider_message_for(&ProviderError::Unauthorized, Some("azcli"))
                .starts_with("Azure CLI sign-in failed")
        );
        assert_eq!(
            provider_message(&ProviderError::Config(
                "Organization \"x\" was not found".into()
            )),
            "Organization \"x\" was not found"
        );
    }

    fn ado_draft(id: &str, org: &str) -> ConnectionDraft {
        ConnectionDraft {
            id: id.into(),
            provider: "ado".into(),
            enabled: true,
            auth_method: "pat".into(),
            bots: vec![],
            settings: std::collections::BTreeMap::from([(
                "organization".to_string(),
                serde_json::json!(org),
            )]),
        }
    }

    fn save(
        path: &Path,
        secrets: &dyn SecretStore,
        draft: &ConnectionDraft,
        secret: Option<&str>,
        original: Option<&str>,
    ) -> Result<bool, String> {
        save_connection_with(path, &build_registry(), secrets, draft, secret, original)
    }

    fn ids(path: &Path) -> Vec<String> {
        Config::load(path)
            .unwrap()
            .connections
            .into_iter()
            .map(|c| c.id)
            .collect()
    }

    #[test]
    fn save_stores_secret_and_writes_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(CONFIG_FILE);
        let secrets = MemorySecretStore::new();
        let stored = save(
            &path,
            &secrets,
            &ado_draft("a", "contoso"),
            Some(" tok "),
            None,
        );
        assert_eq!(stored, Ok(true));
        assert_eq!(secrets.get("a").unwrap().as_deref(), Some("tok"));
        assert_eq!(ids(&path), ["a"]);
        assert_eq!(draft_for(&path, "a").unwrap(), ado_draft("a", "contoso"));
        assert!(draft_for(&path, "zzz").is_err());
    }

    #[test]
    fn failed_save_removes_a_newly_stored_secret() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(CONFIG_FILE);
        let secrets = MemorySecretStore::new();
        let err = save(&path, &secrets, &ado_draft("a", ""), Some("tok"), None).unwrap_err();
        assert!(err.contains("organization"), "{err}");
        assert!(!err.contains("tok"));
        assert_eq!(secrets.get("a").unwrap(), None);
        assert!(!path.exists());
    }

    #[test]
    fn failed_save_restores_the_previous_secret() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(CONFIG_FILE);
        let secrets = MemorySecretStore::new();
        save(
            &path,
            &secrets,
            &ado_draft("a", "contoso"),
            Some("old"),
            None,
        )
        .unwrap();
        let bad = ado_draft("a", "");
        assert!(save(&path, &secrets, &bad, Some("new"), Some("a")).is_err());
        assert_eq!(secrets.get("a").unwrap().as_deref(), Some("old"));
    }

    #[test]
    fn rename_moves_the_stored_secret_when_none_is_given() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(CONFIG_FILE);
        let secrets = MemorySecretStore::new();
        save(
            &path,
            &secrets,
            &ado_draft("a", "contoso"),
            Some("tok"),
            None,
        )
        .unwrap();
        let moved = save(
            &path,
            &secrets,
            &ado_draft("b", "contoso"),
            Some("  "),
            Some("a"),
        );
        assert_eq!(moved, Ok(true));
        assert_eq!(secrets.get("b").unwrap().as_deref(), Some("tok"));
        assert_eq!(secrets.get("a").unwrap(), None);
        assert_eq!(ids(&path), ["b"]);
    }

    #[test]
    fn rename_with_new_secret_uses_it_and_clears_the_old_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(CONFIG_FILE);
        let secrets = MemorySecretStore::new();
        save(
            &path,
            &secrets,
            &ado_draft("a", "contoso"),
            Some("tok"),
            None,
        )
        .unwrap();
        save(
            &path,
            &secrets,
            &ado_draft("b", "contoso"),
            Some("fresh"),
            Some("a"),
        )
        .unwrap();
        assert_eq!(secrets.get("b").unwrap().as_deref(), Some("fresh"));
        assert_eq!(secrets.get("a").unwrap(), None);
    }

    #[test]
    fn failed_rename_keeps_the_old_secret_and_leaves_the_new_id_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(CONFIG_FILE);
        let secrets = MemorySecretStore::new();
        save(
            &path,
            &secrets,
            &ado_draft("a", "contoso"),
            Some("tok"),
            None,
        )
        .unwrap();
        assert!(save(&path, &secrets, &ado_draft("b", ""), None, Some("a")).is_err());
        assert_eq!(secrets.get("a").unwrap().as_deref(), Some("tok"));
        assert_eq!(secrets.get("b").unwrap(), None);
        assert_eq!(ids(&path), ["a"]);
    }

    #[test]
    fn remove_deletes_the_entry_and_its_secret() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(CONFIG_FILE);
        let secrets = MemorySecretStore::new();
        save(
            &path,
            &secrets,
            &ado_draft("a", "contoso"),
            Some("tok"),
            None,
        )
        .unwrap();
        remove_connection_with(&path, &build_registry(), &secrets, "a").unwrap();
        assert!(ids(&path).is_empty());
        assert_eq!(secrets.get("a").unwrap(), None);
        assert!(remove_connection_with(&path, &build_registry(), &secrets, "a").is_err());
    }

    #[test]
    fn config_errors_format_as_path_message_lines() {
        let errors = [
            ConfigError::new("a.b", "bad"),
            ConfigError::new("c", "worse"),
        ];
        assert_eq!(format_config_errors(&errors), "a.b: bad\nc: worse");
    }

    /// Records every write so tests can assert nothing was touched.
    #[derive(Default)]
    struct RecordingStore {
        inner: MemorySecretStore,
        writes: Mutex<Vec<String>>,
    }

    impl SecretStore for RecordingStore {
        fn get(&self, key: &str) -> Result<Option<String>, herald_core::secrets::SecretError> {
            self.inner.get(key)
        }
        fn set(&self, key: &str, value: &str) -> Result<(), herald_core::secrets::SecretError> {
            self.writes.lock().unwrap().push(format!("set {key}"));
            self.inner.set(key, value)
        }
        fn delete(&self, key: &str) -> Result<(), herald_core::secrets::SecretError> {
            self.writes.lock().unwrap().push(format!("delete {key}"));
            self.inner.delete(key)
        }
        fn is_native(&self) -> bool {
            false
        }
    }

    #[test]
    fn invalid_draft_touches_no_secret_at_all() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(CONFIG_FILE);
        let secrets = RecordingStore::default();
        secrets.inner.set("a", "tok").unwrap();
        save(&path, &secrets, &ado_draft("a", "contoso"), None, None).unwrap();
        let before = std::fs::read(&path).unwrap();

        // Bad organization, a rename and a typed secret: every secret path is in play.
        let r = save(&path, &secrets, &ado_draft("b", ""), Some("new"), Some("a"));
        assert!(r.is_err());
        let dup = save(
            &path,
            &secrets,
            &ado_draft("a", "contoso"),
            Some("new"),
            None,
        );
        assert!(dup.is_err());

        assert!(secrets.writes.lock().unwrap().is_empty());
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    #[test]
    fn switching_to_a_method_without_a_secret_clears_the_stored_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(CONFIG_FILE);
        let secrets = MemorySecretStore::new();
        save(
            &path,
            &secrets,
            &ado_draft("a", "contoso"),
            Some("tok"),
            None,
        )
        .unwrap();

        let mut azcli = ado_draft("a", "contoso");
        azcli.auth_method = "azcli".into();
        let stored = save(&path, &secrets, &azcli, Some("ignored"), Some("a"));

        assert_eq!(stored, Ok(false));
        assert_eq!(secrets.get("a").unwrap(), None);

        // Rename plus method switch clears the old id's secret too.
        let mut pat = ado_draft("a", "contoso");
        pat.auth_method = "pat".into();
        save(&path, &secrets, &pat, Some("tok2"), Some("a")).unwrap();
        let mut renamed = ado_draft("c", "contoso");
        renamed.auth_method = "azcli".into();
        save(&path, &secrets, &renamed, None, Some("a")).unwrap();
        assert_eq!(secrets.get("a").unwrap(), None);
        assert_eq!(secrets.get("c").unwrap(), None);
    }

    #[test]
    fn draft_request_plumbs_secret_and_original_id() {
        let d = ado_draft("a", "contoso");
        let req = draft_request(&d, Some("  tok "), Some("old".into())).unwrap();
        assert_eq!(req.cfg.id, "a");
        assert_eq!(req.secret.as_deref(), Some("tok"));
        assert_eq!(req.stored_secret_id.as_deref(), Some("old"));

        let blank = draft_request(&d, Some("  "), None).unwrap();
        assert_eq!(blank.secret, None);
        assert_eq!(blank.stored_secret_id, None);

        let mut bad = d.clone();
        bad.settings.insert("port".into(), serde_json::json!(1));
        let err = draft_request(&bad, Some("tok"), None).err().unwrap();
        assert!(err.contains("settings.port") && !err.contains("tok"));
    }
}
