//! Tauri IPC commands and the app -> UI events they trigger.
//!
//! Without an app ACL manifest (no build.rs command allowlist), Tauri 2.12 allows app
//! commands from any local window, whatever the capability grants. Splitting commands
//! per window (toast vs pane) is a planned follow-up.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use chrono::{DateTime, Utc};
use herald_core::config::ConfigError;
use herald_core::engine::Engine;
use herald_core::event::{Actor, ActorKind, Event, event_types, ids};
use herald_core::provider::{Identity, ProviderError};
use herald_core::scheduler::ConnectionStatus;
use serde::Serialize;
use tauri::{AppHandle, Emitter, Manager, State};
use tokio::sync::watch;

use crate::{EngineHandle, UiErrors, windows};

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

fn provider_message(e: &ProviderError) -> String {
    match e {
        ProviderError::Unauthorized => "The credentials were rejected".into(),
        ProviderError::MissingCredentials => "No credentials are saved for this connection".into(),
        ProviderError::Forbidden(m) => format!("Access denied: {m}"),
        ProviderError::NotFound(m) => format!("Not found: {m}"),
        ProviderError::Throttled { .. } => {
            "The server is throttling requests; try again shortly".into()
        }
        ProviderError::Transient(m) => format!("Temporary error: {m}"),
        ProviderError::Config(m) => format!("Configuration problem: {m}"),
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
    tokio::time::timeout(
        CONNECTION_TEST_TIMEOUT,
        engine.test_connection(&connection_id),
    )
    .await
    .map_err(|_| "Connection test timed out".to_string())?
    .map_err(|e| provider_message(&e))
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
        assert_eq!(
            provider_message(&ProviderError::Unauthorized),
            "The credentials were rejected"
        );
        assert!(provider_message(&ProviderError::Forbidden("x".into())).contains("Access denied"));
    }
}
