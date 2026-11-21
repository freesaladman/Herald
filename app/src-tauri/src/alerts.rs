use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use chrono::{DateTime, Utc};
use herald_core::config::ToastConfig;
use herald_core::event::{Event, Severity, event_types};
use serde::Serialize;
use tauri::{AppHandle, Emitter, Manager};
use tokio::sync::broadcast::error::RecvError;
use tokio::time::Instant;

use crate::EngineHandle;
use crate::commands::{self, ToastHover};
use crate::sound::SoundPlayer;

pub const EVENT_TOASTS: &str = "herald://toasts";
const BATCH_WINDOW: Duration = Duration::from_millis(500);
const MUTED_KEY: &str = "muted";
const SNOOZE_KEY: &str = "snoozed_until_utc";
const DEFAULT_MAX_VISIBLE: usize = 4;
/// A hover reported longer than this is treated as stuck (the toast window can miss the leave event).
const HOVER_MAX: Duration = Duration::from_secs(45);

/// What the toast window renders for one coalesced group of events.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToastCard {
    pub card_id: String,
    pub severity: Severity,
    pub title: String,
    pub subtitle: Option<String>,
    pub count: usize,
    pub duration_ms: u64,
    pub test: bool,
}

/// Owns the alerts task so exit can abort it.
pub struct Alerts(Mutex<Option<tauri::async_runtime::JoinHandle<()>>>);

impl Alerts {
    pub fn spawn(app: &AppHandle) -> Self {
        let bus = app.state::<EngineHandle>().get().map(|e| e.bus());
        let handle = bus.map(|bus| {
            let rx = bus.subscribe();
            let hover = app.state::<ToastHover>().0.subscribe();
            tauri::async_runtime::spawn(run(app.clone(), rx, hover))
        });
        Self(Mutex::new(handle))
    }

    pub fn abort(&self) {
        if let Some(h) = self.0.lock().unwrap_or_else(PoisonError::into_inner).take() {
            h.abort();
        }
    }
}

fn accepts(event: &Event) -> bool {
    event.alert && (!event.catch_up || event.event_type == event_types::CATCHUP_SUMMARY)
}

fn severity_rank(s: Severity) -> u8 {
    match s {
        Severity::Critical => 3,
        Severity::Warn => 2,
        Severity::Success => 1,
        Severity::Info => 0,
    }
}

fn duration_ms(severity: Severity, cfg: &ToastConfig) -> u64 {
    let secs = if severity == Severity::Critical {
        cfg.critical_duration_secs
    } else {
        cfg.duration_secs
    };
    secs.saturating_mul(1000)
}

/// Groups events by subject key (first-seen order) into one card each.
fn build_cards(
    events: &[Arc<Event>],
    cfg: &ToastConfig,
    now_millis: i64,
    seq: &mut u64,
) -> Vec<ToastCard> {
    let mut groups: Vec<(&str, Vec<&Event>)> = Vec::new();
    for event in events {
        match groups.iter_mut().find(|(k, _)| *k == event.subject_key) {
            Some((_, g)) => g.push(event),
            None => groups.push((&event.subject_key, vec![event])),
        }
    }
    groups
        .into_iter()
        .map(|(subject, group)| {
            let first = group[0];
            let severity = group
                .iter()
                .map(|e| e.severity)
                .max_by_key(|s| severity_rank(*s))
                .unwrap_or(first.severity);
            let title = if group.len() == 1 {
                first.title.clone()
            } else {
                let what = first
                    .subtitle
                    .as_deref()
                    .or_else(|| first.pr.as_ref().map(|pr| pr.title.as_str()))
                    .unwrap_or(first.title.as_str());
                format!("{} updates on {what}", group.len())
            };
            *seq += 1;
            ToastCard {
                card_id: format!("{subject}-{now_millis}-{seq}"),
                severity,
                title,
                subtitle: first.subtitle.clone(),
                count: group.len(),
                duration_ms: duration_ms(severity, cfg),
                test: group.iter().any(|e| e.test),
            }
        })
        .collect()
}

fn effective_hover(hovering: bool, since: Option<Instant>, now: Instant) -> bool {
    hovering && since.is_none_or(|s| now.saturating_duration_since(s) <= HOVER_MAX)
}

/// Resolves the configured sound file; relative paths are under the Herald home.
fn sound_path(file: &str, home: &Path) -> Option<PathBuf> {
    if file.is_empty() {
        return None;
    }
    // `join` keeps an absolute `file` as is.
    Some(home.join(file))
}

/// Cards waiting to show or showing, with the time each has left. Only the first
/// `max_visible` count down; the rest are summarised as a "+N more" card.
#[derive(Default)]
struct ToastQueue {
    items: VecDeque<(ToastCard, u64)>,
}

impl ToastQueue {
    fn push(&mut self, cards: Vec<ToastCard>) {
        self.items.extend(cards.into_iter().map(|c| {
            let left = c.duration_ms;
            (c, left)
        }));
    }

    /// Counts down the visible cards; returns whether any expired.
    fn advance(&mut self, elapsed: Duration, hovering: bool, max_visible: usize) -> bool {
        if hovering {
            return false;
        }
        let elapsed = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
        for (_, left) in self.items.iter_mut().take(max_visible) {
            *left = left.saturating_sub(elapsed);
        }
        let before = self.items.len();
        let mut index = 0;
        self.items.retain(|(_, left)| {
            index += 1;
            index > max_visible || *left > 0
        });
        self.items.len() != before
    }

    fn next_expiry(&self, max_visible: usize) -> Option<Duration> {
        self.items
            .iter()
            .take(max_visible)
            .map(|(_, left)| Duration::from_millis(*left))
            .min()
    }

    fn snapshot(&self, max_visible: usize) -> Vec<ToastCard> {
        let mut cards: Vec<ToastCard> = self
            .items
            .iter()
            .take(max_visible)
            .map(|(c, _)| c.clone())
            .collect();
        let extra = self.items.len().saturating_sub(max_visible);
        if extra > 0 {
            cards.push(ToastCard {
                card_id: "more".into(),
                severity: Severity::Info,
                title: format!("+{extra} more"),
                subtitle: None,
                count: extra,
                duration_ms: 0,
                test: false,
            });
        }
        cards
    }
}

fn is_snoozed(value: Option<&str>, now: DateTime<Utc>) -> bool {
    value
        .and_then(|v| DateTime::parse_from_rfc3339(v).ok())
        .is_some_and(|until| until > now)
}

/// Whether the OS says the user shouldn't be interrupted (full-screen app, presentation, quiet time).
fn os_suppressed() -> bool {
    #[cfg(windows)]
    {
        use ::windows::Win32::UI::Shell::{
            QUNS_ACCEPTS_NOTIFICATIONS, SHQueryUserNotificationState,
        };
        // SAFETY: plain query; the API writes its own out-parameter.
        unsafe { SHQueryUserNotificationState() }
            .is_ok_and(|state| state != QUNS_ACCEPTS_NOTIFICATIONS)
    }
    #[cfg(not(windows))]
    {
        false
    }
}

fn emit_cards(app: &AppHandle, cards: &[ToastCard]) {
    if let Err(e) = app.emit(EVENT_TOASTS, cards) {
        tracing::warn!(error = %e, "emit toasts failed");
    }
}

fn max_visible(app: &AppHandle) -> usize {
    app.state::<EngineHandle>()
        .get()
        .map_or(DEFAULT_MAX_VISIBLE, |e| e.config().toast.max_visible.max(1))
}

/// Turns a finished batch into cards and a sound, unless suppressed. Returns whether cards were added.
fn flush_batch(
    app: &AppHandle,
    batch: &[Arc<Event>],
    queue: &mut ToastQueue,
    seq: &mut u64,
) -> bool {
    let Some(engine) = app.state::<EngineHandle>().get() else {
        return false;
    };
    let store = engine.store();
    let ui = |key: &str| store.get_ui_state(key).ok().flatten();
    if is_snoozed(ui(SNOOZE_KEY).as_deref(), Utc::now()) || os_suppressed() {
        return false;
    }
    let config = engine.config();
    let cards = build_cards(batch, &config.toast, Utc::now().timestamp_millis(), seq);
    if cards.is_empty() {
        return false;
    }
    queue.push(cards);
    if ui(MUTED_KEY).as_deref() != Some("true")
        && let Some(player) = app.try_state::<SoundPlayer>()
    {
        let file = sound_path(&config.sound.file, &engine.paths().home);
        player.play(file, config.sound.volume);
    }
    true
}

async fn run(
    app: AppHandle,
    mut rx: tokio::sync::broadcast::Receiver<Arc<Event>>,
    mut hover_rx: tokio::sync::watch::Receiver<bool>,
) {
    let mut queue = ToastQueue::default();
    let mut batch: Vec<Arc<Event>> = Vec::new();
    let mut batch_deadline: Option<Instant> = None;
    let mut hover_since: Option<Instant> = None;
    let mut hovering = *hover_rx.borrow();
    if hovering {
        hover_since = Some(Instant::now());
    }
    let mut unviewed_dirty = false;
    let mut last = Instant::now();
    let mut seq = 0_u64;
    loop {
        let max = max_visible(&app);
        let expiry = if hovering {
            None
        } else {
            queue.next_expiry(max)
        };
        let hover_deadline = hover_since.filter(|_| hovering).map(|s| s + HOVER_MAX);
        tokio::select! {
            received = rx.recv() => match received {
                Ok(event) => {
                    unviewed_dirty |= event.alert;
                    if accepts(&event) {
                        batch.push(event);
                        batch_deadline.get_or_insert_with(|| Instant::now() + BATCH_WINDOW);
                    }
                }
                Err(RecvError::Lagged(n)) => {
                    tracing::warn!(skipped = n, "alerts lagged behind the bus");
                    unviewed_dirty = true;
                }
                Err(RecvError::Closed) => break,
            },
            () = async {
                match batch_deadline {
                    Some(d) => tokio::time::sleep_until(d).await,
                    None => std::future::pending().await,
                }
            } => batch_deadline = None,
            () = async {
                match expiry {
                    Some(d) => tokio::time::sleep(d).await,
                    None => std::future::pending().await,
                }
            } => {}
            () = async {
                match hover_deadline {
                    Some(d) => tokio::time::sleep_until(d).await,
                    None => std::future::pending().await,
                }
            } => {}
            changed = hover_rx.changed() => {
                if changed.is_err() {
                    break;
                }
            }
        }
        let now = Instant::now();
        let mut changed = queue.advance(now - last, hovering, max);
        last = now;
        let raw_hover = *hover_rx.borrow();
        match (raw_hover, hover_since) {
            (true, None) => hover_since = Some(now),
            (false, _) => hover_since = None,
            (true, Some(_)) => {}
        }
        hovering = effective_hover(raw_hover, hover_since, now);
        if batch_deadline.is_none() && !batch.is_empty() {
            changed |= flush_batch(&app, &batch, &mut queue, &mut seq);
            batch.clear();
        }
        if unviewed_dirty && batch_deadline.is_none() {
            commands::emit_unviewed_changed(&app);
            unviewed_dirty = false;
        }
        if changed {
            emit_cards(&app, &queue.snapshot(max_visible(&app)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use herald_core::event::ConnectionRef;

    fn cfg() -> ToastConfig {
        ToastConfig::default()
    }

    fn event(subject: &str, title: &str, severity: Severity) -> Arc<Event> {
        let now = Utc::now();
        let mut e = Event::new(
            ConnectionRef {
                id: "c".into(),
                provider: "ado".into(),
            },
            event_types::PR_CREATED,
            now,
            now,
            title,
            subject,
        );
        e.severity = severity;
        e.alert = true;
        Arc::new(e)
    }

    fn cards(events: &[Arc<Event>]) -> Vec<ToastCard> {
        build_cards(events, &cfg(), 0, &mut 0)
    }

    #[test]
    fn single_event_card_uses_its_title_and_info_duration() {
        let c = cards(&[event("pr:1", "Opened", Severity::Info)]);
        assert_eq!(c.len(), 1);
        assert_eq!((c[0].title.as_str(), c[0].count), ("Opened", 1));
        assert_eq!(c[0].duration_ms, 6000);
    }

    #[test]
    fn same_subject_coalesces_with_max_severity_and_critical_duration() {
        let mut first = event("pr:7055", "a", Severity::Warn);
        Arc::make_mut(&mut first).subtitle = Some("!7055".into());
        let c = cards(&[
            first,
            event("pr:7055", "b", Severity::Critical),
            event("pr:7055", "c", Severity::Info),
            event("pr:2", "other", Severity::Info),
        ]);
        assert_eq!(c.len(), 2);
        assert_eq!(c[0].title, "3 updates on !7055");
        assert_eq!(c[0].severity, Severity::Critical);
        assert_eq!(c[0].count, 3);
        assert_eq!(c[0].duration_ms, 12_000);
        assert_eq!(c[1].title, "other");
    }

    #[test]
    fn filter_drops_non_alerts_and_catch_up_but_keeps_catchup_summary() {
        let mut e = (*event("s", "t", Severity::Info)).clone();
        assert!(accepts(&e));
        e.alert = false;
        assert!(!accepts(&e));
        e.alert = true;
        e.catch_up = true;
        assert!(!accepts(&e));
        e.event_type = event_types::CATCHUP_SUMMARY.into();
        assert!(accepts(&e));
    }

    #[test]
    fn overflow_becomes_plus_n_more_card() {
        let mut q = ToastQueue::default();
        let evs: Vec<_> = (0..6)
            .map(|i| event(&format!("s{i}"), "t", Severity::Info))
            .collect();
        q.push(cards(&evs));
        let snap = q.snapshot(4);
        assert_eq!(snap.len(), 5);
        assert_eq!(snap[4].title, "+2 more");
        assert_eq!(snap[4].count, 2);
        assert_eq!(q.snapshot(6).len(), 6);
    }

    #[test]
    fn cards_expire_unless_hovering_and_queued_ones_promote() {
        let mut q = ToastQueue::default();
        q.push(cards(&[
            event("a", "a", Severity::Info),
            event("b", "b", Severity::Info),
        ]));
        assert!(!q.advance(Duration::from_secs(60), true, 1));
        assert_eq!(q.snapshot(1).len(), 2);
        assert!(!q.advance(Duration::from_secs(5), false, 1));
        assert_eq!(q.next_expiry(1), Some(Duration::from_secs(1)));
        assert!(q.advance(Duration::from_secs(1), false, 1));
        let snap = q.snapshot(1);
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].title, "b");
        assert!(q.advance(Duration::from_secs(6), false, 1));
        assert!(q.snapshot(1).is_empty());
    }

    #[test]
    fn coalesced_title_falls_back_to_pr_title_then_first_title() {
        let mut with_pr = Arc::make_mut(&mut event("pr:1", "a", Severity::Info)).clone();
        with_pr.pr = Some(herald_core::event::PrRef {
            id: 1,
            title: "Fix login".into(),
            source_branch: "s".into(),
            target_branch: "t".into(),
            is_draft: false,
            role: herald_core::event::PrRole::Author,
        });
        let c = cards(&[Arc::new(with_pr), event("pr:1", "b", Severity::Info)]);
        assert_eq!(c[0].title, "2 updates on Fix login");
        let c = cards(&[
            event("pr:2", "Build failed", Severity::Info),
            event("pr:2", "b", Severity::Info),
        ]);
        assert_eq!(c[0].title, "2 updates on Build failed");
    }

    #[test]
    fn hover_expires_after_the_cap() {
        let now = Instant::now();
        assert!(effective_hover(
            true,
            Some(now),
            now + Duration::from_secs(10)
        ));
        assert!(!effective_hover(
            true,
            Some(now),
            now + HOVER_MAX + Duration::from_secs(1)
        ));
        assert!(!effective_hover(false, None, now));
    }

    #[test]
    fn sound_path_resolves_relative_under_home_and_keeps_absolute() {
        let home = Path::new("home");
        assert_eq!(sound_path("", home), None);
        assert_eq!(sound_path("ding.wav", home), Some(home.join("ding.wav")));
        let abs = std::env::temp_dir().join("ding.wav");
        assert_eq!(sound_path(abs.to_str().unwrap(), home), Some(abs));
    }

    #[test]
    fn snooze_suppresses_only_while_in_the_future() {
        let now = Utc::now();
        let future = (now + chrono::Duration::minutes(5)).to_rfc3339();
        let past = (now - chrono::Duration::minutes(5)).to_rfc3339();
        assert!(is_snoozed(Some(&future), now));
        assert!(!is_snoozed(Some(&past), now));
        assert!(!is_snoozed(Some("garbage"), now));
        assert!(!is_snoozed(None, now));
    }
}
