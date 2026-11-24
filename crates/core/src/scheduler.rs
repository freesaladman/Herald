//! Scheduler: one task per (connection, tier) polling that tier's sources, with
//! per-connection auth state, source disabling and the catch-up summary.

use std::collections::{BTreeMap, BTreeSet};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use futures_util::FutureExt;
use serde::Serialize;
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};

use crate::bus::Bus;
use crate::classify::Classifier;
use crate::config::{Config, ConnectionConfig};
use crate::event::{Actor, ActorKind, ConnectionRef, Event, Severity, event_types, ids, subject};
use crate::local_repos::LocalIndex;
use crate::provider::{
    AuthState, Connection, Identity, ProviderError, ProviderRegistry, ScopedSnapshots, Source,
    SyncContext, Tier,
};
use crate::secrets::SecretStore;
use crate::store::Store;

const AUTH_PAUSE: Duration = Duration::from_secs(15 * 60);
const PRUNE_EVERY: Duration = Duration::from_secs(24 * 60 * 60);
const PRUNE_DAYS: u32 = 180;
const SUMMARY_TOP: usize = 5;
const HOUSEKEEPING: &str = "";
/// Task-map key of the cycle watchdog; connection ids never contain `!`.
const WATCHDOG: &str = "!cycle-watchdog";
/// A catch-up cycle still open after this long is summarised and reset.
const CYCLE_DEADLINE: Duration = Duration::from_secs(3 * 60);
const CYCLE_CHECK: Duration = Duration::from_secs(5);
static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionStatus {
    pub id: String,
    pub provider: String,
    pub auth_method: String,
    pub auth_state: AuthState,
    pub identity: Option<Identity>,
    pub secret_label: String,
    pub disabled_sources: Vec<String>,
}

/// Whether a connection's tiers may poll.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gate {
    Open,
    /// Paused until the deadline passes.
    Until(Instant),
    /// Idle until `ConnState::resume`.
    Credentials,
    /// Not polled until the connection is rebuilt from config.
    Halted,
}

struct ConnInner {
    auth: AuthState,
    identity: Option<Identity>,
    gate: Gate,
    disabled: BTreeSet<String>,
    /// Server time of each source's last poll attempt this run, success or not.
    attempts: BTreeMap<String, DateTime<Utc>>,
    /// Bumped by `reset_auth`; an auth outcome from a request that began under an older
    /// epoch predates the credential change and is ignored. Lives under the same lock
    /// as the state it guards so check and write are one step.
    epoch: u64,
    /// Sources whose poll is running now.
    in_flight: BTreeSet<String>,
    /// A missing-credentials alert went out; cleared once auth is Ok again.
    missing_alerted: bool,
}

/// Live state of one configured connection, shared by its tier tasks.
pub struct ConnState {
    pub config: ConnectionConfig,
    pub secret_label: String,
    /// None when the provider is unknown or `connect` failed.
    pub connection: Option<Arc<dyn Connection>>,
    /// Unique per prepared state, so cycle slots of a replaced state stay distinguishable.
    generation: u64,
    /// Wakes the tier tasks; paired with `wake_gens` so a busy tier still sees the wake.
    wake: Notify,
    /// Bumped per tier by a wake, indexed by `tier_index`.
    wake_gens: [AtomicU64; 3],
    /// Name and tier of every source, for joining catch-up cycles.
    sources: Vec<(String, Tier)>,
    /// Set once the tier tasks are spawned; `prepare` won't displace a launched state.
    launched: AtomicBool,
    /// Serialises the first identify so the tier tasks don't each call it.
    identify_lock: tokio::sync::Mutex<()>,
    inner: Mutex<ConnInner>,
    /// Notified of status changes; empty until `prepare` attaches it.
    shared: Weak<Shared>,
}

impl ConnState {
    fn new(
        config: ConnectionConfig,
        secret_label: String,
        connection: Option<Arc<dyn Connection>>,
        sources: Vec<(String, Tier)>,
    ) -> Self {
        let gate = if connection.is_some() {
            Gate::Open
        } else {
            Gate::Halted
        };
        Self {
            config,
            secret_label,
            connection,
            generation: NEXT_GENERATION.fetch_add(1, Ordering::Relaxed),
            wake: Notify::new(),
            wake_gens: Default::default(),
            launched: AtomicBool::new(false),
            sources,
            identify_lock: tokio::sync::Mutex::new(()),
            inner: Mutex::new(ConnInner {
                auth: AuthState::Unknown,
                identity: None,
                gate,
                disabled: BTreeSet::new(),
                attempts: BTreeMap::new(),
                epoch: 0,
                in_flight: BTreeSet::new(),
                missing_alerted: false,
            }),
            shared: Weak::new(),
        }
    }

    fn with_shared(mut self, shared: Weak<Shared>) -> Self {
        self.shared = shared;
        self
    }

    /// Call only with no lock held: it reads every connection's state.
    fn notify_status(&self) {
        if let Some(shared) = self.shared.upgrade() {
            shared.publish_statuses();
        }
    }

    fn lock(&self) -> MutexGuard<'_, ConnInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn id(&self) -> &str {
        &self.config.id
    }

    pub fn connection_ref(&self) -> ConnectionRef {
        ConnectionRef {
            id: self.config.id.clone(),
            provider: self.config.provider.clone(),
        }
    }

    pub fn status(&self) -> ConnectionStatus {
        let inner = self.lock();
        ConnectionStatus {
            id: self.config.id.clone(),
            provider: self.config.provider.clone(),
            auth_method: self.config.auth.method.clone(),
            auth_state: inner.auth,
            identity: inner.identity.clone(),
            secret_label: self.secret_label.clone(),
            disabled_sources: inner.disabled.iter().cloned().collect(),
        }
    }

    pub fn identity(&self) -> Option<Identity> {
        self.lock().identity.clone()
    }

    /// True when polling is allowed; an elapsed pause reopens the gate.
    pub fn gate_open(&self) -> bool {
        Self::reopen_if_elapsed(&mut self.lock()) == Gate::Open
    }

    fn reopen_if_elapsed(inner: &mut ConnInner) -> Gate {
        if let Gate::Until(deadline) = inner.gate
            && Instant::now() >= deadline
        {
            inner.gate = Gate::Open;
        }
        inner.gate
    }

    fn halted(&self) -> bool {
        self.lock().gate == Gate::Halted
    }

    /// Closes the gate if it is open (an elapsed pause counts as open, so it is re-armed
    /// here); true if this call closed it. A live pause or credentials idle is never
    /// replaced, except that `Halted` overrides either.
    fn close(&self, gate: Gate) -> bool {
        self.close_if(gate, None)
    }

    /// `close`, but a no-op when `epoch` is set and a credential change has happened since.
    fn close_if(&self, gate: Gate, epoch: Option<u64>) -> bool {
        let mut inner = self.lock();
        if epoch.is_some_and(|e| e < inner.epoch) {
            return false;
        }
        let current = Self::reopen_if_elapsed(&mut inner);
        let overrides = gate == Gate::Halted && current != Gate::Halted;
        if current != Gate::Open && !overrides {
            return false;
        }
        inner.gate = gate;
        drop(inner);
        self.notify_status();
        true
    }

    /// Clears an auth pause and wakes the tier tasks. A halted connection stays halted.
    pub fn resume(&self) {
        let mut inner = self.lock();
        if matches!(inner.gate, Gate::Until(_) | Gate::Credentials) {
            inner.gate = Gate::Open;
        }
        drop(inner);
        self.notify_status();
        self.wake_tiers(&ALL_TIERS, None);
    }

    /// Like `resume` for a manual poll, but a live auth pause stays: only a changed
    /// credential (or the pause elapsing) ends it.
    fn resume_for_poll_now(&self) {
        let mut inner = self.lock();
        if matches!(inner.gate, Gate::Until(d) if Instant::now() < d) {
            return;
        }
        if matches!(inner.gate, Gate::Until(_) | Gate::Credentials) {
            inner.gate = Gate::Open;
        }
        drop(inner);
        self.notify_status();
        self.wake_tiers(&ALL_TIERS, None);
    }

    /// Makes the given tier tasks, except `skip`, poll now even if they are mid-poll.
    fn wake_tiers(&self, tiers: &[Tier], skip: Option<Tier>) {
        for tier in tiers.iter().filter(|t| Some(**t) != skip) {
            self.wake_gens[tier_index(*tier)].fetch_add(1, Ordering::SeqCst);
        }
        self.wake.notify_waiters();
    }

    fn wake_gen(&self, tier: Tier) -> u64 {
        self.wake_gens[tier_index(tier)].load(Ordering::SeqCst)
    }

    /// Forgets the identity and auth state so the next tick identifies again.
    pub fn reset_auth(&self) {
        {
            let mut inner = self.lock();
            inner.epoch += 1;
            inner.auth = AuthState::Unknown;
            inner.identity = None;
        }
        self.notify_status();
    }

    fn auth_epoch(&self) -> u64 {
        self.lock().epoch
    }

    /// False (nothing written) when a credential change happened since `epoch`.
    fn set_auth(&self, auth: AuthState, epoch: u64) -> bool {
        let mut inner = self.lock();
        if epoch < inner.epoch {
            return false;
        }
        inner.auth = auth;
        if auth == AuthState::Ok {
            inner.missing_alerted = false;
        }
        drop(inner);
        self.notify_status();
        true
    }

    /// Sets auth to Missing. None when `epoch` is stale; else true only for the first time
    /// since auth was last Ok, so one lapse alerts once.
    fn mark_missing(&self, epoch: u64) -> Option<bool> {
        let mut inner = self.lock();
        if epoch < inner.epoch {
            return None;
        }
        inner.auth = AuthState::Missing;
        let first = !std::mem::replace(&mut inner.missing_alerted, true);
        drop(inner);
        self.notify_status();
        Some(first)
    }

    /// Sets auth to Invalid. None when `epoch` is stale; else true only when it was not
    /// already Invalid.
    fn mark_invalid(&self, epoch: u64) -> Option<bool> {
        let mut inner = self.lock();
        if epoch < inner.epoch {
            return None;
        }
        let was = inner.auth;
        inner.auth = AuthState::Invalid;
        drop(inner);
        self.notify_status();
        Some(was != AuthState::Invalid)
    }

    /// False (nothing stored) when a credential change happened since `epoch`.
    fn set_identity(&self, identity: Identity, epoch: u64) -> bool {
        let mut inner = self.lock();
        if epoch < inner.epoch {
            return false;
        }
        inner.identity = Some(identity);
        inner.auth = AuthState::Ok;
        inner.missing_alerted = false;
        drop(inner);
        self.notify_status();
        true
    }

    fn mark_ok(&self, epoch: u64) {
        self.set_auth(AuthState::Ok, epoch);
    }

    fn begin_poll(&self, source: &str) {
        self.lock().in_flight.insert(source.to_string());
    }

    fn end_poll(&self, source: &str) {
        self.lock().in_flight.remove(source);
    }

    fn polling(&self, source: &str) -> bool {
        self.lock().in_flight.contains(source)
    }

    /// True if the source was not already disabled.
    fn disable_source(&self, name: &str) -> bool {
        let added = self.lock().disabled.insert(name.to_string());
        self.notify_status();
        added
    }

    pub fn is_disabled(&self, name: &str) -> bool {
        self.lock().disabled.contains(name)
    }

    fn last_attempt(&self, source: &str) -> Option<DateTime<Utc>> {
        self.lock().attempts.get(source).copied()
    }

    fn note_attempt(&self, source: &str, at: DateTime<Utc>) {
        self.lock().attempts.insert(source.to_string(), at);
    }
}

const ALL_TIERS: [Tier; 3] = [Tier::Fast, Tier::Pr, Tier::Slow];

fn tier_index(tier: Tier) -> usize {
    match tier {
        Tier::Fast => 0,
        Tier::Pr => 1,
        Tier::Slow => 2,
    }
}

struct SourceSlot {
    source: Box<dyn Source>,
    /// True until the first successful poll of a start-up launch; that poll is catch-up
    /// when a watermark exists.
    first_poll: bool,
}

/// A connected connection ready to have its tier tasks launched.
pub struct Prepared {
    state: Arc<ConnState>,
    slots: Vec<(Tier, Vec<SourceSlot>)>,
    shared: Arc<Shared>,
    launched: bool,
}

impl Drop for Prepared {
    fn drop(&mut self) {
        if !self.launched {
            self.shared.discard(&self.state);
        }
    }
}

impl Prepared {
    pub fn state(&self) -> &Arc<ConnState> {
        &self.state
    }
}

struct CycleDone {
    events: Vec<Arc<Event>>,
    since: Option<DateTime<Utc>>,
    connection: ConnectionRef,
}

/// Sources whose current poll is catch-up, and the catch-up events they published.
#[derive(Default)]
struct Cycle {
    /// (connection id, source, ConnState generation).
    pending: BTreeSet<(String, String, u64)>,
    /// When the first source joined; reset with the cycle.
    opened: Option<Instant>,
    events: Vec<Arc<Event>>,
    since: Option<DateTime<Utc>>,
    first: Option<ConnectionRef>,
    mixed: bool,
}

impl Cycle {
    /// True if the source was not already pending.
    fn join(&mut self, state: &ConnState, source: &str, since: Option<DateTime<Utc>>) -> bool {
        let conn = state.connection_ref();
        let added = self.pending.insert(Self::key(state, source));
        self.opened.get_or_insert_with(Instant::now);
        self.since = match (self.since, since) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        match &self.first {
            None => self.first = Some(conn),
            Some(first) if first.id != conn.id => self.mixed = true,
            Some(_) => {}
        }
        added
    }

    fn is_pending(&self, state: &ConnState, source: &str) -> bool {
        self.pending.contains(&Self::key(state, source))
    }

    fn key(state: &ConnState, source: &str) -> (String, String, u64) {
        (state.id().to_string(), source.to_string(), state.generation)
    }

    fn record(&mut self, state: &ConnState, source: &str, e: Arc<Event>) {
        if e.catch_up && self.pending.contains(&Self::key(state, source)) {
            self.events.push(e);
        }
    }

    /// True when sources are still pending past the deadline.
    fn expired(&self) -> bool {
        !self.pending.is_empty() && self.opened.is_some_and(|o| o.elapsed() >= CYCLE_DEADLINE)
    }

    /// Marks the source finished; returns the cycle once its last source finishes.
    fn finish(&mut self, state: &ConnState, source: &str) -> Option<CycleDone> {
        if !self.pending.remove(&Self::key(state, source)) || !self.pending.is_empty() {
            return None;
        }
        Some(self.take_done())
    }

    /// Drops the sources of one connection state, leaving any other state with the same
    /// id alone; returns the cycle if that emptied it.
    fn forget(&mut self, state: &ConnState) -> Option<CycleDone> {
        let before = self.pending.len();
        self.pending
            .retain(|(c, _, g)| !(c == state.id() && *g == state.generation));
        if self.pending.len() == before || !self.pending.is_empty() {
            return None;
        }
        Some(self.take_done())
    }

    fn take_done(&mut self) -> CycleDone {
        let cycle = std::mem::take(self);
        let connection = match cycle.first {
            Some(first) if !cycle.mixed => first,
            _ => ConnectionRef {
                id: subject::APP.into(),
                provider: subject::APP.into(),
            },
        };
        CycleDone {
            events: cycle.events,
            since: cycle.since,
            connection,
        }
    }
}

fn severity_rank(s: Severity) -> u8 {
    match s {
        Severity::Critical => 3,
        Severity::Warn => 2,
        Severity::Success => 1,
        Severity::Info => 0,
    }
}

fn system_actor() -> Actor {
    Actor {
        kind: ActorKind::System,
        id: String::new(),
        display_name: "Herald".into(),
        unique_name: None,
    }
}

fn summary_event(done: CycleDone, now: DateTime<Utc>) -> Option<Event> {
    // Only alert events count; pane-level ones are listed but not summarized.
    let shown: Vec<&Arc<Event>> = done.events.iter().filter(|e| e.alert).collect();
    let total = shown.len();
    if total == 0 {
        return None;
    }
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for e in &shown {
        *counts.entry(e.event_type.as_str()).or_default() += 1;
    }
    let mut notable: Vec<&Arc<Event>> = shown
        .iter()
        .copied()
        .filter(|e| severity_rank(e.severity) >= severity_rank(Severity::Warn))
        .collect();
    notable.sort_by_key(|e| std::cmp::Reverse(severity_rank(e.severity)));
    let top: Vec<&str> = notable
        .iter()
        .take(SUMMARY_TOP)
        .map(|e| e.id.as_str())
        .collect();
    let severity = shown
        .iter()
        .map(|e| e.severity)
        .max_by_key(|s| severity_rank(*s))
        .unwrap_or(Severity::Info);
    let since = done.since.unwrap_or(now);
    let mut e = Event::new(
        done.connection,
        event_types::CATCHUP_SUMMARY,
        now,
        now,
        format!("While you were away: {total} events"),
        subject::APP,
    )
    .with_id(ids::catchup(now));
    e.severity = severity;
    e.actor = system_actor();
    e.data = serde_json::json!({
        "counts": counts,
        "total": total,
        "sinceUtc": since.to_rfc3339_opts(SecondsFormat::Millis, true),
        "top": top,
    });
    Some(e)
}

fn panic_message(p: &(dyn std::any::Any + Send)) -> &str {
    p.downcast_ref::<&str>()
        .copied()
        .or_else(|| p.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}

/// Runs `fut`, logging instead of propagating a panic. None if it panicked.
async fn guarded<T>(connection: &str, what: &str, fut: impl Future<Output = T>) -> Option<T> {
    match AssertUnwindSafe(fut).catch_unwind().await {
        Ok(v) => Some(v),
        Err(panic) => {
            tracing::error!(
                connection,
                what,
                panic = panic_message(&*panic),
                "task panicked; retrying next tick"
            );
            None
        }
    }
}

fn gap_exceeds(gap: TimeDelta, period: Duration) -> bool {
    gap > TimeDelta::from_std(period * 3).unwrap_or(TimeDelta::MAX)
}

/// True when `attempt` (or, with none this run, `watermark`) is over three periods old.
fn attempt_stale(
    attempt: Option<DateTime<Utc>>,
    watermark: DateTime<Utc>,
    t0: DateTime<Utc>,
    period: Duration,
) -> bool {
    gap_exceeds(t0 - attempt.unwrap_or(watermark), period)
}

fn tier_period(config: &Config, tier: Tier) -> Duration {
    let secs = match tier {
        Tier::Fast => config.polling.fast_secs,
        Tier::Pr => config.polling.pr_secs,
        Tier::Slow => config.polling.slow_secs,
    };
    Duration::from_secs(secs.max(1))
}

/// A source's own override, else its tier's interval.
fn source_period(config: &Config, name: &str, tier: Tier) -> Duration {
    match config.polling.sources.get(name) {
        Some(secs) => Duration::from_secs((*secs).max(1)),
        None => tier_period(config, tier),
    }
}

struct Shared {
    bus: Arc<Bus>,
    store: Arc<dyn Store>,
    registry: Arc<ProviderRegistry>,
    secrets: Arc<dyn SecretStore>,
    config: watch::Receiver<Arc<Config>>,
    conns: Mutex<BTreeMap<String, Arc<ConnState>>>,
    cycle: Mutex<Cycle>,
    /// Millis of the last summary, so two summaries never share an id.
    last_summary_ms: Mutex<i64>,
    status_tx: watch::Sender<Vec<ConnectionStatus>>,
    /// Orders compute-then-send so a stale snapshot never overwrites a fresher one.
    status_gate: Mutex<()>,
    /// While non-zero, status publishes are skipped (see `StatusHold`).
    status_holds: AtomicUsize,
    /// Runs once between `join_stale`'s watermark reads and its join.
    #[cfg(test)]
    after_reads: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl Shared {
    fn config(&self) -> Arc<Config> {
        self.config.borrow().clone()
    }

    fn conns(&self) -> MutexGuard<'_, BTreeMap<String, Arc<ConnState>>> {
        self.conns.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn cycle(&self) -> MutexGuard<'_, Cycle> {
        self.cycle.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Sends fresh statuses to subscribers if they changed. Call with no state lock held.
    fn publish_statuses(&self) {
        if self.status_holds.load(Ordering::SeqCst) > 0 {
            return;
        }
        let _order = self
            .status_gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let fresh: Vec<ConnectionStatus> = self.conns().values().map(|c| c.status()).collect();
        self.status_tx.send_if_modified(|current| {
            if *current == fresh {
                return false;
            }
            *current = fresh;
            true
        });
    }

    fn rebuild_classifier(&self) {
        let identities: BTreeMap<String, Identity> = self
            .conns()
            .iter()
            .filter_map(|(id, c)| c.identity().map(|i| (id.clone(), i)))
            .collect();
        self.bus
            .set_classifier(Classifier::new(&self.config(), &identities));
    }

    fn system_event(conn: &ConnState, event_type: &str, id: String, title: String) -> Event {
        let now = Utc::now();
        let mut e = Event::new(
            conn.connection_ref(),
            event_type,
            now,
            now,
            title,
            subject::connection(conn.id()),
        )
        .with_id(id);
        e.actor = system_actor();
        e
    }

    async fn finish_source(&self, conn: &ConnState, source: &str) {
        let done = self.cycle().finish(conn, source);
        self.publish_summary(done).await;
    }

    async fn forget_connection(&self, conn: &ConnState) {
        let done = self.cycle().forget(conn);
        self.publish_summary(done).await;
    }

    /// Removes every trace of a state that never launched. Runs from `Drop`, so the
    /// summary (if any) is published on a spawned task.
    fn discard(self: &Arc<Self>, state: &Arc<ConnState>) {
        {
            let mut conns = self.conns();
            if conns.get(state.id()).is_some_and(|c| Arc::ptr_eq(c, state)) {
                conns.remove(state.id());
            }
        }
        let done = self.cycle().forget(state);
        if done.is_some() {
            match tokio::runtime::Handle::try_current() {
                Ok(rt) => {
                    let shared = self.clone();
                    rt.spawn(async move { shared.publish_summary(done).await });
                }
                Err(_) => tracing::warn!("no runtime to publish a catch-up summary"),
            }
        }
        self.publish_statuses();
    }

    /// Summarises and resets a cycle that has stayed open past its deadline.
    async fn expire_cycle(&self) {
        let done = {
            let mut cycle = self.cycle();
            if !cycle.expired() {
                return;
            }
            let slots: Vec<String> = cycle
                .pending
                .iter()
                .map(|(c, s, _)| format!("{c}:{s}"))
                .collect();
            tracing::warn!(
                ?slots,
                "catch-up cycle deadline passed; summarising what was recorded"
            );
            cycle.take_done()
        };
        self.publish_summary(Some(done)).await;
    }

    async fn run_cycle_watchdog(self: Arc<Self>) {
        let mut interval = tokio::time::interval(CYCLE_CHECK);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            self.expire_cycle().await;
        }
    }

    async fn publish_summary(&self, done: Option<CycleDone>) {
        let Some(done) = done else { return };
        let now = {
            let mut last = self
                .last_summary_ms
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let ms = Utc::now().timestamp_millis().max(*last + 1);
            *last = ms;
            DateTime::from_timestamp_millis(ms).unwrap_or_else(Utc::now)
        };
        if let Some(summary) = summary_event(done, now) {
            self.bus.publish(summary).await;
        }
    }

    /// Joins every non-disabled source, of every connection, whose last poll attempt (or,
    /// with none this run, whose watermark) is older than three of its own tier intervals
    /// at `t0` into the current cycle, then wakes
    /// the tiers that gained a source. `caller` is polling already, so its tier is not woken.
    async fn join_stale(&self, t0: DateTime<Utc>, caller: (&str, Tier)) {
        let cfg = self.config();
        let candidates: Vec<(Arc<ConnState>, String, Tier)> = self
            .conns()
            .values()
            .flat_map(|conn| {
                conn.sources
                    .iter()
                    .map(|(name, tier)| (conn.clone(), name.clone(), *tier))
            })
            .collect();
        // Watermarks are read before the locks below, which must not be held across an await.
        let mut stored = Vec::new();
        for (conn, name, tier) in candidates {
            // A hung poll would be re-joined every pass; its own finish closes its slot.
            if conn.is_disabled(&name) || conn.polling(&name) {
                continue;
            }
            let key = format!("{}:{name}", conn.id());
            match self.store.get_watermark(&key).await {
                Ok(Some(w)) => stored.push((conn, name, tier, w)),
                Ok(None) => {}
                Err(e) => tracing::warn!(key, error = %e, "watermark read failed"),
            }
        }
        #[cfg(test)]
        if let Some(hook) = self
            .after_reads
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            hook();
        }
        let mut woken: Vec<(Arc<ConnState>, Vec<Tier>)> = Vec::new();
        {
            // Held throughout so a concurrent stop + forget can't leave slots for dead
            // tasks. Lock order: conns, then ConnState inner, then cycle.
            let conns = self.conns();
            for (conn, name, tier, w) in stored {
                let registered = conns.get(conn.id()).is_some_and(|c| Arc::ptr_eq(c, &conn));
                if !registered || conn.is_disabled(&name) || conn.polling(&name) {
                    continue;
                }
                if attempt_stale(
                    conn.last_attempt(&name),
                    w.last_success_utc,
                    t0,
                    source_period(&cfg, &name, tier),
                ) && self.cycle().join(&conn, &name, Some(w.last_success_utc))
                {
                    match woken.iter_mut().find(|(c, _)| Arc::ptr_eq(c, &conn)) {
                        Some((_, tiers)) if !tiers.contains(&tier) => tiers.push(tier),
                        Some(_) => {}
                        None => woken.push((conn, vec![tier])),
                    }
                }
            }
        }
        for (conn, tiers) in woken {
            let skip = (conn.id() == caller.0).then_some(caller.1);
            conn.wake_tiers(&tiers, skip);
        }
    }

    /// Applies the shared error mapping. `source` is set for errors from a poll; `epoch`
    /// is the auth epoch when the failed request began.
    async fn handle_error(
        &self,
        conn: &ConnState,
        source: Option<&str>,
        epoch: u64,
        err: ProviderError,
    ) {
        let id = conn.id();
        let auth_failure = matches!(
            err,
            ProviderError::Unauthorized | ProviderError::MissingCredentials
        );
        if auth_failure && epoch < conn.auth_epoch() {
            tracing::debug!(
                connection = id,
                "ignoring auth failure from before a credential change"
            );
            return;
        }
        match err {
            ProviderError::Unauthorized => {
                // An elapsed pause is re-armed by the next 401; the event fires only on the transition.
                let Some(first) = conn.mark_invalid(epoch) else {
                    return;
                };
                conn.close_if(Gate::Until(Instant::now() + AUTH_PAUSE), Some(epoch));
                if first {
                    tracing::warn!(connection = id, "authentication failed; pausing connection");
                    let mut e = Self::system_event(
                        conn,
                        event_types::AUTH_FAILED,
                        ids::auth_failed(id, Utc::now()),
                        format!("Authentication failed for {id}"),
                    );
                    e.data = serde_json::json!({ "reason": "unauthorized" });
                    self.bus.publish(e).await;
                }
            }
            ProviderError::MissingCredentials => {
                let Some(first) = conn.mark_missing(epoch) else {
                    return;
                };
                conn.close_if(Gate::Credentials, Some(epoch));
                tracing::info!(connection = id, "no credentials; idle until they change");
                if first {
                    let mut e = Self::system_event(
                        conn,
                        event_types::AUTH_FAILED,
                        ids::auth_failed(id, Utc::now()),
                        format!("Sign-in needed for {id}"),
                    );
                    e.subtitle = Some("Sign in again, then choose Poll now from the tray".into());
                    e.data = serde_json::json!({ "reason": "missing" });
                    self.bus.publish(e).await;
                }
            }
            ProviderError::Config(msg) => {
                conn.close(Gate::Halted);
                tracing::error!(connection = id, error = %msg, "configuration error; not polling until reload");
            }
            ProviderError::Forbidden(reason) => match source {
                Some(name) => {
                    if conn.disable_source(name) {
                        tracing::warn!(connection = id, source = name, reason = %reason, "source disabled");
                        let mut e = Self::system_event(
                            conn,
                            event_types::SOURCE_DISABLED,
                            ids::source_disabled(id, name, Utc::now()),
                            format!("Source {name} disabled for {id}"),
                        );
                        e.data = serde_json::json!({ "source": name, "reason": reason });
                        self.bus.publish(e).await;
                    }
                }
                None => tracing::warn!(connection = id, error = %reason, "forbidden"),
            },
            ProviderError::Throttled { retry_after } => {
                tracing::warn!(connection = id, ?retry_after, "throttled");
                tokio::time::sleep(retry_after).await;
            }
            other => {
                tracing::warn!(connection = id, error = %other, "request failed; retrying next tick");
            }
        }
    }

    /// Fetches the identity and swaps in a rebuilt classifier; false on error.
    async fn identify(&self, conn: &ConnState, connection: &Arc<dyn Connection>) -> bool {
        let epoch = conn.auth_epoch();
        match connection.identify().await {
            Ok(identity) => {
                // Credentials changed mid-flight: the identity is the old account's.
                if !conn.set_identity(identity, epoch) {
                    return false;
                }
                self.rebuild_classifier();
                true
            }
            Err(e) => {
                self.handle_error(conn, None, epoch, e).await;
                false
            }
        }
    }

    /// The identity if the connection may poll now, identifying first when needed; the
    /// flag is true when this call identified.
    async fn ready(
        &self,
        conn: &ConnState,
        connection: &Arc<dyn Connection>,
    ) -> Option<(Identity, bool)> {
        if !conn.gate_open() {
            return None;
        }
        if let Some(me) = conn.identity() {
            return Some((me, false));
        }
        let _first = conn.identify_lock.lock().await;
        // Another tier may have identified, or failed and paused, while this one waited.
        if let Some(me) = conn.identity() {
            return Some((me, false));
        }
        if !conn.gate_open() || !self.identify(conn, connection).await {
            return None;
        }
        conn.identity().map(|me| (me, true))
    }

    /// Connects without any network call. A start-up prepare registers every source
    /// with a stored watermark in the start-up cycle, since its first poll is catch-up.
    async fn prepare(self: &Arc<Self>, config: ConnectionConfig, startup: bool) -> Prepared {
        let factory = self.registry.get(&config.provider);
        let label = factory
            .as_ref()
            .map(|f| f.secret_label().to_string())
            .unwrap_or_default();
        let connection = match &factory {
            None => {
                tracing::error!(connection = %config.id, provider = %config.provider, "unknown provider");
                None
            }
            Some(f) => match f.connect(&config, self.secrets.clone()) {
                Ok(c) => Some(c),
                Err(e) => {
                    tracing::error!(connection = %config.id, error = %e, "connect failed");
                    None
                }
            },
        };
        let sources = connection.as_ref().map(|c| c.sources()).unwrap_or_default();
        let listed = sources
            .iter()
            .map(|s| (s.name().to_string(), s.tier()))
            .collect();
        // A stored watermark means the first poll is catch-up, so the start-up cycle must wait
        // for the source. Read before registering: a cancelled start must not leave state behind.
        let mut marks = Vec::new();
        if startup {
            let names: Vec<String> = sources.iter().map(|s| s.name().to_string()).collect();
            for name in names {
                marks.push(self.watermark(&config.id, &name).await);
            }
        }
        let state = Arc::new(
            ConnState::new(config, label, connection, listed).with_shared(Arc::downgrade(self)),
        );
        {
            let mut conns = self.conns();
            // Reload stops first; this keeps a live launched state registered if not.
            if conns
                .get(state.id())
                .is_some_and(|c| c.launched.load(Ordering::SeqCst))
            {
                tracing::error!(connection = %state.id(), "prepare while launched; not replacing it");
            } else {
                conns.insert(state.id().to_string(), state.clone());
            }
        }
        self.publish_statuses();
        let mut slots: Vec<(Tier, Vec<SourceSlot>)> = Vec::new();
        let mut marks = marks.into_iter();
        for source in sources {
            let tier = source.tier();
            if let Some(Some(w)) = marks.next() {
                self.cycle().join(&state, source.name(), Some(w));
            }
            let slot = SourceSlot {
                source,
                first_poll: startup,
            };
            match slots.iter_mut().find(|(t, _)| *t == tier) {
                Some((_, v)) => v.push(slot),
                None => slots.push((tier, vec![slot])),
            }
        }
        Prepared {
            state,
            slots,
            shared: self.clone(),
            launched: false,
        }
    }

    /// The stored success time for a source, if any.
    async fn watermark(&self, conn_id: &str, source: &str) -> Option<DateTime<Utc>> {
        let key = format!("{conn_id}:{source}");
        match self.store.get_watermark(&key).await {
            Ok(w) => w.map(|w| w.last_success_utc),
            Err(e) => {
                tracing::warn!(key, error = %e, "watermark read failed");
                None
            }
        }
    }

    async fn run_tier(
        self: Arc<Self>,
        conn: Arc<ConnState>,
        connection: Arc<dyn Connection>,
        tier: Tier,
        mut slots: Vec<SourceSlot>,
    ) {
        let cref = conn.connection_ref();
        let mut config = self.config.clone();
        // When each source (and the slow tier's identity refresh) last started a period;
        // None polls at once. Due time is this plus the interval in the current config,
        // so a reload retimes the next poll without restarting the task.
        let mut started: Vec<Option<Instant>> = vec![None; slots.len()];
        let mut refresh_started: Option<Instant> = None;
        let mut seen = 0;
        let mut config_open = true;
        loop {
            // Enabled before the generation check so a wake in between is not missed.
            let notified = conn.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let cfg = config.borrow_and_update().clone();
            let gen_now = conn.wake_gen(tier);
            let woke = gen_now != seen;
            let now = Instant::now();
            let slot_due: Vec<Option<Instant>> = slots
                .iter()
                .zip(&started)
                .map(|(s, b)| b.map(|b| b + source_period(&cfg, s.source.name(), tier)))
                .collect();
            let refresh_due =
                (tier == Tier::Slow).then(|| refresh_started.map(|b| b + tier_period(&cfg, tier)));
            // A source another pass joined to the open catch-up cycle must poll now, or
            // the cycle waits on it until the deadline.
            let pending: Vec<bool> = {
                let cycle = self.cycle();
                slots
                    .iter()
                    .map(|s| cycle.is_pending(&conn, s.source.name()))
                    .collect()
            };
            let is_due = |d: Option<Instant>| woke || d.is_none_or(|d| d <= now);
            let slot_is_due: Vec<bool> = slot_due
                .iter()
                .zip(&pending)
                .map(|(d, p)| *p || is_due(*d))
                .collect();
            let any_due = slot_is_due.iter().any(|d| *d) || refresh_due.is_some_and(is_due);
            if !any_due {
                let next = slot_due
                    .iter()
                    .flatten()
                    .chain(refresh_due.iter().flatten())
                    .min()
                    .copied();
                // A wake aimed at another tier or a config change also lands here.
                tokio::select! {
                    _ = async {
                        match next {
                            Some(t) => tokio::time::sleep_until(t).await,
                            None => std::future::pending().await,
                        }
                    } => {}
                    _ = &mut notified => {}
                    changed = config.changed(), if config_open => {
                        config_open = changed.is_ok();
                    }
                }
                continue;
            }
            seen = gen_now;
            let due = slot_is_due;
            let refresh = refresh_due.is_some_and(is_due);
            let ready = guarded(&cref.id, "identify", self.ready(&conn, &connection))
                .await
                .flatten();
            if let Some((me, identified)) = ready {
                for (slot, _) in slots.iter_mut().zip(&due).filter(|(_, d)| **d) {
                    let name = slot.source.name();
                    if conn.gate_open() && !conn.is_disabled(name) {
                        let period = source_period(&cfg, name, tier);
                        conn.begin_poll(name);
                        let poll = self.poll_source(&conn, &connection, slot, &me, period);
                        guarded(&cref.id, name, poll).await;
                        conn.end_poll(name);
                    }
                    self.finish_source(&conn, name).await;
                }
                if refresh && !identified && conn.gate_open() {
                    guarded(&cref.id, "identify", self.identify(&conn, &connection)).await;
                }
            } else {
                let names: Vec<String> = slots
                    .iter()
                    .zip(&due)
                    .filter(|(_, d)| **d)
                    .map(|(slot, _)| slot.source.name().to_string())
                    .collect();
                for name in names {
                    self.finish_source(&conn, &name).await;
                }
            }
            // A wake-driven pass restarts every period from its end, so the tick that
            // would have followed the wake does not poll again at once.
            let base = if woke { Instant::now() } else { now };
            for (b, _) in started.iter_mut().zip(&due).filter(|(_, d)| **d) {
                *b = Some(base);
            }
            if refresh {
                refresh_started = Some(base);
            }
        }
    }

    /// One poll of one source. The caller guards panics and finishes the source in the cycle.
    async fn poll_source(
        &self,
        conn: &ConnState,
        connection: &Arc<dyn Connection>,
        slot: &mut SourceSlot,
        me: &Identity,
        period: Duration,
    ) {
        let name = slot.source.name();
        let cref = conn.connection_ref();
        let epoch = conn.auth_epoch();
        let t0 = match connection.server_time().await {
            Ok(t) => t,
            Err(e) => {
                self.handle_error(conn, None, epoch, e).await;
                return;
            }
        };
        let key = format!("{}:{name}", cref.id);
        let stored = match self.store.get_watermark(&key).await {
            Ok(w) => w,
            Err(e) => {
                tracing::warn!(key, error = %e, "watermark read failed");
                return;
            }
        };
        let (watermark, catch_up) = match stored {
            None => (None, false),
            Some(w) => {
                // Judged by attempts, not successes, so failing sources can't wake each other forever.
                if attempt_stale(conn.last_attempt(name), w.last_success_utc, t0, period) {
                    // A wake: the other sources are just as stale and share one summary.
                    self.join_stale(t0, (cref.id.as_str(), slot.source.tier()))
                        .await;
                }
                let gap = gap_exceeds(t0 - w.last_success_utc, period);
                (Some(w.last_success_utc), slot.first_poll || gap)
            }
        };
        conn.note_attempt(name, t0);
        if catch_up {
            self.cycle().join(conn, name, watermark);
        }
        let ctx = SyncContext {
            connection: cref.clone(),
            me: me.clone(),
            config: self.config(),
            connection_config: conn.config.clone(),
            snapshots: ScopedSnapshots::new(self.store.clone(), &cref.id),
            server_now: t0,
            watermark,
            catch_up,
        };
        match slot.source.poll(&ctx).await {
            Err(e) => self.handle_error(conn, Some(name), epoch, e).await,
            Ok(outcome) => {
                conn.mark_ok(epoch);
                let mut store_failed = false;
                for event in outcome.events {
                    match self.bus.try_publish(event).await {
                        Ok(Some(published)) => self.cycle().record(conn, name, published),
                        Ok(None) => {}
                        Err(_) => store_failed = true,
                    }
                }
                // An unsaved event would be skipped by the next window.
                if store_failed {
                    tracing::warn!(key, "event persist failed; watermark not advanced");
                    return;
                }
                if let Err(e) = self.store.set_watermark(&key, t0, t0).await {
                    tracing::warn!(key, error = %e, "watermark write failed");
                }
                slot.first_poll = false;
            }
        }
    }

    async fn run_housekeeping(self: Arc<Self>) {
        let mut watch = self.config.clone();
        let mut watch_open = true;
        let mut last = Instant::now();
        let mut last_prune: Option<Instant> = None;
        loop {
            // Re-read each turn so a changed slow_secs retimes the next rebuild.
            let period = tier_period(&watch.borrow_and_update(), Tier::Slow);
            tokio::select! {
                _ = tokio::time::sleep_until(last + period) => {}
                changed = watch.changed(), if watch_open => {
                    watch_open = changed.is_ok();
                    continue;
                }
            }
            last = Instant::now();
            let config = self.config();
            let local = config.local.clone();
            let connections = config.connections.clone();
            let registry = self.registry.clone();
            match tokio::task::spawn_blocking(move || {
                LocalIndex::build(&local, &connections, &registry)
            })
            .await
            {
                Ok(index) => self.bus.set_local_index(index),
                Err(e) => tracing::error!(error = %e, "local index rebuild failed"),
            }
            if last_prune.is_none_or(|t| t.elapsed() >= PRUNE_EVERY) {
                match self.store.prune(PRUNE_DAYS).await {
                    Ok(()) => last_prune = Some(Instant::now()),
                    Err(e) => tracing::warn!(error = %e, "prune failed"),
                }
            }
        }
    }
}

/// Guard from `Scheduler::hold_statuses`.
pub struct StatusHold<'a> {
    shared: &'a Shared,
}

impl Drop for StatusHold<'_> {
    fn drop(&mut self) {
        self.shared.status_holds.fetch_sub(1, Ordering::SeqCst);
        self.shared.publish_statuses();
    }
}

/// Owns the polling tasks. Per-connection state lives in `ConnState`.
pub struct Scheduler {
    shared: Arc<Shared>,
    tasks: Mutex<BTreeMap<String, Vec<JoinHandle<()>>>>,
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        let tasks = self.tasks.get_mut().unwrap_or_else(PoisonError::into_inner);
        for h in tasks.values().flatten() {
            h.abort();
        }
    }
}

impl Scheduler {
    /// Connects every enabled connection and starts polling; identifying happens lazily
    /// in the tier tasks, so this never waits on the provider.
    pub async fn start(
        bus: Arc<Bus>,
        store: Arc<dyn Store>,
        registry: Arc<ProviderRegistry>,
        secrets: Arc<dyn SecretStore>,
        config: watch::Receiver<Arc<Config>>,
    ) -> Scheduler {
        let shared = Arc::new(Shared {
            bus,
            store,
            registry,
            secrets,
            config,
            conns: Mutex::new(BTreeMap::new()),
            cycle: Mutex::new(Cycle::default()),
            last_summary_ms: Mutex::new(0),
            status_tx: watch::channel(Vec::new()).0,
            status_gate: Mutex::new(()),
            status_holds: AtomicUsize::new(0),
            #[cfg(test)]
            after_reads: Mutex::new(None),
        });
        let scheduler = Scheduler {
            shared: shared.clone(),
            tasks: Mutex::new(BTreeMap::new()),
        };
        let cfg = shared.config();
        // Every connection registers its catch-up sources before any tier polls.
        let mut prepared: Vec<Prepared> = Vec::new();
        for c in cfg.connections.iter().filter(|c| c.enabled) {
            prepared.push(shared.prepare(c.clone(), true).await);
        }
        for p in prepared {
            scheduler.launch(p);
        }
        shared.publish_statuses();
        let housekeeping = tokio::spawn(shared.clone().run_housekeeping());
        let watchdog = tokio::spawn(shared.run_cycle_watchdog());
        scheduler.add_tasks(HOUSEKEEPING, vec![housekeeping]);
        scheduler.add_tasks(WATCHDOG, vec![watchdog]);
        scheduler
    }

    /// Connects one connection without polling it (see `launch`). Only a `startup`
    /// prepare makes the first poll catch-up; a reload's first poll is a normal one.
    pub async fn prepare(&self, config: ConnectionConfig, startup: bool) -> Prepared {
        self.shared.prepare(config, startup).await
    }

    /// Spawns the tier tasks of a prepared connection. A halted connection gets none.
    pub fn launch(&self, prepared: Prepared) {
        let mut prepared = prepared;
        let displaced = self
            .shared
            .conns()
            .get(prepared.state.id())
            .is_some_and(|c| !Arc::ptr_eq(c, &prepared.state));
        if displaced {
            // Dropping it clears its cycle slots; the registered state is untouched.
            return;
        }
        prepared.launched = true;
        prepared.state.launched.store(true, Ordering::SeqCst);
        let state = prepared.state.clone();
        let mut slots = std::mem::take(&mut prepared.slots);
        let Some(connection) = state.connection.clone() else {
            return;
        };
        if state.halted() {
            return;
        }
        // The slow tier always runs: it refreshes the identity.
        if !slots.iter().any(|(t, _)| *t == Tier::Slow) {
            slots.push((Tier::Slow, Vec::new()));
        }
        let handles = slots
            .into_iter()
            .map(|(tier, tier_slots)| {
                tokio::spawn(self.shared.clone().run_tier(
                    state.clone(),
                    connection.clone(),
                    tier,
                    tier_slots,
                ))
            })
            .collect();
        self.add_tasks(state.id(), handles);
    }

    /// Aborts a connection's tasks and forgets its state.
    pub async fn stop_connection(&self, id: &str) {
        let handles = self.tasks().remove(id).unwrap_or_default();
        for h in &handles {
            h.abort();
        }
        for h in handles {
            let _ = h.await;
        }
        let removed = self.shared.conns().remove(id);
        if let Some(state) = removed {
            self.shared.forget_connection(&state).await;
        }
        self.shared.rebuild_classifier();
        self.shared.publish_statuses();
    }

    pub fn rebuild_classifier(&self) {
        self.shared.rebuild_classifier();
    }

    /// True when a live connection cannot recover on its own: it never connected, is
    /// halted by a config error, or has a disabled source. A reload rebuilds it.
    pub fn needs_rebuild(&self, id: &str) -> bool {
        self.connection(id).is_none_or(|c| {
            c.connection.is_none() || c.halted() || !c.status().disabled_sources.is_empty()
        })
    }

    /// Wakes every tier of every connection.
    pub fn poll_now(&self) {
        let states: Vec<Arc<ConnState>> = self.shared.conns().values().cloned().collect();
        for state in states {
            state.resume_for_poll_now();
        }
    }

    /// Suppresses status publishes until the guard drops, which then publishes once.
    /// Holds nest.
    pub fn hold_statuses(&self) -> StatusHold<'_> {
        self.shared.status_holds.fetch_add(1, Ordering::SeqCst);
        StatusHold {
            shared: &self.shared,
        }
    }

    pub fn status_changes(&self) -> watch::Receiver<Vec<ConnectionStatus>> {
        self.shared.status_tx.subscribe()
    }

    pub fn connection(&self, id: &str) -> Option<Arc<ConnState>> {
        self.shared.conns().get(id).cloned()
    }

    pub fn statuses(&self) -> Vec<ConnectionStatus> {
        self.shared.conns().values().map(|c| c.status()).collect()
    }

    pub async fn shutdown(self) {
        let all: Vec<JoinHandle<()>> = std::mem::take(&mut *self.tasks())
            .into_values()
            .flatten()
            .collect();
        for h in &all {
            h.abort();
        }
        for h in all {
            let _ = h.await;
        }
    }

    fn tasks(&self) -> MutexGuard<'_, BTreeMap<String, Vec<JoinHandle<()>>>> {
        self.tasks.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn add_tasks(&self, key: &str, handles: Vec<JoinHandle<()>>) {
        self.tasks()
            .entry(key.to_string())
            .or_default()
            .extend(handles);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{PollOutcome, ProviderFactory};
    use crate::secrets::MemorySecretStore;
    use crate::store::SqliteStore;
    use crate::testing::{FakeProvider, FakeProviderFactory};
    use futures_util::future::BoxFuture;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::broadcast;

    fn t0() -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000, 0).unwrap()
    }

    fn secs(s: i64) -> TimeDelta {
        TimeDelta::seconds(s)
    }

    async fn pass(s: u64) {
        tokio::time::sleep(Duration::from_secs(s)).await;
    }

    fn config() -> Config {
        config_with(&[("c1", "fake")])
    }

    fn config_with(conns: &[(&str, &str)]) -> Config {
        let mut cfg = Config::from_toml_str(Config::default_toml()).unwrap();
        let toml: String = conns
            .iter()
            .map(|(id, provider)| {
                format!(
                    "[[connections]]\nid = \"{id}\"\nprovider = \"{provider}\"\nauth = {{ method = \"pat\" }}\n"
                )
            })
            .collect();
        cfg.connections = toml::from_str::<Config>(&toml).unwrap().connections;
        cfg.polling.fast_secs = 10;
        cfg.polling.pr_secs = 20;
        cfg.polling.slow_secs = 100;
        cfg
    }

    struct Harness {
        store: Arc<dyn Store>,
        sched: Scheduler,
        rx: broadcast::Receiver<Arc<Event>>,
        _config_tx: watch::Sender<Arc<Config>>,
    }

    impl Harness {
        async fn start(fake: &FakeProvider) -> Harness {
            Self::start_with(Arc::new(FakeProviderFactory::new(fake.clone())), None).await
        }

        async fn start_with(
            factory: Arc<dyn ProviderFactory>,
            store: Option<Arc<dyn Store>>,
        ) -> Harness {
            Self::build(vec![factory], store, config()).await
        }

        async fn build(
            factories: Vec<Arc<dyn ProviderFactory>>,
            store: Option<Arc<dyn Store>>,
            cfg: Config,
        ) -> Harness {
            let store = store.unwrap_or_else(|| Arc::new(SqliteStore::open_in_memory().unwrap()));
            let bus = Arc::new(Bus::new(
                store.clone(),
                Classifier::new(&cfg, &BTreeMap::new()),
                LocalIndex::empty(),
            ));
            let rx = bus.subscribe();
            let (config_tx, config_rx) = watch::channel(Arc::new(cfg));
            let sched = Scheduler::start(
                bus,
                store.clone(),
                Arc::new(ProviderRegistry::new(factories)),
                Arc::new(MemorySecretStore::new()),
                config_rx,
            )
            .await;
            Harness {
                store,
                sched,
                rx,
                _config_tx: config_tx,
            }
        }

        fn drain(&mut self) -> Vec<Arc<Event>> {
            let mut out = Vec::new();
            while let Ok(e) = self.rx.try_recv() {
                out.push(e);
            }
            out
        }
    }

    fn fake_with(sources: &[(&'static str, Tier)]) -> FakeProvider {
        let fake = FakeProvider::new();
        for (name, tier) in sources {
            fake.add_source(name, *tier);
        }
        fake.set_server_time(t0());
        fake
    }

    async fn store_with_watermark(key: &str, at: DateTime<Utc>) -> Arc<dyn Store> {
        store_with_watermarks(&[key], at).await
    }

    async fn store_with_watermarks(keys: &[&str], at: DateTime<Utc>) -> Arc<dyn Store> {
        let store: Arc<dyn Store> = Arc::new(SqliteStore::open_in_memory().unwrap());
        for key in keys {
            store.set_watermark(key, at, at).await.unwrap();
        }
        store
    }

    fn ev(id: &str, event_type: &str, catch_up: bool) -> Event {
        let mut e = Event::new(
            ConnectionRef {
                id: "c1".into(),
                provider: "fake".into(),
            },
            event_type,
            t0(),
            t0(),
            "title",
            "c1:connection",
        )
        .with_id(id);
        e.catch_up = catch_up;
        e
    }

    fn ok(events: Vec<Event>) -> Result<PollOutcome, ProviderError> {
        Ok(PollOutcome { events })
    }

    fn of_type<'a>(events: &'a [Arc<Event>], t: &str) -> Vec<&'a Arc<Event>> {
        events.iter().filter(|e| e.event_type == t).collect()
    }

    #[tokio::test(start_paused = true)]
    async fn first_run_seeds_then_polls_normally() {
        let fake = fake_with(&[("s", Tier::Fast)]);
        let mut h = Harness::start(&fake).await;
        pass(1).await;

        let calls = fake.poll_calls("s");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].watermark, None);
        assert!(!calls[0].catch_up);
        assert_eq!(calls[0].server_now, t0());
        assert!(h.drain().is_empty());
        let wm = h.store.get_watermark("c1:s").await.unwrap().unwrap();
        assert_eq!((wm.last_synced_utc, wm.last_success_utc), (t0(), t0()));

        fake.set_server_time(t0() + secs(10));
        fake.push_poll(
            "s",
            ok(vec![ev("c1:run:1:completed", "run.completed", false)]),
        );
        pass(10).await;

        let calls = fake.poll_calls("s");
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[1].watermark, Some(t0()));
        assert!(!calls[1].catch_up);
        assert_eq!(h.drain().len(), 1);
        let wm = h.store.get_watermark("c1:s").await.unwrap().unwrap();
        assert_eq!(wm.last_success_utc, t0() + secs(10));
    }

    #[tokio::test(start_paused = true)]
    async fn restart_catches_up_and_summarises_once() {
        let fake = fake_with(&[("s", Tier::Fast)]);
        let since = t0() - secs(3600);
        fake.push_poll(
            "s",
            ok(vec![
                ev("c1:deploy:1", "deploy.failed", true),
                ev("c1:pr:r:1:created", "pr.created", true),
            ]),
        );
        let mut h = Harness::start_with(
            Arc::new(FakeProviderFactory::new(fake.clone())),
            Some(store_with_watermark("c1:s", since).await),
        )
        .await;
        pass(60).await;

        let calls = fake.poll_calls("s");
        assert!(calls[0].catch_up);
        assert_eq!(calls[0].watermark, Some(since));
        assert!(calls.iter().skip(1).all(|c| !c.catch_up));
        let events = h.drain();
        let summaries = of_type(&events, "catchup.summary");
        assert_eq!(summaries.len(), 1);
        let s = summaries[0];
        assert!(!s.catch_up);
        assert_eq!(s.actor.kind, ActorKind::System);
        assert_eq!(s.severity, Severity::Critical);
        assert_eq!(s.title, "While you were away: 2 events");
        assert_eq!(s.subject_key, "app");
        assert_eq!(s.connection.id, "c1");
        assert_eq!(s.data["total"], 2);
        assert_eq!(s.data["counts"]["deploy.failed"], 1);
        assert_eq!(s.data["counts"]["pr.created"], 1);
        assert_eq!(s.data["top"], serde_json::json!(["c1:deploy:1"]));
        assert_eq!(
            s.data["sinceUtc"],
            since.to_rfc3339_opts(SecondsFormat::Millis, true)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn no_summary_when_catch_up_found_nothing() {
        let fake = fake_with(&[("s", Tier::Fast)]);
        let mut h = Harness::start_with(
            Arc::new(FakeProviderFactory::new(fake.clone())),
            Some(store_with_watermark("c1:s", t0() - secs(3600)).await),
        )
        .await;
        pass(1).await;

        assert!(fake.poll_calls("s")[0].catch_up);
        assert!(h.drain().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn gap_over_three_intervals_is_catch_up() {
        let fake = fake_with(&[("s", Tier::Fast)]);
        let _h = Harness::start(&fake).await;
        pass(1).await;
        fake.set_server_time(t0() + secs(31));
        pass(10).await;
        fake.set_server_time(t0() + secs(41));
        pass(10).await;

        let calls = fake.poll_calls("s");
        assert_eq!(calls.len(), 3);
        assert!(calls[1].catch_up);
        assert!(!calls[2].catch_up);
    }

    #[tokio::test(start_paused = true)]
    async fn unauthorized_pauses_connection_and_publishes_one_auth_failed() {
        let fake = fake_with(&[("a", Tier::Fast), ("b", Tier::Pr)]);
        fake.push_poll("a", Err(ProviderError::Unauthorized));
        let mut h = Harness::start(&fake).await;
        pass(5).await;

        let events = h.drain();
        let failed = of_type(&events, "auth.failed");
        assert_eq!(failed.len(), 1);
        assert!(failed[0].id.starts_with("c1:auth:failed:"));
        assert_eq!(failed[0].severity, Severity::Critical);
        assert_eq!(failed[0].actor.kind, ActorKind::System);
        assert_eq!(h.sched.statuses()[0].auth_state, AuthState::Invalid);

        let (a, b) = (fake.poll_count("a"), fake.poll_count("b"));
        pass(600).await;
        assert_eq!((fake.poll_count("a"), fake.poll_count("b")), (a, b));
        assert!(of_type(&h.drain(), "auth.failed").is_empty());

        pass(400).await;
        assert!(fake.poll_count("a") > a);
        assert_eq!(h.sched.statuses()[0].auth_state, AuthState::Ok);
    }

    #[tokio::test(start_paused = true)]
    async fn forbidden_disables_only_that_source() {
        let fake = fake_with(&[("a", Tier::Fast), ("b", Tier::Fast)]);
        fake.push_poll("a", Err(ProviderError::Forbidden("no access".into())));
        let mut h = Harness::start(&fake).await;
        pass(50).await;

        assert_eq!(fake.poll_count("a"), 1);
        assert!(fake.poll_count("b") >= 5);
        let events = h.drain();
        let disabled = of_type(&events, "source.disabled");
        assert_eq!(disabled.len(), 1);
        assert_eq!(disabled[0].data["source"], "a");
        assert_eq!(disabled[0].data["reason"], "no access");
        assert_eq!(
            h.sched.statuses()[0].disabled_sources,
            vec!["a".to_string()]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn throttled_waits_retry_after() {
        let fake = fake_with(&[("s", Tier::Fast)]);
        fake.push_poll(
            "s",
            Err(ProviderError::Throttled {
                retry_after: Duration::from_secs(25),
            }),
        );
        let _h = Harness::start(&fake).await;
        pass(12).await;
        assert_eq!(fake.poll_count("s"), 1);
        pass(15).await;
        assert_eq!(fake.poll_count("s"), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn missing_credentials_idles_until_resume() {
        let fake = fake_with(&[("s", Tier::Fast)]);
        fake.set_identify_error(Some(ProviderError::MissingCredentials));
        let h = Harness::start(&fake).await;
        pass(100).await;
        assert_eq!(fake.poll_count("s"), 0);
        assert_eq!(h.sched.statuses()[0].auth_state, AuthState::Missing);

        fake.set_identify_error(None);
        h.sched.connection("c1").unwrap().resume();
        pass(1).await;
        assert!(fake.poll_count("s") >= 1);
        let status = h.sched.statuses().remove(0);
        assert_eq!(status.auth_state, AuthState::Ok);
        assert_eq!(status.identity.unwrap().id, "me");
    }

    #[tokio::test(start_paused = true)]
    async fn missing_credentials_alert_once_per_lapse() {
        let fake = fake_with(&[("s", Tier::Fast)]);
        fake.set_identify_error(Some(ProviderError::MissingCredentials));
        let mut h = Harness::start(&fake).await;
        pass(100).await;
        let events = h.drain();
        let failed = of_type(&events, "auth.failed");
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].data["reason"], "missing");
        assert_eq!(failed[0].title, "Sign-in needed for c1");
        assert_eq!(failed[0].severity, Severity::Critical);

        // Still missing after a resume: no second alert.
        let conn = h.sched.connection("c1").unwrap();
        conn.resume();
        pass(10).await;
        assert!(of_type(&h.drain(), "auth.failed").is_empty());

        // Back to Ok, then lapsing again alerts again.
        fake.set_identify_error(None);
        conn.resume();
        pass(1).await;
        assert_eq!(h.sched.statuses()[0].auth_state, AuthState::Ok);
        fake.set_identify_error(Some(ProviderError::MissingCredentials));
        conn.reset_auth();
        conn.resume();
        pass(10).await;
        assert_eq!(of_type(&h.drain(), "auth.failed").len(), 1);
        assert_eq!(h.sched.statuses()[0].auth_state, AuthState::Missing);
    }

    /// Switches that make the wrapped provider misbehave.
    #[derive(Default)]
    struct Chaos {
        panic_poll: AtomicBool,
        panic_server_time: AtomicBool,
        hang_identify: AtomicBool,
        /// While true, `identify` runs the real call but returns only once released.
        identify_hold: Option<watch::Sender<bool>>,
        panics: AtomicUsize,
        /// Polls past this many (0 = unlimited) never return.
        poll_cap: AtomicUsize,
        polls: AtomicUsize,
    }

    struct ChaosSource {
        inner: Box<dyn Source>,
        chaos: Arc<Chaos>,
    }

    impl Source for ChaosSource {
        fn name(&self) -> &'static str {
            self.inner.name()
        }
        fn tier(&self) -> Tier {
            self.inner.tier()
        }
        fn poll<'a>(
            &'a mut self,
            ctx: &'a SyncContext,
        ) -> BoxFuture<'a, Result<PollOutcome, ProviderError>> {
            if self.chaos.panic_poll.load(Ordering::SeqCst) {
                self.chaos.panics.fetch_add(1, Ordering::SeqCst);
                panic!("source exploded");
            }
            let cap = self.chaos.poll_cap.load(Ordering::SeqCst);
            if cap > 0 && self.chaos.polls.fetch_add(1, Ordering::SeqCst) >= cap {
                return futures_util::future::pending().boxed();
            }
            self.inner.poll(ctx)
        }
    }

    struct ChaosConnection {
        inner: Arc<dyn Connection>,
        chaos: Arc<Chaos>,
    }

    impl Connection for ChaosConnection {
        fn id(&self) -> &str {
            self.inner.id()
        }
        fn provider(&self) -> &'static str {
            self.inner.provider()
        }
        fn auth_method(&self) -> &str {
            self.inner.auth_method()
        }
        fn identify(&self) -> BoxFuture<'_, Result<Identity, ProviderError>> {
            if self.chaos.hang_identify.load(Ordering::SeqCst) {
                return futures_util::future::pending().boxed();
            }
            if let Some(hold) = &self.chaos.identify_hold {
                let mut released = hold.subscribe();
                let call = self.inner.identify();
                return async move {
                    let result = call.await;
                    let _ = released.wait_for(|held| !*held).await;
                    result
                }
                .boxed();
            }
            self.inner.identify()
        }
        fn server_time(&self) -> BoxFuture<'_, Result<DateTime<Utc>, ProviderError>> {
            if self.chaos.panic_server_time.load(Ordering::SeqCst) {
                self.chaos.panics.fetch_add(1, Ordering::SeqCst);
                panic!("server_time exploded");
            }
            self.inner.server_time()
        }
        fn sources(&self) -> Vec<Box<dyn Source>> {
            self.inner
                .sources()
                .into_iter()
                .map(|inner| {
                    Box::new(ChaosSource {
                        inner,
                        chaos: self.chaos.clone(),
                    }) as Box<dyn Source>
                })
                .collect()
        }
        fn credentials_changed(&self) {}
    }

    struct ChaosFactory {
        inner: FakeProviderFactory,
        chaos: Arc<Chaos>,
    }

    impl ProviderFactory for ChaosFactory {
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
        ) -> Result<Arc<dyn Connection>, ProviderError> {
            Ok(Arc::new(ChaosConnection {
                inner: self.inner.connect(cfg, secrets)?,
                chaos: self.chaos.clone(),
            }))
        }
        fn parse_remote(&self, url: &str) -> Option<crate::provider::RemoteRepo> {
            self.inner.parse_remote(url)
        }
        fn sources(&self) -> Vec<crate::provider::SourceInfo> {
            self.inner.sources()
        }
    }

    async fn chaos_harness(
        fake: &FakeProvider,
        store: Option<Arc<dyn Store>>,
    ) -> (Harness, Arc<Chaos>) {
        let chaos = Arc::new(Chaos::default());
        let factory = ChaosFactory {
            inner: FakeProviderFactory::new(fake.clone()),
            chaos: chaos.clone(),
        };
        (Harness::start_with(Arc::new(factory), store).await, chaos)
    }

    #[tokio::test(start_paused = true)]
    async fn panicking_poll_does_not_stop_the_tier_or_move_the_watermark() {
        let fake = fake_with(&[("s", Tier::Fast)]);
        let (h, chaos) = chaos_harness(&fake, None).await;
        pass(1).await;
        assert_eq!(fake.poll_count("s"), 1);

        chaos.panic_poll.store(true, Ordering::SeqCst);
        fake.set_server_time(t0() + secs(10));
        pass(10).await;
        fake.set_server_time(t0() + secs(20));
        pass(10).await;
        assert_eq!(chaos.panics.load(Ordering::SeqCst), 2);
        assert_eq!(fake.poll_count("s"), 1);
        let wm = h.store.get_watermark("c1:s").await.unwrap().unwrap();
        assert_eq!(wm.last_success_utc, t0());

        chaos.panic_poll.store(false, Ordering::SeqCst);
        fake.set_server_time(t0() + secs(30));
        pass(10).await;
        let calls = fake.poll_calls("s");
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[1].watermark, Some(t0()));
        let wm = h.store.get_watermark("c1:s").await.unwrap().unwrap();
        assert_eq!(wm.last_success_utc, t0() + secs(30));
    }

    #[tokio::test(start_paused = true)]
    async fn transient_error_keeps_the_watermark() {
        let fake = fake_with(&[("s", Tier::Fast)]);
        let h = Harness::start(&fake).await;
        pass(1).await;

        fake.push_poll("s", Err(ProviderError::Transient("blip".into())));
        fake.set_server_time(t0() + secs(10));
        pass(10).await;
        assert_eq!(fake.poll_count("s"), 2);
        let wm = h.store.get_watermark("c1:s").await.unwrap().unwrap();
        assert_eq!(wm.last_success_utc, t0());

        fake.set_server_time(t0() + secs(20));
        pass(10).await;
        assert_eq!(fake.poll_calls("s")[2].watermark, Some(t0()));
        let wm = h.store.get_watermark("c1:s").await.unwrap().unwrap();
        assert_eq!(wm.last_success_utc, t0() + secs(20));
    }

    #[tokio::test(start_paused = true)]
    async fn panicking_server_time_does_not_stop_the_tier() {
        let fake = fake_with(&[("s", Tier::Fast)]);
        let (_h, chaos) = chaos_harness(&fake, None).await;
        chaos.panic_server_time.store(true, Ordering::SeqCst);
        pass(35).await;
        assert_eq!(chaos.panics.load(Ordering::SeqCst), 4);
        assert_eq!(fake.poll_count("s"), 0);

        chaos.panic_server_time.store(false, Ordering::SeqCst);
        pass(10).await;
        assert_eq!(fake.poll_count("s"), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn start_does_not_wait_for_a_hanging_identify() {
        let fake = fake_with(&[("s", Tier::Fast)]);
        let chaos = Arc::new(Chaos::default());
        chaos.hang_identify.store(true, Ordering::SeqCst);
        let factory = ChaosFactory {
            inner: FakeProviderFactory::new(fake.clone()),
            chaos,
        };
        // Reaching the next line, without the provider's identify call finishing, is the assertion.
        let h = Harness::start_with(Arc::new(factory), None).await;
        pass(100).await;

        assert_eq!(fake.poll_count("s"), 0);
        assert_eq!(h.sched.statuses()[0].auth_state, AuthState::Unknown);
    }

    async fn held_identify_harness(fake: &FakeProvider) -> (Harness, watch::Sender<bool>) {
        let hold = watch::channel(true).0;
        let chaos = Arc::new(Chaos {
            identify_hold: Some(hold.clone()),
            ..Chaos::default()
        });
        let factory = ChaosFactory {
            inner: FakeProviderFactory::new(fake.clone()),
            chaos,
        };
        (Harness::start_with(Arc::new(factory), None).await, hold)
    }

    fn named(id: &str) -> Identity {
        Identity {
            id: id.into(),
            display_name: id.into(),
            unique_name: id.into(),
            group_ids: Vec::new(),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn identify_finishing_after_a_credential_change_is_discarded() {
        let fake = fake_with(&[("s", Tier::Fast)]);
        fake.push_identify(Ok(named("old")));
        let (h, hold) = held_identify_harness(&fake).await;
        pass(1).await;
        assert_eq!(fake.identify_calls(), 1);

        h.sched.connection("c1").unwrap().reset_auth();
        hold.send_replace(false);
        pass(1).await;

        // The old account's identity is dropped and the waiting tier identifies afresh.
        assert_eq!(fake.identify_calls(), 2);
        let status = h.sched.statuses().remove(0);
        assert_eq!(status.identity.unwrap().id, "me");
        assert_eq!(status.auth_state, AuthState::Ok);
    }

    #[tokio::test(start_paused = true)]
    async fn tiers_starting_together_identify_once() {
        let fake = fake_with(&[("a", Tier::Fast), ("b", Tier::Pr)]);
        let (h, hold) = held_identify_harness(&fake).await;
        pass(1).await;
        // Fast, Pr and Slow all want an identity; only one call is in flight.
        assert_eq!(fake.identify_calls(), 1);

        hold.send_replace(false);
        pass(1).await;

        assert_eq!(fake.poll_count("a"), 1);
        assert_eq!(fake.poll_count("b"), 1);
        // One lazy identify plus the slow tier's refresh after its poll pass.
        assert_eq!(fake.identify_calls(), 2);
        drop(h);
    }

    #[test]
    fn stale_epoch_outcomes_are_ignored() {
        let cfg = config();
        let state = ConnState::new(cfg.connections[0].clone(), String::new(), None, Vec::new());
        state.lock().gate = Gate::Open;
        let before = state.auth_epoch();
        state.reset_auth();
        assert!(!state.set_identity(named("old"), before));
        assert_eq!(state.mark_invalid(before), None);
        assert!(!state.close_if(Gate::Credentials, Some(before)));
        assert!(state.identity().is_none());
        assert!(state.gate_open());
        assert_eq!(state.mark_invalid(state.auth_epoch()), Some(true));
    }

    #[tokio::test(start_paused = true)]
    async fn prepare_does_not_displace_a_launched_state() {
        let fake = fake_with(&[("s", Tier::Fast)]);
        let h = Harness::start(&fake).await;
        let live = h.sched.connection("c1").unwrap();
        let config = live.config.clone();

        let second = h.sched.prepare(config, false).await;
        assert!(Arc::ptr_eq(&h.sched.connection("c1").unwrap(), &live));
        h.sched.launch(second);
        assert!(Arc::ptr_eq(&h.sched.connection("c1").unwrap(), &live));
        pass(1).await;
        assert_eq!(fake.poll_count("s"), 1);
    }

    fn with_source_secs(mut cfg: Config, sources: &[(&str, u64)]) -> Config {
        for (name, secs) in sources {
            cfg.polling.sources.insert((*name).into(), *secs);
        }
        cfg
    }

    #[tokio::test(start_paused = true)]
    async fn overridden_source_polls_at_its_own_interval_beside_its_tier_siblings() {
        let fake = fake_with(&[("quick", Tier::Fast), ("plain", Tier::Fast)]);
        let cfg = with_source_secs(config(), &[("quick", 4)]);
        let _h = Harness::build(
            vec![Arc::new(FakeProviderFactory::new(fake.clone()))],
            None,
            cfg,
        )
        .await;
        // Fast tier is 10s: plain polls at 0, 10, 20; quick at 0, 4, ..., 20.
        pass(21).await;
        assert_eq!(fake.poll_count("quick"), 6);
        assert_eq!(fake.poll_count("plain"), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn catch_up_gap_follows_the_sources_own_interval() {
        let fake = fake_with(&[("quick", Tier::Fast), ("plain", Tier::Fast)]);
        let cfg = with_source_secs(config(), &[("quick", 4)]);
        let _h = Harness::build(
            vec![Arc::new(FakeProviderFactory::new(fake.clone()))],
            None,
            cfg,
        )
        .await;
        pass(1).await;
        // 20s is over three of quick's 4s periods but under three of the tier's 10s.
        fake.set_server_time(t0() + secs(20));
        pass(16).await;

        let quick = fake.poll_calls("quick");
        assert!(quick[1].catch_up);
        assert!(!quick[2].catch_up);
        let plain = fake.poll_calls("plain");
        assert_eq!(plain.len(), 2);
        assert!(!plain[1].catch_up);
    }

    #[tokio::test(start_paused = true)]
    async fn stale_long_override_sibling_joins_the_cycle_and_polls_with_it() {
        let fake = fake_with(&[("a", Tier::Fast), ("b", Tier::Fast)]);
        for source in ["a", "b"] {
            fake.push_poll(source, ok(vec![]));
            fake.push_poll(
                source,
                ok(vec![ev(
                    &format!("c1:{source}:wake"),
                    "run.completed",
                    true,
                )]),
            );
        }
        let cfg = with_source_secs(config(), &[("b", 1800)]);
        let mut h = Harness::build(
            vec![Arc::new(FakeProviderFactory::new(fake.clone()))],
            None,
            cfg,
        )
        .await;
        pass(1).await;
        fake.set_server_time_error(Some(ProviderError::Transient("down".into())));
        pass(25).await;
        fake.set_server_time_error(None);
        fake.set_server_time(t0() + secs(10_000));
        pass(15).await;

        // b is not due for 1800s, but joined a's cycle, so it polls with a.
        assert_eq!(fake.poll_count("b"), 2);
        let events = h.drain();
        let summaries = of_type(&events, "catchup.summary");
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].data["total"], 2);
        assert!(h.sched.shared.cycle().pending.is_empty());

        // The watchdog has nothing left to summarise.
        pass(300).await;
        assert!(of_type(&h.drain(), "catchup.summary").is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn reload_retimes_a_running_source_without_extra_polls() {
        let fake = fake_with(&[("s", Tier::Fast)]);
        let h = Harness::start(&fake).await;
        pass(12).await;
        assert_eq!(fake.poll_count("s"), 2);

        // Last poll started at 10s, so the next is due at 13s, then every 3s.
        h._config_tx
            .send_replace(Arc::new(with_source_secs(config(), &[("s", 3)])));
        pass(5).await;
        assert_eq!(fake.poll_count("s"), 4);

        // Dropping the override goes back to the tier's 10s from the poll at 16s.
        h._config_tx.send_replace(Arc::new(config()));
        pass(8).await;
        assert_eq!(fake.poll_count("s"), 4);
        pass(2).await;
        assert_eq!(fake.poll_count("s"), 5);
    }

    #[tokio::test(start_paused = true)]
    async fn poll_now_polls_slow_overridden_and_default_sources_once() {
        let fake = fake_with(&[
            ("rare", Tier::Fast),
            ("plain", Tier::Fast),
            ("slow", Tier::Slow),
        ]);
        let cfg = with_source_secs(config(), &[("rare", 5000)]);
        let h = Harness::build(
            vec![Arc::new(FakeProviderFactory::new(fake.clone()))],
            None,
            cfg,
        )
        .await;
        pass(1).await;
        h.sched.poll_now();
        pass(1).await;
        for name in ["rare", "plain", "slow"] {
            assert_eq!(fake.poll_count(name), 2, "{name}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn wake_poll_restarts_the_interval() {
        let fake = fake_with(&[("s", Tier::Fast)]);
        let h = Harness::start(&fake).await;
        // Fast period is 10s in the test config.
        pass(1).await;
        assert_eq!(fake.poll_count("s"), 1);

        pass(8).await;
        h.sched.poll_now();
        pass(1).await;
        assert_eq!(fake.poll_count("s"), 2);

        // The original tick falls at 10s; the wake pushed the schedule out a full period.
        pass(3).await;
        assert_eq!(fake.poll_count("s"), 2);
        pass(10).await;
        assert_eq!(fake.poll_count("s"), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn join_stale_skips_sources_with_a_poll_in_flight() {
        let fake = fake_with(&[("s", Tier::Fast)]);
        let mut cfg = config();
        let conn = cfg.connections.remove(0);
        let store = store_with_watermark("c1:s", t0() - secs(3600)).await;
        let h = Harness::build(
            vec![Arc::new(FakeProviderFactory::new(fake.clone()))],
            Some(store),
            cfg,
        )
        .await;
        let prepared = h.sched.prepare(conn, false).await;
        let state = prepared.state().clone();
        let late = t0() + secs(10_000);

        state.begin_poll("s");
        h.sched.shared.join_stale(late, ("other", Tier::Fast)).await;
        assert!(h.sched.shared.cycle().pending.is_empty());

        state.end_poll("s");
        h.sched.shared.join_stale(late, ("other", Tier::Fast)).await;
        assert_eq!(h.sched.shared.cycle().pending.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn join_stale_rechecks_after_the_watermark_reads() {
        for case in ["stopped", "replaced", "disabled", "polling"] {
            let fake = fake_with(&[("s", Tier::Fast)]);
            let mut cfg = config();
            let conn = cfg.connections.remove(0);
            let store = store_with_watermark("c1:s", t0() - secs(3600)).await;
            let h = Harness::build(
                vec![Arc::new(FakeProviderFactory::new(fake.clone()))],
                Some(store),
                cfg,
            )
            .await;
            // Prepared last, so it is the registered state join_stale reads.
            let replacement = h.sched.prepare(conn.clone(), false).await;
            let prepared = h.sched.prepare(conn, false).await;
            let state = prepared.state().clone();
            let shared = h.sched.shared.clone();
            let swap = replacement.state().clone();
            let hook_state = state.clone();
            *h.sched.shared.after_reads.lock().unwrap() = Some(Box::new(move || match case {
                "stopped" => {
                    shared.conns().remove("c1");
                }
                "replaced" => {
                    shared.conns().insert("c1".into(), swap);
                }
                "disabled" => {
                    hook_state.disable_source("s");
                }
                _ => hook_state.begin_poll("s"),
            }));

            h.sched
                .shared
                .join_stale(t0() + secs(10_000), ("other", Tier::Fast))
                .await;
            assert!(h.sched.shared.cycle().pending.is_empty(), "{case}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_unauthorized_publishes_auth_failed_once() {
        let fake = fake_with(&[("a", Tier::Fast)]);
        fake.push_poll("a", Err(ProviderError::Unauthorized));
        fake.push_poll("a", Err(ProviderError::Unauthorized));
        let mut cfg = config();
        // Keep the slow-tier identify refresh from resetting the auth state mid-test.
        cfg.polling.slow_secs = 100_000;
        let mut h = Harness::build(
            vec![Arc::new(FakeProviderFactory::new(fake.clone()))],
            None,
            cfg,
        )
        .await;
        pass(5).await;
        assert_eq!(fake.poll_count("a"), 1);
        assert_eq!(of_type(&h.drain(), "auth.failed").len(), 1);

        // The pause ends at 15 min, the next poll fails again and re-arms it.
        pass(905).await;
        assert_eq!(fake.poll_count("a"), 2);
        assert_eq!(h.sched.statuses()[0].auth_state, AuthState::Invalid);
        assert!(of_type(&h.drain(), "auth.failed").is_empty());

        let polls = fake.poll_count("a");
        pass(500).await;
        assert_eq!(fake.poll_count("a"), polls);

        pass(500).await;
        assert!(fake.poll_count("a") > polls);
        assert_eq!(h.sched.statuses()[0].auth_state, AuthState::Ok);
        assert!(of_type(&h.drain(), "auth.failed").is_empty());
    }

    async fn two_connection_harness(
        fake1: &FakeProvider,
        fake2: &FakeProvider,
        store: Arc<dyn Store>,
    ) -> Harness {
        let cfg = config_with(&[("c1", "fake"), ("c2", "fake2")]);
        Harness::build(
            vec![
                Arc::new(FakeProviderFactory::new(fake1.clone())),
                Arc::new(FakeProviderFactory::with_kind(fake2.clone(), "fake2")),
            ],
            Some(store),
            cfg,
        )
        .await
    }

    #[tokio::test(start_paused = true)]
    async fn multi_tier_connections_share_one_summary_at_start_and_one_per_wake() {
        let fake1 = fake_with(&[("a", Tier::Fast), ("b", Tier::Pr), ("c", Tier::Slow)]);
        let fake2 = fake_with(&[("d", Tier::Fast), ("e", Tier::Pr)]);
        for (fake, source, conn) in [
            (&fake1, "a", "c1"),
            (&fake1, "b", "c1"),
            (&fake1, "c", "c1"),
            (&fake2, "d", "c2"),
            (&fake2, "e", "c2"),
        ] {
            for round in ["start", "wake"] {
                fake.push_poll(
                    source,
                    ok(vec![ev(
                        &format!("{conn}:{source}:{round}"),
                        "run.completed",
                        true,
                    )]),
                );
            }
        }
        let store =
            store_with_watermarks(&["c1:a", "c1:b", "c1:c", "c2:d", "c2:e"], t0() - secs(3600))
                .await;
        let mut h = two_connection_harness(&fake1, &fake2, store).await;
        pass(1).await;

        let events = h.drain();
        let summaries = of_type(&events, "catchup.summary");
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].data["total"], 5);
        assert_eq!(summaries[0].connection.id, "app");

        // Sleep/wake: both providers' clocks jump. The fast source notices first, and
        // every other source joins the same cycle before it polls.
        fake1.set_server_time(t0() + secs(10_000));
        fake2.set_server_time(t0() + secs(10_000));
        pass(150).await;

        let events = h.drain();
        let summaries = of_type(&events, "catchup.summary");
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].data["total"], 5);
        assert!(!summaries[0].catch_up);
    }

    #[tokio::test(start_paused = true)]
    async fn wake_joins_slow_tiers_without_waiting_for_their_period() {
        let fake = fake_with(&[("a", Tier::Fast), ("b", Tier::Pr), ("c", Tier::Slow)]);
        for source in ["a", "b", "c"] {
            fake.push_poll(source, ok(vec![]));
            fake.push_poll(
                source,
                ok(vec![ev(
                    &format!("c1:{source}:wake"),
                    "run.completed",
                    true,
                )]),
            );
        }
        let mut cfg = config();
        cfg.polling.slow_secs = 100_000;
        let store = store_with_watermarks(&["c1:a", "c1:b", "c1:c"], t0()).await;
        let mut h = Harness::build(
            vec![Arc::new(FakeProviderFactory::new(fake.clone()))],
            Some(store),
            cfg,
        )
        .await;
        pass(1).await;
        assert!(h.drain().is_empty());

        fake.set_server_time(t0() + secs(1_000_000));
        pass(20).await;

        let events = h.drain();
        let summaries = of_type(&events, "catchup.summary");
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].data["total"], 3);
        assert_eq!(fake.poll_count("c"), 2);
    }

    #[test]
    fn halted_overrides_an_active_pause() {
        let cfg = config();
        let state = ConnState::new(cfg.connections[0].clone(), String::new(), None, Vec::new());
        state.lock().gate = Gate::Open;
        assert!(state.close(Gate::Credentials));
        assert!(!state.close(Gate::Until(Instant::now() + AUTH_PAUSE)));
        assert!(state.close(Gate::Halted));
        assert!(!state.close(Gate::Halted));
        assert!(state.halted());
        state.resume();
        assert!(state.halted());
    }

    #[tokio::test(start_paused = true)]
    async fn dropped_prepared_does_not_hold_the_startup_cycle() {
        let fake = fake_with(&[("s", Tier::Fast)]);
        let mut cfg = config();
        let conn = cfg.connections.remove(0);
        let store = store_with_watermark("c1:s", t0() - secs(3600)).await;
        let h = Harness::build(
            vec![Arc::new(FakeProviderFactory::new(fake.clone()))],
            Some(store),
            cfg,
        )
        .await;
        let prepared = h.sched.prepare(conn.clone(), true).await;
        assert_eq!(h.sched.shared.cycle().pending.len(), 1);
        assert!(h.sched.connection("c1").is_some());
        drop(prepared);
        assert!(h.sched.shared.cycle().pending.is_empty());
        assert!(h.sched.connection("c1").is_none());

        let prepared = h.sched.prepare(conn, false).await;
        drop(prepared);
        assert!(h.sched.connection("c1").is_none());
        h.sched
            .shared
            .join_stale(t0() + secs(10_000), ("other", Tier::Fast))
            .await;
        assert!(h.sched.shared.cycle().pending.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_a_stale_prepared_leaves_the_live_one_alone() {
        let fake = fake_with(&[("s", Tier::Fast)]);
        let mut cfg = config();
        let conn = cfg.connections.remove(0);
        let store = store_with_watermark("c1:s", t0() - secs(3600)).await;
        let h = Harness::build(
            vec![Arc::new(FakeProviderFactory::new(fake.clone()))],
            Some(store),
            cfg,
        )
        .await;
        let stale = h.sched.prepare(conn.clone(), true).await;
        let live = h.sched.prepare(conn, true).await;
        assert_eq!(h.sched.shared.cycle().pending.len(), 2);
        drop(stale);
        assert_eq!(h.sched.shared.cycle().pending.len(), 1);
        assert!(Arc::ptr_eq(
            &h.sched.connection("c1").unwrap(),
            live.state()
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn failing_sources_in_different_tiers_do_not_wake_each_other_forever() {
        let fake = fake_with(&[("a", Tier::Fast), ("b", Tier::Pr)]);
        for _ in 0..200 {
            fake.push_poll("a", Err(ProviderError::Transient("down".into())));
            fake.push_poll("b", Err(ProviderError::Transient("down".into())));
        }
        let store = store_with_watermarks(&["c1:a", "c1:b"], t0() - secs(3600)).await;
        let (_h, chaos) = chaos_harness(&fake, Some(store)).await;
        // A spin would exceed the cap and hang the poll, failing the counts below.
        chaos.poll_cap.store(100, Ordering::SeqCst);
        pass(30).await;

        assert_eq!(fake.poll_count("a"), 3);
        assert_eq!(fake.poll_count("b"), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn open_cycle_is_summarised_at_the_deadline_and_later_cycles_still_work() {
        let fake = fake_with(&[("a", Tier::Slow), ("d", Tier::Fast)]);
        fake.hold_polls("a");
        fake.push_poll("d", ok(vec![ev("c1:d:start", "run.completed", true)]));
        let store = store_with_watermarks(&["c1:a", "c1:d"], t0() - secs(3600)).await;
        let mut h = Harness::start_with(
            Arc::new(FakeProviderFactory::new(fake.clone())),
            Some(store),
        )
        .await;
        pass(100).await;
        assert!(of_type(&h.drain(), "catchup.summary").is_empty());

        pass(90).await;
        let events = h.drain();
        let summaries = of_type(&events, "catchup.summary");
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].data["total"], 1);
        assert!(h.sched.shared.cycle().pending.is_empty());
        pass(200).await;
        assert!(of_type(&h.drain(), "catchup.summary").is_empty());

        fake.release_polls("a");
        pass(1).await;
        fake.push_poll("a", ok(vec![ev("c1:a:wake", "run.completed", true)]));
        fake.push_poll("d", ok(vec![ev("c1:d:wake", "run.completed", true)]));
        fake.set_server_time(t0() + secs(10_000));
        pass(20).await;
        let events = h.drain();
        let summaries = of_type(&events, "catchup.summary");
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].data["total"], 2);
    }

    #[tokio::test(start_paused = true)]
    async fn stop_connection_mid_cycle_publishes_the_summary_and_later_cycles_still_work() {
        let fake1 = fake_with(&[("a", Tier::Fast)]);
        let fake2 = fake_with(&[("d", Tier::Fast)]);
        fake1.hold_polls("a");
        fake2.push_poll("d", ok(vec![ev("c2:d:start", "run.completed", true)]));
        let store = store_with_watermarks(&["c1:a", "c2:d"], t0() - secs(3600)).await;
        let mut h = two_connection_harness(&fake1, &fake2, store).await;
        pass(1).await;
        assert!(of_type(&h.drain(), "catchup.summary").is_empty());

        h.sched.stop_connection("c1").await;
        let events = h.drain();
        let summaries = of_type(&events, "catchup.summary");
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].data["total"], 1);

        fake2.push_poll("d", ok(vec![ev("c2:d:wake", "run.completed", true)]));
        fake2.set_server_time(t0() + secs(1000));
        pass(10).await;
        let events = h.drain();
        let summaries = of_type(&events, "catchup.summary");
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].data["total"], 1);
        assert_eq!(summaries[0].connection.id, "c2");
    }

    #[tokio::test(start_paused = true)]
    async fn summary_counts_only_alerting_events() {
        let fake = fake_with(&[("s", Tier::Fast)]);
        let mut bot = ev("c1:bot", "pr.created", true);
        bot.actor.kind = ActorKind::Bot;
        fake.push_poll("s", ok(vec![bot]));
        let mut h = Harness::start_with(
            Arc::new(FakeProviderFactory::new(fake.clone())),
            Some(store_with_watermark("c1:s", t0() - secs(3600)).await),
        )
        .await;
        pass(1).await;

        assert_eq!(fake.poll_count("s"), 1);
        assert!(of_type(&h.drain(), "catchup.summary").is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn summary_skips_pane_level_events() {
        let fake = fake_with(&[("s", Tier::Fast)]);
        fake.push_poll(
            "s",
            ok(vec![
                ev("c1:pr:1", "pr.created", true),
                ev("c1:deploy:1", "deploy.failed", true),
            ]),
        );
        let mut cfg = config();
        cfg.notify.levels.insert("pr".into(), "pane".into());
        let mut h = Harness::build(
            vec![Arc::new(FakeProviderFactory::new(fake.clone()))],
            Some(store_with_watermark("c1:s", t0() - secs(3600)).await),
            cfg,
        )
        .await;
        pass(1).await;

        let events = h.drain();
        let summaries = of_type(&events, "catchup.summary");
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].data["total"], 1);
        assert!(summaries[0].data["counts"].get("pr.created").is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn reload_prepare_polls_normally_but_startup_prepare_catches_up() {
        let fake = fake_with(&[("s", Tier::Fast)]);
        let store = store_with_watermark("c1:s", t0() - secs(5)).await;
        let mut cfg = config();
        let conn = cfg.connections.remove(0);
        let mut h = Harness::build(
            vec![Arc::new(FakeProviderFactory::new(fake.clone()))],
            Some(store),
            cfg,
        )
        .await;
        let prepared = h.sched.prepare(conn.clone(), false).await;
        h.sched.launch(prepared);
        pass(1).await;
        assert!(!fake.poll_calls("s")[0].catch_up);

        h.sched.stop_connection("c1").await;
        fake.set_server_time(t0() + secs(5));
        h.store
            .set_watermark("c1:s", t0() + secs(1), t0() + secs(1))
            .await
            .unwrap();
        let prepared = h.sched.prepare(conn, true).await;
        h.sched.launch(prepared);
        pass(1).await;
        assert!(fake.poll_calls("s")[1].catch_up);
        assert!(h.drain().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_stops_all_tasks() {
        let fake = fake_with(&[("s", Tier::Fast)]);
        let h = Harness::start(&fake).await;
        pass(25).await;
        let before = fake.poll_count("s");
        assert!(before >= 3);

        h.sched.shutdown().await;
        pass(100).await;
        assert_eq!(fake.poll_count("s"), before);
    }
}
