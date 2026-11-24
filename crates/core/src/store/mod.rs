//! Persistence for events, watermarks, provider snapshots, hook runs and UI state.

use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::event::Event;

#[cfg(any(test, feature = "testing"))]
pub mod conformance;
mod sqlite;

pub use sqlite::SqliteStore;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("store backend error: {0}")]
    Backend(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("store json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("store lock poisoned")]
    Poisoned,
    #[error(
        "database schema version {found} is newer than this build supports ({supported}); upgrade Herald"
    )]
    SchemaTooNew { found: i64, supported: i64 },
    #[error(
        "this database predates event sequence numbers; open the Herald app once to upgrade it"
    )]
    NeedsUpgrade,
}

pub type Result<T> = std::result::Result<T, StoreError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Watermark {
    pub last_synced_utc: DateTime<Utc>,
    pub last_success_utc: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookRun {
    pub hook: String,
    pub event_id: String,
    pub started_utc: DateTime<Utc>,
    pub finished_utc: Option<DateTime<Utc>>,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
    pub stderr_tail: Option<String>,
}

/// Backend-neutral store. Implementations must pass `conformance::run`.
///
/// Event sequence numbers (`Event.seq`): assigned when an event is inserted, strictly increasing,
/// and never reused, including after `prune`. Events become visible to readers in increasing seq
/// order (a lower seq never appears after a higher one), so a client that has seen seq N has seen
/// every surviving event up to N. Seqs may have gaps; clients must not assume density.
/// `Event.seq` is set only by reads, from the store's own column.
pub trait Store: Send + Sync + 'static {
    /// Resolves to the new event's seq, or None when an event with the same id already exists.
    fn insert_event_if_new<'a>(&'a self, e: &'a Event) -> BoxFuture<'a, Result<Option<u64>>>;
    fn get_event<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<Option<Event>>>;
    /// Pane-listed events not yet viewed, newest first.
    fn unviewed<'a>(&'a self, limit: u32) -> BoxFuture<'a, Result<Vec<Event>>>;
    /// Alerting events not yet viewed.
    fn unviewed_count<'a>(&'a self) -> BoxFuture<'a, Result<u32>>;
    fn mark_viewed<'a>(&'a self, ids: &'a [String]) -> BoxFuture<'a, Result<()>>;
    fn mark_all_viewed<'a>(&'a self) -> BoxFuture<'a, Result<()>>;
    /// Events with `occurred_utc >= since`, oldest first.
    fn events_since<'a>(
        &'a self,
        since: DateTime<Utc>,
        limit: u32,
    ) -> BoxFuture<'a, Result<Vec<Event>>>;
    fn get_watermark<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Watermark>>>;
    fn set_watermark<'a>(
        &'a self,
        key: &'a str,
        synced: DateTime<Utc>,
        success: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<()>>;
    fn get_snapshot_json<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<serde_json::Value>>>;
    fn put_snapshot_json<'a>(
        &'a self,
        key: &'a str,
        value: serde_json::Value,
    ) -> BoxFuture<'a, Result<()>>;
    fn record_hook_run<'a>(&'a self, run: HookRun) -> BoxFuture<'a, Result<()>>;
    fn get_ui_state<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<String>>>;
    fn set_ui_state<'a>(&'a self, key: &'a str, value: &'a str) -> BoxFuture<'a, Result<()>>;
    /// Deletes events (by `occurred_utc`) and hook runs (by `started_utc`) older than the
    /// cutoff. Watermarks, snapshots and UI state are never pruned.
    fn prune<'a>(&'a self, older_than_days: u32) -> BoxFuture<'a, Result<()>>;
    /// Events with `seq > after`, ascending by seq. The seq methods below return
    /// `StoreError::NeedsUpgrade` on a database that has no seq column yet (a read-only open of
    /// an older database).
    fn events_after_seq<'a>(&'a self, after: u64, limit: u32) -> BoxFuture<'a, Result<Vec<Event>>>;
    /// The highest seq ever assigned, 0 when no event was ever inserted. Not lowered by `prune`.
    fn latest_seq<'a>(&'a self) -> BoxFuture<'a, Result<u64>>;
    /// The lowest seq still stored, None when there are no events.
    fn oldest_seq<'a>(&'a self) -> BoxFuture<'a, Result<Option<u64>>>;
    /// Identifies this store instance; stable across opens, so a client can detect a replaced store.
    fn store_id<'a>(&'a self) -> BoxFuture<'a, Result<String>>;
}

impl dyn Store {
    pub async fn get_snapshot<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        self.get_snapshot_json(key)
            .await?
            .map(|v| Ok(serde_json::from_value(v)?))
            .transpose()
    }

    pub async fn put_snapshot<T: Serialize>(&self, key: &str, value: &T) -> Result<()> {
        self.put_snapshot_json(key, serde_json::to_value(value)?)
            .await
    }
}
