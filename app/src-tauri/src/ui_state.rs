//! In-memory copy of the app's `ui_state` rows, so tray, window and alert code reads
//! synchronously and never blocks on the async store.
//!
//! Writes update the cache first, then queue `(key, value)` to one worker that persists in
//! order, so the store ends up equal to the cache. The hook runner reads snooze from the
//! store, so a snooze or resume reaches it only once the worker has written it (milliseconds).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use herald_core::store::Store;
use tokio::sync::{mpsc, oneshot};

pub const PINNED_KEY: &str = "pinned";
pub const MUTED_KEY: &str = "muted";
pub const SNOOZE_KEY: &str = "snoozed_until_utc";
pub const PANE_SIZE_KEY: &str = "pane_size";
pub const AUTOSTART_KEY: &str = "autostart_initialized";

const KEYS: [&str; 5] = [
    PINNED_KEY,
    MUTED_KEY,
    SNOOZE_KEY,
    PANE_SIZE_KEY,
    AUTOSTART_KEY,
];

enum Job {
    Write(&'static str, String),
    Barrier(oneshot::Sender<()>),
}

pub struct UiStateCache {
    values: Mutex<BTreeMap<&'static str, String>>,
    queue: mpsc::UnboundedSender<Job>,
}

impl UiStateCache {
    /// Reads every key once and starts the persist worker, which ends when the cache is
    /// dropped. Call from within the tokio runtime.
    pub async fn load(store: Arc<dyn Store>) -> Self {
        let mut values = BTreeMap::new();
        for key in KEYS {
            match store.get_ui_state(key).await {
                Ok(Some(v)) => {
                    values.insert(key, v);
                }
                Ok(None) => {}
                Err(e) => tracing::warn!(error = %e, key, "loading ui state failed"),
            }
        }
        let (queue, rx) = mpsc::unbounded_channel();
        tokio::spawn(persist(store, rx));
        Self {
            values: Mutex::new(values),
            queue,
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.values
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(key)
            .cloned()
    }

    pub fn pinned(&self) -> bool {
        self.get(PINNED_KEY).as_deref() == Some("true")
    }

    pub fn muted(&self) -> bool {
        self.get(MUTED_KEY).as_deref() == Some("true")
    }

    /// Updates the cache, then queues the write. `key` must be one of the cached keys.
    pub fn set(&self, key: &'static str, value: &str) {
        debug_assert!(KEYS.contains(&key), "uncached ui_state key {key}");
        // Queue under the lock so the queue order matches the cache's last-write order.
        let mut values = self.values.lock().unwrap_or_else(PoisonError::into_inner);
        values.insert(key, value.to_owned());
        if self.queue.send(Job::Write(key, value.to_owned())).is_err() {
            tracing::warn!(key, "ui state persist worker is gone");
        }
    }

    pub fn set_flag(&self, key: &'static str, on: bool) {
        self.set(key, if on { "true" } else { "false" });
    }

    /// Resolves once every write queued before this call has reached the store.
    pub async fn flush(&self) {
        let (tx, rx) = oneshot::channel();
        if self.queue.send(Job::Barrier(tx)).is_ok() {
            let _ = rx.await;
        }
    }
}

async fn persist(store: Arc<dyn Store>, mut rx: mpsc::UnboundedReceiver<Job>) {
    while let Some(job) = rx.recv().await {
        match job {
            Job::Write(key, value) => {
                if let Err(e) = store.set_ui_state(key, &value).await {
                    tracing::warn!(error = %e, key, "saving ui state failed");
                }
            }
            Job::Barrier(done) => {
                let _ = done.send(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use herald_core::store::SqliteStore;

    fn store() -> Arc<dyn Store> {
        Arc::new(SqliteStore::open_in_memory().unwrap())
    }

    async fn stored(store: &Arc<dyn Store>, key: &str) -> Option<String> {
        store.get_ui_state(key).await.unwrap()
    }

    #[tokio::test]
    async fn load_reads_existing_rows_and_defaults_the_rest() {
        let s = store();
        s.set_ui_state(PINNED_KEY, "true").await.unwrap();
        s.set_ui_state(PANE_SIZE_KEY, "400x600").await.unwrap();
        let cache = UiStateCache::load(s).await;
        assert!(cache.pinned());
        assert!(!cache.muted());
        assert_eq!(cache.get(PANE_SIZE_KEY).as_deref(), Some("400x600"));
        assert_eq!(cache.get(SNOOZE_KEY), None);
    }

    #[tokio::test]
    async fn write_is_visible_at_once_and_reaches_the_store() {
        let s = store();
        let cache = UiStateCache::load(s.clone()).await;
        cache.set_flag(MUTED_KEY, true);
        assert!(cache.muted());
        cache.flush().await;
        assert_eq!(stored(&s, MUTED_KEY).await.as_deref(), Some("true"));
    }

    #[tokio::test]
    async fn rapid_writes_persist_in_order_and_end_equal_to_the_cache() {
        let s = store();
        let cache = UiStateCache::load(s.clone()).await;
        cache.set(SNOOZE_KEY, "2030-01-01T00:00:00.000Z");
        cache.set(SNOOZE_KEY, "");
        for _ in 0..2 {
            let flipped = !cache.muted();
            cache.set_flag(MUTED_KEY, flipped);
        }
        for i in 0..50 {
            cache.set(PANE_SIZE_KEY, &format!("{}x500", 400 + i));
        }
        cache.flush().await;
        for key in KEYS {
            assert_eq!(stored(&s, key).await, cache.get(key), "{key}");
        }
        assert_eq!(stored(&s, SNOOZE_KEY).await.as_deref(), Some(""));
        assert_eq!(stored(&s, MUTED_KEY).await.as_deref(), Some("false"));
        assert_eq!(stored(&s, PANE_SIZE_KEY).await.as_deref(), Some("449x500"));
    }
}
