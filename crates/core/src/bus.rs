//! In-process event bus: classify, enrich with local paths, persist, broadcast.

use std::sync::{Arc, RwLock};

use tokio::sync::{Mutex, broadcast};

use crate::classify::Classifier;
use crate::event::Event;
use crate::local_repos::LocalIndex;
use crate::store::{Store, StoreError};

const CAPACITY: usize = 1024;

pub struct Bus {
    store: Arc<dyn Store>,
    classifier: RwLock<Arc<Classifier>>,
    local_index: RwLock<Arc<LocalIndex>>,
    tx: broadcast::Sender<Arc<Event>>,
    /// Held across insert and send so broadcast order matches seq order. Async because it spans an await.
    publish_order: Arc<Mutex<()>>,
}

const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Bus>();
};

impl Bus {
    pub fn new(store: Arc<dyn Store>, classifier: Classifier, local_index: LocalIndex) -> Bus {
        let (tx, _) = broadcast::channel(CAPACITY);
        Bus {
            store,
            classifier: RwLock::new(Arc::new(classifier)),
            local_index: RwLock::new(Arc::new(local_index)),
            tx,
            publish_order: Arc::new(Mutex::new(())),
        }
    }

    /// Persists and broadcasts a new event; `Some` means the event was new. `None` for a duplicate id, an empty id or a store error.
    pub async fn publish(&self, e: Event) -> Option<Arc<Event>> {
        self.try_publish(e).await.ok().flatten()
    }

    /// Like `publish`, but a store error is returned instead of folded into `None`.
    /// `Ok(None)` is a duplicate id or an empty id.
    pub async fn try_publish(&self, mut e: Event) -> Result<Option<Arc<Event>>, StoreError> {
        if e.id.is_empty() {
            tracing::error!(event_type = %e.event_type, "rejected event with empty id");
            return Ok(None);
        }
        self.prepare(&mut e);
        // Spawned so cancelling the caller (the scheduler aborts tier tasks) cannot land between the
        // commit and the broadcast: a committed seq must always reach subscribers.
        let store = self.store.clone();
        let tx = self.tx.clone();
        let order = self.publish_order.clone();
        tokio::spawn(async move {
            let _order = order.lock_owned().await;
            match store.insert_event_if_new(&e).await {
                Ok(Some(seq)) => {
                    e.seq = Some(seq);
                    let e = Arc::new(e);
                    // Err only means no subscribers.
                    let _ = tx.send(e.clone());
                    Ok(Some(e))
                }
                Ok(None) => Ok(None),
                Err(err) => {
                    tracing::error!(id = %e.id, error = %err, "failed to persist event");
                    Err(err)
                }
            }
        })
        .await
        .map_err(|err| StoreError::Backend(format!("publish task failed: {err}").into()))?
    }

    /// Broadcasts without persisting. Has no seq, so it skips the ordering lock and never waits on the store.
    pub fn publish_transient(&self, mut e: Event) -> Arc<Event> {
        self.prepare(&mut e);
        e.seq = None;
        let e = Arc::new(e);
        let _ = self.tx.send(e.clone());
        e
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<Event>> {
        self.tx.subscribe()
    }

    pub fn set_classifier(&self, c: Classifier) {
        *self.classifier.write().unwrap_or_else(|p| p.into_inner()) = Arc::new(c);
    }

    pub fn set_local_index(&self, i: LocalIndex) {
        *self.local_index.write().unwrap_or_else(|p| p.into_inner()) = Arc::new(i);
    }

    pub fn store(&self) -> Arc<dyn Store> {
        self.store.clone()
    }

    fn prepare(&self, e: &mut Event) {
        let classifier = self
            .classifier
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        classifier.classify(e);
        let index = self
            .local_index
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        index.enrich(e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, LocalConfig};
    use crate::event::{ConnectionRef, RepoRef, Severity, event_types as t};
    use crate::provider::ProviderRegistry;
    use crate::store::SqliteStore;
    use crate::store::{HookRun, Watermark};
    use crate::testing::{FakeProvider, FakeProviderFactory};
    use chrono::{DateTime, Utc};
    use futures_util::future::BoxFuture;
    use std::collections::BTreeMap;
    use std::time::Duration;

    fn classifier(mute: &[&str]) -> Classifier {
        let mut cfg = Config::from_toml_str(Config::default_toml()).unwrap();
        cfg.alerts.mute_types = mute.iter().map(|s| s.to_string()).collect();
        Classifier::new(&cfg, &BTreeMap::new())
    }

    /// Delegates to an in-memory store, but `insert_event_if_new` sleeps after committing.
    struct SlowInsert(SqliteStore);

    impl Store for SlowInsert {
        fn insert_event_if_new<'a>(
            &'a self,
            e: &'a Event,
        ) -> BoxFuture<'a, Result<Option<u64>, StoreError>> {
            Box::pin(async move {
                let seq = self.0.insert_event_if_new(e).await;
                tokio::time::sleep(Duration::from_millis(300)).await;
                seq
            })
        }
        fn get_event<'a>(
            &'a self,
            id: &'a str,
        ) -> BoxFuture<'a, Result<Option<Event>, StoreError>> {
            self.0.get_event(id)
        }
        fn unviewed<'a>(&'a self, limit: u32) -> BoxFuture<'a, Result<Vec<Event>, StoreError>> {
            self.0.unviewed(limit)
        }
        fn unviewed_count<'a>(&'a self) -> BoxFuture<'a, Result<u32, StoreError>> {
            self.0.unviewed_count()
        }
        fn mark_viewed<'a>(&'a self, ids: &'a [String]) -> BoxFuture<'a, Result<(), StoreError>> {
            self.0.mark_viewed(ids)
        }
        fn mark_all_viewed<'a>(&'a self) -> BoxFuture<'a, Result<(), StoreError>> {
            self.0.mark_all_viewed()
        }
        fn events_since<'a>(
            &'a self,
            since: DateTime<Utc>,
            limit: u32,
        ) -> BoxFuture<'a, Result<Vec<Event>, StoreError>> {
            self.0.events_since(since, limit)
        }
        fn get_watermark<'a>(
            &'a self,
            key: &'a str,
        ) -> BoxFuture<'a, Result<Option<Watermark>, StoreError>> {
            self.0.get_watermark(key)
        }
        fn set_watermark<'a>(
            &'a self,
            key: &'a str,
            synced: DateTime<Utc>,
            success: DateTime<Utc>,
        ) -> BoxFuture<'a, Result<(), StoreError>> {
            self.0.set_watermark(key, synced, success)
        }
        fn get_snapshot_json<'a>(
            &'a self,
            key: &'a str,
        ) -> BoxFuture<'a, Result<Option<serde_json::Value>, StoreError>> {
            self.0.get_snapshot_json(key)
        }
        fn put_snapshot_json<'a>(
            &'a self,
            key: &'a str,
            value: serde_json::Value,
        ) -> BoxFuture<'a, Result<(), StoreError>> {
            self.0.put_snapshot_json(key, value)
        }
        fn record_hook_run<'a>(&'a self, run: HookRun) -> BoxFuture<'a, Result<(), StoreError>> {
            self.0.record_hook_run(run)
        }
        fn get_ui_state<'a>(
            &'a self,
            key: &'a str,
        ) -> BoxFuture<'a, Result<Option<String>, StoreError>> {
            self.0.get_ui_state(key)
        }
        fn set_ui_state<'a>(
            &'a self,
            key: &'a str,
            value: &'a str,
        ) -> BoxFuture<'a, Result<(), StoreError>> {
            self.0.set_ui_state(key, value)
        }
        fn prune<'a>(&'a self, older_than_days: u32) -> BoxFuture<'a, Result<(), StoreError>> {
            self.0.prune(older_than_days)
        }
        fn events_after_seq<'a>(
            &'a self,
            after: u64,
            limit: u32,
        ) -> BoxFuture<'a, Result<Vec<Event>, StoreError>> {
            self.0.events_after_seq(after, limit)
        }
        fn latest_seq<'a>(&'a self) -> BoxFuture<'a, Result<u64, StoreError>> {
            self.0.latest_seq()
        }
        fn oldest_seq<'a>(&'a self) -> BoxFuture<'a, Result<Option<u64>, StoreError>> {
            self.0.oldest_seq()
        }
        fn store_id<'a>(&'a self) -> BoxFuture<'a, Result<String, StoreError>> {
            self.0.store_id()
        }
    }

    fn bus(c: Classifier, idx: LocalIndex) -> Bus {
        Bus::new(Arc::new(SqliteStore::open_in_memory().unwrap()), c, idx)
    }

    fn event(id: &str) -> Event {
        let now = Utc::now();
        Event::new(
            ConnectionRef {
                id: "c".into(),
                provider: "fake".into(),
            },
            t::PR_COMPLETED,
            now,
            now,
            "t",
            "c:pr:1:1",
        )
        .with_id(id)
    }

    #[tokio::test]
    async fn publish_persists_and_broadcasts_once() {
        let bus = bus(classifier(&[]), LocalIndex::empty());
        let mut rx = bus.subscribe();

        let published = bus.publish(event("c:1")).await.unwrap();
        assert_eq!(published.id, "c:1");
        assert_eq!(rx.recv().await.unwrap().id, "c:1");
        assert!(bus.store().get_event("c:1").await.unwrap().is_some());

        assert!(bus.publish(event("c:1")).await.is_none());
        assert!(matches!(bus.try_publish(event("c:1")).await, Ok(None)));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_publishers_broadcast_in_seq_order() {
        const N: usize = 50;
        let dir = tempfile::tempdir().unwrap();
        // A file store hops through spawn_blocking, so inserts really interleave.
        let store = SqliteStore::open(&dir.path().join("herald.db")).unwrap();
        let bus = Arc::new(Bus::new(
            Arc::new(store),
            classifier(&[]),
            LocalIndex::empty(),
        ));
        let mut rx = bus.subscribe();

        let tasks: Vec<_> = (0..N)
            .map(|i| {
                let bus = bus.clone();
                tokio::spawn(async move { bus.publish(event(&format!("c:{i}"))).await })
            })
            .collect();
        for t in tasks {
            assert!(t.await.unwrap().is_some());
        }

        let mut last = 0;
        for _ in 0..N {
            let seq = rx.recv().await.unwrap().seq.unwrap();
            assert!(seq > last, "seq {seq} after {last}");
            last = seq;
        }
    }

    #[tokio::test]
    async fn aborted_publish_still_broadcasts_its_committed_event() {
        let bus = Arc::new(Bus::new(
            Arc::new(SlowInsert(SqliteStore::open_in_memory().unwrap())),
            classifier(&[]),
            LocalIndex::empty(),
        ));
        let mut rx = bus.subscribe();

        let caller = {
            let bus = bus.clone();
            tokio::spawn(async move { bus.publish(event("c:1")).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());

        let got = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.id, "c:1");
        assert_eq!(got.seq, Some(1));
        assert!(bus.store().get_event("c:1").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn publish_rejects_empty_id() {
        let bus = bus(classifier(&[]), LocalIndex::empty());
        let mut rx = bus.subscribe();

        assert!(bus.publish(event("")).await.is_none());
        assert!(rx.try_recv().is_err());
        assert!(bus.store().get_event("").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn publish_classifies_before_persisting() {
        let bus = bus(classifier(&[]), LocalIndex::empty());
        bus.publish(event("c:1")).await.unwrap();

        let stored = bus.store().get_event("c:1").await.unwrap().unwrap();
        assert_eq!(stored.severity, Severity::Success);
        assert!(stored.alert);
        assert!(stored.local.is_none());
    }

    #[tokio::test]
    async fn publish_enriches_with_local_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let clone = tmp.path().join("widgets");
        std::fs::create_dir_all(clone.join(".git")).unwrap();
        std::fs::write(
            clone.join(".git/config"),
            "[remote \"origin\"]\n\turl = fake://host/proj/Widgets\n",
        )
        .unwrap();
        std::fs::write(clone.join(".git/HEAD"), "ref: refs/heads/main\n").unwrap();
        let registry = ProviderRegistry::new(vec![Arc::new(FakeProviderFactory::new(
            FakeProvider::new(),
        ))]);
        let idx = LocalIndex::build(
            &LocalConfig {
                scan_roots: vec![tmp.path().display().to_string()],
                scan_depth: 2,
            },
            &[],
            &registry,
        );
        let bus = bus(classifier(&[]), idx);

        let mut e = event("c:1");
        e.repo = Some(RepoRef {
            id: "1".into(),
            name: "Widgets".into(),
            namespace: Some("proj".into()),
        });
        bus.publish(e).await.unwrap();

        let stored = bus.store().get_event("c:1").await.unwrap().unwrap();
        assert_eq!(
            stored.local.unwrap().repo_path,
            Some(clone.display().to_string())
        );
    }

    #[tokio::test]
    async fn publish_transient_broadcasts_without_storing() {
        let bus = bus(classifier(&[]), LocalIndex::empty());
        let mut rx = bus.subscribe();

        let mut with_seq = event("ui:1");
        with_seq.seq = Some(7);
        let e = bus.publish_transient(with_seq);
        assert_eq!(e.seq, None);
        assert_eq!(rx.recv().await.unwrap().id, e.id);
        assert!(bus.store().get_event("ui:1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn set_classifier_applies_to_later_publishes() {
        let bus = bus(classifier(&[]), LocalIndex::empty());
        assert!(bus.publish(event("c:1")).await.unwrap().alert);

        bus.set_classifier(classifier(&["pr.completed"]));
        assert!(!bus.publish(event("c:2")).await.unwrap().alert);
    }
}
