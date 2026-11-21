//! In-process event bus: classify, enrich with local paths, persist, broadcast.

use std::sync::{Arc, RwLock};

use tokio::sync::broadcast;

use crate::classify::Classifier;
use crate::event::Event;
use crate::local_repos::LocalIndex;
use crate::store::{Store, StoreError};

const CAPACITY: usize = 1024;

pub struct Bus {
    store: Arc<Store>,
    classifier: RwLock<Arc<Classifier>>,
    local_index: RwLock<Arc<LocalIndex>>,
    tx: broadcast::Sender<Arc<Event>>,
}

impl Bus {
    pub fn new(store: Arc<Store>, classifier: Classifier, local_index: LocalIndex) -> Bus {
        let (tx, _) = broadcast::channel(CAPACITY);
        Bus {
            store,
            classifier: RwLock::new(Arc::new(classifier)),
            local_index: RwLock::new(Arc::new(local_index)),
            tx,
        }
    }

    /// Persists and broadcasts a new event. `None` for a duplicate id, an empty id or a store error.
    pub fn publish(&self, e: Event) -> Option<Arc<Event>> {
        self.try_publish(e).ok().flatten()
    }

    /// Like `publish`, but a store error is returned instead of folded into `None`.
    /// `Ok(None)` is a duplicate id or an empty id.
    pub fn try_publish(&self, mut e: Event) -> Result<Option<Arc<Event>>, StoreError> {
        if e.id.is_empty() {
            tracing::error!(event_type = %e.event_type, "rejected event with empty id");
            return Ok(None);
        }
        self.prepare(&mut e);
        match self.store.insert_event_if_new(&e) {
            Ok(true) => {
                let e = Arc::new(e);
                // Err only means no subscribers.
                let _ = self.tx.send(e.clone());
                Ok(Some(e))
            }
            Ok(false) => Ok(None),
            Err(err) => {
                tracing::error!(id = %e.id, error = %err, "failed to persist event");
                Err(err)
            }
        }
    }

    /// Broadcasts without persisting.
    pub fn publish_transient(&self, mut e: Event) -> Arc<Event> {
        self.prepare(&mut e);
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

    pub fn store(&self) -> Arc<Store> {
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
    use crate::testing::{FakeProvider, FakeProviderFactory};
    use chrono::Utc;
    use std::collections::BTreeMap;

    fn classifier(mute: &[&str]) -> Classifier {
        let mut cfg = Config::from_toml_str(Config::default_toml()).unwrap();
        cfg.alerts.mute_types = mute.iter().map(|s| s.to_string()).collect();
        Classifier::new(&cfg, &BTreeMap::new())
    }

    fn bus(c: Classifier, idx: LocalIndex) -> Bus {
        Bus::new(Arc::new(Store::open_in_memory().unwrap()), c, idx)
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

        let published = bus.publish(event("c:1")).unwrap();
        assert_eq!(published.id, "c:1");
        assert_eq!(rx.recv().await.unwrap().id, "c:1");
        assert!(bus.store().get_event("c:1").unwrap().is_some());

        assert!(bus.publish(event("c:1")).is_none());
        assert!(matches!(bus.try_publish(event("c:1")), Ok(None)));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn publish_rejects_empty_id() {
        let bus = bus(classifier(&[]), LocalIndex::empty());
        let mut rx = bus.subscribe();

        assert!(bus.publish(event("")).is_none());
        assert!(rx.try_recv().is_err());
        assert!(bus.store().get_event("").unwrap().is_none());
    }

    #[test]
    fn publish_classifies_before_persisting() {
        let bus = bus(classifier(&[]), LocalIndex::empty());
        bus.publish(event("c:1")).unwrap();

        let stored = bus.store().get_event("c:1").unwrap().unwrap();
        assert_eq!(stored.severity, Severity::Success);
        assert!(stored.alert);
        assert!(stored.local.is_none());
    }

    #[test]
    fn publish_enriches_with_local_paths() {
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
            &registry,
        );
        let bus = bus(classifier(&[]), idx);

        let mut e = event("c:1");
        e.repo = Some(RepoRef {
            id: "1".into(),
            name: "Widgets".into(),
            namespace: Some("proj".into()),
        });
        bus.publish(e).unwrap();

        let stored = bus.store().get_event("c:1").unwrap().unwrap();
        assert_eq!(
            stored.local.unwrap().repo_path,
            Some(clone.display().to_string())
        );
    }

    #[tokio::test]
    async fn publish_transient_broadcasts_without_storing() {
        let bus = bus(classifier(&[]), LocalIndex::empty());
        let mut rx = bus.subscribe();

        let e = bus.publish_transient(event("ui:1"));
        assert_eq!(rx.recv().await.unwrap().id, e.id);
        assert!(bus.store().get_event("ui:1").unwrap().is_none());
    }

    #[test]
    fn set_classifier_applies_to_later_publishes() {
        let bus = bus(classifier(&[]), LocalIndex::empty());
        assert!(bus.publish(event("c:1")).unwrap().alert);

        bus.set_classifier(classifier(&["pr.completed"]));
        assert!(!bus.publish(event("c:2")).unwrap().alert);
    }
}
