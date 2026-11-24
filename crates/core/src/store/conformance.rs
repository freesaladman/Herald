//! Behaviour suite every `Store` backend must pass. Callers supply a fresh, empty store.

use chrono::{DateTime, Duration, TimeZone, Utc};

use super::{HookRun, Store, Watermark};
use crate::event::{ActorKind, ConnectionRef, Event, Level};

fn at(mins: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap() + Duration::minutes(mins)
}

fn event(id: &str, occurred: DateTime<Utc>, alert: bool) -> Event {
    let conn = ConnectionRef {
        id: "c".into(),
        provider: "p".into(),
    };
    let mut e = Event::new(conn, "pr.created", occurred, occurred, "t", "c:pr:1:1").with_id(id);
    e.alert = alert;
    e.level = if alert { Level::Alert } else { Level::Pane };
    e
}

fn ids(v: &[Event]) -> Vec<&str> {
    v.iter().map(|e| e.id.as_str()).collect()
}

pub async fn run(store: &dyn Store) {
    events_dedupe_and_read_back(store).await;
    unviewed_and_mark_viewed(store).await;
    events_since_is_ascending_and_inclusive(store).await;
    watermarks(store).await;
    snapshots(store).await;
    ui_state(store).await;
    prune_removes_only_old_events_and_hook_runs(store).await;
    seq_contract(store).await;
    pane_levels(store).await;
}

async fn events_dedupe_and_read_back(store: &dyn Store) {
    let mut e = event("dedupe", at(0), false);
    e.level = Level::Ignore;
    let seq = store.insert_event_if_new(&e).await.unwrap();
    assert!(seq.is_some());
    assert_eq!(store.insert_event_if_new(&e).await.unwrap(), None);
    let mut stored = e;
    stored.seq = seq;
    assert_eq!(store.get_event("dedupe").await.unwrap(), Some(stored));
    assert_eq!(store.get_event("missing").await.unwrap(), None);
}

async fn unviewed_and_mark_viewed(store: &dyn Store) {
    for (id, mins, alert) in [
        ("uv-old", 101, true),
        ("uv-new", 103, true),
        ("uv-mid", 102, true),
        ("uv-quiet", 104, false),
    ] {
        let mut e = event(id, at(mins), alert);
        if id == "uv-quiet" {
            e.level = Level::Ignore;
        }
        assert!(store.insert_event_if_new(&e).await.unwrap().is_some());
    }
    assert_eq!(store.unviewed_count().await.unwrap(), 3);

    let all = store.unviewed(1000).await.unwrap();
    assert_eq!(ids(&all), ["uv-new", "uv-mid", "uv-old"]);
    assert_eq!(ids(&store.unviewed(2).await.unwrap()), ["uv-new", "uv-mid"]);

    store.mark_viewed(&["uv-new".to_string()]).await.unwrap();
    assert_eq!(store.unviewed_count().await.unwrap(), 2);
    assert_eq!(
        ids(&store.unviewed(1000).await.unwrap()),
        ["uv-mid", "uv-old"]
    );
    // A repeat mark is harmless.
    store.mark_viewed(&["uv-new".to_string()]).await.unwrap();
    assert_eq!(store.unviewed_count().await.unwrap(), 2);

    store.mark_all_viewed().await.unwrap();
    assert_eq!(store.unviewed_count().await.unwrap(), 0);
    assert!(store.unviewed(10).await.unwrap().is_empty());
}

async fn events_since_is_ascending_and_inclusive(store: &dyn Store) {
    for (id, mins) in [
        ("es-c", 230),
        ("es-a", 210),
        ("es-b", 220),
        ("es-early", 205),
    ] {
        store
            .insert_event_if_new(&event(id, at(mins), false))
            .await
            .unwrap();
    }
    let got = store.events_since(at(210), 1000).await.unwrap();
    assert_eq!(ids(&got), ["es-a", "es-b", "es-c"]);
    let limited = store.events_since(at(210), 2).await.unwrap();
    assert_eq!(ids(&limited), ["es-a", "es-b"]);
}

async fn watermarks(store: &dyn Store) {
    assert_eq!(store.get_watermark("c:prs").await.unwrap(), None);
    store.set_watermark("c:prs", at(1), at(0)).await.unwrap();
    store.set_watermark("c:prs", at(5), at(4)).await.unwrap();
    assert_eq!(
        store.get_watermark("c:prs").await.unwrap(),
        Some(Watermark {
            last_synced_utc: at(5),
            last_success_utc: at(4)
        })
    );
    assert_eq!(store.get_watermark("c:other").await.unwrap(), None);
}

async fn snapshots(store: &dyn Store) {
    assert_eq!(store.get_snapshot_json("snap-a").await.unwrap(), None);
    let v = serde_json::json!({ "ids": [1, 2], "name": "x" });
    store.put_snapshot_json("snap-a", v.clone()).await.unwrap();
    assert_eq!(store.get_snapshot_json("snap-a").await.unwrap(), Some(v));
    store
        .put_snapshot_json("snap-a", serde_json::json!(7))
        .await
        .unwrap();
    assert_eq!(store.get_snapshot::<u32>("snap-a").await.unwrap(), Some(7));

    store
        .put_snapshot("snap-b", &vec!["a".to_string()])
        .await
        .unwrap();
    assert_eq!(
        store.get_snapshot::<Vec<String>>("snap-b").await.unwrap(),
        Some(vec!["a".to_string()])
    );
    assert_eq!(store.get_snapshot::<u32>("snap-none").await.unwrap(), None);
}

async fn ui_state(store: &dyn Store) {
    assert_eq!(store.get_ui_state("pinned").await.unwrap(), None);
    store.set_ui_state("pinned", "true").await.unwrap();
    store.set_ui_state("pinned", "false").await.unwrap();
    assert_eq!(
        store.get_ui_state("pinned").await.unwrap().as_deref(),
        Some("false")
    );
}

async fn prune_removes_only_old_events_and_hook_runs(store: &dyn Store) {
    let now = Utc::now();
    store
        .insert_event_if_new(&event("pr-old", now - Duration::days(200), true))
        .await
        .unwrap();
    store
        .insert_event_if_new(&event("pr-recent", now - Duration::days(1), true))
        .await
        .unwrap();
    store
        .set_watermark("pr-w", now - Duration::days(400), now - Duration::days(400))
        .await
        .unwrap();
    store.put_snapshot("pr-snap", &1u32).await.unwrap();
    store.set_ui_state("pr-k", "v").await.unwrap();
    for (name, age) in [("old-run", 200), ("new-run", 1)] {
        store
            .record_hook_run(HookRun {
                hook: name.into(),
                event_id: "e".into(),
                started_utc: now - Duration::days(age),
                finished_utc: Some(now),
                exit_code: Some(0),
                error: None,
                stderr_tail: Some("tail".into()),
            })
            .await
            .unwrap();
    }

    store.prune(180).await.unwrap();

    assert_eq!(store.get_event("pr-old").await.unwrap(), None);
    assert!(store.get_event("pr-recent").await.unwrap().is_some());
    assert!(store.get_watermark("pr-w").await.unwrap().is_some());
    assert_eq!(store.get_snapshot::<u32>("pr-snap").await.unwrap(), Some(1));
    assert_eq!(
        store.get_ui_state("pr-k").await.unwrap().as_deref(),
        Some("v")
    );
}

/// Asserts only ordering and monotonicity, so a backend with seq gaps passes.
async fn seq_contract(store: &dyn Store) {
    let id = store.store_id().await.unwrap();
    assert!(!id.is_empty());
    assert_eq!(store.store_id().await.unwrap(), id);

    let first = store
        .insert_event_if_new(&event("sq-1", at(300), false))
        .await
        .unwrap()
        .unwrap();
    let second = store
        .insert_event_if_new(&event("sq-2", at(299), false))
        .await
        .unwrap()
        .unwrap();
    let third = store
        .insert_event_if_new(&event("sq-3", at(301), false))
        .await
        .unwrap()
        .unwrap();
    assert!(first < second && second < third);
    assert_eq!(
        store
            .insert_event_if_new(&event("sq-2", at(0), false))
            .await
            .unwrap(),
        None
    );

    let after = store.events_after_seq(first, 1000).await.unwrap();
    assert_eq!(ids(&after), ["sq-2", "sq-3"]);
    assert_eq!(
        after.iter().map(|e| e.seq).collect::<Vec<_>>(),
        [Some(second), Some(third)]
    );
    assert_eq!(
        ids(&store.events_after_seq(first, 1).await.unwrap()),
        ["sq-2"]
    );
    assert!(
        store
            .events_after_seq(third, 1000)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store.get_event("sq-1").await.unwrap().unwrap().seq,
        Some(first)
    );

    assert!(store.latest_seq().await.unwrap() >= third);
    assert!(store.oldest_seq().await.unwrap().unwrap() <= first);

    // A pruned seq is never handed out again.
    let old = store
        .insert_event_if_new(&event("sq-old", Utc::now() - Duration::days(200), false))
        .await
        .unwrap()
        .unwrap();
    store.prune(180).await.unwrap();
    assert_eq!(store.get_event("sq-old").await.unwrap(), None);
    assert!(store.latest_seq().await.unwrap() >= old);
    let next = store
        .insert_event_if_new(&event("sq-next", at(302), false))
        .await
        .unwrap()
        .unwrap();
    assert!(next > old);
}

async fn pane_levels(store: &dyn Store) {
    store.mark_all_viewed().await.unwrap();
    let alert = event("pl-alert", at(301), true);
    let pane = event("pl-pane", at(302), false);
    let mut ignore = event("pl-ignore", at(303), false);
    ignore.level = Level::Ignore;
    let mut mine = event("pl-me", at(304), false);
    mine.actor.kind = ActorKind::Me;
    for e in [&alert, &pane, &ignore, &mine] {
        assert!(store.insert_event_if_new(e).await.unwrap().is_some());
    }

    // Ignore and Me events are stored and replayable but never listed.
    assert_eq!(
        ids(&store.unviewed(1000).await.unwrap()),
        ["pl-pane", "pl-alert"]
    );
    assert_eq!(store.unviewed_count().await.unwrap(), 1);
    let mine_only = |v: &'_ [Event]| -> Vec<String> {
        v.iter()
            .filter(|e| e.id.starts_with("pl-"))
            .map(|e| e.id.clone())
            .collect()
    };
    let all = ["pl-alert", "pl-pane", "pl-ignore", "pl-me"];
    let since = store.events_since(at(301), 1000).await.unwrap();
    assert_eq!(mine_only(&since), all);
    let after = store.events_after_seq(0, 1000).await.unwrap();
    assert_eq!(mine_only(&after), all);

    store.mark_all_viewed().await.unwrap();
    assert!(store.unviewed(1000).await.unwrap().is_empty());
    assert_eq!(store.unviewed_count().await.unwrap(), 0);
}
