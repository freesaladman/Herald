//! SQLite persistence: events, watermarks, provider snapshots, hook runs and UI state.

use std::path::Path;
use std::sync::{Mutex, MutexGuard};

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::event::Event;

const SCHEMA_VERSION: i64 = 1;

const BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    connection_id TEXT NOT NULL,
    occurred_utc TEXT NOT NULL,
    detected_utc TEXT NOT NULL,
    subject_key TEXT NOT NULL,
    alert INTEGER NOT NULL,
    catch_up INTEGER NOT NULL,
    test INTEGER NOT NULL,
    viewed_utc TEXT NULL,
    json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_alert_viewed ON events (alert, viewed_utc, occurred_utc);
CREATE INDEX IF NOT EXISTS idx_events_occurred ON events (occurred_utc);
CREATE TABLE IF NOT EXISTS watermarks (
    key TEXT PRIMARY KEY,
    last_synced_utc TEXT NOT NULL,
    last_success_utc TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS snapshots (
    key TEXT PRIMARY KEY,
    json TEXT NOT NULL,
    updated_utc TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS hook_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hook TEXT NOT NULL,
    event_id TEXT NOT NULL,
    started_utc TEXT NOT NULL,
    finished_utc TEXT NULL,
    exit_code INTEGER NULL,
    error TEXT NULL,
    stderr_tail TEXT NULL
);
CREATE TABLE IF NOT EXISTS ui_state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
";

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid timestamp in store: {0}")]
    Timestamp(#[from] chrono::ParseError),
    #[error("store lock poisoned")]
    Poisoned,
    #[error(
        "database schema version {found} is newer than this build supports ({supported}); upgrade Herald"
    )]
    SchemaTooNew { found: i64, supported: i64 },
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

pub struct Store {
    conn: Mutex<Connection>,
}

fn ts(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn parse_ts(s: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(s)?.with_timezone(&Utc))
}

/// Refuses a database written by a newer build; a missing meta table or version is fine.
fn check_schema_version(conn: &Connection) -> Result<()> {
    let has_meta: bool = conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'meta')",
        [],
        |r| r.get(0),
    )?;
    if !has_meta {
        return Ok(());
    }
    let value: Option<String> = conn
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |r| r.get(0),
        )
        .optional()?;
    match value.and_then(|v| v.trim().parse::<i64>().ok()) {
        Some(found) if found > SCHEMA_VERSION => Err(StoreError::SchemaTooNew {
            found,
            supported: SCHEMA_VERSION,
        }),
        _ => Ok(()),
    }
}

impl Store {
    /// Opens (creating if needed) a read-write database and migrates its schema.
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.busy_timeout(BUSY_TIMEOUT)?;
        // Before the WAL pragma, so a newer database is refused without being modified.
        check_schema_version(&conn)?;
        conn.query_row("PRAGMA journal_mode = WAL", [], |_| Ok(()))?;
        Self::init(conn)
    }

    pub fn open_in_memory() -> Result<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    /// Opens an existing database without creating or migrating anything.
    pub fn open_read_only(path: &Path) -> Result<Self> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.busy_timeout(BUSY_TIMEOUT)?;
        check_schema_version(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn init(conn: Connection) -> Result<Self> {
        // Check first so a newer database is refused before the DDL touches it.
        check_schema_version(&conn)?;
        conn.execute_batch(SCHEMA)?;
        conn.execute(
            "INSERT OR IGNORE INTO meta (key, value) VALUES ('schema_version', ?1)",
            params![SCHEMA_VERSION.to_string()],
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn lock(&self) -> Result<MutexGuard<'_, Connection>> {
        self.conn.lock().map_err(|_| StoreError::Poisoned)
    }

    /// Returns false when an event with the same id already exists.
    pub fn insert_event_if_new(&self, e: &Event) -> Result<bool> {
        let json = serde_json::to_string(e)?;
        let n = self.lock()?.execute(
            "INSERT OR IGNORE INTO events
             (id, type, connection_id, occurred_utc, detected_utc, subject_key, alert, catch_up, test, viewed_utc, json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, ?10)",
            params![
                e.id,
                e.event_type,
                e.connection.id,
                ts(e.occurred_utc),
                ts(e.detected_utc),
                e.subject_key,
                e.alert,
                e.catch_up,
                e.test,
                json
            ],
        )?;
        Ok(n > 0)
    }

    pub fn get_event(&self, id: &str) -> Result<Option<Event>> {
        let json: Option<String> = self
            .lock()?
            .query_row("SELECT json FROM events WHERE id = ?1", params![id], |r| {
                r.get(0)
            })
            .optional()?;
        json.map(|j| Ok(serde_json::from_str(&j)?)).transpose()
    }

    fn query_events(&self, sql: &str, p: impl rusqlite::Params) -> Result<Vec<Event>> {
        let conn = self.lock()?;
        let mut stmt = conn.prepare(sql)?;
        let rows = stmt.query_map(p, |r| r.get::<_, String>(0))?;
        let mut out = Vec::new();
        for json in rows {
            out.push(serde_json::from_str(&json?)?);
        }
        Ok(out)
    }

    /// Alerting events not yet viewed, newest first.
    pub fn unviewed(&self, limit: u32) -> Result<Vec<Event>> {
        self.query_events(
            "SELECT json FROM events WHERE alert = 1 AND viewed_utc IS NULL
             ORDER BY occurred_utc DESC, id LIMIT ?1",
            params![limit],
        )
    }

    pub fn unviewed_count(&self) -> Result<u32> {
        Ok(self.lock()?.query_row(
            "SELECT COUNT(*) FROM events WHERE alert = 1 AND viewed_utc IS NULL",
            [],
            |r| r.get(0),
        )?)
    }

    pub fn mark_viewed(&self, ids: &[String]) -> Result<()> {
        let mut conn = self.lock()?;
        let now = ts(Utc::now());
        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "UPDATE events SET viewed_utc = ?1 WHERE id = ?2 AND viewed_utc IS NULL",
            )?;
            for id in ids {
                stmt.execute(params![now, id])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn mark_all_viewed(&self) -> Result<()> {
        self.lock()?.execute(
            "UPDATE events SET viewed_utc = ?1 WHERE alert = 1 AND viewed_utc IS NULL",
            params![ts(Utc::now())],
        )?;
        Ok(())
    }

    /// Events with `occurred_utc >= since`, oldest first.
    pub fn events_since(&self, since: DateTime<Utc>, limit: u32) -> Result<Vec<Event>> {
        self.query_events(
            "SELECT json FROM events WHERE occurred_utc >= ?1
             ORDER BY occurred_utc ASC, id LIMIT ?2",
            params![ts(since), limit],
        )
    }

    pub fn get_watermark(&self, key: &str) -> Result<Option<Watermark>> {
        let row: Option<(String, String)> = self
            .lock()?
            .query_row(
                "SELECT last_synced_utc, last_success_utc FROM watermarks WHERE key = ?1",
                params![key],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        row.map(|(s, ok)| {
            Ok(Watermark {
                last_synced_utc: parse_ts(&s)?,
                last_success_utc: parse_ts(&ok)?,
            })
        })
        .transpose()
    }

    pub fn set_watermark(
        &self,
        key: &str,
        synced: DateTime<Utc>,
        success: DateTime<Utc>,
    ) -> Result<()> {
        self.lock()?.execute(
            "INSERT INTO watermarks (key, last_synced_utc, last_success_utc) VALUES (?1, ?2, ?3)
             ON CONFLICT(key) DO UPDATE SET
                last_synced_utc = excluded.last_synced_utc,
                last_success_utc = excluded.last_success_utc",
            params![key, ts(synced), ts(success)],
        )?;
        Ok(())
    }

    pub fn get_snapshot<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        let json: Option<String> = self
            .lock()?
            .query_row(
                "SELECT json FROM snapshots WHERE key = ?1",
                params![key],
                |r| r.get(0),
            )
            .optional()?;
        json.map(|j| Ok(serde_json::from_str(&j)?)).transpose()
    }

    pub fn put_snapshot<T: Serialize>(&self, key: &str, value: &T) -> Result<()> {
        let json = serde_json::to_string(value)?;
        self.lock()?.execute(
            "INSERT INTO snapshots (key, json, updated_utc) VALUES (?1, ?2, ?3)
             ON CONFLICT(key) DO UPDATE SET json = excluded.json, updated_utc = excluded.updated_utc",
            params![key, json, ts(Utc::now())],
        )?;
        Ok(())
    }

    pub fn record_hook_run(&self, run: HookRun) -> Result<()> {
        self.lock()?.execute(
            "INSERT INTO hook_runs
             (hook, event_id, started_utc, finished_utc, exit_code, error, stderr_tail)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                run.hook,
                run.event_id,
                ts(run.started_utc),
                run.finished_utc.map(ts),
                run.exit_code,
                run.error,
                run.stderr_tail
            ],
        )?;
        Ok(())
    }

    pub fn get_ui_state(&self, key: &str) -> Result<Option<String>> {
        Ok(self
            .lock()?
            .query_row(
                "SELECT value FROM ui_state WHERE key = ?1",
                params![key],
                |r| r.get(0),
            )
            .optional()?)
    }

    pub fn set_ui_state(&self, key: &str, value: &str) -> Result<()> {
        self.lock()?.execute(
            "INSERT INTO ui_state (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    /// Deletes events (by `occurred_utc`) and hook runs (by `started_utc`) older than the
    /// cutoff. Watermarks, snapshots and UI state are never pruned.
    pub fn prune(&self, older_than_days: u32) -> Result<()> {
        let cutoff = ts(Utc::now() - Duration::days(i64::from(older_than_days)));
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        tx.execute(
            "DELETE FROM events WHERE occurred_utc < ?1",
            params![cutoff],
        )?;
        tx.execute(
            "DELETE FROM hook_runs WHERE started_utc < ?1",
            params![cutoff],
        )?;
        tx.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::ConnectionRef;
    use chrono::TimeZone;
    use pretty_assertions::assert_eq;

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
        e
    }

    fn ids(v: &[Event]) -> Vec<&str> {
        v.iter().map(|e| e.id.as_str()).collect()
    }

    #[test]
    fn insert_event_if_new_dedupes_by_id() {
        let s = Store::open_in_memory().unwrap();
        let e = event("a", at(0), true);
        assert!(s.insert_event_if_new(&e).unwrap());
        assert!(!s.insert_event_if_new(&e).unwrap());
        assert_eq!(s.get_event("a").unwrap(), Some(e));
        assert_eq!(s.get_event("zzz").unwrap(), None);
    }

    #[test]
    fn unviewed_orders_newest_first_and_mark_viewed_clears() {
        let s = Store::open_in_memory().unwrap();
        s.insert_event_if_new(&event("old", at(1), true)).unwrap();
        s.insert_event_if_new(&event("new", at(3), true)).unwrap();
        s.insert_event_if_new(&event("mid", at(2), true)).unwrap();
        s.insert_event_if_new(&event("quiet", at(4), false))
            .unwrap();

        assert_eq!(s.unviewed_count().unwrap(), 3);
        assert_eq!(ids(&s.unviewed(10).unwrap()), ["new", "mid", "old"]);
        assert_eq!(ids(&s.unviewed(2).unwrap()), ["new", "mid"]);

        s.mark_viewed(&["new".to_string()]).unwrap();
        assert_eq!(ids(&s.unviewed(10).unwrap()), ["mid", "old"]);

        s.mark_all_viewed().unwrap();
        assert_eq!(s.unviewed_count().unwrap(), 0);
        assert!(s.unviewed(10).unwrap().is_empty());
    }

    #[test]
    fn watermark_round_trips_and_updates() {
        let s = Store::open_in_memory().unwrap();
        assert_eq!(s.get_watermark("c:prs").unwrap(), None);
        s.set_watermark("c:prs", at(1), at(0)).unwrap();
        s.set_watermark("c:prs", at(5), at(4)).unwrap();
        assert_eq!(
            s.get_watermark("c:prs").unwrap(),
            Some(Watermark {
                last_synced_utc: at(5),
                last_success_utc: at(4)
            })
        );
    }

    #[test]
    fn snapshot_round_trips_a_struct() {
        #[derive(Debug, PartialEq, Serialize, serde::Deserialize)]
        struct Snap {
            ids: Vec<u32>,
            name: String,
        }
        let s = Store::open_in_memory().unwrap();
        assert_eq!(s.get_snapshot::<Snap>("k").unwrap(), None);
        let v = Snap {
            ids: vec![1, 2],
            name: "x".into(),
        };
        s.put_snapshot("k", &v).unwrap();
        assert_eq!(s.get_snapshot::<Snap>("k").unwrap(), Some(v));
    }

    #[test]
    fn ui_state_round_trips() {
        let s = Store::open_in_memory().unwrap();
        assert_eq!(s.get_ui_state("pinned").unwrap(), None);
        s.set_ui_state("pinned", "true").unwrap();
        s.set_ui_state("pinned", "false").unwrap();
        assert_eq!(s.get_ui_state("pinned").unwrap().as_deref(), Some("false"));
    }

    #[test]
    fn events_since_is_ascending_and_inclusive() {
        let s = Store::open_in_memory().unwrap();
        s.insert_event_if_new(&event("c", at(30), false)).unwrap();
        s.insert_event_if_new(&event("a", at(10), true)).unwrap();
        s.insert_event_if_new(&event("b", at(20), true)).unwrap();
        s.insert_event_if_new(&event("early", at(5), true)).unwrap();

        assert_eq!(ids(&s.events_since(at(10), 10).unwrap()), ["a", "b", "c"]);
        assert_eq!(ids(&s.events_since(at(10), 2).unwrap()), ["a", "b"]);
    }

    #[test]
    fn prune_removes_old_events_and_hook_runs_only() {
        let s = Store::open_in_memory().unwrap();
        let now = Utc::now();
        s.insert_event_if_new(&event("old", now - Duration::days(200), true))
            .unwrap();
        s.insert_event_if_new(&event("recent", now - Duration::days(1), true))
            .unwrap();
        s.set_watermark("w", now - Duration::days(400), now - Duration::days(400))
            .unwrap();
        s.put_snapshot("snap", &1u32).unwrap();
        s.set_ui_state("k", "v").unwrap();
        for (name, age) in [("old-run", 200), ("new-run", 1)] {
            s.record_hook_run(HookRun {
                hook: name.into(),
                event_id: "e".into(),
                started_utc: now - Duration::days(age),
                finished_utc: None,
                exit_code: Some(0),
                error: None,
                stderr_tail: None,
            })
            .unwrap();
        }

        s.prune(180).unwrap();

        assert_eq!(s.get_event("old").unwrap(), None);
        assert!(s.get_event("recent").unwrap().is_some());
        assert!(s.get_watermark("w").unwrap().is_some());
        assert_eq!(s.get_snapshot::<u32>("snap").unwrap(), Some(1));
        assert_eq!(s.get_ui_state("k").unwrap().as_deref(), Some("v"));
        let conn = s.lock().unwrap();
        let mut stmt = conn.prepare("SELECT hook FROM hook_runs").unwrap();
        let runs: Vec<String> = stmt
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(runs, ["new-run"]);
    }

    #[test]
    fn open_sets_schema_version_and_read_only_cannot_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("herald.db");
        {
            let s = Store::open(&path).unwrap();
            s.insert_event_if_new(&event("a", at(0), true)).unwrap();
            let v: String = s
                .lock()
                .unwrap()
                .query_row(
                    "SELECT value FROM meta WHERE key = 'schema_version'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(v, "1");
        }
        let ro = Store::open_read_only(&path).unwrap();
        assert_eq!(ro.unviewed_count().unwrap(), 1);
        assert!(ro.get_event("a").unwrap().is_some());
        assert!(ro.set_ui_state("k", "v").is_err());
        assert!(ro.insert_event_if_new(&event("b", at(1), true)).is_err());
    }

    #[test]
    fn reader_reads_while_writer_inserts_in_wal_mode() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("herald.db");
        let writer = Store::open(&path).unwrap();
        let reader = Store::open_read_only(&path).unwrap();

        let mode: String = reader
            .lock()
            .unwrap()
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert_eq!(mode, "wal");

        std::thread::scope(|scope| {
            scope.spawn(|| {
                for i in 0..200 {
                    let id = format!("e{i}");
                    assert!(
                        writer
                            .insert_event_if_new(&event(&id, at(i), true))
                            .unwrap()
                    );
                }
            });
            scope.spawn(|| {
                for _ in 0..200 {
                    reader.unviewed_count().unwrap();
                }
            });
        });
        assert_eq!(reader.unviewed_count().unwrap(), 200);
    }

    #[test]
    fn open_refuses_a_newer_schema_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("herald.db");
        let newer = (SCHEMA_VERSION + 1).to_string();
        {
            let s = Store::open(&path).unwrap();
            let conn = s.lock().unwrap();
            conn.execute(
                "UPDATE meta SET value = ?1 WHERE key = 'schema_version'",
                params![newer],
            )
            .unwrap();
            conn.execute_batch("DROP TABLE ui_state").unwrap();
        }
        for result in [Store::open(&path), Store::open_read_only(&path)] {
            match result {
                Err(StoreError::SchemaTooNew { found, supported }) => {
                    assert_eq!(found, SCHEMA_VERSION + 1);
                    assert_eq!(supported, SCHEMA_VERSION);
                }
                other => panic!("expected SchemaTooNew, got {:?}", other.err()),
            }
        }
        let conn = Connection::open(&path).unwrap();
        let has_ui_state: bool = conn
            .query_row(
                "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE name = 'ui_state')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(!has_ui_state, "refused open must not run the schema DDL");
    }
}
