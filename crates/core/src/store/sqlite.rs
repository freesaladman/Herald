//! SQLite persistence: events, watermarks, provider snapshots, hook runs and UI state.

use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use futures_util::future::BoxFuture;
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::{HookRun, Result, Store, StoreError, Watermark};
use crate::event::Event;

const SCHEMA_VERSION: i64 = 3;

const BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// The current `events` table and its indexes. A macro so the migration and `SCHEMA` share one text.
macro_rules! events_ddl {
    () => {
        "
CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL,
    connection_id TEXT NOT NULL,
    occurred_utc TEXT NOT NULL,
    detected_utc TEXT NOT NULL,
    subject_key TEXT NOT NULL,
    alert INTEGER NOT NULL,
    catch_up INTEGER NOT NULL,
    test INTEGER NOT NULL,
    viewed_utc TEXT NULL,
    json TEXT NOT NULL,
    pane INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_events_alert_viewed ON events (alert, viewed_utc, occurred_utc);
CREATE INDEX IF NOT EXISTS idx_events_pane_viewed ON events (pane, viewed_utc, occurred_utc);
CREATE INDEX IF NOT EXISTS idx_events_occurred ON events (occurred_utc);
"
    };
}

const SCHEMA: &str = concat!(
    events_ddl!(),
    "
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
"
);

impl From<rusqlite::Error> for StoreError {
    fn from(e: rusqlite::Error) -> Self {
        StoreError::Backend(Box::new(e))
    }
}

impl From<chrono::ParseError> for StoreError {
    fn from(e: chrono::ParseError) -> Self {
        StoreError::Backend(Box::new(e))
    }
}

/// Clones share one connection.
#[derive(Clone)]
pub struct SqliteStore {
    conn: Arc<Mutex<Connection>>,
    /// False only for a read-only open of a v1 database, whose `events` table has no `seq` column.
    has_seq: bool,
    /// False only for a read-only open of a pre-v3 database, whose `events` table has no `pane` column.
    has_pane: bool,
    /// Test builds (unit tests, or the `testing` feature) run in-memory stores on the calling task: tokio's paused clock treats a
    /// pending `spawn_blocking` as idle and would jump time past it.
    #[cfg(any(test, feature = "testing"))]
    inline: bool,
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

fn events_has_pane(conn: &Connection) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM pragma_table_info('events') WHERE name = 'pane')",
        [],
        |r| r.get(0),
    )?)
}

fn events_has_seq(conn: &Connection) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM pragma_table_info('events') WHERE name = 'seq')",
        [],
        |r| r.get(0),
    )?)
}

/// A v1 database has an `events` table without `seq`.
fn needs_v2_migration(conn: &Connection) -> Result<bool> {
    let has_events: bool = conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'events')",
        [],
        |r| r.get(0),
    )?;
    Ok(has_events && !events_has_seq(conn)?)
}

fn migration_error(version: u32, what: &str, e: impl std::fmt::Display) -> StoreError {
    StoreError::Backend(format!("schema migration to v{version} failed ({what}): {e}").into())
}

/// `<db>.v<N>.bak`, or `<db>.v<N>.<UTC yyyymmddThhmmss>.bak` when that already exists, where N is the
/// schema version being backed up.
fn backup_path(db: &Path, version: u32) -> std::path::PathBuf {
    let with_suffix = |suffix: &str| {
        let mut name = db.as_os_str().to_owned();
        name.push(suffix);
        std::path::PathBuf::from(name)
    };
    let plain = with_suffix(&format!(".v{version}.bak"));
    if !plain.exists() {
        return plain;
    }
    with_suffix(&format!(
        ".v{version}.{}.bak",
        Utc::now().format("%Y%m%dT%H%M%S")
    ))
}

/// Rebuilds `events` with an AUTOINCREMENT `seq` primary key, keeping rows in their old rowid
/// order, and records version 2, all in one transaction: a failure leaves the database at v1 and
/// the next open retries. A file database is first copied with `VACUUM INTO`, which includes the
/// WAL contents (a plain file copy would not).
fn migrate_v1_to_v2(conn: &mut Connection, db_path: Option<&Path>) -> Result<()> {
    migrate_v1_to_v2_with(conn, db_path, || {})
}

/// `before_begin` runs after the backup and before the write lock is taken, where a racing opener can slip in.
fn migrate_v1_to_v2_with(
    conn: &mut Connection,
    db_path: Option<&Path>,
    before_begin: impl FnOnce(),
) -> Result<()> {
    if !needs_v2_migration(conn)? {
        return Ok(());
    }
    if let Some(db) = db_path {
        let bak = backup_path(db, 1);
        conn.execute("VACUUM INTO ?1", params![bak.to_string_lossy()])
            .map_err(|e| migration_error(2, "backup", e))?;
        tracing::info!(backup = %bak.display(), "backed up database before schema migration");
    }
    before_begin();
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| migration_error(2, "begin", e))?;
    // Another opener may have migrated while this one waited for the lock; a second rebuild would
    // renumber seqs densely and reuse pruned ones.
    if !needs_v2_migration(&tx)? {
        return Ok(());
    }
    tx.execute_batch(concat!(
        "DROP INDEX IF EXISTS idx_events_alert_viewed;
         DROP INDEX IF EXISTS idx_events_occurred;
         DROP TABLE IF EXISTS events_v1;
         ALTER TABLE events RENAME TO events_v1;",
        events_ddl!(),
        "INSERT INTO events
             (id, type, connection_id, occurred_utc, detected_utc, subject_key, alert, catch_up, test, viewed_utc, json)
         SELECT id, type, connection_id, occurred_utc, detected_utc, subject_key, alert, catch_up, test, viewed_utc, json
         FROM events_v1 ORDER BY rowid;
         DROP TABLE events_v1;
         CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
         INSERT OR IGNORE INTO meta (key, value) VALUES ('schema_version', '1');",
    ))
    .map_err(|e| migration_error(2, "rebuild", e))?;
    tx.execute(
        "UPDATE meta SET value = ?1 WHERE key = 'schema_version'",
        params!["2"],
    )
    .map_err(|e| migration_error(2, "version", e))?;
    tx.commit().map_err(|e| migration_error(2, "commit", e))?;
    Ok(())
}

fn stored_schema_version(conn: &Connection) -> Result<Option<i64>> {
    let has_meta: bool = conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'meta')",
        [],
        |r| r.get(0),
    )?;
    if !has_meta {
        return Ok(None);
    }
    let value: Option<String> = conn
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |r| r.get(0),
        )
        .optional()?;
    Ok(value.and_then(|v| v.trim().parse::<i64>().ok()))
}

/// A pre-v3 database has an `events` table and a stored version below 3 (or none). A brand-new
/// database has no `events` table and is created current by `SCHEMA`.
fn needs_v3_migration(conn: &Connection) -> Result<bool> {
    let has_events: bool = conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'events')",
        [],
        |r| r.get(0),
    )?;
    Ok(has_events && stored_schema_version(conn)?.is_none_or(|v| v < 3))
}

/// Adds `events.pane` (when missing), backfills it from `alert`, gives stored json a `level`, indexes
/// the column and records version 3, all in one transaction. A file database is first copied with
/// `VACUUM INTO`.
fn migrate_v2_to_v3(conn: &mut Connection, db_path: Option<&Path>) -> Result<()> {
    migrate_v2_to_v3_with(conn, db_path, || {})
}

/// `before_begin` runs after the backup and before the write lock is taken, where a racing opener can slip in.
fn migrate_v2_to_v3_with(
    conn: &mut Connection,
    db_path: Option<&Path>,
    before_begin: impl FnOnce(),
) -> Result<()> {
    if !needs_v3_migration(conn)? {
        return Ok(());
    }
    if let Some(db) = db_path {
        let bak = backup_path(db, 2);
        conn.execute("VACUUM INTO ?1", params![bak.to_string_lossy()])
            .map_err(|e| migration_error(3, "backup", e))?;
        tracing::info!(backup = %bak.display(), "backed up database before schema migration");
    }
    before_begin();
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| migration_error(3, "begin", e))?;
    if !needs_v3_migration(&tx)? {
        return Ok(());
    }
    if !events_has_pane(&tx)? {
        tx.execute_batch("ALTER TABLE events ADD COLUMN pane INTEGER NOT NULL DEFAULT 0;")
            .map_err(|e| migration_error(3, "column", e))?;
    }
    // Before v3 only alerting events were listed, so pane = alert preserves what the pane showed.
    tx.execute_batch(
        "UPDATE events SET pane = alert;
         UPDATE events SET json = json_set(json, '$.level', CASE WHEN alert = 1 THEN 'alert' ELSE 'pane' END)
             WHERE json_extract(json, '$.level') IS NULL;
         CREATE INDEX IF NOT EXISTS idx_events_pane_viewed ON events (pane, viewed_utc, occurred_utc);",
    )
    .map_err(|e| migration_error(3, "backfill", e))?;
    tx.execute(
        "INSERT INTO meta (key, value) VALUES ('schema_version', '3')
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [],
    )
    .map_err(|e| migration_error(3, "version", e))?;
    tx.commit().map_err(|e| migration_error(3, "commit", e))?;
    Ok(())
}

/// Sync methods kept so existing callers compile; the `Store` impl below delegates to them.
impl SqliteStore {
    /// Opens (creating if needed) a read-write database and migrates its schema.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_busy_timeout(path, BUSY_TIMEOUT)
    }

    fn open_with_busy_timeout(path: &Path, busy: std::time::Duration) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.busy_timeout(busy)?;
        // Before the WAL pragma, so a newer database is refused without being modified.
        check_schema_version(&conn)?;
        conn.query_row("PRAGMA journal_mode = WAL", [], |_| Ok(()))?;
        Self::init(conn, Some(path))
    }

    pub fn open_in_memory() -> Result<Self> {
        #[allow(unused_mut)]
        let mut store = Self::init(Connection::open_in_memory()?, None)?;
        #[cfg(any(test, feature = "testing"))]
        {
            store.inline = true;
        }
        Ok(store)
    }

    /// Opens an existing database without creating or migrating anything.
    pub fn open_read_only(path: &Path) -> Result<Self> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.busy_timeout(BUSY_TIMEOUT)?;
        check_schema_version(&conn)?;
        let has_seq = events_has_seq(&conn)?;
        let has_pane = events_has_pane(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            has_seq,
            has_pane,
            #[cfg(any(test, feature = "testing"))]
            inline: false,
        })
    }

    /// `db_path` is the file behind `conn` (None for in-memory); it names the pre-migration backup.
    fn init(mut conn: Connection, db_path: Option<&Path>) -> Result<Self> {
        // Check first so a newer database is refused before the DDL touches it.
        check_schema_version(&conn)?;
        // Before `SCHEMA`: the rebuild needs the v1 `events` table, which `SCHEMA` would not replace.
        migrate_v1_to_v2(&mut conn, db_path)?;
        // Also before `SCHEMA`: its index on `pane` fails on a v2 table.
        migrate_v2_to_v3(&mut conn, db_path)?;
        conn.execute_batch(SCHEMA)?;
        conn.execute(
            "INSERT OR IGNORE INTO meta (key, value) VALUES ('schema_version', ?1)",
            params![SCHEMA_VERSION.to_string()],
        )?;
        conn.execute(
            "INSERT OR IGNORE INTO meta (key, value) VALUES ('store_id', ?1)",
            params![uuid::Uuid::new_v4().to_string()],
        )?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            has_seq: true,
            has_pane: true,
            #[cfg(any(test, feature = "testing"))]
            inline: false,
        })
    }

    fn require_seq(&self) -> Result<()> {
        if self.has_seq {
            Ok(())
        } else {
            Err(StoreError::NeedsUpgrade)
        }
    }

    /// A closure that panicked while holding the lock leaves the connection consistent (sqlite rolls back
    /// any open transaction), so a poisoned mutex is recovered instead of failing every later call.
    fn lock(&self) -> Result<MutexGuard<'_, Connection>> {
        Ok(self.conn.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// Returns the new seq, or None when an event with the same id already exists.
    fn insert_event_if_new(&self, e: &Event) -> Result<Option<u64>> {
        // seq lives in its own column; keep it out of the stored json.
        let mut stored = e.clone();
        stored.seq = None;
        let json = serde_json::to_string(&stored)?;
        // The NOT EXISTS guard matters: a bare OR IGNORE still burns an AUTOINCREMENT value on a
        // duplicate. OR IGNORE stays for a concurrent writer racing past the guard.
        let conn = self.lock()?;
        let n = conn.execute(
            "INSERT OR IGNORE INTO events
             (id, type, connection_id, occurred_utc, detected_utc, subject_key, alert, catch_up, test, viewed_utc, json, pane)
             SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, ?10, ?11
             WHERE NOT EXISTS (SELECT 1 FROM events WHERE id = ?1)",
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
                json,
                e.in_pane()
            ],
        )?;
        Ok((n > 0).then(|| conn.last_insert_rowid().cast_unsigned()))
    }

    fn get_event(&self, id: &str) -> Result<Option<Event>> {
        Ok(self
            .query_events("SELECT json, {seq} FROM events WHERE id = ?1", params![id])?
            .pop())
    }

    /// `sql` selects `json` then `{seq}`, which becomes `seq`, or `NULL` on a v1 database.
    /// `Event.seq` always comes from the column, never from the stored json.
    fn query_events(&self, sql: &str, p: impl rusqlite::Params) -> Result<Vec<Event>> {
        let sql = sql.replace("{seq}", if self.has_seq { "seq" } else { "NULL" });
        let conn = self.lock()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(p, |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, Option<i64>>(1)?))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (json, seq) = row?;
            let mut e: Event = serde_json::from_str(&json)?;
            e.seq = seq.map(i64::cast_unsigned);
            out.push(e);
        }
        Ok(out)
    }

    /// Events with `seq > after`, ascending.
    fn events_after_seq(&self, after: u64, limit: u32) -> Result<Vec<Event>> {
        self.require_seq()?;
        self.query_events(
            "SELECT json, {seq} FROM events WHERE seq > ?1 ORDER BY seq ASC LIMIT ?2",
            params![i64::try_from(after).unwrap_or(i64::MAX), limit],
        )
    }

    /// The highest seq ever assigned (survives prune), 0 when none.
    fn latest_seq(&self) -> Result<u64> {
        self.require_seq()?;
        Ok(self
            .lock()?
            .query_row(
                "SELECT seq FROM sqlite_sequence WHERE name = 'events'",
                [],
                |r| r.get::<_, i64>(0),
            )
            .optional()?
            .map_or(0, i64::cast_unsigned))
    }

    fn oldest_seq(&self) -> Result<Option<u64>> {
        self.require_seq()?;
        Ok(self
            .lock()?
            .query_row("SELECT MIN(seq) FROM events", [], |r| {
                r.get::<_, Option<i64>>(0)
            })?
            .map(i64::cast_unsigned))
    }

    fn store_id(&self) -> Result<String> {
        self.require_seq()?;
        Ok(self
            .lock()?
            .query_row("SELECT value FROM meta WHERE key = 'store_id'", [], |r| {
                r.get(0)
            })?)
    }

    /// Pane-listed events not yet viewed, newest first. A read-only pre-v3 database has no `pane`
    /// column and listed alerting events only.
    fn unviewed(&self, limit: u32) -> Result<Vec<Event>> {
        let column = if self.has_pane { "pane" } else { "alert" };
        self.query_events(
            &format!(
                "SELECT json, {{seq}} FROM events WHERE {column} = 1 AND viewed_utc IS NULL
                 ORDER BY occurred_utc DESC, id LIMIT ?1"
            ),
            params![limit],
        )
    }

    fn unviewed_count(&self) -> Result<u32> {
        Ok(self.lock()?.query_row(
            "SELECT COUNT(*) FROM events WHERE alert = 1 AND viewed_utc IS NULL",
            [],
            |r| r.get(0),
        )?)
    }

    fn mark_viewed(&self, ids: &[String]) -> Result<()> {
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

    fn mark_all_viewed(&self) -> Result<()> {
        self.lock()?.execute(
            "UPDATE events SET viewed_utc = ?1 WHERE pane = 1 AND viewed_utc IS NULL",
            params![ts(Utc::now())],
        )?;
        Ok(())
    }

    /// Events with `occurred_utc >= since`, oldest first.
    fn events_since(&self, since: DateTime<Utc>, limit: u32) -> Result<Vec<Event>> {
        self.query_events(
            "SELECT json, {seq} FROM events WHERE occurred_utc >= ?1
             ORDER BY occurred_utc ASC, id LIMIT ?2",
            params![ts(since), limit],
        )
    }

    fn get_watermark(&self, key: &str) -> Result<Option<Watermark>> {
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

    fn set_watermark(
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

    fn get_snapshot<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
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

    fn put_snapshot<T: Serialize>(&self, key: &str, value: &T) -> Result<()> {
        let json = serde_json::to_string(value)?;
        self.lock()?.execute(
            "INSERT INTO snapshots (key, json, updated_utc) VALUES (?1, ?2, ?3)
             ON CONFLICT(key) DO UPDATE SET json = excluded.json, updated_utc = excluded.updated_utc",
            params![key, json, ts(Utc::now())],
        )?;
        Ok(())
    }

    fn record_hook_run(&self, run: HookRun) -> Result<()> {
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

    fn get_ui_state(&self, key: &str) -> Result<Option<String>> {
        Ok(self
            .lock()?
            .query_row(
                "SELECT value FROM ui_state WHERE key = ?1",
                params![key],
                |r| r.get(0),
            )
            .optional()?)
    }

    fn set_ui_state(&self, key: &str, value: &str) -> Result<()> {
        self.lock()?.execute(
            "INSERT INTO ui_state (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    /// Deletes events (by `occurred_utc`) and hook runs (by `started_utc`) older than the
    /// cutoff. Watermarks, snapshots and UI state are never pruned.
    fn prune(&self, older_than_days: u32) -> Result<()> {
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

impl SqliteStore {
    fn blocking<'a, T: Send + 'static>(
        &self,
        f: impl FnOnce(SqliteStore) -> Result<T> + Send + 'static,
    ) -> BoxFuture<'a, Result<T>> {
        let this = self.clone();
        #[cfg(any(test, feature = "testing"))]
        if this.inline {
            return Box::pin(async move { f(this) });
        }
        Box::pin(async move {
            tokio::task::spawn_blocking(move || f(this))
                .await
                .map_err(|e| StoreError::Backend(Box::new(e)))?
        })
    }
}

impl Store for SqliteStore {
    fn insert_event_if_new<'a>(&'a self, e: &'a Event) -> BoxFuture<'a, Result<Option<u64>>> {
        let e = e.clone();
        self.blocking(move |s| s.insert_event_if_new(&e))
    }

    fn get_event<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<Option<Event>>> {
        let id = id.to_owned();
        self.blocking(move |s| s.get_event(&id))
    }

    fn unviewed<'a>(&'a self, limit: u32) -> BoxFuture<'a, Result<Vec<Event>>> {
        self.blocking(move |s| s.unviewed(limit))
    }

    fn unviewed_count<'a>(&'a self) -> BoxFuture<'a, Result<u32>> {
        self.blocking(|s| s.unviewed_count())
    }

    fn mark_viewed<'a>(&'a self, ids: &'a [String]) -> BoxFuture<'a, Result<()>> {
        let ids = ids.to_vec();
        self.blocking(move |s| s.mark_viewed(&ids))
    }

    fn mark_all_viewed<'a>(&'a self) -> BoxFuture<'a, Result<()>> {
        self.blocking(|s| s.mark_all_viewed())
    }

    fn events_since<'a>(
        &'a self,
        since: DateTime<Utc>,
        limit: u32,
    ) -> BoxFuture<'a, Result<Vec<Event>>> {
        self.blocking(move |s| s.events_since(since, limit))
    }

    fn get_watermark<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Watermark>>> {
        let key = key.to_owned();
        self.blocking(move |s| s.get_watermark(&key))
    }

    fn set_watermark<'a>(
        &'a self,
        key: &'a str,
        synced: DateTime<Utc>,
        success: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<()>> {
        let key = key.to_owned();
        self.blocking(move |s| s.set_watermark(&key, synced, success))
    }

    fn get_snapshot_json<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<serde_json::Value>>> {
        let key = key.to_owned();
        self.blocking(move |s| s.get_snapshot::<serde_json::Value>(&key))
    }

    fn put_snapshot_json<'a>(
        &'a self,
        key: &'a str,
        value: serde_json::Value,
    ) -> BoxFuture<'a, Result<()>> {
        let key = key.to_owned();
        self.blocking(move |s| s.put_snapshot(&key, &value))
    }

    fn record_hook_run<'a>(&'a self, run: HookRun) -> BoxFuture<'a, Result<()>> {
        self.blocking(move |s| s.record_hook_run(run))
    }

    fn get_ui_state<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<String>>> {
        let key = key.to_owned();
        self.blocking(move |s| s.get_ui_state(&key))
    }

    fn set_ui_state<'a>(&'a self, key: &'a str, value: &'a str) -> BoxFuture<'a, Result<()>> {
        let (key, value) = (key.to_owned(), value.to_owned());
        self.blocking(move |s| s.set_ui_state(&key, &value))
    }

    fn prune<'a>(&'a self, older_than_days: u32) -> BoxFuture<'a, Result<()>> {
        self.blocking(move |s| s.prune(older_than_days))
    }

    fn events_after_seq<'a>(&'a self, after: u64, limit: u32) -> BoxFuture<'a, Result<Vec<Event>>> {
        self.blocking(move |s| s.events_after_seq(after, limit))
    }

    fn latest_seq<'a>(&'a self) -> BoxFuture<'a, Result<u64>> {
        self.blocking(|s| s.latest_seq())
    }

    fn oldest_seq<'a>(&'a self) -> BoxFuture<'a, Result<Option<u64>>> {
        self.blocking(|s| s.oldest_seq())
    }

    fn store_id<'a>(&'a self) -> BoxFuture<'a, Result<String>> {
        self.blocking(|s| s.store_id())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{ConnectionRef, Level};
    use chrono::TimeZone;
    use pretty_assertions::assert_eq;

    #[tokio::test]
    async fn panicking_closure_does_not_poison_later_calls() {
        let mut s = SqliteStore::open_in_memory().unwrap();
        s.inline = false;

        let res = s
            .blocking(|s| -> Result<()> {
                let _guard = s.lock()?;
                panic!("boom");
            })
            .await;
        assert!(matches!(res, Err(StoreError::Backend(_))));

        assert!(Store::get_event(&s, "missing").await.unwrap().is_none());
    }

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

    #[tokio::test]
    async fn conformance_suite_passes_in_memory() {
        let s = SqliteStore::open_in_memory().unwrap();
        crate::store::conformance::run(&s).await;
    }

    #[tokio::test]
    async fn conformance_suite_passes_on_a_file_database() {
        let dir = tempfile::tempdir().unwrap();
        let s = SqliteStore::open(&dir.path().join("herald.db")).unwrap();
        crate::store::conformance::run(&s).await;
    }

    #[test]
    fn insert_event_if_new_dedupes_by_id() {
        let s = SqliteStore::open_in_memory().unwrap();
        let e = event("a", at(0), true);
        assert_eq!(s.insert_event_if_new(&e).unwrap(), Some(1));
        assert_eq!(s.insert_event_if_new(&e).unwrap(), None);
        let mut stored = e;
        stored.seq = Some(1);
        assert_eq!(s.get_event("a").unwrap(), Some(stored));
        assert_eq!(s.get_event("zzz").unwrap(), None);
    }

    #[test]
    fn unviewed_orders_newest_first_and_mark_viewed_clears() {
        let s = SqliteStore::open_in_memory().unwrap();
        s.insert_event_if_new(&event("old", at(1), true)).unwrap();
        s.insert_event_if_new(&event("new", at(3), true)).unwrap();
        s.insert_event_if_new(&event("mid", at(2), true)).unwrap();
        let mut quiet = event("quiet", at(4), false);
        quiet.level = Level::Ignore;
        s.insert_event_if_new(&quiet).unwrap();

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
        let s = SqliteStore::open_in_memory().unwrap();
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
        let s = SqliteStore::open_in_memory().unwrap();
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
        let s = SqliteStore::open_in_memory().unwrap();
        assert_eq!(s.get_ui_state("pinned").unwrap(), None);
        s.set_ui_state("pinned", "true").unwrap();
        s.set_ui_state("pinned", "false").unwrap();
        assert_eq!(s.get_ui_state("pinned").unwrap().as_deref(), Some("false"));
    }

    #[test]
    fn events_since_is_ascending_and_inclusive() {
        let s = SqliteStore::open_in_memory().unwrap();
        s.insert_event_if_new(&event("c", at(30), false)).unwrap();
        s.insert_event_if_new(&event("a", at(10), true)).unwrap();
        s.insert_event_if_new(&event("b", at(20), true)).unwrap();
        s.insert_event_if_new(&event("early", at(5), true)).unwrap();

        assert_eq!(ids(&s.events_since(at(10), 10).unwrap()), ["a", "b", "c"]);
        assert_eq!(ids(&s.events_since(at(10), 2).unwrap()), ["a", "b"]);
    }

    #[test]
    fn prune_removes_old_events_and_hook_runs_only() {
        let s = SqliteStore::open_in_memory().unwrap();
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
            let s = SqliteStore::open(&path).unwrap();
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
            assert_eq!(v, "3");
        }
        let ro = SqliteStore::open_read_only(&path).unwrap();
        assert_eq!(ro.unviewed_count().unwrap(), 1);
        assert!(ro.get_event("a").unwrap().is_some());
        assert!(ro.set_ui_state("k", "v").is_err());
        assert!(ro.insert_event_if_new(&event("b", at(1), true)).is_err());
    }

    #[test]
    fn reader_reads_while_writer_inserts_in_wal_mode() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("herald.db");
        let writer = SqliteStore::open(&path).unwrap();
        let reader = SqliteStore::open_read_only(&path).unwrap();

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
                            .is_some()
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
            let s = SqliteStore::open(&path).unwrap();
            let conn = s.lock().unwrap();
            conn.execute(
                "UPDATE meta SET value = ?1 WHERE key = 'schema_version'",
                params![newer],
            )
            .unwrap();
            conn.execute_batch("DROP TABLE ui_state").unwrap();
        }
        for result in [SqliteStore::open(&path), SqliteStore::open_read_only(&path)] {
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

    /// The v1 schema as the previous release created it.
    const V1_SCHEMA: &str = "
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

    /// Builds a WAL-mode v1 database with the given events, inserted in order. The WAL is never
    /// checkpointed, so the rows live in the WAL file until every connection has closed.
    fn make_v1_db(path: &Path, ids: &[&str]) -> Connection {
        let conn = Connection::open(path).unwrap();
        conn.query_row("PRAGMA journal_mode = WAL", [], |_| Ok(()))
            .unwrap();
        conn.execute_batch("PRAGMA wal_autocheckpoint = 0").unwrap();
        conn.execute_batch(V1_SCHEMA).unwrap();
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('schema_version', '1')",
            [],
        )
        .unwrap();
        for (i, id) in ids.iter().enumerate() {
            let e = event(id, at(i64::try_from(i).unwrap()), true);
            conn.execute(
                "INSERT INTO events
                 (id, type, connection_id, occurred_utc, detected_utc, subject_key, alert, catch_up, test, viewed_utc, json)
                 VALUES (?1, 'pr.created', 'c', ?2, ?2, 't', 1, 0, 0, NULL, ?3)",
                params![id, ts(e.occurred_utc), serde_json::to_string(&e).unwrap()],
            )
            .unwrap();
        }
        conn
    }

    fn schema_version(conn: &Connection) -> String {
        conn.query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn bak_files(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".bak"))
            .collect();
        names.sort();
        names
    }

    #[test]
    fn v1_database_migrates_keeping_order_and_continuing_seq() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("herald.db");
        let v1 = make_v1_db(&path, &["b", "a", "c"]);
        v1.execute(
            "UPDATE events SET viewed_utc = '2026-01-01T00:00:00.000Z' WHERE id = 'a'",
            [],
        )
        .unwrap();
        drop(v1);

        let s = SqliteStore::open(&path).unwrap();

        let all = s.events_after_seq(0, 10).unwrap();
        assert_eq!(ids(&all), ["b", "a", "c"]);
        assert_eq!(
            all.iter().map(|e| e.seq).collect::<Vec<_>>(),
            [Some(1), Some(2), Some(3)]
        );
        assert_eq!(s.unviewed_count().unwrap(), 2);
        assert_eq!(
            s.insert_event_if_new(&event("d", at(9), true)).unwrap(),
            Some(4)
        );
        assert_eq!(s.latest_seq().unwrap(), 4);
        assert!(!s.store_id().unwrap().is_empty());

        assert_eq!(ids(&s.unviewed(10).unwrap()), ["d", "c", "b"]);
        let conn = s.lock().unwrap();
        assert_eq!(schema_version(&conn), "3");
        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE tbl_name = 'events' ORDER BY name")
            .unwrap();
        let names: Vec<String> = stmt
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert!(names.contains(&"idx_events_alert_viewed".to_string()));
        assert!(names.contains(&"idx_events_occurred".to_string()));
        assert!(names.contains(&"idx_events_pane_viewed".to_string()));
        assert!(!names.iter().any(|n| n.contains("v1")));
    }

    #[test]
    fn migration_is_skipped_when_another_opener_migrated_first() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("herald.db");
        drop(make_v1_db(&path, &["a", "b", "c"]));
        let mut first = Connection::open(&path).unwrap();
        first.busy_timeout(BUSY_TIMEOUT).unwrap();

        migrate_v1_to_v2_with(&mut first, Some(&path), || {
            let other = SqliteStore::open(&path).unwrap();
            // Prune seq 1: a second rebuild would renumber b and c down to 1 and 2.
            other
                .lock()
                .unwrap()
                .execute("DELETE FROM events WHERE id = 'a'", [])
                .unwrap();
        })
        .unwrap();

        let s = SqliteStore::open(&path).unwrap();
        let seqs: Vec<_> = s
            .events_after_seq(0, 10)
            .unwrap()
            .iter()
            .map(|e| (e.id.clone(), e.seq))
            .collect();
        assert_eq!(
            seqs,
            [("b".to_string(), Some(2)), ("c".to_string(), Some(3))]
        );
    }

    #[test]
    fn migration_backs_up_wal_contents_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("herald.db");
        // Held open so the rows stay in the WAL, never checkpointed into the main file.
        let _v1 = make_v1_db(&path, &["a", "b", "c"]);

        drop(SqliteStore::open(&path).unwrap());
        drop(SqliteStore::open(&path).unwrap());

        assert_eq!(
            bak_files(dir.path()),
            ["herald.db.v1.bak", "herald.db.v2.bak"]
        );
        let bak = Connection::open_with_flags(
            dir.path().join("herald.db.v1.bak"),
            OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        let rows: i64 = bak
            .query_row("SELECT COUNT(*) FROM events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 3);
        assert_eq!(schema_version(&bak), "1");
        assert!(!events_has_seq(&bak).unwrap());
    }

    #[test]
    fn migration_does_not_overwrite_an_existing_backup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("herald.db");
        drop(make_v1_db(&path, &["a"]));
        let existing = dir.path().join("herald.db.v1.bak");
        std::fs::write(&existing, b"keep").unwrap();

        drop(SqliteStore::open(&path).unwrap());

        assert_eq!(std::fs::read(&existing).unwrap(), b"keep");
        let names = bak_files(dir.path());
        assert_eq!(names.len(), 3);
        let stamped = names
            .iter()
            .find(|n| n.starts_with("herald.db.v1.") && *n != "herald.db.v1.bak")
            .unwrap();
        let stamp = stamped
            .strip_prefix("herald.db.v1.")
            .and_then(|n| n.strip_suffix(".bak"))
            .unwrap();
        assert_eq!(stamp.len(), 15);
        assert_eq!(stamp.as_bytes()[8], b'T');
    }

    #[test]
    fn failed_migration_leaves_version_1_and_the_next_open_retries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("herald.db");
        drop(make_v1_db(&path, &["a", "b"]));

        let blocker = Connection::open(&path).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
        let err = SqliteStore::open_with_busy_timeout(&path, std::time::Duration::from_millis(50))
            .err()
            .expect("migration must fail while the database is write-locked");
        assert!(err.to_string().contains("schema migration to v2 failed"));
        blocker.execute_batch("ROLLBACK").unwrap();

        let raw = Connection::open(&path).unwrap();
        assert_eq!(schema_version(&raw), "1");
        assert!(!events_has_seq(&raw).unwrap());
        drop(raw);

        let s = SqliteStore::open(&path).unwrap();
        assert_eq!(ids(&s.events_after_seq(0, 10).unwrap()), ["a", "b"]);
        assert_eq!(schema_version(&s.lock().unwrap()), "3");
    }

    #[test]
    fn read_only_v1_database_reads_without_seq_and_refuses_seq_methods() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("herald.db");
        drop(make_v1_db(&path, &["a", "b"]));

        let ro = SqliteStore::open_read_only(&path).unwrap();

        assert_eq!(ro.get_event("a").unwrap().unwrap().seq, None);
        assert_eq!(ro.unviewed_count().unwrap(), 2);
        assert!(ro.unviewed(10).unwrap().iter().all(|e| e.seq.is_none()));
        assert_eq!(ro.events_since(at(0), 10).unwrap().len(), 2);
        assert!(matches!(
            ro.events_after_seq(0, 10),
            Err(StoreError::NeedsUpgrade)
        ));
        assert!(matches!(ro.latest_seq(), Err(StoreError::NeedsUpgrade)));
        assert!(matches!(ro.oldest_seq(), Err(StoreError::NeedsUpgrade)));
        assert!(matches!(ro.store_id(), Err(StoreError::NeedsUpgrade)));
        assert!(bak_files(dir.path()).is_empty());
    }

    /// The v2 `events` table as the previous release created it: `seq`, no `pane`.
    const V2_EVENTS: &str = "
CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
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
";

    /// Builds a v2 database with an alerting row `a` and a non-alerting row `q`, whose stored json
    /// predates `level`.
    fn make_v2_db(path: &Path) {
        let conn = Connection::open(path).unwrap();
        conn.query_row("PRAGMA journal_mode = WAL", [], |_| Ok(()))
            .unwrap();
        conn.execute_batch(V2_EVENTS).unwrap();
        conn.execute_batch(V1_SCHEMA).unwrap();
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('schema_version', '2')",
            [],
        )
        .unwrap();
        for (i, (id, alert)) in [("a", true), ("q", false)].into_iter().enumerate() {
            let e = event(id, at(i64::try_from(i).unwrap()), alert);
            let mut json = serde_json::to_value(&e).unwrap();
            json.as_object_mut().unwrap().remove("level");
            conn.execute(
                "INSERT INTO events
                 (id, type, connection_id, occurred_utc, detected_utc, subject_key, alert, catch_up, test, viewed_utc, json)
                 VALUES (?1, 'pr.created', 'c', ?2, ?2, 't', ?3, 0, 0, NULL, ?4)",
                params![id, ts(e.occurred_utc), alert, json.to_string()],
            )
            .unwrap();
        }
    }

    #[test]
    fn v2_database_migrates_to_v3_with_pane_backfilled_and_a_backup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("herald.db");
        make_v2_db(&path);

        let s = SqliteStore::open(&path).unwrap();

        assert_eq!(bak_files(dir.path()), ["herald.db.v2.bak"]);
        assert_eq!(ids(&s.unviewed(10).unwrap()), ["a"]);
        assert_eq!(s.unviewed_count().unwrap(), 1);
        let levels: Vec<Level> = s
            .events_after_seq(0, 10)
            .unwrap()
            .iter()
            .map(|e| e.level)
            .collect();
        assert_eq!(levels, [Level::Alert, Level::Pane]);
        let stored: String = s
            .lock()
            .unwrap()
            .query_row("SELECT json FROM events WHERE id = 'q'", [], |r| r.get(0))
            .unwrap();
        assert!(stored.contains("\"level\":\"pane\""));
        assert_eq!(schema_version(&s.lock().unwrap()), "3");
        drop(s);

        // Reopening is a no-op: no second backup, rows unchanged.
        let s = SqliteStore::open(&path).unwrap();
        assert_eq!(bak_files(dir.path()), ["herald.db.v2.bak"]);
        assert_eq!(ids(&s.unviewed(10).unwrap()), ["a"]);
        assert_eq!(schema_version(&s.lock().unwrap()), "3");
    }

    #[test]
    fn v3_migration_survives_a_column_added_by_an_earlier_partial_run() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("herald.db");
        make_v2_db(&path);
        Connection::open(&path)
            .unwrap()
            .execute_batch("ALTER TABLE events ADD COLUMN pane INTEGER NOT NULL DEFAULT 0;")
            .unwrap();

        let s = SqliteStore::open(&path).unwrap();

        assert_eq!(ids(&s.unviewed(10).unwrap()), ["a"]);
        assert_eq!(schema_version(&s.lock().unwrap()), "3");
    }

    #[test]
    fn v3_migration_is_skipped_when_another_opener_migrated_first() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("herald.db");
        make_v2_db(&path);
        let mut first = Connection::open(&path).unwrap();
        first.busy_timeout(BUSY_TIMEOUT).unwrap();

        migrate_v2_to_v3_with(&mut first, Some(&path), || {
            let other = SqliteStore::open(&path).unwrap();
            // A pane-level row written at v3: a second backfill (pane = alert) would unlist it.
            other
                .insert_event_if_new(&event("p", at(5), false))
                .unwrap();
        })
        .unwrap();

        let s = SqliteStore::open(&path).unwrap();
        assert_eq!(ids(&s.unviewed(10).unwrap()), ["p", "a"]);
        assert_eq!(schema_version(&s.lock().unwrap()), "3");
        let baks = bak_files(dir.path());
        assert_eq!(baks.len(), 2);
        assert!(baks.contains(&"herald.db.v2.bak".to_string()));
        let stamped = baks
            .iter()
            .find(|n| *n != "herald.db.v2.bak")
            .unwrap()
            .strip_prefix("herald.db.v2.")
            .and_then(|n| n.strip_suffix(".bak"))
            .unwrap();
        assert!(
            chrono::NaiveDateTime::parse_from_str(stamped, "%Y%m%dT%H%M%S").is_ok(),
            "{stamped}"
        );
    }

    #[test]
    fn read_only_v2_database_still_reads_by_seq() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("herald.db");
        make_v2_db(&path);

        let ro = SqliteStore::open_read_only(&path).unwrap();

        assert_eq!(ids(&ro.events_after_seq(0, 10).unwrap()), ["a", "q"]);
        assert_eq!(ro.latest_seq().unwrap(), 2);
        assert_eq!(ids(&ro.unviewed(10).unwrap()), ["a"]);
        assert_eq!(ro.unviewed_count().unwrap(), 1);
        assert_eq!(ids(&ro.events_since(at(0), 10).unwrap()), ["a", "q"]);
        assert!(bak_files(dir.path()).is_empty());
    }

    #[test]
    fn seq_is_not_reused_after_the_highest_rows_are_pruned() {
        let s = SqliteStore::open_in_memory().unwrap();
        let old = Utc::now() - Duration::days(200);
        s.insert_event_if_new(&event("keep", Utc::now(), true))
            .unwrap();
        s.insert_event_if_new(&event("old1", old, true)).unwrap();
        s.insert_event_if_new(&event("old2", old, true)).unwrap();

        s.prune(180).unwrap();

        assert_eq!(s.oldest_seq().unwrap(), Some(1));
        assert_eq!(s.latest_seq().unwrap(), 3);
        assert_eq!(
            s.insert_event_if_new(&event("next", old, true)).unwrap(),
            Some(4)
        );
    }

    #[test]
    fn duplicate_insert_consumes_no_seq() {
        let s = SqliteStore::open_in_memory().unwrap();
        assert_eq!(s.latest_seq().unwrap(), 0);
        assert_eq!(s.oldest_seq().unwrap(), None);
        let a = event("a", at(0), true);
        assert_eq!(s.insert_event_if_new(&a).unwrap(), Some(1));
        assert_eq!(s.insert_event_if_new(&a).unwrap(), None);
        assert_eq!(
            s.insert_event_if_new(&event("b", at(1), true)).unwrap(),
            Some(2)
        );
        assert_eq!(s.latest_seq().unwrap(), 2);
    }

    #[test]
    fn seq_comes_from_the_column_not_the_stored_json() {
        let s = SqliteStore::open_in_memory().unwrap();
        let mut e = event("a", at(0), true);
        e.seq = Some(999);
        assert_eq!(s.insert_event_if_new(&e).unwrap(), Some(1));
        {
            let conn = s.lock().unwrap();
            let json: String = conn
                .query_row("SELECT json FROM events WHERE id = 'a'", [], |r| r.get(0))
                .unwrap();
            assert!(!json.contains("seq"));
            conn.execute("UPDATE events SET json = json_set(json, '$.seq', 777)", [])
                .unwrap();
        }
        assert_eq!(s.get_event("a").unwrap().unwrap().seq, Some(1));
        assert_eq!(s.events_after_seq(0, 10).unwrap()[0].seq, Some(1));
    }
}
