//! The flag table replication and retention share: while a standby replays the primary's log
//! or a node rewinds unreplicated entries, `_weirdnet_guard` holds a row with `value = 1`,
//! and the crate's own triggers stand aside. App triggers can do the same with
//! [`REPLAYING`].

/// The guard table.
#[cfg(feature = "replication")]
pub(crate) const TABLE: &str = "_weirdnet_guard";

/// An SQL condition that is true while this crate replays or rewinds replicated changes.
/// Guard app triggers with `WHEN NOT (...)` so a standby doesn't run them a second time (the
/// rows they wrote on the primary are already in the replicated changes):
///
/// ```sql
/// CREATE TRIGGER audit_insert AFTER INSERT ON orders
/// WHEN NOT EXISTS (SELECT 1 FROM _weirdnet_guard WHERE value = 1)
/// BEGIN INSERT INTO audit (order_id) VALUES (new.id); END;
/// ```
///
/// The guard table exists once a replication or retention API has run on the database.
pub const REPLAYING: &str = "EXISTS (SELECT 1 FROM _weirdnet_guard WHERE value = 1)";

#[cfg(feature = "replication")]
pub(crate) fn ensure(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    conn.execute_batch("CREATE TABLE IF NOT EXISTS _weirdnet_guard (name TEXT PRIMARY KEY, value INTEGER NOT NULL);")
}

#[cfg(feature = "replication")]
pub(crate) fn set(conn: &rusqlite::Connection, name: &str, on: bool) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO _weirdnet_guard (name, value) VALUES (?1, ?2) ON CONFLICT(name) DO UPDATE SET value = excluded.value",
        rusqlite::params![name, on as i64],
    )?;
    Ok(())
}

/// Milliseconds since the Unix epoch.
pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}
