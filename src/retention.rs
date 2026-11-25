//! Retention: keep records for at least a minimum age, refuse earlier deletes, and make
//! changes to kept records evident.
//!
//! - [`protect`] stores a [`Policy`] and adds triggers: deleting a row younger than the
//!   policy's minimum age aborts, and so does updating one in an `immutable` table. Policies
//!   can only be strengthened; the policy table refuses deletes.
//! - [`seal`] hashes new and changed rows into an append-only, hash-chained ledger
//!   (`_weirdnet_seal`); [`purge_expired`] removes only rows past the minimum age and records
//!   them; [`verify`] recomputes the chain and every row.
//! - [`archive_to`] copies sealed rows into a separate SQLCipher archive (its own key),
//!   checked on the way; [`verify_archive`] checks an archive later.
//! - [`guard`] makes a connection refuse `DROP` and `ALTER` of protected tables, their
//!   triggers and this crate's internal tables.
//!
//! Triggers and the guard stop the app's own code and SQL. Someone holding the key can still
//! rewrite the file; [`verify`], replicas (`replication` feature) and archives make that
//! evident. Publish or sign [`head`] to anchor it outside the database.
//!
//! Protected tables must be rowid tables (not `WITHOUT ROWID`). With replication, protect
//! through `Primary::schema(&protect_sql(..)?)` and seal inside `Primary::write`.
use crate::error::{Result, StorageError};
use crate::{guard, quote_ident, table_exists};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::types::ValueRef;
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::path::Path;

const DAY_MS: i64 = 86_400_000;
const ROW_LABEL: &[u8] = b"weirdnet-storage/row/1\0";
const ROWS_LABEL: &[u8] = b"weirdnet-storage/seal-rows/1\0";
const SEAL_LABEL: &[u8] = b"weirdnet-storage/seal/1\0";

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS _weirdnet_retention (
    tbl TEXT PRIMARY KEY, col TEXT NOT NULL, unit TEXT NOT NULL, min_age_ms INTEGER NOT NULL,
    immutable INTEGER NOT NULL, since_ms INTEGER NOT NULL);
CREATE TRIGGER IF NOT EXISTS _weirdnet_retention_keep BEFORE DELETE ON _weirdnet_retention
BEGIN SELECT RAISE(ABORT, 'weirdnet-storage retention: policies cannot be removed'); END;
CREATE TRIGGER IF NOT EXISTS _weirdnet_retention_strengthen BEFORE UPDATE ON _weirdnet_retention
WHEN new.tbl <> old.tbl OR new.col <> old.col OR new.unit <> old.unit OR new.min_age_ms < old.min_age_ms
  OR new.immutable < old.immutable OR new.since_ms <> old.since_ms
BEGIN SELECT RAISE(ABORT, 'weirdnet-storage retention: policies can only be strengthened'); END;
CREATE TABLE IF NOT EXISTS _weirdnet_seal (
    seq INTEGER PRIMARY KEY, tbl TEXT NOT NULL, kind TEXT NOT NULL, at_ms INTEGER NOT NULL,
    rows INTEGER NOT NULL, digest BLOB NOT NULL, prev BLOB NOT NULL, hash BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS _weirdnet_seal_rows (
    seal INTEGER NOT NULL, tbl TEXT NOT NULL, rid INTEGER NOT NULL, hash BLOB NOT NULL,
    PRIMARY KEY (seal, tbl, rid));
CREATE INDEX IF NOT EXISTS _weirdnet_seal_rows_by_row ON _weirdnet_seal_rows (tbl, rid, seal);
CREATE TRIGGER IF NOT EXISTS _weirdnet_seal_append_only BEFORE UPDATE ON _weirdnet_seal
BEGIN SELECT RAISE(ABORT, 'weirdnet-storage retention: the seal ledger is append-only'); END;
CREATE TRIGGER IF NOT EXISTS _weirdnet_seal_keep BEFORE DELETE ON _weirdnet_seal
WHEN NOT EXISTS (SELECT 1 FROM _weirdnet_guard WHERE value = 1)
BEGIN SELECT RAISE(ABORT, 'weirdnet-storage retention: the seal ledger is append-only'); END;
CREATE TRIGGER IF NOT EXISTS _weirdnet_seal_rows_append_only BEFORE UPDATE ON _weirdnet_seal_rows
BEGIN SELECT RAISE(ABORT, 'weirdnet-storage retention: the seal ledger is append-only'); END;
CREATE TRIGGER IF NOT EXISTS _weirdnet_seal_rows_keep BEFORE DELETE ON _weirdnet_seal_rows
WHEN NOT EXISTS (SELECT 1 FROM _weirdnet_guard WHERE value = 1)
BEGIN SELECT RAISE(ABORT, 'weirdnet-storage retention: the seal ledger is append-only'); END;
";

/// A minimum age.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Period {
    ms: i64,
}

impl Period {
    pub fn days(days: u32) -> Self {
        Period { ms: days as i64 * DAY_MS }
    }

    /// At least `years` years: 365.25 days a year, rounded up to whole days (7 years is 2557
    /// days).
    pub fn years(years: u32) -> Self {
        Period::days(((years as u64 * 36525).div_ceil(100)) as u32)
    }

    pub fn from_millis(ms: i64) -> Self {
        Period { ms: ms.max(0) }
    }

    pub fn as_millis(&self) -> i64 {
        self.ms
    }

    /// Whole days, rounded up.
    pub fn as_days(&self) -> i64 {
        (self.ms + DAY_MS - 1) / DAY_MS
    }
}

/// How the time column stores a moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeUnit {
    /// Milliseconds since the Unix epoch (an integer).
    Millis,
    /// Seconds since the Unix epoch (an integer).
    Seconds,
}

impl TimeUnit {
    fn as_str(self) -> &'static str {
        match self {
            TimeUnit::Millis => "ms",
            TimeUnit::Seconds => "s",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "ms" => Some(TimeUnit::Millis),
            "s" => Some(TimeUnit::Seconds),
            _ => None,
        }
    }

    /// The cut-off in this unit: rows at or before it are past the minimum age.
    fn cutoff(self, now_ms: i64, min_age: Period) -> i64 {
        let ms = now_ms - min_age.ms;
        match self {
            TimeUnit::Millis => ms,
            TimeUnit::Seconds => ms.div_euclid(1000),
        }
    }

    fn cutoff_sql(self, min_age: Period) -> String {
        match self {
            TimeUnit::Millis => format!("(CAST(strftime('%s', 'now') AS INTEGER) * 1000 - {})", min_age.ms),
            TimeUnit::Seconds => format!("(CAST(strftime('%s', 'now') AS INTEGER) - {})", min_age.ms / 1000),
        }
    }
}

/// Keep the rows of `table` for at least `min_age`, measured from `column`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Policy {
    pub table: String,
    /// The row's moment (see `unit`). Rows where it is NULL are always kept.
    pub column: String,
    pub unit: TimeUnit,
    pub min_age: Period,
    /// Also refuse updates while a row is kept.
    pub immutable: bool,
}

impl Policy {
    /// Millisecond times, updates allowed.
    pub fn new(table: impl Into<String>, column: impl Into<String>, min_age: Period) -> Self {
        Policy { table: table.into(), column: column.into(), unit: TimeUnit::Millis, min_age, immutable: false }
    }

    /// The column holds seconds.
    pub fn seconds(mut self) -> Self {
        self.unit = TimeUnit::Seconds;
        self
    }

    /// Refuse updates as well as deletes while a row is kept.
    pub fn immutable(mut self) -> Self {
        self.immutable = true;
        self
    }

    fn trigger_name(&self, op: &str) -> String {
        format!("_weirdnet_keep_{}_{}_{}{}", self.table, op, self.min_age.as_days(), if self.immutable { "_i" } else { "" })
    }
}

fn sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!("SELECT name FROM pragma_table_info({}) ORDER BY cid", sql_string(table)))?;
    let cols: Vec<String> = stmt.query_map([], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?;
    Ok(cols)
}

fn check_table(conn: &Connection, p: &Policy) -> Result<()> {
    if !table_exists(conn, &p.table)? {
        return Err(StorageError::invalid("retention table", format!("`{}` does not exist", p.table)));
    }
    if p.table.starts_with("_weirdnet_") || p.table.starts_with("sqlite_") {
        return Err(StorageError::invalid("retention table", format!("`{}` is an internal table", p.table)));
    }
    if conn.prepare(&format!("SELECT rowid FROM {} LIMIT 0", quote_ident(&p.table))).is_err() {
        return Err(StorageError::invalid("retention table", format!("`{}` has no rowid (WITHOUT ROWID)", p.table)));
    }
    if !columns(conn, &p.table)?.iter().any(|c| c == &p.column) {
        return Err(StorageError::invalid("retention column", format!("`{}` has no column `{}`", p.table, p.column)));
    }
    if p.min_age.ms <= 0 {
        return Err(StorageError::invalid("retention period", "must be positive"));
    }
    Ok(())
}

/// The policy for `table`, if any.
pub fn policy(conn: &Connection, table: &str) -> Result<Option<Policy>> {
    if !table_exists(conn, "_weirdnet_retention")? {
        return Ok(None);
    }
    Ok(conn
        .query_row("SELECT tbl, col, unit, min_age_ms, immutable FROM _weirdnet_retention WHERE tbl = ?1", [table], policy_row)
        .optional()?
        .flatten())
}

fn policy_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Option<Policy>> {
    let unit: String = r.get(2)?;
    Ok(TimeUnit::parse(&unit).map(|unit| Policy {
        table: r.get(0).unwrap_or_default(),
        column: r.get(1).unwrap_or_default(),
        unit,
        min_age: Period::from_millis(r.get(3).unwrap_or_default()),
        immutable: r.get::<_, i64>(4).unwrap_or_default() != 0,
    }))
}

/// Every policy.
pub fn policies(conn: &Connection) -> Result<Vec<Policy>> {
    if !table_exists(conn, "_weirdnet_retention")? {
        return Ok(Vec::new());
    }
    let mut stmt = conn.prepare("SELECT tbl, col, unit, min_age_ms, immutable FROM _weirdnet_retention ORDER BY tbl")?;
    let v: Vec<Option<Policy>> = stmt.query_map([], policy_row)?.collect::<rusqlite::Result<_>>()?;
    Ok(v.into_iter().flatten().collect())
}

/// The SQL that applies `policy` (deterministic: run it through a replicated primary's
/// `schema`). A policy for a table that has one must be at least as strong (same column and
/// unit, no shorter, not less immutable); the stronger triggers are added alongside.
/// `since_ms` records when the policy took effect (pass the current time); it is part of the
/// SQL, so every replica stores the same value.
pub fn protect_sql(conn: &Connection, policy: &Policy, since_ms: i64) -> Result<String> {
    check_table(conn, policy)?;
    if let Some(old) = self::policy(conn, &policy.table)?
        && (old.column != policy.column || old.unit != policy.unit || old.min_age > policy.min_age || (old.immutable && !policy.immutable))
    {
        return Err(StorageError::invalid("retention policy", format!("`{}` already has a stronger or different policy", policy.table)));
    }
    let t = quote_ident(&policy.table);
    let c = quote_ident(&policy.column);
    let cutoff = policy.unit.cutoff_sql(policy.min_age);
    let msg = sql_string(&format!("weirdnet-storage retention: rows in {} are kept for at least {} days", policy.table, policy.min_age.as_days()));
    let young = format!("(old.{c} IS NULL OR old.{c} > {cutoff}) AND NOT {}", guard::REPLAYING);
    let mut sql = String::from("CREATE TABLE IF NOT EXISTS _weirdnet_guard (name TEXT PRIMARY KEY, value INTEGER NOT NULL);\n");
    sql.push_str(SCHEMA);
    sql.push_str(&format!(
        "INSERT INTO _weirdnet_retention (tbl, col, unit, min_age_ms, immutable, since_ms) VALUES ({}, {}, {}, {}, {}, {})
         ON CONFLICT(tbl) DO UPDATE SET min_age_ms = MAX(min_age_ms, excluded.min_age_ms), immutable = MAX(immutable, excluded.immutable);\n",
        sql_string(&policy.table),
        sql_string(&policy.column),
        sql_string(policy.unit.as_str()),
        policy.min_age.ms,
        policy.immutable as i64,
        since_ms
    ));
    sql.push_str(&format!(
        "CREATE TRIGGER IF NOT EXISTS {} BEFORE DELETE ON {t} FOR EACH ROW WHEN {young} BEGIN SELECT RAISE(ABORT, {msg}); END;\n",
        quote_ident(&policy.trigger_name("delete"))
    ));
    if policy.immutable {
        sql.push_str(&format!(
            "CREATE TRIGGER IF NOT EXISTS {} BEFORE UPDATE ON {t} FOR EACH ROW WHEN {young} BEGIN SELECT RAISE(ABORT, {msg}); END;\n",
            quote_ident(&policy.trigger_name("update"))
        ));
    }
    Ok(sql)
}

/// Apply `policy` on `conn` (not replicated; see [`protect_sql`]).
pub fn protect(conn: &Connection, policy: &Policy) -> Result<()> {
    let sql = protect_sql(conn, policy, guard::now_ms())?;
    crate::savepoint(conn, "weirdnet_protect", || Ok(conn.execute_batch(&sql)?))
}

/// A row's hash: table name, rowid and every column value in order.
fn row_hash(table: &str, rid: i64, values: &[ValueRef<'_>]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(ROW_LABEL);
    h.update((table.len() as u64).to_be_bytes());
    h.update(table.as_bytes());
    h.update(rid.to_be_bytes());
    h.update((values.len() as u64).to_be_bytes());
    for v in values {
        match v {
            ValueRef::Null => h.update([0u8]),
            ValueRef::Integer(i) => {
                h.update([1u8]);
                h.update(i.to_be_bytes());
            }
            ValueRef::Real(f) => {
                h.update([2u8]);
                h.update(f.to_bits().to_be_bytes());
            }
            ValueRef::Text(t) => {
                h.update([3u8]);
                h.update((t.len() as u64).to_be_bytes());
                h.update(t);
            }
            ValueRef::Blob(b) => {
                h.update([4u8]);
                h.update((b.len() as u64).to_be_bytes());
                h.update(b);
            }
        }
    }
    h.finalize().into()
}

/// Current hashes of the rows of `table` (rowid order), optionally only those with
/// `column <= cutoff`.
fn current_rows(conn: &Connection, table: &str, filter: Option<(&str, i64)>) -> Result<BTreeMap<i64, [u8; 32]>> {
    let ncols = columns(conn, table)?.len();
    let sql = match filter {
        Some((col, _)) => format!("SELECT rowid, * FROM {} WHERE {} <= ?1 ORDER BY rowid", quote_ident(table), quote_ident(col)),
        None => format!("SELECT rowid, * FROM {} ORDER BY rowid", quote_ident(table)),
    };
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = match filter {
        Some((_, cutoff)) => stmt.query([cutoff])?,
        None => stmt.query([])?,
    };
    let mut out = BTreeMap::new();
    while let Some(r) = rows.next()? {
        let rid: i64 = r.get(0)?;
        let mut vals = Vec::with_capacity(ncols);
        for i in 0..ncols {
            vals.push(r.get_ref(i + 1)?);
        }
        out.insert(rid, row_hash(table, rid, &vals));
    }
    Ok(out)
}

/// The latest ledger record per row of `table`: (hash, purged).
fn sealed_rows(conn: &Connection, table: &str) -> Result<BTreeMap<i64, ([u8; 32], bool)>> {
    let mut stmt = conn.prepare(
        "SELECT r.rid, r.hash, s.kind FROM _weirdnet_seal_rows r JOIN _weirdnet_seal s ON s.seq = r.seal
         WHERE r.tbl = ?1 ORDER BY r.rid, r.seal",
    )?;
    let mut out = BTreeMap::new();
    for row in stmt.query_map([table], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?, r.get::<_, String>(2)?)))? {
        let (rid, hash, kind) = row?;
        out.insert(rid, (hash.try_into().unwrap_or([0; 32]), kind == "purge"));
    }
    Ok(out)
}

fn rows_digest(rows: &BTreeMap<i64, [u8; 32]>) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(ROWS_LABEL);
    for (rid, hash) in rows {
        h.update(rid.to_be_bytes());
        h.update(hash);
    }
    h.finalize().into()
}

#[allow(clippy::too_many_arguments)]
fn seal_hash(prev: &[u8; 32], seq: i64, table: &str, kind: &str, at_ms: i64, rows: i64, digest: &[u8; 32]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(SEAL_LABEL);
    h.update(prev);
    h.update(seq.to_be_bytes());
    h.update((table.len() as u64).to_be_bytes());
    h.update(table.as_bytes());
    h.update((kind.len() as u64).to_be_bytes());
    h.update(kind.as_bytes());
    h.update(at_ms.to_be_bytes());
    h.update(rows.to_be_bytes());
    h.update(digest);
    h.finalize().into()
}

/// The end of the seal ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealHead {
    pub seq: i64,
    pub hash: [u8; 32],
}

/// The ledger's head, if anything was sealed. Sign or publish it to anchor the ledger.
pub fn head(conn: &Connection) -> Result<Option<SealHead>> {
    if !table_exists(conn, "_weirdnet_seal")? {
        return Ok(None);
    }
    Ok(conn
        .query_row("SELECT seq, hash FROM _weirdnet_seal ORDER BY seq DESC LIMIT 1", [], |r| {
            Ok(SealHead { seq: r.get(0)?, hash: r.get::<_, Vec<u8>>(1)?.try_into().unwrap_or([0; 32]) })
        })
        .optional()?)
}

fn append_seal(conn: &Connection, table: &str, kind: &str, rows: &BTreeMap<i64, [u8; 32]>) -> Result<SealHead> {
    let prev = head(conn)?;
    let seq = prev.map(|h| h.seq + 1).unwrap_or(1);
    let prev_hash = prev.map(|h| h.hash).unwrap_or([0; 32]);
    let at_ms = guard::now_ms();
    let digest = rows_digest(rows);
    let hash = seal_hash(&prev_hash, seq, table, kind, at_ms, rows.len() as i64, &digest);
    conn.execute(
        "INSERT INTO _weirdnet_seal (seq, tbl, kind, at_ms, rows, digest, prev, hash) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![seq, table, kind, at_ms, rows.len() as i64, digest.to_vec(), prev_hash.to_vec(), hash.to_vec()],
    )?;
    let mut stmt = conn.prepare("INSERT INTO _weirdnet_seal_rows (seal, tbl, rid, hash) VALUES (?1, ?2, ?3, ?4)")?;
    for (rid, h) in rows {
        stmt.execute(params![seq, table, rid, h.to_vec()])?;
    }
    Ok(SealHead { seq, hash })
}

fn require_policy(conn: &Connection, table: &str) -> Result<Policy> {
    policy(conn, table)?.ok_or_else(|| StorageError::invalid("retention table", format!("`{table}` has no retention policy")))
}

/// Seal the rows of `table` that are new or changed since the last seal. A changed row in an
/// `immutable` table is refused (it is tampering). Returns how many rows were sealed.
pub fn seal(conn: &Connection, table: &str) -> Result<usize> {
    seal_rows(conn, table, true)
}

/// Seal new rows; with `changes`, also changed rows of a table that isn't immutable.
fn seal_rows(conn: &Connection, table: &str, changes: bool) -> Result<usize> {
    let p = require_policy(conn, table)?;
    let current = current_rows(conn, table, None)?;
    let sealed = sealed_rows(conn, table)?;
    let mut fresh = BTreeMap::new();
    for (rid, h) in &current {
        match sealed.get(rid) {
            Some((s, false)) if s == h => {}
            Some((_, false)) if p.immutable || !changes => {
                return Err(StorageError::invalid("sealed row", format!("row {rid} of `{table}` changed after it was sealed")));
            }
            _ => {
                fresh.insert(*rid, *h);
            }
        }
    }
    if fresh.is_empty() {
        return Ok(0);
    }
    crate::savepoint(conn, "weirdnet_seal", || append_seal(conn, table, "seal", &fresh))?;
    Ok(fresh.len())
}

/// Seal every protected table.
pub fn seal_all(conn: &Connection) -> Result<usize> {
    let mut n = 0;
    for p in policies(conn)? {
        n += seal(conn, &p.table)?;
    }
    Ok(n)
}

/// Delete the rows of `table` past its minimum age and record them in the ledger. New rows
/// are sealed first; a sealed row that changed since must be sealed with [`seal`] before (so
/// a purge never records an unsealed change). Returns how many were deleted.
pub fn purge_expired(conn: &Connection, table: &str) -> Result<usize> {
    let p = require_policy(conn, table)?;
    seal_rows(conn, table, false)?;
    let cutoff = p.unit.cutoff(guard::now_ms(), p.min_age);
    let old = current_rows(conn, table, Some((&p.column, cutoff)))?;
    if old.is_empty() {
        return Ok(0);
    }
    crate::savepoint(conn, "weirdnet_purge", || {
        let mut del = conn.prepare(&format!("DELETE FROM {} WHERE rowid = ?1", quote_ident(table)))?;
        for rid in old.keys() {
            del.execute([rid])?;
        }
        append_seal(conn, table, "purge", &old)
    })?;
    Ok(old.len())
}

/// What [`verify`] found for one table.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableReport {
    pub table: String,
    pub rows: usize,
    pub sealed: usize,
    /// Rows not sealed yet.
    pub unsealed: usize,
    /// Sealed rows whose content differs from their last seal (tampering in an immutable
    /// table; in other tables, a change not sealed yet).
    pub changed: Vec<i64>,
    /// Sealed rows that are gone without a purge record.
    pub missing: Vec<i64>,
    pub immutable: bool,
}

/// What [`verify`] found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Report {
    /// The first ledger entry that fails its hash check, if any.
    pub broken_at: Option<i64>,
    pub head: Option<SealHead>,
    pub tables: Vec<TableReport>,
}

impl Report {
    /// The ledger is whole, no sealed row is missing, and no immutable row changed.
    pub fn is_intact(&self) -> bool {
        self.broken_at.is_none() && self.tables.iter().all(|t| t.missing.is_empty() && (!t.immutable || t.changed.is_empty()))
    }
}

/// A `_weirdnet_seal` row: seq, table, kind, at_ms, rows, digest, prev, hash.
type SealRow = (i64, String, String, i64, i64, Vec<u8>, Vec<u8>, Vec<u8>);

fn verify_chain(conn: &Connection) -> Result<Option<i64>> {
    let mut stmt = conn.prepare("SELECT seq, tbl, kind, at_ms, rows, digest, prev, hash FROM _weirdnet_seal ORDER BY seq")?;
    let mut prev = [0u8; 32];
    let entries: Vec<SealRow> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?, r.get(7)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let mut rows_stmt = conn.prepare("SELECT rid, hash FROM _weirdnet_seal_rows WHERE seal = ?1 ORDER BY rid")?;
    for (expect, (seq, tbl, kind, at_ms, rows, digest, stored_prev, hash)) in (1i64..).zip(entries) {
        let listed: BTreeMap<i64, [u8; 32]> = rows_stmt
            .query_map([seq], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?)))?
            .filter_map(|r| r.ok())
            .map(|(rid, h)| (rid, h.try_into().unwrap_or([0; 32])))
            .collect();
        let d = rows_digest(&listed);
        let ok = seq == expect
            && stored_prev.as_slice() == prev
            && digest.as_slice() == d
            && listed.len() as i64 == rows
            && hash.as_slice() == seal_hash(&prev, seq, &tbl, &kind, at_ms, rows, &d);
        if !ok {
            return Ok(Some(seq));
        }
        prev = hash.try_into().unwrap_or([0; 32]);
    }
    Ok(None)
}

/// Recompute the ledger and every protected row.
pub fn verify(conn: &Connection) -> Result<Report> {
    let mut report = Report::default();
    if !table_exists(conn, "_weirdnet_seal")? {
        return Ok(report);
    }
    report.broken_at = verify_chain(conn)?;
    report.head = head(conn)?;
    for p in policies(conn)? {
        let current = current_rows(conn, &p.table, None)?;
        let sealed = sealed_rows(conn, &p.table)?;
        let mut t = TableReport { table: p.table.clone(), rows: current.len(), immutable: p.immutable, ..Default::default() };
        for (rid, h) in &current {
            match sealed.get(rid) {
                Some((s, false)) => {
                    t.sealed += 1;
                    if s != h {
                        t.changed.push(*rid);
                    }
                }
                _ => t.unsealed += 1,
            }
        }
        for (rid, (_, purged)) in &sealed {
            if !purged && !current.contains_key(rid) {
                t.missing.push(*rid);
            }
        }
        report.tables.push(t);
    }
    Ok(report)
}

fn create_sql(conn: &Connection, table: &str) -> Result<String> {
    let sql: String = conn.query_row("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1", [table], |r| r.get(0))?;
    let trimmed = sql.trim_start();
    let upper = trimmed.to_ascii_uppercase();
    Ok(if upper.starts_with("CREATE TABLE IF NOT EXISTS") {
        trimmed.to_string()
    } else if let Some(rest) = upper.strip_prefix("CREATE TABLE").map(|_| &trimmed["CREATE TABLE".len()..]) {
        format!("CREATE TABLE IF NOT EXISTS{rest}")
    } else {
        trimmed.to_string()
    })
}

const ARCHIVE_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS _weirdnet_archive (
    tbl TEXT NOT NULL, rid INTEGER NOT NULL, hash BLOB NOT NULL, archived_ms INTEGER NOT NULL,
    PRIMARY KEY (tbl, rid));
CREATE TRIGGER IF NOT EXISTS _weirdnet_archive_append_only BEFORE UPDATE ON _weirdnet_archive
BEGIN SELECT RAISE(ABORT, 'weirdnet-storage retention: archive records are append-only'); END;
CREATE TRIGGER IF NOT EXISTS _weirdnet_archive_keep BEFORE DELETE ON _weirdnet_archive
BEGIN SELECT RAISE(ABORT, 'weirdnet-storage retention: archive records are kept'); END;
";

/// Copy the sealed rows of `table` with `column <= up_to` (in the policy's unit) into the
/// archive at `dest` (created if needed, keyed with `key`), with their hashes and the same
/// policy. New rows are sealed first; rows already archived are skipped; a sealed row whose
/// content no longer matches its seal is refused (seal a legitimate change first). The source rows stay (delete them later with [`purge_expired`]). Returns how many
/// rows were copied.
pub fn archive_to(conn: &Connection, table: &str, up_to: i64, dest: &Path, key: Option<&crate::RawKey>) -> Result<usize> {
    let p = require_policy(conn, table)?;
    seal_rows(conn, table, false)?;
    let since: i64 = conn.query_row("SELECT since_ms FROM _weirdnet_retention WHERE tbl = ?1", [table], |r| r.get(0))?;
    let rows = current_rows(conn, table, Some((&p.column, up_to)))?;
    let sealed = sealed_rows(conn, table)?;
    for (rid, h) in &rows {
        if sealed.get(rid).map(|(s, purged)| s != h || *purged).unwrap_or(true) {
            return Err(StorageError::invalid("archived row", format!("row {rid} of `{table}` does not match its seal")));
        }
    }
    let cols = columns(conn, table)?;
    let dst = crate::open(dest, key, "archive")?;
    dst.execute_batch(&create_sql(conn, table)?)?;
    dst.execute_batch(ARCHIVE_SCHEMA)?;
    let tx = dst.unchecked_transaction()?;
    tx.execute_batch(&protect_sql(&tx, &p, since)?)?;
    let have: HashSet<i64> = {
        let mut s = tx.prepare("SELECT rid FROM _weirdnet_archive WHERE tbl = ?1")?;
        s.query_map([table], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?
    };
    let col_list = cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    let marks = (1..=cols.len() + 1).map(|i| format!("?{i}")).collect::<Vec<_>>().join(", ");
    let mut read = conn.prepare(&format!("SELECT rowid, * FROM {} WHERE rowid = ?1", quote_ident(table)))?;
    let mut write = tx.prepare(&format!("INSERT INTO {} (rowid, {col_list}) VALUES ({marks})", quote_ident(table)))?;
    let mut mark = tx.prepare("INSERT INTO _weirdnet_archive (tbl, rid, hash, archived_ms) VALUES (?1, ?2, ?3, ?4)")?;
    let now = guard::now_ms();
    let mut copied = 0;
    for (rid, h) in &rows {
        if have.contains(rid) {
            continue;
        }
        let values: Vec<rusqlite::types::Value> =
            read.query_row([rid], |r| (0..=cols.len()).map(|i| r.get::<_, rusqlite::types::Value>(i)).collect())?;
        write.execute(rusqlite::params_from_iter(values.iter()))?;
        mark.execute(params![table, rid, h.to_vec(), now])?;
        copied += 1;
    }
    drop(write);
    drop(mark);
    tx.commit()?;
    Ok(copied)
}

/// Check an archive written by [`archive_to`]: every archived row must still match the hash
/// recorded when it was archived.
pub fn verify_archive(conn: &Connection) -> Result<Report> {
    let mut report = Report::default();
    if !table_exists(conn, "_weirdnet_archive")? {
        return Ok(report);
    }
    let mut stmt = conn.prepare("SELECT tbl, rid, hash FROM _weirdnet_archive ORDER BY tbl, rid")?;
    let recs: Vec<(String, i64, Vec<u8>)> = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?.collect::<rusqlite::Result<_>>()?;
    let mut by_table: BTreeMap<String, Vec<(i64, Vec<u8>)>> = BTreeMap::new();
    for (t, rid, h) in recs {
        by_table.entry(t).or_default().push((rid, h));
    }
    for (table, recs) in by_table {
        let current = current_rows(conn, &table, None)?;
        let mut t = TableReport { table: table.clone(), rows: current.len(), immutable: true, ..Default::default() };
        for (rid, h) in recs {
            match current.get(&rid) {
                Some(c) if c.as_slice() == h.as_slice() => t.sealed += 1,
                Some(_) => t.changed.push(rid),
                None => t.missing.push(rid),
            }
        }
        report.tables.push(t);
    }
    Ok(report)
}

/// Make `conn` refuse `DROP TABLE`, `ALTER TABLE` and `DROP TRIGGER` on protected tables,
/// their triggers and this crate's `_weirdnet_*` tables. Call it again after adding policies.
pub fn guard(conn: &Connection) -> Result<()> {
    let protected: HashSet<String> = policies(conn)?.into_iter().map(|p| p.table).collect();
    let is_protected = move |t: &str| t.starts_with("_weirdnet_") || protected.contains(t);
    conn.authorizer(Some(move |ctx: AuthContext<'_>| match ctx.action {
        AuthAction::DropTable { table_name } | AuthAction::AlterTable { table_name, .. } if is_protected(table_name) => Authorization::Deny,
        AuthAction::DropTrigger { trigger_name, table_name } if trigger_name.starts_with("_weirdnet_") || is_protected(table_name) => {
            Authorization::Deny
        }
        _ => Authorization::Allow,
    }));
    Ok(())
}
