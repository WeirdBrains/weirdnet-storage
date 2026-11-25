//! Replication: a primary and hot standbys kept current from an append-only, hash-chained log,
//! with epochs that fence an old primary, automatic or manual failover, and fail-back.
//!
//! - [`Primary::write`] runs a transaction and records every row change it made (SQLite's
//!   session extension) as one log [`Entry`] in the same transaction, so the data and the log
//!   never disagree. Schema changes go through [`Primary::schema`] and [`Primary::migrate`].
//! - A [`Standby`] pulls entries from a [`Source`] (the primary, another standby, or a remote
//!   one through [`service`] over weirdnet-net), checks the hash chain and applies each page
//!   in one transaction.
//! - Every entry carries the epoch of the primary that wrote it. A standby accepts only the
//!   epoch it has applied (or an [`Kind::Epoch`] entry that raises it) and refuses sources
//!   older than the newest epoch it has seen. A primary checks its [`Leadership`] before and
//!   after each write, so once its term is gone it can't commit.
//! - [`Replica::tick`] promotes a standby that gains leadership (a weirdnet-sync lease with the
//!   `lease` feature, or [`Manual`]) and steps a primary down when it loses it. A returning
//!   ex-primary finds where it diverged, rewinds its unreplicated entries (kept in
//!   `_weirdnet_rewound`) and follows the new primary.
//!
//! Every node keeps its own SQLCipher key; the log lives inside the encrypted database.
//! Replicated tables need a `PRIMARY KEY` (the session extension skips tables without one, so
//! [`Primary`] refuses them). Replication is asynchronous: a failover can lose the old
//! primary's last unreplicated writes, which that node rewinds and keeps on return.
use crate::error::StorageError;
use crate::guard;
use rusqlite::session::{ConflictAction, ConflictType, Session, invert_strm};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::sync::{Arc, Mutex};

#[cfg(feature = "lease")]
mod lease;
#[cfg(feature = "replication-service")]
pub mod service;

/// The replication log table.
pub const LOG_TABLE: &str = "_weirdnet_replog";
const STATE_TABLE: &str = "_weirdnet_repl_state";
/// Entries a returning ex-primary rewound, kept for audit.
pub const REWOUND_TABLE: &str = "_weirdnet_rewound";
const LABEL: &[u8] = b"weirdnet-storage/replog/1\0";
/// Default page size for pulls.
pub const DEFAULT_PAGE: u32 = 256;
const MAX_PAGE_BYTES: u32 = 8 << 20;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS _weirdnet_replog (
    seq INTEGER PRIMARY KEY, epoch INTEGER NOT NULL, at_ms INTEGER NOT NULL,
    kind INTEGER NOT NULL, body BLOB NOT NULL, hash BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS _weirdnet_repl_state (
    id INTEGER PRIMARY KEY CHECK (id = 1), node TEXT NOT NULL,
    epoch INTEGER NOT NULL, fence INTEGER NOT NULL,
    base_seq INTEGER NOT NULL, base_hash BLOB NOT NULL, base_epoch INTEGER NOT NULL,
    trim_below INTEGER NOT NULL, rewind_above INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS _weirdnet_rewound (
    seq INTEGER NOT NULL, epoch INTEGER NOT NULL, at_ms INTEGER NOT NULL, kind INTEGER NOT NULL,
    body BLOB NOT NULL, hash BLOB NOT NULL, rewound_ms INTEGER NOT NULL, fork INTEGER NOT NULL);
CREATE TRIGGER IF NOT EXISTS _weirdnet_replog_append_only BEFORE UPDATE ON _weirdnet_replog
BEGIN SELECT RAISE(ABORT, 'weirdnet-storage: the replication log is append-only'); END;
CREATE TRIGGER IF NOT EXISTS _weirdnet_replog_keep BEFORE DELETE ON _weirdnet_replog
WHEN old.seq >= (SELECT trim_below FROM _weirdnet_repl_state) AND old.seq <= (SELECT rewind_above FROM _weirdnet_repl_state)
BEGIN SELECT RAISE(ABORT, 'weirdnet-storage: the replication log is append-only'); END;
CREATE TRIGGER IF NOT EXISTS _weirdnet_rewound_keep BEFORE DELETE ON _weirdnet_rewound
BEGIN SELECT RAISE(ABORT, 'weirdnet-storage: rewound entries are kept'); END;
";

/// A SHA-256 digest: an entry's hash in the chain.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Digest(pub [u8; 32]);

impl Digest {
    /// The chain's starting point.
    pub const ZERO: Digest = Digest([0; 32]);

    /// Lower-case hex.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    fn from_slice(b: &[u8]) -> Option<Digest> {
        b.try_into().ok().map(Digest)
    }
}

impl std::fmt::Debug for Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Digest({})", &self.to_hex()[..16])
    }
}

impl std::fmt::Display for Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl Serialize for Digest {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for Digest {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let b = serde_bytes::ByteBuf::deserialize(d)?;
        Digest::from_slice(&b).ok_or_else(|| serde::de::Error::invalid_length(b.len(), &"32 bytes"))
    }
}

/// What a log entry holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Kind {
    /// A changeset: the row changes of one primary transaction.
    Changes,
    /// SQL replayed as is (schema changes and migrations): UTF-8 text.
    Schema,
    /// A primary started this epoch; the body is its node name.
    Epoch,
}

impl Kind {
    fn code(self) -> i64 {
        match self {
            Kind::Changes => 1,
            Kind::Schema => 2,
            Kind::Epoch => 3,
        }
    }

    fn from_code(c: i64) -> Option<Kind> {
        match c {
            1 => Some(Kind::Changes),
            2 => Some(Kind::Schema),
            3 => Some(Kind::Epoch),
            _ => None,
        }
    }
}

/// One replication log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub seq: u64,
    /// The epoch of the primary that wrote it.
    pub epoch: u64,
    /// When the primary wrote it (milliseconds since the Unix epoch).
    pub at_ms: i64,
    pub kind: Kind,
    #[serde(with = "serde_bytes")]
    pub body: Vec<u8>,
    /// `SHA-256(label, previous hash, seq, epoch, at_ms, kind, body)`.
    pub hash: Digest,
}

impl Entry {
    /// The hash an entry with these fields must have after `prev`.
    pub fn expected_hash(prev: &Digest, seq: u64, epoch: u64, at_ms: i64, kind: Kind, body: &[u8]) -> Digest {
        let mut h = Sha256::new();
        h.update(LABEL);
        h.update(prev.0);
        h.update(seq.to_be_bytes());
        h.update(epoch.to_be_bytes());
        h.update(at_ms.to_be_bytes());
        h.update([kind.code() as u8]);
        h.update((body.len() as u64).to_be_bytes());
        h.update(body);
        Digest(h.finalize().into())
    }

    /// Whether this entry follows `prev` in the chain.
    pub fn follows(&self, prev: &Digest) -> bool {
        Entry::expected_hash(prev, self.seq, self.epoch, self.at_ms, self.kind, &self.body) == self.hash
    }
}

/// The end of a node's log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Head {
    /// The last entry (0 for an empty log).
    pub seq: u64,
    pub hash: Digest,
    /// The epoch the node has applied.
    pub epoch: u64,
}

/// Ask for the entries after `after_seq`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pull {
    pub after_seq: u64,
    /// The asker's hash at `after_seq`, so a diverged history is noticed.
    pub after_hash: Digest,
    /// The newest epoch the asker has seen: a source older than this is fenced.
    pub fence: u64,
    pub max_entries: u32,
}

/// A request to a [`Source`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Request {
    Pull(Pull),
    /// The source's hash at an entry (fork search).
    Hash {
        seq: u64,
    },
    Head,
}

/// A [`Source`]'s answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Reply {
    /// The next entries (possibly none) and the source's head.
    Entries {
        entries: Vec<Entry>,
        head: Head,
    },
    /// The source's history differs from the asker's at or before `at`.
    Diverged {
        at: u64,
    },
    /// The asker has seen a newer epoch than the source's: the source is fenced.
    Stale {
        epoch: u64,
        fence: u64,
    },
    /// The source no longer has entries that old: reseed from a snapshot.
    Trimmed {
        base: u64,
    },
    Hash(Option<Digest>),
    Head(Head),
}

/// Replication errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ReplError {
    /// No leadership is held: this node may not write.
    #[error("not the primary: no leadership held")]
    NotLeader,
    /// A newer epoch exists: this node's writes are refused.
    #[error("fenced: epoch {ours} is older than epoch {newer}")]
    Fenced { ours: u64, newer: u64 },
    /// The source is older than an epoch this node has seen.
    #[error("the source is stale: epoch {epoch}, but epoch {fence} has been seen")]
    StaleSource { epoch: u64, fence: u64 },
    /// The histories differ; [`Standby::rejoin`] rewinds to where they agree.
    #[error("diverged from the source at or before entry {at}")]
    Diverged { at: u64 },
    /// The source's log starts later than this node's head: reseed from a snapshot.
    #[error("the source's log starts after entry {base}; reseed from a snapshot")]
    Trimmed { base: u64 },
    /// An entry fails its hash check.
    #[error("replication log entry {seq} fails its hash check")]
    Tampered { seq: u64 },
    /// An entry can't be applied: out of order, the wrong epoch, or a conflicting change.
    #[error("entry {seq} refused: {msg}")]
    Rejected { seq: u64, msg: String },
    /// A rewind would have to undo something it can't (a schema change).
    #[error("cannot rewind entry {seq}: {msg}")]
    CannotRewind { seq: u64, msg: String },
    /// The source failed or answered something unexpected.
    #[error("source: {0}")]
    Source(String),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

impl From<rusqlite::Error> for ReplError {
    fn from(e: rusqlite::Error) -> Self {
        ReplError::Storage(StorageError::Sqlite(e))
    }
}

type Result<T> = std::result::Result<T, ReplError>;

/// Who may write: the current term while this node holds leadership. The term becomes the
/// epoch, so terms must only grow (a weirdnet-sync lease term does; see the `lease` feature).
pub trait Leadership: Send + Sync {
    /// The term this node holds now, if any.
    fn held(&self) -> Option<u64>;
}

impl<F: Fn() -> Option<u64> + Send + Sync> Leadership for F {
    fn held(&self) -> Option<u64> {
        self()
    }
}

/// Leadership an operator sets: grant a term to promote, revoke to stop writes.
#[derive(Debug, Clone, Default)]
pub struct Manual(Arc<Mutex<Option<u64>>>);

impl Manual {
    /// Nothing held.
    pub fn new() -> Self {
        Self::default()
    }

    /// Holding `term` from the start.
    pub fn granted(term: u64) -> Self {
        let m = Self::new();
        m.grant(term);
        m
    }

    /// Hold `term` (use a term above every epoch seen so far).
    pub fn grant(&self, term: u64) {
        if let Ok(mut t) = self.0.lock() {
            *t = Some(term);
        }
    }

    /// Hold nothing.
    pub fn revoke(&self) {
        if let Ok(mut t) = self.0.lock() {
            *t = None;
        }
    }
}

impl Leadership for Manual {
    fn held(&self) -> Option<u64> {
        self.0.lock().ok().and_then(|t| *t)
    }
}

/// Where a standby gets entries from.
pub trait Source {
    fn call(&self, request: &Request) -> Result<Reply>;
}

#[derive(Debug, Clone)]
struct State {
    node: String,
    epoch: u64,
    fence: u64,
    base_seq: u64,
    base_hash: Digest,
    base_epoch: u64,
}

fn local_table(t: &str) -> bool {
    matches!(t, LOG_TABLE | STATE_TABLE | REWOUND_TABLE | guard::TABLE)
}

fn init(conn: &Connection, node: &str) -> Result<()> {
    if node.is_empty() || node.len() > 200 {
        return Err(StorageError::invalid("node name", "1 to 200 characters").into());
    }
    guard::ensure(conn)?;
    conn.execute_batch(SCHEMA)?;
    conn.execute(
        "INSERT OR IGNORE INTO _weirdnet_repl_state VALUES (1, ?1, 0, 0, 0, ?2, 0, 0, 9223372036854775807)",
        params![node, Digest::ZERO.0.to_vec()],
    )?;
    Ok(())
}

fn state(conn: &Connection) -> Result<State> {
    Ok(conn.query_row("SELECT node, epoch, fence, base_seq, base_hash, base_epoch FROM _weirdnet_repl_state WHERE id = 1", [], |r| {
        Ok(State {
            node: r.get(0)?,
            epoch: r.get::<_, i64>(1)? as u64,
            fence: r.get::<_, i64>(2)? as u64,
            base_seq: r.get::<_, i64>(3)? as u64,
            base_hash: Digest::from_slice(&r.get::<_, Vec<u8>>(4)?).unwrap_or_default(),
            base_epoch: r.get::<_, i64>(5)? as u64,
        })
    })?)
}

fn set_epoch(conn: &Connection, epoch: u64, fence: u64) -> Result<()> {
    conn.execute("UPDATE _weirdnet_repl_state SET epoch = ?1, fence = MAX(fence, ?2) WHERE id = 1", params![epoch as i64, fence as i64])?;
    Ok(())
}

fn raise_fence(conn: &Connection, fence: u64) -> Result<()> {
    conn.execute("UPDATE _weirdnet_repl_state SET fence = MAX(fence, ?1) WHERE id = 1", params![fence as i64])?;
    Ok(())
}

fn head(conn: &Connection) -> Result<Head> {
    let st = state(conn)?;
    let last = conn
        .query_row("SELECT seq, hash FROM _weirdnet_replog ORDER BY seq DESC LIMIT 1", [], |r| {
            Ok((r.get::<_, i64>(0)? as u64, r.get::<_, Vec<u8>>(1)?))
        })
        .optional()?;
    Ok(match last {
        Some((seq, hash)) => Head { seq, hash: Digest::from_slice(&hash).unwrap_or_default(), epoch: st.epoch },
        None => Head { seq: st.base_seq, hash: st.base_hash, epoch: st.epoch },
    })
}

fn hash_at(conn: &Connection, seq: u64) -> Result<Option<Digest>> {
    let st = state(conn)?;
    if seq == st.base_seq {
        return Ok(Some(st.base_hash));
    }
    if seq < st.base_seq {
        return Ok(None);
    }
    let h: Option<Vec<u8>> = conn.query_row("SELECT hash FROM _weirdnet_replog WHERE seq = ?1", [seq as i64], |r| r.get(0)).optional()?;
    Ok(h.and_then(|h| Digest::from_slice(&h)))
}

fn row_entry(r: &rusqlite::Row<'_>) -> rusqlite::Result<Entry> {
    let kind = r.get::<_, i64>(3)?;
    Ok(Entry {
        seq: r.get::<_, i64>(0)? as u64,
        epoch: r.get::<_, i64>(1)? as u64,
        at_ms: r.get(2)?,
        kind: Kind::from_code(kind).unwrap_or(Kind::Changes),
        body: r.get(4)?,
        hash: Digest::from_slice(&r.get::<_, Vec<u8>>(5)?).unwrap_or_default(),
    })
}

fn entries_after(conn: &Connection, after: u64, max_entries: u32) -> Result<Vec<Entry>> {
    let mut stmt = conn.prepare("SELECT seq, epoch, at_ms, kind, body, hash FROM _weirdnet_replog WHERE seq > ?1 ORDER BY seq LIMIT ?2")?;
    let mut out = Vec::new();
    let mut bytes = 0usize;
    for e in stmt.query_map(params![after as i64, max_entries.clamp(1, 100_000) as i64], row_entry)? {
        let e = e?;
        bytes += e.body.len();
        out.push(e);
        if bytes >= MAX_PAGE_BYTES as usize {
            break;
        }
    }
    Ok(out)
}

fn insert_entry(conn: &Connection, e: &Entry) -> Result<()> {
    conn.execute(
        "INSERT INTO _weirdnet_replog (seq, epoch, at_ms, kind, body, hash) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![e.seq as i64, e.epoch as i64, e.at_ms, e.kind.code(), e.body, e.hash.0.to_vec()],
    )?;
    Ok(())
}

fn append(conn: &Connection, epoch: u64, kind: Kind, body: Vec<u8>) -> Result<Entry> {
    let h = head(conn)?;
    let seq = h.seq + 1;
    let at_ms = guard::now_ms();
    let hash = Entry::expected_hash(&h.hash, seq, epoch, at_ms, kind, &body);
    let e = Entry { seq, epoch, at_ms, kind, body, hash };
    insert_entry(conn, &e)?;
    Ok(e)
}

/// Check the whole log's hash chain; returns the head.
fn verify_log(conn: &Connection) -> Result<Head> {
    let st = state(conn)?;
    let mut prev = st.base_hash;
    let mut seq = st.base_seq;
    let mut stmt = conn.prepare("SELECT seq, epoch, at_ms, kind, body, hash FROM _weirdnet_replog ORDER BY seq")?;
    for e in stmt.query_map([], row_entry)? {
        let e = e?;
        if e.seq != seq + 1 || !e.follows(&prev) {
            return Err(ReplError::Tampered { seq: e.seq });
        }
        prev = e.hash;
        seq = e.seq;
    }
    Ok(Head { seq, hash: prev, epoch: st.epoch })
}

/// Answer a [`Request`] from a node's log. A pull from a node that has seen a newer epoch
/// raises this node's fence, so a stale primary stops writing.
pub fn serve(conn: &Connection, request: &Request) -> Result<Reply> {
    match request {
        Request::Head => Ok(Reply::Head(head(conn)?)),
        Request::Hash { seq } => Ok(Reply::Hash(hash_at(conn, *seq)?)),
        Request::Pull(p) => {
            let st = state(conn)?;
            if p.fence > st.epoch {
                // A read-only connection can't record it; the leadership check still applies.
                let _ = raise_fence(conn, p.fence);
                return Ok(Reply::Stale { epoch: st.epoch, fence: p.fence });
            }
            if p.after_seq < st.base_seq {
                return Ok(Reply::Trimmed { base: st.base_seq });
            }
            match hash_at(conn, p.after_seq)? {
                Some(h) if h == p.after_hash => Ok(Reply::Entries { entries: entries_after(conn, p.after_seq, p.max_entries)?, head: head(conn)? }),
                _ => Ok(Reply::Diverged { at: p.after_seq }),
            }
        }
    }
}

fn check_tables(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")?;
    let names: Vec<String> = stmt.query_map([], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?;
    for name in names {
        if local_table(&name) {
            continue;
        }
        let pk: i64 =
            conn.query_row(&format!("SELECT COUNT(*) FROM pragma_table_info({}) WHERE pk > 0", sql_string(&name)), [], |r| r.get(0)).unwrap_or(0);
        if pk == 0 {
            return Err(StorageError::invalid("replicated table", format!("`{name}` has no PRIMARY KEY; replication needs one")).into());
        }
    }
    Ok(())
}

fn sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn check_leader(leader: &dyn Leadership, conn: &Connection, epoch: u64) -> Result<()> {
    match leader.held() {
        Some(t) if t == epoch => {}
        Some(t) if t > epoch => return Err(ReplError::Fenced { ours: epoch, newer: t }),
        _ => return Err(ReplError::NotLeader),
    }
    let st = state(conn)?;
    let newest = st.epoch.max(st.fence);
    if newest > epoch {
        return Err(ReplError::Fenced { ours: epoch, newer: newest });
    }
    Ok(())
}

/// The node that takes writes.
pub struct Primary {
    conn: Connection,
    leader: Arc<dyn Leadership>,
    epoch: u64,
}

impl std::fmt::Debug for Primary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Primary").field("epoch", &self.epoch).finish_non_exhaustive()
    }
}

impl Primary {
    /// Become the primary of `conn` at the term `leader` holds. A fresh database (or one
    /// that was never replicated) starts its log here; take the first standby from a
    /// [`snapshot_to`](crate::snapshot_to) of it after this call. A term above the
    /// database's epoch starts a new epoch; a term below the newest epoch seen is refused.
    pub fn open(conn: Connection, node: &str, leader: Arc<dyn Leadership>) -> Result<Self> {
        Primary::become_primary(conn, Some(node), leader).map_err(|(_, e)| e)
    }

    #[allow(clippy::result_large_err)] // the connection is handed back on failure
    fn become_primary(conn: Connection, node: Option<&str>, leader: Arc<dyn Leadership>) -> std::result::Result<Self, (Connection, ReplError)> {
        let r = (|| -> Result<u64> {
            if let Some(node) = node {
                init(&conn, node)?;
            }
            check_tables(&conn)?;
            let term = leader.held().ok_or(ReplError::NotLeader)?;
            let st = state(&conn)?;
            let newest = st.epoch.max(st.fence);
            if term < newest || term == 0 {
                return Err(ReplError::Fenced { ours: term, newer: newest.max(1) });
            }
            if term > st.epoch {
                let tx = conn.unchecked_transaction()?;
                append(&tx, term, Kind::Epoch, st.node.as_bytes().to_vec())?;
                set_epoch(&tx, term, term)?;
                tx.commit()?;
            }
            Ok(term)
        })();
        match r {
            Ok(term) => Ok(Primary { conn, leader, epoch: term }),
            Err(e) => Err((conn, e)),
        }
    }

    /// The epoch this primary writes under.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// The connection, for reads. Write through [`write`](Self::write) only: changes made
    /// outside it are not replicated.
    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    /// Whether this node may still write.
    pub fn check(&self) -> Result<()> {
        check_leader(&*self.leader, &self.conn, self.epoch)
    }

    /// Run `f` in a transaction and replicate what it changed. Leadership is checked before
    /// and again just before the commit; if it is gone, nothing is committed.
    pub fn write<T, E: From<ReplError>>(&mut self, f: impl FnOnce(&Transaction<'_>) -> std::result::Result<T, E>) -> std::result::Result<T, E> {
        let epoch = self.epoch;
        check_leader(&*self.leader, &self.conn, epoch)?;
        let tx = self.conn.transaction().map_err(ReplError::from)?;
        let (out, body) = {
            let mut s = Session::new(&tx).map_err(ReplError::from)?;
            s.table_filter(Some(|t: &str| !local_table(t)));
            s.attach::<&str>(None).map_err(ReplError::from)?;
            let out = f(&tx)?;
            let mut body = Vec::new();
            if !s.is_empty() {
                s.changeset_strm(&mut body).map_err(ReplError::from)?;
            }
            (out, body)
        };
        if !body.is_empty() {
            append(&tx, epoch, Kind::Changes, body)?;
        }
        check_leader(&*self.leader, &tx, epoch)?;
        tx.commit().map_err(ReplError::from)?;
        Ok(out)
    }

    /// Run SQL (schema changes, or data changes that must replay exactly) and replicate it
    /// as is. It must give the same result on every node: no `random()`, no current time.
    pub fn schema(&mut self, sql: &str) -> Result<Entry> {
        let epoch = self.epoch;
        check_leader(&*self.leader, &self.conn, epoch)?;
        let tx = self.conn.transaction()?;
        tx.execute_batch(sql)?;
        check_tables(&tx)?;
        let e = append(&tx, epoch, Kind::Schema, sql.as_bytes().to_vec())?;
        check_leader(&*self.leader, &tx, epoch)?;
        tx.commit()?;
        Ok(e)
    }

    /// [`migrate`](crate::migrate) for a replicated database: each pending migration and its
    /// `schema_version` row replicate as one entry. Returns how many ran.
    pub fn migrate(&mut self, migrations: &[&str], upto: usize) -> Result<usize> {
        self.conn.execute_batch("PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")?;
        if !crate::table_exists(&self.conn, "schema_version")? {
            self.schema("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY);")?;
        }
        let v: i64 = self.conn.query_row("SELECT COALESCE(MAX(version), 0) FROM schema_version", [], |r| r.get(0))?;
        let mut ran = 0;
        for (i, m) in migrations.iter().enumerate().take(upto) {
            let target = i as i64 + 1;
            if v < target {
                self.schema(&format!("{m}\n;INSERT INTO schema_version (version) VALUES ({target});"))?;
                ran += 1;
            }
        }
        Ok(ran)
    }

    /// The end of the log.
    pub fn head(&self) -> Result<Head> {
        head(&self.conn)
    }

    /// Check the log's hash chain.
    pub fn verify(&self) -> Result<Head> {
        verify_log(&self.conn)
    }

    /// Drop log entries before `below` (every standby must be past them; a new standby then
    /// starts from a snapshot). The chain continues from the last dropped entry's hash.
    pub fn trim_log(&mut self, below: u64) -> Result<()> {
        let h = head(&self.conn)?;
        let st = state(&self.conn)?;
        if below <= st.base_seq + 1 {
            return Ok(());
        }
        if below > h.seq + 1 {
            return Err(StorageError::invalid("trim point", format!("{below} is past the head {}", h.seq)).into());
        }
        let base = below - 1;
        let tx = self.conn.transaction()?;
        let (hash, epoch): (Vec<u8>, i64) =
            tx.query_row("SELECT hash, epoch FROM _weirdnet_replog WHERE seq = ?1", [base as i64], |r| Ok((r.get(0)?, r.get(1)?)))?;
        tx.execute(
            "UPDATE _weirdnet_repl_state SET trim_below = ?1, base_seq = ?2, base_hash = ?3, base_epoch = ?4 WHERE id = 1",
            params![below as i64, base as i64, hash, epoch],
        )?;
        tx.execute("DELETE FROM _weirdnet_replog WHERE seq < ?1", [below as i64])?;
        tx.commit()?;
        Ok(())
    }

    /// Stop writing and follow another primary.
    pub fn step_down(self) -> Standby {
        Standby { conn: self.conn }
    }

    /// The connection back.
    pub fn into_connection(self) -> Connection {
        self.conn
    }
}

impl Source for Primary {
    fn call(&self, request: &Request) -> Result<Reply> {
        serve(&self.conn, request)
    }
}

/// How far a standby got.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Progress {
    /// Entries applied by this call.
    pub applied: usize,
    pub head: Head,
    /// The source's head when it last answered.
    pub source_head: Head,
}

impl Progress {
    /// Entries the source has that this node hasn't applied yet.
    pub fn behind(&self) -> u64 {
        self.source_head.seq.saturating_sub(self.head.seq)
    }
}

/// What a rejoin did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rejoined {
    /// The last entry both histories share.
    pub fork: u64,
    /// Entries rewound (kept in `_weirdnet_rewound`).
    pub rewound: usize,
    pub progress: Progress,
}

/// A hot standby: applies a primary's log.
pub struct Standby {
    conn: Connection,
}

impl std::fmt::Debug for Standby {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Standby").finish_non_exhaustive()
    }
}

fn abort_on_conflict(_: ConflictType, _: rusqlite::session::ChangesetItem) -> ConflictAction {
    ConflictAction::SQLITE_CHANGESET_ABORT
}

fn apply_changes(conn: &Connection, body: &[u8]) -> rusqlite::Result<()> {
    let mut input: &[u8] = body;
    conn.apply_strm(&mut input, None::<fn(&str) -> bool>, abort_on_conflict)
}

impl Standby {
    /// Follow a primary with `conn`: a fresh database, or a snapshot of the primary
    /// (`snapshot_to`, keyed with this node's own key).
    pub fn open(conn: Connection, node: &str) -> Result<Self> {
        init(&conn, node)?;
        conn.execute("UPDATE _weirdnet_repl_state SET node = ?1 WHERE id = 1", [node])?;
        Ok(Standby { conn })
    }

    /// The connection, for reads. Don't write to a standby.
    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    pub fn head(&self) -> Result<Head> {
        head(&self.conn)
    }

    /// The newest epoch this node has seen (it refuses older sources).
    pub fn fence(&self) -> Result<u64> {
        let st = state(&self.conn)?;
        Ok(st.epoch.max(st.fence))
    }

    /// Learn that `epoch` exists (from a lease, an operator, another node): sources older
    /// than it are refused from now on.
    pub fn observe_epoch(&mut self, epoch: u64) -> Result<()> {
        raise_fence(&self.conn, epoch)
    }

    /// Check the log's hash chain.
    pub fn verify(&self) -> Result<Head> {
        verify_log(&self.conn)
    }

    /// The next pull.
    pub fn pull_request(&self, max_entries: u32) -> Result<Request> {
        let h = head(&self.conn)?;
        Ok(Request::Pull(Pull { after_seq: h.seq, after_hash: h.hash, fence: self.fence()?, max_entries }))
    }

    /// Apply a source's answer to [`pull_request`](Self::pull_request).
    pub fn apply(&mut self, reply: Reply) -> Result<Progress> {
        match reply {
            Reply::Entries { entries, head: source_head } => {
                let applied = self.apply_entries(&entries)?;
                Ok(Progress { applied, head: head(&self.conn)?, source_head })
            }
            Reply::Diverged { at } => Err(ReplError::Diverged { at }),
            Reply::Stale { epoch, fence } => Err(ReplError::StaleSource { epoch, fence }),
            Reply::Trimmed { base } => Err(ReplError::Trimmed { base }),
            other => Err(ReplError::Source(format!("unexpected answer to a pull: {other:?}"))),
        }
    }

    fn apply_entries(&mut self, entries: &[Entry]) -> Result<usize> {
        if entries.is_empty() {
            return Ok(0);
        }
        let tx = self.conn.transaction()?;
        guard::set(&tx, "replay", true)?;
        let mut st = state(&tx)?;
        let mut h = head(&tx)?;
        for e in entries {
            if e.seq != h.seq + 1 {
                return Err(ReplError::Rejected { seq: e.seq, msg: format!("expected entry {}", h.seq + 1) });
            }
            if !e.follows(&h.hash) {
                return Err(ReplError::Tampered { seq: e.seq });
            }
            match e.kind {
                Kind::Epoch => {
                    if e.epoch <= st.epoch || e.epoch < st.fence {
                        return Err(ReplError::Rejected {
                            seq: e.seq,
                            msg: format!("epoch {} does not follow epoch {}", e.epoch, st.epoch.max(st.fence)),
                        });
                    }
                    st.epoch = e.epoch;
                    st.fence = st.fence.max(e.epoch);
                    set_epoch(&tx, st.epoch, st.fence)?;
                }
                Kind::Changes | Kind::Schema => {
                    if e.epoch != st.epoch || st.epoch < st.fence {
                        return Err(ReplError::Fenced { ours: e.epoch, newer: st.epoch.max(st.fence) });
                    }
                    if e.kind == Kind::Changes {
                        apply_changes(&tx, &e.body).map_err(|err| ReplError::Rejected { seq: e.seq, msg: err.to_string() })?;
                    } else {
                        let sql =
                            std::str::from_utf8(&e.body).map_err(|_| ReplError::Rejected { seq: e.seq, msg: "schema entry is not UTF-8".into() })?;
                        tx.execute_batch(sql).map_err(|err| ReplError::Rejected { seq: e.seq, msg: err.to_string() })?;
                    }
                }
            }
            insert_entry(&tx, e)?;
            h = Head { seq: e.seq, hash: e.hash, epoch: st.epoch };
        }
        guard::set(&tx, "replay", false)?;
        tx.commit()?;
        Ok(entries.len())
    }

    /// Pull and apply until the source has nothing newer.
    pub fn catch_up(&mut self, source: &dyn Source) -> Result<Progress> {
        let mut total = 0;
        loop {
            let reply = source.call(&self.pull_request(DEFAULT_PAGE)?)?;
            let p = self.apply(reply)?;
            total += p.applied;
            if p.applied == 0 || p.behind() == 0 {
                return Ok(Progress { applied: total, ..p });
            }
        }
    }

    /// Follow a source this node diverged from (a returning ex-primary following the new
    /// one): find the last shared entry, rewind past it, then catch up.
    pub fn rejoin(&mut self, source: &dyn Source) -> Result<Rejoined> {
        let theirs = match source.call(&Request::Head)? {
            Reply::Head(h) => h,
            other => return Err(ReplError::Source(format!("unexpected answer to head: {other:?}"))),
        };
        let mut search = ForkSearch::new(&self.conn, &theirs)?;
        while let Some(seq) = search.next_probe() {
            let h = match source.call(&Request::Hash { seq })? {
                Reply::Hash(h) => h,
                other => return Err(ReplError::Source(format!("unexpected answer to hash: {other:?}"))),
            };
            search.answer(&self.conn, seq, h)?;
        }
        let fork = search.fork()?;
        self.observe_epoch(theirs.epoch)?;
        let rewound = self.rewind_to(fork)?;
        let progress = self.catch_up(source)?;
        Ok(Rejoined { fork, rewound, progress })
    }

    /// Undo every entry after `fork`, newest first (inverted changesets), keeping them in
    /// `_weirdnet_rewound`. A schema entry can't be undone: reseed from a snapshot instead.
    pub fn rewind_to(&mut self, fork: u64) -> Result<usize> {
        let tx = self.conn.transaction()?;
        let st = state(&tx)?;
        if fork < st.base_seq {
            return Err(ReplError::Trimmed { base: st.base_seq });
        }
        let mut stmt = tx.prepare("SELECT seq, epoch, at_ms, kind, body, hash FROM _weirdnet_replog WHERE seq > ?1 ORDER BY seq DESC")?;
        let entries: Vec<Entry> = stmt.query_map([fork as i64], row_entry)?.collect::<rusqlite::Result<_>>()?;
        drop(stmt);
        if entries.is_empty() {
            return Ok(0);
        }
        guard::set(&tx, "rewind", true)?;
        let now = guard::now_ms();
        for e in &entries {
            match e.kind {
                Kind::Changes => {
                    let mut inv = Vec::new();
                    let mut input: &[u8] = &e.body;
                    invert_strm(&mut input, &mut inv)?;
                    apply_changes(&tx, &inv).map_err(|err| ReplError::CannotRewind { seq: e.seq, msg: err.to_string() })?;
                }
                Kind::Schema => return Err(ReplError::CannotRewind { seq: e.seq, msg: "a schema change; reseed from a snapshot".into() }),
                Kind::Epoch => {}
            }
            tx.execute(
                "INSERT INTO _weirdnet_rewound VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![e.seq as i64, e.epoch as i64, e.at_ms, e.kind.code(), e.body, e.hash.0.to_vec(), now, fork as i64],
            )?;
        }
        tx.execute("UPDATE _weirdnet_repl_state SET rewind_above = ?1 WHERE id = 1", [fork as i64])?;
        tx.execute("DELETE FROM _weirdnet_replog WHERE seq > ?1", [fork as i64])?;
        tx.execute("UPDATE _weirdnet_repl_state SET rewind_above = 9223372036854775807 WHERE id = 1", [])?;
        let epoch: u64 = if fork == st.base_seq {
            st.base_epoch
        } else {
            tx.query_row("SELECT epoch FROM _weirdnet_replog WHERE seq = ?1", [fork as i64], |r| r.get::<_, i64>(0))? as u64
        };
        tx.execute("UPDATE _weirdnet_repl_state SET epoch = ?1 WHERE id = 1", [epoch as i64])?;
        guard::set(&tx, "rewind", false)?;
        tx.commit()?;
        Ok(entries.len())
    }

    /// Become the primary at the term `leader` holds (it must be newer than every epoch this
    /// node has seen). Catch up first: entries the old primary had but this node hasn't seen
    /// are not part of the new history.
    pub fn promote(self, leader: Arc<dyn Leadership>) -> Result<Primary> {
        self.try_promote(leader).map_err(|(_, e)| e)
    }

    /// [`promote`](Self::promote), handing the standby back when it fails.
    #[allow(clippy::result_large_err)] // the standby is handed back on failure
    pub fn try_promote(self, leader: Arc<dyn Leadership>) -> std::result::Result<Primary, (Standby, ReplError)> {
        let newest = match self.fence() {
            Ok(f) => f,
            Err(e) => return Err((self, e)),
        };
        match leader.held() {
            Some(term) if term > newest => {}
            Some(term) => return Err((self, ReplError::Fenced { ours: term, newer: newest })),
            None => return Err((self, ReplError::NotLeader)),
        }
        Primary::become_primary(self.conn, None, leader).map_err(|(conn, e)| (Standby { conn }, e))
    }

    /// The connection back.
    pub fn into_connection(self) -> Connection {
        self.conn
    }
}

impl Source for Standby {
    fn call(&self, request: &Request) -> Result<Reply> {
        serve(&self.conn, request)
    }
}

/// Binary search for the last entry two histories share (hash chains agree up to a point and
/// differ after it). Drive it with [`next_probe`](Self::next_probe) and
/// [`answer`](Self::answer), over any transport.
#[derive(Debug, Clone)]
pub struct ForkSearch {
    lo: u64,
    hi: u64,
    base_checked: bool,
}

impl ForkSearch {
    /// Search between this node's log (`conn`) and a source whose head is `theirs`.
    pub fn new(conn: &Connection, theirs: &Head) -> Result<Self> {
        let st = state(conn)?;
        let ours = head(conn)?;
        Ok(ForkSearch { lo: st.base_seq, hi: ours.seq.min(theirs.seq), base_checked: false })
    }

    /// The next entry to ask the source's hash for, or `None` when done.
    pub fn next_probe(&self) -> Option<u64> {
        if !self.base_checked {
            return Some(self.lo);
        }
        (self.lo < self.hi).then(|| self.lo + (self.hi - self.lo).div_ceil(2))
    }

    /// The source's hash at `seq`.
    pub fn answer(&mut self, conn: &Connection, seq: u64, theirs: Option<Digest>) -> Result<()> {
        let ours = hash_at(conn, seq)?;
        let same = ours.is_some() && ours == theirs;
        if !self.base_checked {
            if !same {
                return Err(ReplError::Trimmed { base: seq });
            }
            self.base_checked = true;
        } else if same {
            self.lo = seq;
        } else {
            self.hi = seq - 1;
        }
        Ok(())
    }

    /// The last shared entry, once [`next_probe`](Self::next_probe) is `None`.
    pub fn fork(&self) -> Result<u64> {
        if self.next_probe().is_some() {
            return Err(ReplError::Source("fork search not finished".into()));
        }
        Ok(self.lo)
    }
}

/// A node's role after a [`Replica::tick`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Primary { epoch: u64 },
    Standby { head: Head },
}

enum Inner {
    Primary(Primary),
    Standby(Standby),
}

/// A node that changes role with its leadership: automatic failover.
pub struct Replica {
    inner: Option<Inner>,
    leader: Arc<dyn Leadership>,
}

impl std::fmt::Debug for Replica {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replica").field("role", &self.role().ok()).finish_non_exhaustive()
    }
}

impl Replica {
    /// Start as a standby; [`tick`](Self::tick) promotes it while it holds leadership.
    pub fn open(conn: Connection, node: &str, leader: Arc<dyn Leadership>) -> Result<Self> {
        Ok(Replica { inner: Some(Inner::Standby(Standby::open(conn, node)?)), leader })
    }

    fn inner(&self) -> &Inner {
        self.inner.as_ref().expect("replica role")
    }

    /// The current role.
    pub fn role(&self) -> Result<Role> {
        Ok(match self.inner() {
            Inner::Primary(p) => Role::Primary { epoch: p.epoch },
            Inner::Standby(s) => Role::Standby { head: s.head()? },
        })
    }

    /// The primary, while this node is one.
    pub fn primary(&mut self) -> Option<&mut Primary> {
        match self.inner.as_mut() {
            Some(Inner::Primary(p)) => Some(p),
            _ => None,
        }
    }

    /// The standby, while this node is one.
    pub fn standby(&mut self) -> Option<&mut Standby> {
        match self.inner.as_mut() {
            Some(Inner::Standby(s)) => Some(s),
            _ => None,
        }
    }

    /// The connection, for reads.
    pub fn connection(&self) -> &Connection {
        match self.inner() {
            Inner::Primary(p) => &p.conn,
            Inner::Standby(s) => &s.conn,
        }
    }

    /// One step: a primary without its term steps down; a standby that holds a term newer
    /// than every epoch it has seen catches up from `source` if it can and promotes itself;
    /// otherwise a standby follows `source` (rejoining after a divergence). Call it on a
    /// timer, faster than the lease runs out.
    pub fn tick(&mut self, source: Option<&dyn Source>) -> Result<Role> {
        let held = self.leader.held();
        let inner = self.inner.take().expect("replica role");
        let (next, result) = match inner {
            Inner::Primary(p) => {
                if held == Some(p.epoch) && p.check().is_ok() {
                    (Inner::Primary(p), Ok(()))
                } else {
                    (Inner::Standby(p.step_down()), Ok(()))
                }
            }
            Inner::Standby(mut s) => match held {
                Some(term) if s.fence().map(|f| term > f).unwrap_or(false) => {
                    if let Some(src) = source {
                        let _ = s.catch_up(src);
                    }
                    match s.try_promote(self.leader.clone()) {
                        Ok(p) => (Inner::Primary(p), Ok(())),
                        Err((s, e)) => (Inner::Standby(s), Err(e)),
                    }
                }
                _ => {
                    let r = match source {
                        Some(src) => match s.catch_up(src) {
                            Err(ReplError::Diverged { .. }) => s.rejoin(src).map(|_| ()),
                            Err(ReplError::StaleSource { .. }) => Ok(()),
                            other => other.map(|_| ()),
                        },
                        None => Ok(()),
                    };
                    (Inner::Standby(s), r)
                }
            },
        };
        self.inner = Some(next);
        result?;
        self.role()
    }
}

impl Source for Replica {
    fn call(&self, request: &Request) -> Result<Reply> {
        serve(self.connection(), request)
    }
}
