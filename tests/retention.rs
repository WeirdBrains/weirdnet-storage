//! Retention policies, the seal ledger, purges, archives and the guard.
use weirdnet_storage::retention::{self, Period, Policy};
use weirdnet_storage::{StorageError, open};

const DAY: i64 = 86_400_000;

fn now() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64
}

fn setup(dir: &std::path::Path) -> rusqlite::Connection {
    let c = open(&dir.join("records.db"), Some(&[4; 32]), "records").unwrap();
    c.execute_batch(
        "CREATE TABLE invoices (id INTEGER PRIMARY KEY, at INTEGER, amount REAL NOT NULL);
         CREATE TABLE notes (id INTEGER PRIMARY KEY, at_s INTEGER, body TEXT NOT NULL);",
    )
    .unwrap();
    retention::protect(&c, &Policy::new("invoices", "at", Period::years(7)).immutable()).unwrap();
    retention::protect(&c, &Policy::new("notes", "at_s", Period::days(30)).seconds()).unwrap();
    let old = now() - 8 * 366 * DAY;
    c.execute("INSERT INTO invoices VALUES (1, ?1, 1.0), (2, ?2, 2.0), (3, NULL, 3.0)", [old, now()]).unwrap();
    c.execute("INSERT INTO notes VALUES (1, ?1, 'old'), (2, ?2, 'new')", [(now() - 40 * DAY) / 1000, now() / 1000]).unwrap();
    c
}

fn refused(r: rusqlite::Result<usize>) -> bool {
    matches!(r, Err(e) if e.to_string().contains("kept for at least"))
}

#[test]
fn young_records_cannot_be_deleted_or_changed() {
    let t = tempfile::tempdir().unwrap();
    let c = setup(t.path());
    assert_eq!(Period::years(7).as_days(), 2557);
    assert!(refused(c.execute("DELETE FROM invoices WHERE id = 2", [])));
    assert!(refused(c.execute("DELETE FROM invoices WHERE id = 3", [])), "a row without a time is kept");
    assert!(refused(c.execute("UPDATE invoices SET amount = 9 WHERE id = 2", [])), "immutable");
    assert!(refused(c.execute("DELETE FROM invoices", [])), "a bulk delete stops at the first young row");
    assert_eq!(c.query_row("SELECT COUNT(*) FROM invoices", [], |r| r.get::<_, i64>(0)).unwrap(), 3);
    // Not immutable: updates are fine; seconds are handled.
    c.execute("UPDATE notes SET body = 'edited' WHERE id = 2", []).unwrap();
    assert!(refused(c.execute("DELETE FROM notes WHERE id = 2", [])));
    assert_eq!(c.execute("DELETE FROM notes WHERE id = 1", []).unwrap(), 1, "past 30 days");
}

#[test]
fn policies_only_get_stronger() {
    let t = tempfile::tempdir().unwrap();
    let c = setup(t.path());
    assert!(retention::protect(&c, &Policy::new("invoices", "at", Period::years(1))).is_err());
    assert!(retention::protect(&c, &Policy::new("invoices", "at", Period::years(7))).is_err(), "can't drop immutable");
    assert!(c.execute("DELETE FROM _weirdnet_retention", []).is_err());
    assert!(c.execute("UPDATE _weirdnet_retention SET min_age_ms = 1", []).is_err());
    retention::protect(&c, &Policy::new("notes", "at_s", Period::days(90)).seconds()).unwrap();
    assert_eq!(retention::policy(&c, "notes").unwrap().unwrap().min_age, Period::days(90));
    // The 40-day-old note is now kept.
    c.execute("INSERT INTO notes VALUES (3, ?1, 'x')", [(now() - 40 * DAY) / 1000]).unwrap();
    assert!(refused(c.execute("DELETE FROM notes WHERE id = 3", [])));
    assert!(retention::protect(&c, &Policy::new("missing", "at", Period::days(1))).is_err());
    assert!(retention::protect(&c, &Policy::new("notes", "nope", Period::days(1))).is_err());
}

#[test]
fn seals_make_changes_evident() {
    let t = tempfile::tempdir().unwrap();
    let c = setup(t.path());
    assert_eq!(retention::seal_all(&c).unwrap(), 5);
    assert_eq!(retention::seal_all(&c).unwrap(), 0);
    let report = retention::verify(&c).unwrap();
    assert!(report.is_intact(), "{report:?}");
    let head = retention::head(&c).unwrap().unwrap();
    assert_eq!(head.seq, 2);

    // A mutable change is reported until it is sealed.
    c.execute("UPDATE notes SET body = 'edited' WHERE id = 2", []).unwrap();
    let r = retention::verify(&c).unwrap();
    assert!(r.is_intact());
    assert_eq!(r.tables.iter().find(|t| t.table == "notes").unwrap().changed, vec![2]);
    assert_eq!(retention::seal(&c, "notes").unwrap(), 1);

    // The old invoice is past its seven years, so the trigger lets an edit through: the seal shows it.
    c.execute("UPDATE invoices SET amount = 100 WHERE id = 1", []).unwrap();
    let r = retention::verify(&c).unwrap();
    assert!(!r.is_intact());
    assert_eq!(r.tables.iter().find(|t| t.table == "invoices").unwrap().changed, vec![1]);
    assert!(retention::seal(&c, "invoices").is_err(), "an immutable row that changed is not re-sealed");
    c.execute("UPDATE invoices SET amount = 1.0 WHERE id = 1", []).unwrap();
    assert!(retention::verify(&c).unwrap().is_intact());

    // Deleting it outside a purge leaves it missing.
    c.execute("DELETE FROM invoices WHERE id = 1", []).unwrap();
    assert_eq!(retention::verify(&c).unwrap().tables.iter().find(|t| t.table == "invoices").unwrap().missing, vec![1]);

    // The ledger itself is append-only, and a forced edit breaks the chain.
    assert!(c.execute("UPDATE _weirdnet_seal SET rows = 0", []).is_err());
    assert!(c.execute("DELETE FROM _weirdnet_seal_rows", []).is_err());
    c.execute("INSERT INTO _weirdnet_guard VALUES ('test', 1) ON CONFLICT(name) DO UPDATE SET value = 1", []).unwrap();
    c.execute("DELETE FROM _weirdnet_seal_rows WHERE seal = 1 AND rid = 2", []).unwrap();
    c.execute("UPDATE _weirdnet_guard SET value = 0", []).unwrap();
    assert_eq!(retention::verify(&c).unwrap().broken_at, Some(1));
}

#[test]
fn purges_take_only_expired_rows() {
    let t = tempfile::tempdir().unwrap();
    let c = setup(t.path());
    assert_eq!(retention::purge_expired(&c, "invoices").unwrap(), 1);
    assert_eq!(retention::purge_expired(&c, "notes").unwrap(), 1);
    assert_eq!(retention::purge_expired(&c, "invoices").unwrap(), 0);
    let ids: Vec<i64> = c.prepare("SELECT id FROM invoices ORDER BY id").unwrap().query_map([], |r| r.get(0)).unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(ids, vec![2, 3]);
    let r = retention::verify(&c).unwrap();
    assert!(r.is_intact(), "{r:?}");
    assert!(r.tables.iter().all(|t| t.missing.is_empty()));
}

#[test]
fn archives_are_encrypted_checked_and_kept() {
    let t = tempfile::tempdir().unwrap();
    let c = setup(t.path());
    let path = t.path().join("cold").join("invoices-archive.db");
    let cut = now() - 365 * DAY;
    assert_eq!(retention::archive_to(&c, "invoices", cut, &path, Some(&[9; 32])).unwrap(), 1);
    assert_eq!(retention::archive_to(&c, "invoices", now() + DAY, &path, Some(&[9; 32])).unwrap(), 1, "only the new row");
    assert_ne!(&std::fs::read(&path).unwrap()[..16], b"SQLite format 3\0");
    assert!(matches!(open(&path, Some(&[4; 32]), "archive"), Err(StorageError::WrongKey { .. })));
    let a = open(&path, Some(&[9; 32]), "archive").unwrap();
    let r = retention::verify_archive(&a).unwrap();
    assert!(r.is_intact() && r.tables[0].sealed == 2, "{r:?}");
    // The archive keeps the same policy.
    assert!(refused(a.execute("DELETE FROM invoices WHERE id = 2", [])));
    assert!(a.execute("DELETE FROM _weirdnet_archive", []).is_err());
    // An archived row edited later is caught.
    a.execute("UPDATE invoices SET amount = 7 WHERE id = 1", []).unwrap();
    assert_eq!(retention::verify_archive(&a).unwrap().tables[0].changed, vec![1]);
    // A source row that no longer matches its seal is refused.
    retention::seal_all(&c).unwrap();
    c.execute("UPDATE notes SET body = 'tampered' WHERE id = 1", []).unwrap();
    assert!(retention::archive_to(&c, "notes", now(), &t.path().join("notes.db"), Some(&[9; 32])).is_err());
}

#[test]
fn the_guard_refuses_drops_and_alters() {
    let t = tempfile::tempdir().unwrap();
    let c = setup(t.path());
    retention::guard(&c).unwrap();
    assert!(c.execute_batch("DROP TABLE invoices").is_err());
    assert!(c.execute_batch("ALTER TABLE invoices RENAME TO gone").is_err());
    assert!(c.execute_batch("DROP TABLE _weirdnet_seal").is_err());
    let trigger: String =
        c.query_row("SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = 'invoices' LIMIT 1", [], |r| r.get(0)).unwrap();
    assert!(c.execute_batch(&format!("DROP TRIGGER \"{trigger}\"")).is_err());
    c.execute_batch("CREATE TABLE scratch (x INTEGER); DROP TABLE scratch;").unwrap();
}

#[cfg(feature = "replication")]
#[test]
fn retention_replicates_with_the_data() {
    use std::sync::Arc;
    use weirdnet_storage::replicate::{Manual, Primary, ReplError, Standby};
    let t = tempfile::tempdir().unwrap();
    let pc = open(&t.path().join("p.db"), Some(&[1; 32]), "p").unwrap();
    let mut p = Primary::open(pc, "a", Arc::new(Manual::granted(1))).unwrap();
    p.schema("CREATE TABLE invoices (id INTEGER PRIMARY KEY, at INTEGER, amount REAL NOT NULL);").unwrap();
    let sql = retention::protect_sql(p.connection(), &Policy::new("invoices", "at", Period::years(7)).immutable(), 1_700_000_000_000).unwrap();
    p.schema(&sql).unwrap();
    let old = now() - 8 * 366 * DAY;
    p.write(|tx| -> Result<(), ReplError> {
        tx.execute("INSERT INTO invoices VALUES (1, ?1, 1.0), (2, ?2, 2.0)", [old, now()])?;
        retention::seal(tx, "invoices")?;
        Ok(())
    })
    .unwrap();
    p.write(|tx| -> Result<usize, ReplError> { Ok(retention::purge_expired(tx, "invoices")?) }).unwrap();
    let mut s = Standby::open(open(&t.path().join("s.db"), Some(&[2; 32]), "s").unwrap(), "b").unwrap();
    s.catch_up(&p).unwrap();
    let r = retention::verify(s.connection()).unwrap();
    assert!(r.is_intact(), "{r:?}");
    assert_eq!(retention::head(s.connection()).unwrap(), retention::head(p.connection()).unwrap());
    assert!(refused(s.connection().execute("DELETE FROM invoices WHERE id = 2", [])), "the standby enforces it too");
    // A young row can't be deleted through the primary either.
    assert!(p.write(|tx| -> Result<usize, ReplError> { Ok(tx.execute("DELETE FROM invoices WHERE id = 2", [])?) }).is_err());
}
