//! Primary, standbys, epochs and failover between in-process nodes, each on its own
//! encrypted file and key.
use std::path::Path;
use std::sync::{Arc, Mutex};
use weirdnet_storage::replicate::{Kind, Leadership, Manual, Primary, ReplError, Replica, Reply, Request, Role, Source, Standby};
use weirdnet_storage::{REPLAYING, StorageError, open, snapshot_to};

const MIGRATIONS: &[&str] = &[
    "CREATE TABLE orders (id INTEGER PRIMARY KEY, product TEXT NOT NULL, size REAL NOT NULL, state TEXT NOT NULL);",
    "CREATE TABLE audit (id INTEGER PRIMARY KEY AUTOINCREMENT, order_id INTEGER NOT NULL);",
];

fn audit_trigger() -> String {
    format!("CREATE TRIGGER orders_audit AFTER INSERT ON orders WHEN NOT {REPLAYING} BEGIN INSERT INTO audit (order_id) VALUES (new.id); END;")
}

fn key(n: u8) -> [u8; 32] {
    [n; 32]
}

fn db(dir: &Path, name: &str, k: u8) -> rusqlite::Connection {
    open(&dir.join(format!("{name}.db")), Some(&key(k)), name).unwrap()
}

fn rows(conn: &rusqlite::Connection) -> Vec<(i64, String, f64, String)> {
    let mut s = conn.prepare("SELECT id, product, size, state FROM orders ORDER BY id").unwrap();
    s.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))).unwrap().map(|r| r.unwrap()).collect()
}

fn count(conn: &rusqlite::Connection, table: &str) -> i64 {
    conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0)).unwrap()
}

fn insert(p: &mut Primary, id: i64, product: &str) {
    p.write(|tx| -> Result<(), ReplError> {
        tx.execute("INSERT INTO orders VALUES (?1, ?2, 1.5, 'new')", rusqlite::params![id, product])?;
        Ok(())
    })
    .unwrap();
}

fn primary(dir: &Path) -> Primary {
    let mut p = Primary::open(db(dir, "primary", 1), "node-a", Arc::new(Manual::granted(1))).unwrap();
    p.migrate(MIGRATIONS, MIGRATIONS.len()).unwrap();
    p.schema(&audit_trigger()).unwrap();
    p
}

#[test]
fn a_standby_follows_the_primary_on_its_own_key() {
    let t = tempfile::tempdir().unwrap();
    let mut p = primary(t.path());
    insert(&mut p, 1, "apples");
    insert(&mut p, 2, "pears");
    p.write(|tx| -> Result<(), ReplError> {
        tx.execute("UPDATE orders SET state = 'shipped' WHERE id = 1", [])?;
        tx.execute("DELETE FROM orders WHERE id = 2", [])?;
        tx.execute("INSERT INTO orders VALUES (3, 'plums', 2.0, 'new')", [])?;
        Ok(())
    })
    .unwrap();
    // A write that changes nothing adds no entry.
    let before = p.head().unwrap();
    p.write(|tx| -> Result<(), ReplError> {
        tx.execute("UPDATE orders SET state = state WHERE id = 99", [])?;
        Ok(())
    })
    .unwrap();
    assert_eq!(p.head().unwrap(), before);

    let mut s = Standby::open(db(t.path(), "standby", 2), "node-b").unwrap();
    let progress = s.catch_up(&p).unwrap();
    assert_eq!(progress.behind(), 0);
    assert_eq!(s.head().unwrap(), p.head().unwrap());
    assert_eq!(rows(s.connection()), rows(p.connection()));
    assert_eq!(rows(s.connection()), vec![(1, "apples".into(), 1.5, "shipped".into()), (3, "plums".into(), 2.0, "new".into())]);
    // The audit trigger ran once on the primary; its rows replicated and it stood aside on the standby.
    assert_eq!(count(p.connection(), "audit"), 3);
    assert_eq!(count(s.connection(), "audit"), 3);
    assert_eq!(weirdnet_storage::schema_version(s.connection()).unwrap(), 2);
    assert_eq!(s.verify().unwrap(), p.verify().unwrap());

    // Both files are encrypted, each with its own key.
    for name in ["primary", "standby"] {
        let head = std::fs::read(t.path().join(format!("{name}.db"))).unwrap();
        assert_ne!(&head[..16], b"SQLite format 3\0");
    }
    drop(s);
    assert!(matches!(open(&t.path().join("standby.db"), Some(&key(1)), "standby"), Err(StorageError::WrongKey { .. })));

    // Nothing new: an empty page.
    let mut s = Standby::open(db(t.path(), "standby", 2), "node-b").unwrap();
    assert_eq!(s.catch_up(&p).unwrap().applied, 0);
}

#[test]
fn tables_need_a_primary_key() {
    let t = tempfile::tempdir().unwrap();
    let mut p = Primary::open(db(t.path(), "p", 1), "a", Arc::new(Manual::granted(1))).unwrap();
    assert!(p.schema("CREATE TABLE loose (x INTEGER)").is_err());
    assert_eq!(count(p.connection(), "sqlite_master WHERE name = 'loose'"), 0);
}

#[test]
fn tampered_or_out_of_order_entries_are_refused() {
    let t = tempfile::tempdir().unwrap();
    let mut p = primary(t.path());
    insert(&mut p, 1, "apples");
    let mut s = Standby::open(db(t.path(), "s", 2), "b").unwrap();
    let Reply::Entries { mut entries, head } = p.call(&s.pull_request(100).unwrap()).unwrap() else { panic!("entries") };
    let last = entries.len() - 1;
    entries[last].body[0] ^= 1;
    let seq = entries[last].seq;
    assert!(matches!(s.apply(Reply::Entries { entries: entries.clone(), head }), Err(ReplError::Tampered { seq: n }) if n == seq));
    // Nothing from the refused page was applied.
    assert_eq!(s.head().unwrap().seq, 0);
    assert!(matches!(s.apply(Reply::Entries { entries: entries[1..].to_vec(), head }), Err(ReplError::Rejected { .. })));
}

/// One lease for the cluster: a holder and an ever-growing term, seen from each node.
#[derive(Default)]
struct Lease(Mutex<(Option<String>, u64)>);

impl Lease {
    fn give(&self, node: &str) -> u64 {
        let mut g = self.0.lock().unwrap();
        g.1 += 1;
        g.0 = Some(node.to_string());
        g.1
    }

    fn lapse(&self) {
        self.0.lock().unwrap().0 = None;
    }

    fn view(self: &Arc<Self>, node: &str) -> Arc<dyn Leadership> {
        let lease = self.clone();
        let node = node.to_string();
        Arc::new(move || {
            let g = lease.0.lock().unwrap();
            (g.0.as_deref() == Some(node.as_str())).then_some(g.1)
        })
    }
}

fn write_on(r: &mut Replica, id: i64, product: &str) -> Result<(), ReplError> {
    let p = r.primary().ok_or(ReplError::NotLeader)?;
    p.write(|tx| -> Result<(), ReplError> {
        tx.execute("INSERT INTO orders VALUES (?1, ?2, 1.0, 'new')", rusqlite::params![id, product])?;
        Ok(())
    })
}

#[test]
fn failover_fences_the_old_primary_and_it_rejoins() {
    let t = tempfile::tempdir().unwrap();
    let lease = Arc::new(Lease::default());
    let mut a = Replica::open(db(t.path(), "a", 1), "a", lease.view("a")).unwrap();
    let mut b = Replica::open(db(t.path(), "b", 2), "b", lease.view("b")).unwrap();
    let mut c = Replica::open(db(t.path(), "c", 3), "c", lease.view("c")).unwrap();

    // A takes the lease and becomes the primary at term 1.
    assert_eq!(lease.give("a"), 1);
    assert_eq!(a.tick(None).unwrap(), Role::Primary { epoch: 1 });
    {
        let p = a.primary().unwrap();
        p.migrate(MIGRATIONS, MIGRATIONS.len()).unwrap();
    }
    write_on(&mut a, 1, "apples").unwrap();
    write_on(&mut a, 2, "pears").unwrap();
    assert!(matches!(b.tick(Some(&a)).unwrap(), Role::Standby { .. }));
    assert!(matches!(c.tick(Some(&a)).unwrap(), Role::Standby { .. }));
    assert_eq!(rows(b.connection()), rows(a.connection()));

    // A writes once more, then goes quiet before anyone pulls it.
    write_on(&mut a, 3, "lost").unwrap();
    let a_head_before = a.role().unwrap();

    // The lease lapses and B wins it at term 2. A is unreachable, so B promotes with what it has.
    lease.lapse();
    assert_eq!(lease.give("b"), 2);
    assert_eq!(b.tick(None).unwrap(), Role::Primary { epoch: 2 });
    write_on(&mut b, 4, "plums").unwrap();
    assert!(matches!(c.tick(Some(&b)).unwrap(), Role::Standby { .. }));
    assert_eq!(rows(c.connection()).iter().map(|r| r.0).collect::<Vec<_>>(), vec![1, 2, 4]);

    // A has no term any more: its writes are refused and it steps down.
    assert!(matches!(write_on(&mut a, 5, "late"), Err(ReplError::NotLeader)));
    assert!(matches!(a.tick(None).unwrap(), Role::Standby { .. }));
    assert_ne!(a.role().unwrap(), a_head_before);

    // C has seen epoch 2, so A's epoch-1 log is refused as a source.
    let c_head = c.role().unwrap();
    assert!(matches!(c.standby().unwrap().catch_up(&a), Err(ReplError::StaleSource { epoch: 1, fence: 2 })));
    assert_eq!(c.tick(Some(&a)).unwrap(), c_head);
    assert_eq!(rows(c.connection()).iter().map(|r| r.0).collect::<Vec<_>>(), vec![1, 2, 4]);

    // A follows B: it diverged at its unreplicated write, rewinds it, keeps it, and catches up.
    assert!(matches!(a.tick(Some(&b)).unwrap(), Role::Standby { .. }));
    assert_eq!(rows(a.connection()), rows(b.connection()));
    assert_eq!(rows(a.connection()).iter().map(|r| r.0).collect::<Vec<_>>(), vec![1, 2, 4]);
    let rewound: Vec<i64> = {
        let mut s = a.connection().prepare("SELECT kind FROM _weirdnet_rewound ORDER BY seq").unwrap();
        s.query_map([], |r| r.get(0)).unwrap().map(|r| r.unwrap()).collect()
    };
    assert_eq!(rewound, vec![1], "the one unreplicated change is kept on A");

    // All three agree, and every chain checks out.
    let heads: Vec<_> = [&mut a, &mut b, &mut c].into_iter().map(|r| r.role().unwrap()).collect();
    let Role::Primary { epoch: 2 } = heads[1] else { panic!("B is the primary") };
    assert_eq!(heads[0], heads[2]);
    assert_eq!(a.standby().unwrap().verify().unwrap(), b.primary().unwrap().verify().unwrap());

    // Fail back on purpose: A takes the lease at term 3, B steps down and follows.
    assert_eq!(lease.give("a"), 3);
    assert_eq!(a.tick(Some(&b)).unwrap(), Role::Primary { epoch: 3 });
    assert!(matches!(b.tick(Some(&a)).unwrap(), Role::Standby { .. }));
    write_on(&mut a, 6, "ada").unwrap();
    assert!(matches!(b.tick(Some(&a)).unwrap(), Role::Standby { .. }));
    assert!(matches!(c.tick(Some(&a)).unwrap(), Role::Standby { .. }));
    assert_eq!(rows(b.connection()), rows(a.connection()));
    assert_eq!(rows(c.connection()), rows(a.connection()));
    assert!(matches!(write_on(&mut b, 7, "nope"), Err(ReplError::NotLeader)));
}

#[test]
fn a_stale_primary_is_fenced_by_a_newer_pull() {
    let t = tempfile::tempdir().unwrap();
    // A primary whose view of its lease is stale (a paused process still thinks it holds term 1).
    let mut p = primary(t.path());
    insert(&mut p, 1, "apples");
    let mut s = Standby::open(db(t.path(), "s", 2), "b").unwrap();
    s.catch_up(&p).unwrap();
    s.observe_epoch(2).unwrap();
    assert!(matches!(s.catch_up(&p), Err(ReplError::StaleSource { epoch: 1, fence: 2 })));
    // The pull told the primary that epoch 2 exists: it refuses to commit from now on.
    let err = p
        .write(|tx| -> Result<(), ReplError> {
            tx.execute("INSERT INTO orders VALUES (9, 'x', 1, 'new')", [])?;
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(err, ReplError::Fenced { ours: 1, newer: 2 }));
    assert_eq!(count(p.connection(), "orders"), 1);
    // And a manual promotion must use a term above every epoch seen.
    assert!(matches!(s.promote(Arc::new(Manual::granted(2))), Err(ReplError::Fenced { .. })));
}

#[test]
fn manual_failover_and_a_lost_term_stop_writes() {
    let t = tempfile::tempdir().unwrap();
    let lead = Manual::granted(1);
    let mut p = Primary::open(db(t.path(), "p", 1), "a", Arc::new(lead.clone())).unwrap();
    p.migrate(MIGRATIONS, 1).unwrap();
    insert(&mut p, 1, "apples");
    let mut s = Standby::open(db(t.path(), "s", 2), "b").unwrap();
    s.catch_up(&p).unwrap();
    lead.revoke();
    assert!(matches!(p.check(), Err(ReplError::NotLeader)));
    // The operator promotes the standby at term 2.
    let mut q = s.promote(Arc::new(Manual::granted(2))).unwrap();
    assert_eq!(q.epoch(), 2);
    insert(&mut q, 2, "pears");
    let entries = match q
        .call(&Request::Pull(weirdnet_storage::replicate::Pull {
            after_seq: 0,
            after_hash: weirdnet_storage::replicate::Digest::ZERO,
            fence: 0,
            max_entries: 100,
        }))
        .unwrap()
    {
        Reply::Entries { entries, .. } => entries,
        other => panic!("{other:?}"),
    };
    let epochs: Vec<(Kind, u64)> = entries.iter().map(|e| (e.kind, e.epoch)).collect();
    assert_eq!(epochs.first(), Some(&(Kind::Epoch, 1)));
    assert!(epochs.contains(&(Kind::Epoch, 2)));
}

#[test]
fn schema_changes_cannot_be_rewound() {
    let t = tempfile::tempdir().unwrap();
    let mut p = primary(t.path());
    insert(&mut p, 1, "apples");
    let fork = p.head().unwrap().seq;
    p.schema("CREATE TABLE extra (id INTEGER PRIMARY KEY)").unwrap();
    let mut s = p.step_down();
    assert!(matches!(s.rewind_to(fork), Err(ReplError::CannotRewind { .. })));
}

#[test]
fn a_trimmed_log_needs_a_snapshot() {
    let t = tempfile::tempdir().unwrap();
    let mut p = primary(t.path());
    for i in 1..=5 {
        insert(&mut p, i, "apples");
    }
    let head = p.head().unwrap();
    p.trim_log(head.seq - 1).unwrap();
    assert_eq!(p.verify().unwrap(), head);
    let mut fresh = Standby::open(db(t.path(), "fresh", 2), "b").unwrap();
    assert!(matches!(fresh.catch_up(&p), Err(ReplError::Trimmed { .. })));
    // Seed from an encrypted snapshot (the standby's own key), then follow.
    snapshot_to(p.connection(), &t.path().join("seed.db"), Some(&key(3))).unwrap();
    let mut seeded = Standby::open(open(&t.path().join("seed.db"), Some(&key(3)), "seed").unwrap(), "c").unwrap();
    insert(&mut p, 6, "pears");
    assert_eq!(seeded.catch_up(&p).unwrap().applied, 1);
    assert_eq!(rows(seeded.connection()), rows(p.connection()));
}

#[cfg(feature = "lease")]
#[test]
fn the_sync_lease_is_a_leadership_source() {
    fn leadership<T: weirdnet_storage::replicate::Leadership>() {}
    leadership::<weirdnet_sync::lease::Elector>();
}
