//! What an app pays in binary size for weirdnet-storage: opens an encrypted database, runs a
//! migration, writes and reads a row. `scripts/size_report.sh` builds it next to
//! `size_baseline` (a program that does nothing) and reports the difference per target.
fn main() {
    let dir = std::env::temp_dir().join(format!("wn-size-{}", std::process::id()));
    let path = dir.join("probe.db");
    let key = [7u8; 32];
    let mut conn = weirdnet_storage::open(&path, Some(&key), "probe").expect("open");
    weirdnet_storage::migrate::<weirdnet_storage::StorageError>(&mut conn, &["CREATE TABLE t (k TEXT PRIMARY KEY, v TEXT)"], 1, |_, _| Ok(()))
        .expect("migrate");
    conn.execute("INSERT OR REPLACE INTO t VALUES ('a', 'b')", []).expect("insert");
    let v: String = conn.query_row("SELECT v FROM t WHERE k = 'a'", [], |r| r.get(0)).expect("select");
    println!("cipher {} value {v}", weirdnet_storage::has_cipher(&conn));
    drop(conn);
    let _ = std::fs::remove_dir_all(dir);
}
