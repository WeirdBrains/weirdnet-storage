# weirdnet-storage

General storage for SQLite databases encrypted with SQLCipher. The crate knows no schema
and no brand: callers bring their migrations, table registry and queries.

- `open`, `open_readonly`, `open_in_memory`: open with a raw 32-byte key the caller hands in,
  verify it (wrong key is `StorageError::WrongKey`), private folder (0700) and file (0600).
- `rekey`: re-encrypt in place; `encrypt_plaintext_in_place`: convert a plaintext file,
  verified before it replaces the original.
- `migrate`: an ordered list of migrations in one transaction, with a hook after each one.
- `savepoint`, `begin`/`release`/`rollback`: all-or-nothing units that nest.
- `files`: private folders, 0600 files written through a side file and renamed into place.
- `snapshot_to`: an online copy of an open database, keyed the same way.
- `sync` (default feature): a change log kept by triggers on the tables you register, row
  read and write by primary key, a row codec and hash, and the three-way merge decision.
- `replicate` (`replication` feature): a primary and hot standbys kept current from an
  append-only, hash-chained log, epochs that fence an old primary, manual or automatic
  failover, and fail-back.
- `retention` (`retention` feature): minimum-age policies (for example seven years) that
  refuse earlier deletes and edits, a hash-chained seal ledger, purges of expired rows only,
  verification, and encrypted archives for cold storage.

## Keys

The crate never derives, wraps, stores, recovers or exports a key, and never sees a
password. It applies the raw key it is given (`PRAGMA key`/`rekey`) and zeroizes the
literals it builds. Key management belongs to the caller (for example `weirdnet-keys`).
Opening with a key fails with `StorageError::NoCipher` if SQLite was built without SQLCipher,
so a key is never silently ignored.

## Replication and failover

`Primary::write` runs a transaction and records every row change it made (SQLite's session
extension) as one log entry in the same transaction, so the data and the log never disagree.
Schema changes go through `Primary::schema` and `Primary::migrate`. A `Standby` pulls
entries from a primary or another standby, checks the hash chain and applies each page in one
transaction; any conflict stops it rather than letting it drift.

```rust,ignore
use std::sync::Arc;
use weirdnet_storage::{open, replicate::{Manual, Primary, ReplError, Standby}};

let leader = Manual::granted(1);
let mut primary = Primary::open(open(&primary_path, Some(&primary_key), "orders")?, "node-a", Arc::new(leader))?;
primary.migrate(&["CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL NOT NULL);"], 1)?;
primary.write(|tx| -> Result<(), ReplError> {
    tx.execute("INSERT INTO orders (amount) VALUES (?1)", [2.5])?;
    Ok(())
})?;

let mut standby = Standby::open(open(&standby_path, Some(&standby_key), "orders")?, "node-b")?;
standby.catch_up(&primary)?;
```

- Fencing: every entry carries the epoch of the primary that wrote it. A standby accepts only
  the epoch it has applied (or an epoch entry that raises it) and refuses a source older than
  the newest epoch it has seen. A primary checks its `Leadership` before and after each
  write, so once its term is gone it can't commit.
- Failover: `Leadership` is a trait. `Manual` is for operator-driven failover; with the
  `lease` feature, [weirdnet-sync](https://crates.io/crates/weirdnet-sync)'s majority-vote
  `Elector` provides it, and the lease term becomes the epoch. `Replica::tick` promotes a
  standby that gains leadership and steps a primary down when it loses it.
- Fail-back: a returning ex-primary finds where it diverged (`ForkSearch`), rewinds its
  unreplicated entries (kept in `_weirdnet_rewound`) and follows the new primary.
- Over the network (`replication-service` feature): `ReplicationService` serves the log as
  a [weirdnet-service](https://crates.io/crates/weirdnet-service) service, and `follow` and
  `rejoin` drive a standby over a session. On
  [weirdnet-net](https://crates.io/crates/weirdnet-net) streams, weirdnet-service's
  `RosterAuth` admits linked devices or roster grants with the `storage.replicate`
  permission.

Every node has its own SQLCipher key; the log lives inside the encrypted database, and
entries travel inside encrypted sessions. Limits:

- Replication is asynchronous. A failover can lose the old primary's last unreplicated
  writes; that node rewinds them and keeps them on return.
- Replicated tables need a `PRIMARY KEY`. Schema SQL must be deterministic. A rewind past a
  schema change is refused; reseed that node from a snapshot instead.
- App triggers should be guarded with `WHEN NOT` `REPLAYING` so a standby doesn't run them
  twice (the rows they wrote are already in the replicated changes).
- After `Primary::trim_log`, a new standby starts from a `snapshot_to` copy, rekeyed with
  its own key.

## Retention

`retention::protect` stores a policy and adds triggers: deleting a row younger than the
minimum age aborts, and so does updating one in an immutable table. Policies can only be
strengthened.

```rust,ignore
use weirdnet_storage::retention::{self, Period, Policy};

retention::protect(&conn, &Policy::new("invoices", "at_ms", Period::years(7)).immutable())?;
retention::seal(&conn, "invoices")?;            // hash new rows into the ledger
assert!(retention::verify(&conn)?.is_intact());
retention::purge_expired(&conn, "invoices")?;   // only rows past seven years
retention::archive_to(&conn, "invoices", cutoff_ms, &archive_path, Some(&archive_key))?;
```

- `seal` hashes new rows (and changed rows of tables that aren't immutable) into an
  append-only, hash-chained ledger; `verify` recomputes the chain and every row and reports
  changed, missing and unsealed rows. `head` gives the chain head to publish or sign.
- `archive_to` copies sealed rows into a separate SQLCipher file with its own key, refusing
  rows that no longer match their seal; the archive keeps the policy, and `verify_archive`
  checks it later.
- `guard` makes a connection refuse `DROP` and `ALTER` of protected tables, their triggers
  and the internal tables.
- With replication, apply a policy through `Primary::schema(&protect_sql(..)?)` and seal
  inside `Primary::write`; the ledger and the policy reach every standby.

Triggers and the guard stop the app's own code and SQL. Someone holding the key can still
rewrite the file; `verify`, replicas and archives make that evident. Protected tables must be
rowid tables.

## Features

- `sync` (default): the change-log module; adds serde, serde_json, sha2 and base64.
- `vendored-openssl` (default): SQLCipher is bundled with OpenSSL built from source and
  linked statically, the same everywhere (Android has no system libcrypto for apps).
  Without it SQLCipher is still bundled but uses CommonCrypto on Apple targets (iOS and
  macOS, nothing extra linked in) and the system libcrypto elsewhere (`OPENSSL_DIR` or
  pkg-config). For iOS: `default-features = false, features = ["sync"]`, and set
  `IPHONEOS_DEPLOYMENT_TARGET` so the bundled C code and Rust are built for the same iOS.
- `replication`: the `replicate` module; adds SQLite's session extension, serde and sha2.
- `replication-service`: `replicate::service`; adds weirdnet-service (no default features).
- `lease`: `Leadership` for weirdnet-sync's `Elector`; adds weirdnet-sync and its networking
  dependencies.
- `retention`: the `retention` module; adds SQLite's authorizer hooks and sha2.

## Platforms and size

CI tests on Linux, macOS and Windows, builds for Android (aarch64, armv7, x86_64, API 26)
and iOS, and writes a size report to each run's summary: `examples/size_probe.rs` (open an
encrypted database, migrate, write and read a row) against an empty program, built with the
`size` profile (opt-level z, LTO, stripped). Run `scripts/size_report.sh [TARGET]
[--no-vendored]` for one row. On x86_64 Linux the vendored build adds about 5.4 MiB, the
system-crypto build about 1.1 MiB (SQLCipher itself), so most of it is OpenSSL's libcrypto.
