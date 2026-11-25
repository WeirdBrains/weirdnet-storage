# Handoff

Running notes for [weirdnet-storage](https://github.com/WeirdBrains/weirdnet-storage), newest first.

## 2026-11-25: v0.1.2, replication, failover and retention release

- What changed: v0.1.2 publishes the replication, hot standby, failover and retention features (see the entry below) as an additive patch on the 0.1 line, so `^0.1` dependents can enable them without a new release of their own.
- Commits: `c5639a4` (main, CI green on Linux, macOS, Windows, Android and iOS) and the `release: v0.1.2` commit `aee0fb9`, tagged [v0.1.2](https://github.com/WeirdBrains/weirdnet-storage/releases/tag/v0.1.2). On [crates.io](https://crates.io/crates/weirdnet-storage/0.1.2); docs on [docs.rs](https://docs.rs/weirdnet-storage/0.1.2).
- Checks: the release script's fmt, clippy, test and doc checks with all features, then `cargo package` and publish.
- Not tested: as in the entry below.
- Left: nothing for this release.
- Next: the weirdnet-storage page and guide on [net.weirdbrains.com](https://net.weirdbrains.com/crates/storage).

## 2026-11-25: replication, hot standby, failover and retention

- What changed: four new optional features. `replication`: a primary and hot standbys kept current from an append-only, hash-chained log written in the same transaction as the data (SQLite's session extension), epochs that fence an old primary, `Replica::tick` for automatic failover and stepping down, and fail-back that rewinds a returning node's unreplicated entries and keeps them. `replication-service`: the log as a weirdnet-service service with async `follow` and `rejoin`. `lease`: weirdnet-sync's majority-vote `Elector` as the leadership and epoch source. `retention`: minimum-age policies, a hash-chained seal ledger, `purge_expired`, `verify`, encrypted archives and a drop/alter guard. Defaults and existing APIs are unchanged; the new error type is `replicate::ReplError`. Release script vendored (weirdnet-project-helpers 0.4.0) with `release.toml`.
- Commits: `6383e6a` (vendor), `e4c5799`, `157f527` (replication), `9b36255`, `4718025` (retention), `142d321` (tests), `d774a72` (docs and CI), merged from `feature/replication-retention`.
- Checks: cargo fmt; clippy with default, all, no default features and each new feature on its own; tests with default, all, no default and `sync` features, including a failover test between three in-process nodes (promotion, fencing of the old primary, rejoin with rewind, fail-back), a standby following over in-memory weirdnet-service sessions behind an `Authorize` hook, and retention (refused deletes and edits, policy strengthening, tamper evidence, purge, encrypted archives, guard, replicated retention); `cargo doc` with `-D warnings`; `cargo package`. CI builds the new features for Android and iOS and tests them on macOS and Windows.
- Not tested: replication over real weirdnet-net connections and `RosterAuth` (the service test uses in-memory channels and a stand-in hook); the lease adapter against real weirdnet-sync voters (compile check only); large databases and long logs; behavior under concurrent writers on one primary from several processes.
- Left: replication is asynchronous; a new standby after `trim_log` needs a snapshot copied out of band (no snapshot transfer over the network).
- Next: released as v0.1.2 (entry above).

## 2026-11-21: v0.1.1, public-content patch release

- What changed: v0.1.1 republishes the 0.1 line with the public-content rules applied (Weird Brains LLC as the copyright holder, m3talsmith as the author, no machine details), on top of `6133126`. No code or API changes.
- Commits: `6133126` and the `release: v0.1.1` commit tagged [v0.1.1](https://github.com/WeirdBrains/weirdnet-storage/releases/tag/v0.1.1).
- Checks: cargo fmt, clippy, test and package; the packaged crate was scanned for private terms before publishing.
- Not tested: nothing beyond the crate's own test suite.
- Left: earlier versions on this line are yanked on [crates.io](https://crates.io/crates/weirdnet-storage/versions).
- Next: nothing for this release.

