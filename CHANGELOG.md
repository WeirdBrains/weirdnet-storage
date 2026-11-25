# Changelog

All notable changes to weirdnet-storage are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `replication` feature: a primary and hot standbys kept current from an append-only,
  hash-chained log written in the same transaction as the data; epochs that fence an old
  primary; `Replica::tick` for automatic failover and stepping down; fail-back that rewinds a
  returning node's unreplicated entries and keeps them.
- `replication-service` feature: `ReplicationService` and async `follow`/`rejoin` over
  weirdnet-service sessions (linked devices or roster grants with `RosterAuth`).
- `lease` feature: weirdnet-sync's majority-vote lease as the leadership and epoch source.
- `retention` feature: minimum-age policies that refuse earlier deletes and edits, a
  hash-chained seal ledger, `purge_expired`, `verify`, encrypted archives with
  `archive_to`/`verify_archive`, and a `guard` against drops and alters.
- `REPLAYING`, a trigger condition for app triggers that should not run on replay.
- A vendored release script (`scripts/weirdnet_release.py`) and `release.toml`.

Nothing changes for existing users: the new code is behind new features, and the default
features are unchanged.

## [0.1.1] - 2026-11-21

### Changed

- Package metadata and docs follow the WeirdBrains public-content rules (Weird Brains LLC as the copyright holder, m3talsmith as the author, no machine details). No code or API changes.

### Added

- A `vendored-openssl` feature (default); without it SQLCipher uses CommonCrypto on Apple
  targets and the system libcrypto elsewhere.
- CI builds for Android and iOS, tests on macOS and Windows, and a binary size report.

## [0.1.0] - 2026-11-11

### Added

- First release, extracted from Muninn: SQLCipher databases with a caller-supplied key,
  rekeying, migrations, savepoints, private files, snapshots and the sync change log.

[Unreleased]: https://github.com/WeirdBrains/weirdnet-storage/compare/5825c66...HEAD
[0.1.0]: https://crates.io/crates/weirdnet-storage/0.1.0
