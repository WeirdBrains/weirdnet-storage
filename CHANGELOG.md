# Changelog

All notable changes to weirdnet-storage are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

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
