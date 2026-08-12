# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Linearizable follower reads via leader-confirmed ReadIndex forwarding
  (`MatrixRaftMultiRaftServer::forwarded_read_index_on_node` and
  `forwarded_read_index_for_group`). A follower read is forwarded to the group
  leader for a quorum-confirmed read index and served as safe only once the
  follower has applied up to that index. See
  [`docs/read_index_safety_review.md`](docs/read_index_safety_review.md).
- Continuous integration: formatting, warnings-as-errors build, tests, docs, an
  MSRV (Rust 1.82) job, a `cargo-deny` license/advisory gate, a blocking Clippy
  gate, and an informational coverage report (`cargo-llvm-cov`, ~85% overall).
- Error-path tests for forwarded reads (unknown group / node fail closed).
- Release automation: a tag-triggered crates.io publish workflow (with tag/version
  verification), a `CODEOWNERS` file, and documented panic/error-handling
  conventions (no `.unwrap()` in shipped library code).
- Open-source project files: `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`,
  `SECURITY.md`, issue and pull-request templates, and Dependabot configuration.
- Published-crate metadata: declared MSRV, documentation/homepage links, packaging
  `exclude`, and docs.rs configuration.

### Changed

- Read-index fanout tests assert the honest per-role safety contract — the
  quorum-confirmed leader certifies the read as safe while not-yet-applied
  followers report bounded-stale status — instead of requiring every node to be
  safe.
- Moved lint configuration (Clippy allows, rustdoc broken-link deny) to the
  Cargo.toml `[lints]` table so it applies package-wide.

### Fixed

- Applied rustfmt formatting and resolved all Clippy lints across the crate.
  Clippy now runs as a **blocking** CI gate (`cargo clippy --all-targets -- -D warnings`).

## [0.1.0]

- Initial MatrixRaft contract and readiness library extracted for TemporalStore:
  request/response types, storage and transport traits, read/write safety
  decisions, metric names, and fail-closed production-readiness reports.
