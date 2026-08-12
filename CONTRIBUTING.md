# Contributing to MatrixRaft

Thanks for your interest in improving MatrixRaft — the TemporalStore-owned Rust
Raft contract and readiness library.

## Ground rules

- MatrixRaft owns generic, portable Raft-facing contracts: request/response types,
  storage and transport traits, read/write safety decisions, metric names, and
  fail-closed production-readiness reports. Keep product-specific logic in the
  consumer, not here.
- **Never weaken a safety predicate to make a test pass.** Read-index, lease, and
  applied-index-fence rules encode linearizability guarantees. See
  [`docs/read_index_safety_review.md`](docs/read_index_safety_review.md) for the
  model and the invariants that must never regress.
- The crate is `#![forbid(unsafe_code)]`. Keep it that way.

## Panics and error handling

Shipped library code contains no `.unwrap()`; fallible operations return `Result`.
The only sanctioned panics are:

- lock-poison propagation (`.expect("... mutex poisoned")`), which surfaces a bug in
  another thread rather than masking it, and
- internal invariant assertions on data the same function just constructed (e.g. the
  single element of a single-group plan).

Do not add `.unwrap()` to library code, and prefer returning a `RaftError` over
`.expect()` for anything that depends on caller input.

## Development

```bash
cargo build --all-targets
cargo test
cargo fmt --all
cargo clippy --all-targets
cargo doc --no-deps
```

The Minimum Supported Rust Version (MSRV) is **1.82**. CI runs formatting, a
warnings-as-errors build, the test suite, doc generation, the MSRV check, and
`cargo-deny` (license + advisory) — all must pass.

## Pull requests

1. Keep changes focused, and add tests for any behavior change.
2. Ensure `cargo fmt --all -- --check`, `cargo test`, and `cargo doc --no-deps`
   are green.
3. Update [`CHANGELOG.md`](CHANGELOG.md) under **Unreleased**.
4. For any change to read/write/membership/read-index/lease/fence paths, describe
   the safety implications in the PR.

## Reporting issues

Use the issue templates. For security-sensitive reports, follow
[`SECURITY.md`](SECURITY.md) instead of opening a public issue.

## License

By contributing, you agree that your contributions are licensed under the
Apache-2.0 license (see [`LICENSE`](LICENSE)).
