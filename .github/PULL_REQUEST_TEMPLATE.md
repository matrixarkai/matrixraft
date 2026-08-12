## Summary

<!-- What does this change, and why? -->

## Safety impact

<!--
Does this touch read/write, membership, read-index, lease, or applied-index-fence
paths? If so, describe the linearizability/safety implications. If not, write "none".
-->

## Checklist

- [ ] `cargo fmt --all -- --check` is clean
- [ ] `cargo test` passes
- [ ] `cargo clippy --all-targets` reviewed
- [ ] `cargo doc --no-deps` builds
- [ ] Added or updated tests for behavior changes
- [ ] Updated `CHANGELOG.md` (Unreleased)
- [ ] No safety predicate was weakened to make a test pass
