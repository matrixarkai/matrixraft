// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The README's claim that some storage interfaces reach nothing has to stay
//! true.
//!
//! It is the same shape as `tests/settings_contract.rs` and exists for the
//! same reason: a host could implement a whole storage engine against one of
//! these traits and find the crate never calls it. Saying so is only useful
//! while it is accurate, and a trait getting wired up is exactly the change
//! that would quietly make the prose wrong.
//!
//! The list is read out of the README rather than copied here, so there is one
//! place for it to be right.

/// The traits the README says the engine does not drive.
fn traits_the_readme_calls_unwired() -> Vec<String> {
    let readme = include_str!("../README.md");
    let marker = "<!-- unwired-traits: ";
    let line = readme
        .lines()
        .find(|line| line.trim_start().starts_with(marker))
        .expect(
            "README has no `<!-- unwired-traits: ... -->` marker; the storage section \
             must name them in a form this test can read",
        );
    line.trim()
        .trim_start_matches(marker)
        .trim_end_matches("-->")
        .trim()
        .split(',')
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .collect()
}

/// The engine's sources, minus the file where these traits are declared and
/// where their in-memory implementations live.
fn engine_sources() -> Vec<(&'static str, &'static str)> {
    vec![
        ("src/lib.rs", include_str!("../src/lib.rs")),
        ("src/driver.rs", include_str!("../src/driver.rs")),
        ("src/applier.rs", include_str!("../src/applier.rs")),
        ("src/wal.rs", include_str!("../src/wal.rs")),
        ("src/fsm.rs", include_str!("../src/fsm.rs")),
        ("src/pipeline.rs", include_str!("../src/pipeline.rs")),
        ("src/membership.rs", include_str!("../src/membership.rs")),
        ("src/snapshot.rs", include_str!("../src/snapshot.rs")),
        ("src/mailbox.rs", include_str!("../src/mailbox.rs")),
        ("src/log_buffer.rs", include_str!("../src/log_buffer.rs")),
        ("src/read_safety.rs", include_str!("../src/read_safety.rs")),
        ("src/status.rs", include_str!("../src/status.rs")),
        ("src/metrics.rs", include_str!("../src/metrics.rs")),
        ("src/readiness.rs", include_str!("../src/readiness.rs")),
        (
            "src/facade/node_runtime.rs",
            include_str!("../src/facade/node_runtime.rs"),
        ),
        (
            "src/facade/cluster_runtime.rs",
            include_str!("../src/facade/cluster_runtime.rs"),
        ),
        (
            "src/facade/wal_runtime.rs",
            include_str!("../src/facade/wal_runtime.rs"),
        ),
        (
            "src/facade/snapshot_runtime.rs",
            include_str!("../src/facade/snapshot_runtime.rs"),
        ),
        (
            "src/facade/matrixraft_compat.rs",
            include_str!("../src/facade/matrixraft_compat.rs"),
        ),
    ]
}

/// Code with line comments removed, so a doc comment naming a trait is not
/// mistaken for a use of it.
fn code_only(source: &str) -> String {
    source
        .lines()
        .map(|line| match line.find("//") {
            Some(at) => &line[..at],
            None => line,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Whether `code` actually *uses* `name` as a trait, rather than mentioning
/// it.
///
/// The forms that count: an implementation, a `dyn`, an `impl Trait`
/// position, and a bound after a colon or a plus.
///
/// **The distinction this turns on is a path versus a bound**, and getting it
/// right needs more than looking at the character before the name:
///
/// - `RaftError::Storage(..)` is a path. The first version of the scan behind
///   this test counted `::` as a bound, so every one of the dozens of those in
///   the crate read as a use of the `Storage` trait and the surface came back
///   thoroughly wired.
/// - `fn f<S: crate::storage::Storage>()` is a **bound**, and also ends in
///   `::`. Skipping everything after `::` would have missed it -- which is how
///   the fix for the first mistake becomes the second one.
///
/// So this walks back over any path prefix to where the path starts, and asks
/// what precedes *that*. A `:` or `+` there is a bound however long the path
/// is; a `(`, a `,` or whitespace is not.
fn uses_as_trait(code: &str, name: &str) -> bool {
    let bytes = code.as_bytes();
    let is_ident = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_';
    let mut from = 0;
    while let Some(at) = code[from..].find(name) {
        let start = from + at;
        let end = start + name.len();
        from = end;
        // A longer identifier that merely begins with the name.
        if bytes.get(end).is_some_and(|byte| is_ident(*byte)) {
            continue;
        }
        // An identifier that merely ENDS with the name, e.g. `MyStorage`.
        if start > 0 && is_ident(bytes[start - 1]) {
            continue;
        }
        // Walk back over `a::b::` so a path-qualified bound is still a bound.
        let mut path_start = start;
        loop {
            let head = code[..path_start].trim_end();
            if !head.ends_with("::") {
                break;
            }
            let mut cut = head.len() - 2;
            while cut > 0 && is_ident(bytes[cut - 1]) {
                cut -= 1;
            }
            if cut == path_start {
                break;
            }
            path_start = cut;
        }
        let before = code[..path_start].trim_end();
        // No `!ends_with("::")` guard here, deliberately: the walk above has
        // already consumed any path prefix, so `before` cannot end in `::`. A
        // plant that removed such a guard changed nothing, which is how it was
        // found to be dead. The walk is what tells a path from a bound, so the
        // plant that matters is removing the walk.
        let is_bound = before.ends_with(':');
        let is_plus_bound = before.ends_with('+');
        let is_impl = before.ends_with("impl") || before.ends_with("dyn");
        let is_impl_for = code[end..].trim_start().starts_with("for ");
        if is_bound || is_plus_bound || is_impl || is_impl_for {
            return true;
        }
    }
    false
}

#[test]
fn the_storage_traits_the_readme_calls_unwired_really_are() {
    let claimed = traits_the_readme_calls_unwired();
    assert!(
        !claimed.is_empty(),
        "the README's unwired-traits marker is empty, so this test would pass \
         whatever the code did"
    );

    let mut wired = Vec::new();
    for name in &claimed {
        for (path, source) in engine_sources() {
            if uses_as_trait(&code_only(source), name) {
                wired.push(format!("{name} is used as a trait in {path}"));
            }
        }
    }
    assert!(
        wired.is_empty(),
        "the README says the engine does not drive these storage interfaces, \
         but it does:\n  {}\nWire-up is good news; the README and the trait \
         doc comments need updating with it.",
        wired.join("\n  ")
    );
}

#[test]
fn every_trait_the_readme_calls_unwired_is_a_real_one() {
    // The other direction: a name that no longer exists, or never did, would
    // make the test above pass by describing nothing.
    let storage = include_str!("../src/storage.rs");
    for name in &traits_the_readme_calls_unwired() {
        assert!(
            storage.contains(&format!("pub trait {name} ")),
            "the README calls `{name}` an unwired storage interface, but no such \
             trait is declared in src/storage.rs"
        );
    }
}

#[test]
fn the_scan_can_tell_a_path_from_a_bound() {
    // The test above is only worth anything if its matcher works, and its
    // first version did not: `RaftError::Storage(..)` read as a bound. These
    // are the cases that distinction turns on.
    assert!(
        !uses_as_trait("Err(RaftError::Storage(\"x\".to_string()))", "Storage"),
        "a path must not read as a bound"
    );
    assert!(
        !uses_as_trait("let s = StorageApplyFence::default();", "Storage"),
        "a longer identifier must not read as the trait"
    );
    assert!(
        uses_as_trait("fn hold<S: Storage>(store: S) {}", "Storage"),
        "a bound is a use"
    );
    assert!(
        uses_as_trait("fn hold(store: &dyn Storage) {}", "Storage"),
        "a dyn is a use"
    );
    assert!(
        uses_as_trait("impl Storage for Mine {}", "Storage"),
        "an implementation is a use"
    );
    assert!(
        uses_as_trait("fn hold(store: impl Storage) {}", "Storage"),
        "an impl-argument is a use"
    );
    assert!(
        uses_as_trait("fn hold<S: Send + Storage>(store: S) {}", "Storage"),
        "a bound after a plus is a use"
    );
    assert!(
        uses_as_trait(
            "fn hold<S: crate::storage::Storage>(store: S) {}",
            "Storage"
        ),
        "a PATH-QUALIFIED bound is still a bound -- the obvious fix for the \
         path-versus-bound mistake is to skip everything after `::`, which \
         misses exactly this"
    );
    assert!(
        uses_as_trait("fn hold(store: &dyn crate::storage::Storage) {}", "Storage"),
        "and so is a path-qualified dyn"
    );
    assert!(
        !uses_as_trait("let x = some::other::Storage;", "Storage"),
        "a path in an expression is still not a bound"
    );
    assert!(
        !uses_as_trait("struct MyStorage;", "Storage"),
        "an identifier that merely ends with the name is not the trait"
    );
    // And a doc comment naming it is not.
    assert!(
        !uses_as_trait(
            &code_only("/// See [`Storage`] for the interface.\nfn f() {}"),
            "Storage"
        ),
        "a doc comment must not read as a use"
    );
}
