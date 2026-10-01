// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The README's list of settings that reach nothing has to stay true.
//!
//! It is the kind of claim that rots quietly: a setting gets wired up and
//! the prose still calls it inert, or a new one is added and never listed.
//! Both mislead a host in the direction of trusting a knob that does
//! nothing, which is the worse way round.
//!
//! It had already rotted. The list named four settings, two of which are
//! not fields of the same struct, and the sentence after it said "of them
//! only `flexible_apply` reaches an implementation" -- naming a setting
//! that is not in the list and that has twenty uses.
//!
//! So the list is read out of the README and checked against the source
//! rather than against a copy kept here, which would rot the same way.

/// Every setting the README calls recorded-only, read from the README.
///
/// Parsed rather than copied: a list kept in this file would go stale in
/// exactly the way this test exists to prevent.
fn settings_the_readme_calls_inert() -> Vec<String> {
    let readme = include_str!("../README.md");
    let marker = "<!-- inert-settings: ";
    let line = readme
        .lines()
        .find(|line| line.trim_start().starts_with(marker))
        .expect(
            "README has no `<!-- inert-settings: ... -->` marker; the settings section \
             must name them in a form this test can read",
        );
    let inside = line
        .trim()
        .trim_start_matches(marker)
        .trim_end_matches("-->")
        .trim();
    inside
        .split(',')
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .collect()
}

/// Whether `name` appears outside a comment.
///
/// Matching anywhere would fire on prose: a doc comment reading "unlike
/// `reader_num`, this one is live" would fail the build, which is a false
/// alarm aimed precisely at someone documenting the thing.
fn mentions_in_code(source: &str, name: &str) -> bool {
    source
        .lines()
        .map(|line| match line.find("//") {
            Some(at) => &line[..at],
            None => line,
        })
        .any(|code| code.contains(name))
}

/// The crate's sources, minus the one file where a recorded setting is
/// declared, defaulted and reported.
fn sources_outside_the_facade() -> Vec<(&'static str, &'static str)> {
    vec![
        ("src/driver.rs", include_str!("../src/driver.rs")),
        ("src/applier.rs", include_str!("../src/applier.rs")),
        (
            "src/heartbeat_merge.rs",
            include_str!("../src/heartbeat_merge.rs"),
        ),
        (
            "src/snapshot_pools.rs",
            include_str!("../src/snapshot_pools.rs"),
        ),
        ("src/rate_limit.rs", include_str!("../src/rate_limit.rs")),
        (
            "src/facade/node_runtime.rs",
            include_str!("../src/facade/node_runtime.rs"),
        ),
        (
            "src/facade/transport_runtime.rs",
            include_str!("../src/facade/transport_runtime.rs"),
        ),
        (
            "src/facade/cluster_runtime.rs",
            include_str!("../src/facade/cluster_runtime.rs"),
        ),
    ]
}

#[test]
fn the_settings_the_readme_calls_inert_really_reach_nothing() {
    let claimed = settings_the_readme_calls_inert();
    assert!(
        !claimed.is_empty(),
        "the README's inert-settings marker is empty, so this test would pass \
         whatever the code did"
    );

    let mut wired = Vec::new();
    for name in &claimed {
        for (path, source) in sources_outside_the_facade() {
            if mentions_in_code(source, name) {
                wired.push(format!("{name} is used in {path}"));
            }
        }
    }
    assert!(
        wired.is_empty(),
        "the README calls these settings recorded-only, but they reach something:\n  \
         {}\nWire-up is good news; the README needs updating with it.",
        wired.join("\n  ")
    );
}

#[test]
fn every_setting_the_readme_calls_inert_is_a_real_one() {
    // The other direction: a name that no longer exists, or never did,
    // would make the test above pass by describing nothing. The previous
    // list named `store_id` among the context's settings when it is the
    // creator's, which is how a list drifts without anyone noticing.
    let compat = include_str!("../src/facade/matrixraft_compat.rs");
    let claimed = settings_the_readme_calls_inert();
    for name in &claimed {
        assert!(
            compat.contains(&format!("pub {name}:")),
            "the README calls `{name}` a recorded-only setting, but no such field is \
             declared in the facade"
        );
    }
}

#[test]
fn a_new_context_setting_has_to_be_considered_for_the_list() {
    // The direction that actually protects a host: a knob added to the
    // context and wired to nothing must reach the README, or someone will
    // set it and expect something to happen.
    //
    // This pins the count rather than deciding for itself which settings
    // are live. An earlier version tried the latter, by counting how often
    // a name appeared in the facade, and got two of its own inputs wrong --
    // it took the struct's `pub struct ...` header for a field, and called
    // `shared_runtime` inert because its uses are inside the facade. A
    // judgement that cannot be made reliably is better not automated: the
    // count cannot be wrong, and it stops exactly the thing worth stopping.
    let compat = include_str!("../src/facade/matrixraft_compat.rs");
    let start = compat
        .find("pub struct MatrixRaftGroupContext {")
        .expect("the context struct moved");
    let body = &compat[start..];
    let open = body.find('{').expect("no opening brace");
    let end = body
        .find(
            "
}",
        )
        .expect("unterminated struct");
    let fields: Vec<&str> = body[open..end]
        .lines()
        .filter_map(|line| line.trim().strip_prefix("pub "))
        .filter_map(|rest| rest.split(':').next())
        .filter(|name| !name.contains(' '))
        .collect();

    assert_eq!(
        fields.len(),
        21,
        "`MatrixRaftGroupContext` now has {} settings rather than 21, so one was          added or removed: {fields:?}.

If the new one reaches nothing, add it to          the README's `inert-settings` marker. If it reaches something, the README's          settings section should say so. Then update this count.",
        fields.len()
    );
}
