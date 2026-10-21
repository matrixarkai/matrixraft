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

/// The marker names five settings, and dropping one is not a silent act.
///
/// The two tests either side of this check what the list *claims* -- that each
/// name reaches nothing, and that each name is a real field. Neither notices a
/// name being removed, so a shortfall could be undocumented again by deleting
/// a word. This cannot say which settings belong on the list, and does not
/// try: that judgement is the one this file declines to automate. It says only
/// that the list is the length someone last decided it should be.
#[test]
fn the_inert_settings_marker_is_the_length_it_was_last_decided_to_be() {
    let claimed = settings_the_readme_calls_inert();
    assert_eq!(
        claimed.len(),
        7,
        "the README's `inert-settings` marker names {} settings rather than 7: \
         {claimed:?}.\n\nThree are on `MatrixRaftGroupContext`, two on \
         `MatrixRaftOptions` and one on `NodeOptions`. Removing one means a setting became live -- say so \
         in the settings section -- or that a documented shortfall is being \
         undocumented, which is the thing to stop. Then update this count.",
        claimed.len()
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

/// The settings that arrive as a method parameter, which no field count sees.
///
/// The three counts below watch struct fields. `recover_fsm_from_snapshot`
/// reaches the node through `MatrixRaftNode::restart(bool)`, is stored on the
/// node, and is reported back through the per-node, per-group and
/// per-route-key accessors -- and nothing branches on it. It was inert and
/// unlisted while every field count passed, which is the fourth time that hole
/// has mattered and the first time it was not a struct field.
///
/// Two blind spots hid it, not one. The field counts watch struct fields, and
/// this is a parameter. And `the_settings_the_readme_calls_inert_really_reach_nothing`
/// looks for uses *outside* the facade -- excluded deliberately, after it
/// called `shared_runtime` inert for having its uses inside -- while every use
/// of this one is inside the facade. So that test passes for this name without
/// examining anything, which is worth knowing before treating it as evidence
/// that a listed name is inert.
///
/// This is a named check rather than a mechanism, deliberately. Enumerating
/// "settings that arrive as parameters" means deciding which parameters are
/// settings, and this file already records why that judgement is not
/// automated: an earlier attempt to decide live-vs-inert by counting name
/// occurrences got two of its own inputs wrong. A named check cannot be wrong
/// about the one thing it names.
#[test]
fn the_node_setting_that_is_not_a_struct_field_is_still_listed() {
    let claimed = settings_the_readme_calls_inert();
    assert!(
        claimed
            .iter()
            .any(|name| name == "recover_fsm_from_snapshot"),
        "`recover_fsm_from_snapshot` is recorded on the node, reported back by \
         several accessors, and branched on by nothing, so the README's \
         `inert-settings` marker has to name it. If it gets wired up, the \
         README's settings section should say what it does and this test should \
         go: {claimed:?}"
    );
}

/// The same pin on `MatrixRaftOptions`, which had none.
///
/// Three tests guarded the context and nothing guarded this struct, so four of
/// its settings were recorded-only while they all passed -- including two that
/// named timeouts the engine ran off a different setting entirely. A count
/// cannot decide which settings are live, and does not try to; it stops a new
/// one being added without anyone deciding.
/// `NodeOptions` had no field-count guard, which is the third time that hole
/// has mattered.
///
/// The README records the first two: `MatrixRaftOptions` was outside the
/// machine-checked marker until its list grew, and before that the transport
/// options were all inert while the section named none of them. Both times a
/// passing guard certified its own list rather than the property. This struct
/// was the remaining one a new setting could be added to without anything
/// noticing -- and `snapshot_dir`, which reaches nothing that runs, has been
/// sitting on it undeclared.
#[test]
fn a_new_node_setting_has_to_be_considered_for_the_list() {
    let source = include_str!("../src/facade/api_messages.rs");
    let start = source
        .find("pub struct NodeOptions {")
        .expect("NodeOptions is declared in the facade");
    let body = &source[start..];
    let open = body.find('{').expect("no opening brace");
    let end = body
        .find(
            "
}",
        )
        .expect("unterminated struct");
    // From the brace, not from `start`: the declaration line itself begins
    // with `pub `, and counting it was an off-by-one that read 10 for 9.
    let fields = body[open..end]
        .lines()
        .filter_map(|line| line.trim().strip_prefix("pub "))
        .filter_map(|rest| rest.split(':').next())
        .filter(|name| !name.contains(' '))
        .count();
    assert_eq!(
        fields, 9,
        "NodeOptions has {} fields rather than the 9 this list was last decided \
         against. A new one is either live -- and then nothing here needs to \
         change -- or it only gets recorded, and then it belongs in the \
         README's inert-settings marker beside snapshot_dir.",
        fields
    );
}

#[test]
fn a_new_options_setting_has_to_be_considered_for_the_list() {
    let compat = include_str!("../src/facade/matrixraft_compat.rs");
    let start = compat
        .find("pub struct MatrixRaftOptions {")
        .expect("the options struct moved");
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
        31,
        "`MatrixRaftOptions` now has {} settings rather than 31, so one was \
         added or removed: {fields:?}.\n\nIf the new one reaches nothing, add it \
         to the README's `inert-settings` marker. If it reaches something, the \
         README's settings section should say so. Then update this count.",
        fields.len()
    );
}
