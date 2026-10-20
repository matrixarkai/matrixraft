// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Every per-peer counter is either exported as a series or on the backlog.
//!
//! A replication pipeline keeps dozens of numeric counters per peer, and three
//! of them had a metric name. The README now lists which are exported and
//! which are not, and this asserts the two lists **partition the struct
//! exactly**: a counter added to the pipeline and left out of both fails here,
//! so whoever adds one has to decide which side it is on.
//!
//! That is the same mechanism as `tests/settings_contract.rs` and
//! `tests/read_surface_contract.rs`, and it exists for the same reason: a
//! surface is only honest while something checks it. The lists live in the
//! README so there is one place for them to be right.
//!
//! **What this test had to get right:** the field list is read out of
//! `PeerProgress` rather than typed here, because a list typed twice is a list
//! that disagrees with itself. Reading the struct is also what makes the
//! partition claim mean anything -- a hand-written field list would pass the
//! test while omitting the field someone just added, which is the only case
//! the test exists for.

use matrixraft::matrixraft_metric_names;

/// The metric names the README says are exported, as written.
fn exported_metric_names() -> Vec<String> {
    readme_list("<!-- peer-metrics-exported: ")
}

/// The per-peer counters the README lists as not yet exported.
fn backlog_fields() -> Vec<String> {
    readme_list("<!-- peer-metrics-backlog: ")
}

fn readme_list(marker: &str) -> Vec<String> {
    let readme = include_str!("../README.md");
    let line = readme
        .lines()
        .find(|line| line.trim_start().starts_with(marker))
        .unwrap_or_else(|| panic!("README has no `{marker}...` marker for this test to read"));
    line.trim()
        .trim_start_matches(marker)
        .trim_end_matches("-->")
        .trim()
        .split(',')
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .collect()
}

/// The numeric fields of `PeerProgress`, read out of the source.
///
/// `u64` and `LogIndex` only: a `bool` is a state, not a series, and the
/// README's two lists are about series.
fn peer_progress_numeric_fields() -> Vec<String> {
    let source = include_str!("../src/pipeline.rs");
    let head = "pub struct PeerProgress {";
    let start = source
        .find(head)
        .expect("src/pipeline.rs no longer declares PeerProgress")
        + head.len();
    let body = &source[start..];
    let end = body
        .find("\n}")
        .expect("PeerProgress declaration is not closed");
    let mut fields = Vec::new();
    for line in body[..end].lines() {
        let line = line.trim();
        let Some(rest) = line.strip_prefix("pub ") else {
            continue;
        };
        let Some((name, ty)) = rest.split_once(':') else {
            continue;
        };
        let ty = ty.trim().trim_end_matches(',');
        if ty == "u64" || ty == "LogIndex" {
            fields.push(name.trim().to_string());
        }
    }
    fields
}

/// The `PeerProgress` field a metric name refers to.
fn field_of(metric: &str) -> String {
    metric
        .trim_start_matches("rustraft_peer_")
        .trim_end_matches("_total")
        .to_string()
}

#[test]
fn the_two_lists_partition_the_peer_counters_exactly() {
    let fields = peer_progress_numeric_fields();
    assert!(
        fields.len() > 40,
        "only {} numeric fields were read out of PeerProgress, so the scan is broken \
         and this test would pass by checking almost nothing",
        fields.len()
    );

    let exported: Vec<String> = exported_metric_names()
        .iter()
        .map(|m| field_of(m))
        .collect();
    let backlog = backlog_fields();

    let mut missing = Vec::new();
    for field in &fields {
        let in_exported = exported.contains(field);
        let in_backlog = backlog.contains(field);
        if in_exported && in_backlog {
            missing.push(format!("{field} is in BOTH lists"));
        } else if !in_exported && !in_backlog {
            missing.push(format!(
                "{field} is in neither: export it as rustraft_peer_{field}_total, or add it \
                 to the backlog marker"
            ));
        }
    }
    assert!(
        missing.is_empty(),
        "the README's two per-peer metric lists have to partition PeerProgress:\n  {}",
        missing.join("\n  ")
    );

    for field in exported.iter().chain(backlog.iter()) {
        assert!(
            fields.contains(field),
            "the README lists `{field}` as a per-peer counter, but PeerProgress has no \
             such numeric field -- a renamed field leaves the list describing nothing"
        );
    }
}

#[test]
fn every_exported_name_is_a_real_metric_name() {
    // The other direction: the README could claim a series the registry does
    // not actually publish, which is the failure mode that matters to a host
    // wiring a scrape endpoint.
    let names = matrixraft_metric_names();
    let published = serde_json::to_value(&names).expect("metric names serialize");
    let published = published
        .as_object()
        .expect("metric names are a flat object")
        .values()
        .filter_map(|value| value.as_str().map(|s| s.to_string()))
        .collect::<Vec<_>>();

    for metric in exported_metric_names() {
        assert!(
            published.contains(&metric),
            "the README says `{metric}` is exported, but matrixraft_metric_names() does \
             not publish it"
        );
    }
}

#[test]
fn the_refusal_counters_are_all_exported() {
    // The rule the section states, asserted rather than left to the lists: a
    // counter that counts a refusal, a timeout, a drop or a retry has a series.
    // Without this the two lists could partition the struct perfectly while
    // putting every refusal on the backlog.
    //
    // The rule is about a **count**, so a field ending in `_ms` is excluded: a
    // duration is not a tally of anything. That exclusion is not free
    // phrasing -- it is what `next_retry_after_ms` and `backoff_ms` are, and
    // the first version of this test demanded a series for the first of them
    // while leaving the second on the backlog, which would have drawn the line
    // between two fields of the same kind. Both are on the backlog now.
    const REFUSAL_WORDS: [&str; 5] = ["rejection", "rejected", "timeout", "dropped", "retry"];
    let is_a_count = |field: &str| !field.ends_with("_ms");
    let exported: Vec<String> = exported_metric_names()
        .iter()
        .map(|m| field_of(m))
        .collect();
    let mut unexported = Vec::new();
    let mut excluded_as_durations = Vec::new();
    for field in peer_progress_numeric_fields() {
        let looks_like_a_refusal = REFUSAL_WORDS.iter().any(|word| field.contains(word));
        if !looks_like_a_refusal {
            continue;
        }
        if !is_a_count(&field) {
            excluded_as_durations.push(field);
            continue;
        }
        if !exported.contains(&field) {
            unexported.push(field);
        }
    }
    assert!(
        unexported.is_empty(),
        "these count a refusal, a timeout, a drop or a retry and have no series: {}",
        unexported.join(", ")
    );
    // The exclusion has to be doing something, or it is a hole the next
    // `_ms` counter falls through silently.
    assert_eq!(
        excluded_as_durations,
        vec!["next_retry_after_ms".to_string()],
        "the duration exclusion covers exactly the field it was written for; a new one          means deciding whether it is a count"
    );
}

#[test]
fn the_field_scan_reads_the_struct_and_not_the_whole_file() {
    // The partition claim rests on this scan. If it ever read the whole file
    // it would pull in every other struct's fields and the test would start
    // demanding series for them; if it read nothing the test would pass
    // vacuously. Both are checked by name.
    let fields = peer_progress_numeric_fields();
    assert!(
        fields.contains(&"match_index".to_string()),
        "a field near the top of PeerProgress is missing: {fields:?}"
    );
    assert!(
        fields.contains(&"witness_quorum_acked".to_string()),
        "a field near the bottom of PeerProgress is missing, so the scan stops early"
    );
    assert!(
        !fields.contains(&"max_apply_batch_bytes".to_string()),
        "max_apply_batch_bytes is on PipelineLimits, not PeerProgress -- the scan is \
         reading past the struct it is supposed to read"
    );
    assert!(
        !fields.contains(&"paused".to_string()),
        "a bool is a state rather than a series and must not be in the list"
    );
}
