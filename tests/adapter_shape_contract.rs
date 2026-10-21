// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The README's adapter snippet is the one the crate reports.
//!
//! `matrixraft_temporalstore_adapter_shape()` exposes the intended adapter
//! shape as a typed report, and the README prints a snippet and says that is
//! what the function exposes. Nothing compared them, and they drifted: the
//! README named the crate `rustraft` while the report named it `matrixraft`.
//!
//! One word, and not a cosmetic one -- it is a crate path in the integration
//! example for this crate's largest intended consumer, so a reader copying it
//! reaches for a crate that does not exist. The README's own policy line keeps
//! the `rustraft_*` and `RustRaft*` spellings for metric names and evidence
//! keys, of which there are 232 and 100 in `src/`; a module path in a code
//! example is neither a name nor an evidence key, and it was the only
//! `rustraft::` in the file.
//!
//! Why nothing caught it: the README's rust blocks are not doctests. The
//! README is not pulled into the crate docs, there are no runnable doctests at
//! all, and all 24 of its rust blocks are illustrative fragments that
//! reference values the surrounding prose introduces -- so none of them
//! compiles standalone and none is meant to. This block is the exception
//! worth pinning, because the crate already holds the same text in code.
//!
//! Note for a reader rather than a change here: the shape is described as
//! *intended*, and today's `NodeRuntime` carries no type parameters, so the
//! snippet is a target rather than something that compiles now.

use matrixraft::matrixraft_temporalstore_adapter_shape;

/// The fenced rust block that follows the sentence introducing the shape.
fn readme_adapter_snippet() -> String {
    let readme = include_str!("../README.md");
    let marker = "The intended TemporalStore adapter shape is:";
    let start = readme
        .find(marker)
        .expect("the README no longer introduces the adapter shape with that sentence");
    let rest = &readme[start..];
    let fence = "`".repeat(3);
    let open = rest
        .find(&format!("{fence}rust"))
        .expect("no rust block follows the adapter-shape sentence");
    let body_start = rest[open..]
        .find('\n')
        .map(|offset| open + offset + 1)
        .expect("unterminated fence opener");
    let body_end = rest[body_start..]
        .find(&fence)
        .map(|offset| body_start + offset)
        .expect("unterminated rust block");
    rest[body_start..body_end].trim_end().to_string()
}

#[test]
fn the_readme_snippet_is_the_shape_the_crate_reports() {
    let reported = matrixraft_temporalstore_adapter_shape().example;
    assert_eq!(
        readme_adapter_snippet(),
        reported,
        "the README prints this snippet and says the function exposes it, so \
         the two have to be the same text. They drifted once already, by the \
         crate name."
    );
}

#[test]
fn the_reported_shape_names_this_crate() {
    // Independent of the README: whichever way a future edit resolves a
    // mismatch, the crate path has to name this crate.
    let reported = matrixraft_temporalstore_adapter_shape().example;
    assert!(
        reported.contains("matrixraft::node::NodeRuntime"),
        "the adapter shape has to name this crate in its path: {reported}"
    );
    assert!(
        !reported.contains("rustraft::"),
        "a crate path is not one of the `rustraft_*` names the README keeps \
         deliberately: {reported}"
    );
}
