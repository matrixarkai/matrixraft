// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A replication pipeline carries at most one snapshot transfer at a time.
//! `begin_snapshot_send` and `begin_snapshot_install` each refuse when either
//! direction is already active, which is **four clauses** across the two
//! methods:
//!
//! ```text
//! if self.status.snapshot_sending || self.status.snapshot_installing {
//! ```
//!
//! All four could be deleted with 1175 tests green. The state they guard is
//! well covered -- several tests begin a send, assert `snapshot_sending`, and
//! drive it to completion -- but it is always covered as a **premise**. One
//! test even sets up an in-flight transfer under the name
//! `transfer-already-in-flight` and then asserts only that an unrelated event
//! does not cancel it. Nothing ever asked the pipeline for a *second* transfer
//! and checked that it said no.
//!
//! ## What is lost without them
//!
//! `begin_snapshot_send` and `begin_snapshot_install` both end by overwriting
//! `snapshot_transfer` with a freshly built state -- id, index, chunk count,
//! byte counters, all of it. With the refusal gone, a second begin does not
//! fail and does not queue: it **silently replaces the transfer already in
//! flight**, so the chunks still arriving are accounted against the wrong
//! snapshot and the first transfer is abandoned with nobody told.
//!
//! So each test below asserts three things, because only together do they
//! distinguish a refusal from a no-op: that the call errs, that the
//! backpressure counter moved (the refusal's own observable, which nothing
//! else touches), and that the in-flight transfer is **still the first one** --
//! checked by giving the two calls different chunk counts, so an overwrite
//! cannot hide.

use matrixraft::{PipelineLimits, ReplicationPipeline};

const FIRST_CHUNKS: u64 = 2;
const SECOND_CHUNKS: u64 = 9;

fn pipeline() -> ReplicationPipeline {
    ReplicationPipeline::new(2, 10, PipelineLimits::default())
}

#[test]
fn a_second_send_is_refused_while_a_send_is_in_flight() {
    let mut pipeline = pipeline();
    pipeline
        .begin_snapshot_send("the-first-transfer", 20, FIRST_CHUNKS)
        .expect("the first send begins");
    assert!(pipeline.status().snapshot_sending, "the premise");
    let rejections_before = pipeline.status().snapshot_backpressure_rejections;

    pipeline
        .begin_snapshot_send("the-second-transfer", 40, SECOND_CHUNKS)
        .expect_err("a pipeline carries one transfer at a time");

    assert_eq!(
        pipeline.status().snapshot_backpressure_rejections,
        rejections_before + 1,
        "the refusal must register as backpressure, which is its own observable"
    );
    assert_eq!(
        pipeline.status().snapshot_install_total_chunks,
        FIRST_CHUNKS,
        "the transfer in flight must still be the first one -- a second begin that overwrote it would show {SECOND_CHUNKS} here"
    );
    assert_eq!(
        pipeline.status().snapshot_send_attempts,
        1,
        "a refused begin is not an attempt"
    );
}

#[test]
fn a_send_is_refused_while_an_install_is_in_flight() {
    let mut pipeline = pipeline();
    pipeline
        .begin_snapshot_install("the-incoming-transfer", 20, FIRST_CHUNKS)
        .expect("the install begins");
    assert!(pipeline.status().snapshot_installing, "the premise");
    let rejections_before = pipeline.status().snapshot_backpressure_rejections;

    pipeline
        .begin_snapshot_send("an-outgoing-transfer", 40, SECOND_CHUNKS)
        .expect_err("a pipeline receiving a snapshot may not also start sending one");

    assert_eq!(
        pipeline.status().snapshot_backpressure_rejections,
        rejections_before + 1,
        "the refusal must register as backpressure"
    );
    assert!(
        pipeline.status().snapshot_installing,
        "the incoming transfer must survive the refused outgoing one"
    );
    assert!(
        !pipeline.status().snapshot_sending,
        "a refused send must not leave the pipeline believing it is sending"
    );
    assert_eq!(
        pipeline.status().snapshot_install_total_chunks,
        FIRST_CHUNKS,
        "the transfer in flight must still be the install that was already running"
    );
}

#[test]
fn an_install_is_refused_while_a_send_is_in_flight() {
    let mut pipeline = pipeline();
    pipeline
        .begin_snapshot_send("the-outgoing-transfer", 20, FIRST_CHUNKS)
        .expect("the send begins");
    assert!(pipeline.status().snapshot_sending, "the premise");
    let rejections_before = pipeline.status().snapshot_backpressure_rejections;

    pipeline
        .begin_snapshot_install("an-incoming-transfer", 40, SECOND_CHUNKS)
        .expect_err("a pipeline sending a snapshot may not also start receiving one");

    assert_eq!(
        pipeline.status().snapshot_backpressure_rejections,
        rejections_before + 1,
        "the refusal must register as backpressure"
    );
    assert!(
        pipeline.status().snapshot_sending,
        "the outgoing transfer must survive the refused incoming one"
    );
    assert!(
        !pipeline.status().snapshot_installing,
        "a refused install must not leave the pipeline believing it is installing"
    );
    assert_eq!(
        pipeline.status().snapshot_install_total_chunks,
        FIRST_CHUNKS,
        "the transfer in flight must still be the send that was already running"
    );
}

#[test]
fn a_second_install_is_refused_while_an_install_is_in_flight() {
    let mut pipeline = pipeline();
    pipeline
        .begin_snapshot_install("the-first-incoming", 20, FIRST_CHUNKS)
        .expect("the first install begins");
    assert!(pipeline.status().snapshot_installing, "the premise");
    let rejections_before = pipeline.status().snapshot_backpressure_rejections;

    pipeline
        .begin_snapshot_install("the-second-incoming", 40, SECOND_CHUNKS)
        .expect_err("a pipeline receives one snapshot at a time");

    assert_eq!(
        pipeline.status().snapshot_backpressure_rejections,
        rejections_before + 1,
        "the refusal must register as backpressure"
    );
    assert_eq!(
        pipeline.status().snapshot_install_total_chunks,
        FIRST_CHUNKS,
        "the install in flight must still be the first one"
    );
}

#[test]
fn a_transfer_that_has_finished_releases_the_pipeline_for_the_next_one() {
    // The control. Without it, every test above would also pass against a
    // pipeline that refuses *every* transfer after the first, which is a
    // different and worse engine.
    let mut pipeline = pipeline();
    pipeline
        .begin_snapshot_send("the-first-transfer", 20, 1)
        .expect("the first send begins");
    pipeline
        .acknowledge_snapshot_chunk()
        .expect("its only chunk is acknowledged");
    assert!(
        !pipeline.status().snapshot_sending,
        "the premise: the first transfer is done"
    );

    pipeline
        .begin_snapshot_send("the-next-transfer", 40, SECOND_CHUNKS)
        .expect("a finished transfer leaves room for the next");
    assert_eq!(
        pipeline.status().snapshot_install_total_chunks,
        SECOND_CHUNKS,
        "the second transfer is the one in flight now"
    );
    assert_eq!(
        pipeline.status().snapshot_send_attempts,
        2,
        "both sends were real attempts"
    );
}
