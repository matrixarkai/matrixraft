// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! One pipeline holds both halves of a snapshot transfer: `snapshot_sending`
//! for the side pushing chunks out, `snapshot_installing` for the side taking
//! them in. Both create the same `snapshot_transfer` state, and
//! `update_snapshot_progress` is the **sender's** timeout accounting -- it
//! retires a transfer the remote has stopped acknowledging.
//!
//! So it carries a guard that reads oddly until you know which half is which:
//!
//! ```text
//! let Some(transfer) = self.snapshot_transfer.as_mut() else { return false };
//! if !self.status.snapshot_sending {
//!     return false;
//! }
//! ```
//!
//! The transfer exists on both sides, so the first line lets an **install**
//! through; the second is what stops the sender's clock retiring it. Without
//! it, a node in the middle of receiving a snapshot would have that install
//! cancelled by timeout accounting that was never about it -- and
//! `snapshot_send_timeouts` would be charged for a send that was not
//! happening.
//!
//! Neither that guard nor the one in `retry_snapshot_chunk` had a test: either
//! could be deleted with all 185 test targets green.

use matrixraft::{PipelineLimits, ReplicationPipeline};

fn pipeline() -> ReplicationPipeline {
    ReplicationPipeline::new(2, 1, PipelineLimits::default())
}

/// Long enough past any timeout used below that the accounting would fire if
/// it applied at all.
const WELL_PAST_THE_TIMEOUT: u64 = 10_000;
const TIMEOUT_MS: u64 = 100;

#[test]
fn an_install_is_not_retired_by_the_senders_clock() {
    let mut pipeline = pipeline();
    pipeline
        .begin_snapshot_install("incoming-snapshot", 10, 4)
        .expect("an install starts");
    assert!(
        pipeline.status().snapshot_installing,
        "the premise: this pipeline is receiving, not sending"
    );
    assert!(
        !pipeline.status().snapshot_sending,
        "and the sending half is idle, which is what the guard keys on"
    );

    let retired = pipeline.update_snapshot_progress(false, WELL_PAST_THE_TIMEOUT, TIMEOUT_MS);

    assert!(
        !retired,
        "the sender's timeout accounting must not retire an install"
    );
    assert!(
        pipeline.status().snapshot_installing,
        "and the install is still under way"
    );
    assert_eq!(
        pipeline.status().snapshot_send_timeouts,
        0,
        "nor is a send timeout charged for a send that was not happening"
    );
    // The transfer itself must survive, or the next chunk would have nothing
    // to be acknowledged against.
    pipeline
        .retry_snapshot_chunk()
        .expect("the transfer is still there");
}

#[test]
fn a_send_the_remote_stopped_acknowledging_is_retired() {
    // The control. Without it the test above would also pass against accounting
    // that never retired anything, which is the point of having it at all.
    let mut pipeline = pipeline();
    pipeline
        .begin_snapshot_send("outgoing-snapshot", 10, 4)
        .expect("a send starts");
    assert!(pipeline.status().snapshot_sending);

    let retired = pipeline.update_snapshot_progress(false, WELL_PAST_THE_TIMEOUT, TIMEOUT_MS);

    assert!(retired, "a send past its timeout is retired");
    assert!(
        !pipeline.status().snapshot_sending,
        "and the sending half is cleared"
    );
    assert_eq!(
        pipeline.status().snapshot_send_timeouts,
        1,
        "charged once, to the side it happened on"
    );
}

#[test]
fn a_remote_that_is_still_receiving_resets_the_clock() {
    // The other way out of the same function, and the reason the timeout is a
    // running total rather than a single elapsed value: a remote that reports
    // progress puts the accounting back to zero, so a transfer is only retired
    // for silence.
    let mut pipeline = pipeline();
    pipeline
        .begin_snapshot_send("outgoing-snapshot", 10, 4)
        .expect("a send starts");

    assert!(!pipeline.update_snapshot_progress(false, 60, TIMEOUT_MS));
    assert!(
        !pipeline.update_snapshot_progress(true, 60, TIMEOUT_MS),
        "the remote is receiving, so nothing is retired"
    );
    assert!(
        !pipeline.update_snapshot_progress(false, 60, TIMEOUT_MS),
        "and the total restarted from zero: 60 again is still under 100"
    );
    assert!(
        pipeline.status().snapshot_sending,
        "the send is still in flight"
    );

    // Only once the silence passes the timeout does it go.
    assert!(
        pipeline.update_snapshot_progress(false, 60, TIMEOUT_MS),
        "120 of silence is past the timeout"
    );
    assert_eq!(pipeline.status().snapshot_send_timeouts, 1);
}

#[test]
fn silence_exactly_at_the_timeout_is_not_yet_a_timeout() {
    // The boundary, and it needs saying on its own because the tests above
    // never land on it: 60 twice is 120, which is past 100, and 60 once is
    // under it. Read as a strict comparison the rule would retire a transfer
    // the instant its silence *reached* the timeout rather than passed it, and
    // nothing here would have noticed.
    let mut pipeline = pipeline();
    pipeline
        .begin_snapshot_send("outgoing-snapshot", 10, 4)
        .expect("a send starts");

    assert!(
        !pipeline.update_snapshot_progress(false, TIMEOUT_MS, TIMEOUT_MS),
        "silence of exactly the timeout has not passed it"
    );
    assert!(
        pipeline.status().snapshot_sending,
        "so the send is still in flight"
    );
    assert_eq!(pipeline.status().snapshot_send_timeouts, 0);

    // One millisecond more does it.
    assert!(
        pipeline.update_snapshot_progress(false, 1, TIMEOUT_MS),
        "one past the timeout retires it"
    );
    assert_eq!(pipeline.status().snapshot_send_timeouts, 1);
}

#[test]
fn a_chunk_retry_needs_a_transfer_to_retry() {
    let mut pipeline = pipeline();

    let error = pipeline
        .retry_snapshot_chunk()
        .expect_err("there is nothing to retry");
    assert!(
        error
            .to_string()
            .contains("snapshot transfer is not active"),
        "{error:?}"
    );
    assert_eq!(
        pipeline.status().snapshot_chunk_retry_count,
        0,
        "and a refused retry is not counted as one -- the count is of retries \
         that happened, not of calls that asked"
    );

    pipeline
        .begin_snapshot_send("outgoing-snapshot", 10, 4)
        .expect("a send starts");
    pipeline
        .retry_snapshot_chunk()
        .expect("now there is a transfer to retry");
    assert_eq!(
        pipeline.status().snapshot_chunk_retry_count,
        1,
        "and this one is counted"
    );
}
