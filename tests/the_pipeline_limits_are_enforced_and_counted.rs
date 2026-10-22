// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The replication pipeline's limits are what stop one slow or one enormous
//! entry from taking a node's memory with it. Four of them had no test: each
//! could be deleted with all 184 test targets green.
//!
//! | limit | enforced in | counter |
//! |---|---|---|
//! | an entry larger than a whole apply batch | `queue_append` | `oversized_log_rejections` |
//! | an apply batch larger than the limit | `begin_apply_task` | `oversized_log_rejections` |
//! | more apply tasks in flight than allowed | `begin_apply_task` | `apply_backpressure_rejections` |
//! | an entry below the peer's next index | `receive_out_of_order` | `out_of_order_append_rejections` |
//!
//! **Every one of them keeps a counter, and the counter is the sharper
//! assertion.** An error alone says a call failed; the counter says *this* rule
//! is the one that refused it, and that it refused exactly once. Two of the
//! four share `oversized_log_rejections`, so the error alone could not tell
//! them apart at all.
//!
//! Each limit is a comparison, so each test also pins the value **at** the
//! limit, which must be accepted. A limit read one off would either let an
//! oversized entry through or refuse an entry that exactly fits, and only the
//! boundary case can tell.
//!
//! The last rule is not a resource limit but the same shape: an entry below
//! `next_index` is one the peer has already been sent, so accepting it would
//! quietly rewind the pipeline's own position.

use matrixraft::{LogEntry, LogId, PipelineLimits, ReplicationPipeline};

/// Limits small enough to reach in a test: an apply batch of 8 bytes and two
/// apply tasks in flight.
fn small_limits() -> PipelineLimits {
    PipelineLimits {
        max_inflights_replicate: 4,
        max_memory_replicate_log_bytes: 1024,
        max_inflights_apply_task: 2,
        max_apply_batch_bytes: 8,
        enable_reorder_queue: true,
        reorder_window_size: 4,
        reorder_timeout_us: 10,
    }
}

fn entry_of(index: u64, payload_bytes: usize) -> LogEntry {
    LogEntry {
        log_id: LogId { term: 1, index },
        payload: vec![b'x'; payload_bytes],
        is_command: true,
    }
}

fn pipeline() -> ReplicationPipeline {
    ReplicationPipeline::new(2, 1, small_limits())
}

#[test]
fn an_entry_larger_than_an_apply_batch_is_refused_and_counted() {
    let mut pipeline = pipeline();
    let before = pipeline.status().oversized_log_rejections;

    let error = pipeline
        .queue_append(&entry_of(1, 9))
        .expect_err("an entry larger than a whole apply batch cannot be applied");
    assert!(
        error.to_string().contains("exceeds max apply batch bytes"),
        "{error:?}"
    );
    assert_eq!(
        pipeline.status().oversized_log_rejections,
        before + 1,
        "and the refusal is counted once, which is how an operator sees it"
    );

    // The boundary: exactly the limit fits, so the rule cannot be read as
    // refusing an entry that would have been applied.
    let mut at_the_limit = self::pipeline();
    at_the_limit
        .queue_append(&entry_of(1, 8))
        .expect("an entry of exactly one batch is queued");
    assert_eq!(
        at_the_limit.status().oversized_log_rejections,
        0,
        "and nothing is counted against it"
    );
}

#[test]
fn an_apply_batch_larger_than_the_limit_is_refused_and_counted() {
    let mut pipeline = pipeline();
    let before = pipeline.status().oversized_log_rejections;

    let error = pipeline
        .begin_apply_task(1, 9)
        .expect_err("a batch larger than the limit is refused");
    assert!(
        error.to_string().contains("exceeds max apply batch bytes"),
        "{error:?}"
    );
    assert_eq!(
        pipeline.status().oversized_log_rejections,
        before + 1,
        "counted against the oversize rule, not against backpressure"
    );
    assert_eq!(
        pipeline.status().apply_backpressure_rejections,
        0,
        "and the two rules share no counter reading -- this is what separates \
         an oversized batch from a busy one"
    );

    let mut at_the_limit = self::pipeline();
    at_the_limit
        .begin_apply_task(1, 8)
        .expect("a batch of exactly the limit is accepted");
}

#[test]
fn more_apply_tasks_than_allowed_are_refused_and_counted() {
    let mut pipeline = pipeline();

    // Two is the limit, and both are accepted.
    pipeline.begin_apply_task(1, 4).expect("first apply task");
    pipeline.begin_apply_task(2, 4).expect("second apply task");
    assert_eq!(
        pipeline.status().apply_backpressure_rejections,
        0,
        "the premise: reaching the limit is not itself a refusal"
    );

    let error = pipeline
        .begin_apply_task(3, 4)
        .expect_err("the third exceeds the inflight limit");
    assert!(
        error
            .to_string()
            .contains("apply inflight task limit reached"),
        "{error:?}"
    );
    assert_eq!(
        pipeline.status().apply_backpressure_rejections,
        1,
        "counted as backpressure"
    );
    assert_eq!(
        pipeline.status().oversized_log_rejections,
        0,
        "and not as an oversized batch: 4 bytes is well within the limit"
    );
}

#[test]
fn an_entry_below_the_peers_next_index_is_refused_and_counted() {
    // A pipeline whose peer is already expecting index 5.
    let mut pipeline = ReplicationPipeline::new(2, 5, small_limits());
    let before = pipeline.status().out_of_order_append_rejections;

    let error = pipeline
        .receive_out_of_order(&entry_of(4, 1))
        .expect_err("an entry the peer has already been sent is refused");
    assert!(
        error.to_string().contains("below peer next index"),
        "{error:?}"
    );
    assert_eq!(
        pipeline.status().out_of_order_append_rejections,
        before + 1,
        "accepting it would rewind the pipeline's own position, so it is \
         counted as an out-of-order append"
    );
    assert_eq!(
        pipeline.status().next_index,
        5,
        "and the position itself does not move"
    );

    // The boundary: the entry the peer is actually waiting for is accepted.
    pipeline
        .receive_out_of_order(&entry_of(5, 1))
        .expect("the entry at the next index is the one being waited for");
}
