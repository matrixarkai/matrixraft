// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{LogEntry, LogId, PipelineLimits, ReplicationPipeline};

fn entry(index: u64) -> LogEntry {
    LogEntry {
        log_id: LogId { term: 1, index },
        payload: b"payload".to_vec(),
        is_command: true,
    }
}

fn limits(reorder_timeout_us: u64, reorder_window_size: u64) -> PipelineLimits {
    PipelineLimits {
        max_inflights_replicate: 128,
        max_memory_replicate_log_bytes: 1024 * 1024,
        max_inflights_apply_task: 8,
        max_apply_batch_bytes: 1024 * 1024,
        enable_reorder_queue: true,
        reorder_window_size,
        reorder_timeout_us,
    }
}

// Queue three entries past next_index, then hand the pipeline every elapsed time
// it will accept. Returns the depth and the timeout counter afterwards.
fn depth_after_time_passes(reorder_timeout_us: u64) -> (u64, u64) {
    let mut pipeline = ReplicationPipeline::new(2, 1, limits(reorder_timeout_us, 8));
    for index in [3, 5, 7] {
        pipeline
            .receive_out_of_order(&entry(index))
            .expect("queues past next_index");
    }
    assert_eq!(pipeline.status().reorder_queue_depth, 3);

    // The only elapsed-time input the pipeline takes, driven far past any
    // plausible timeout, repeatedly.
    for _ in 0..5 {
        pipeline.record_retry_backoff_tick(u64::MAX / 8);
    }

    let status = pipeline.status();
    (status.reorder_queue_depth, status.reorder_entry_timeouts)
}

#[test]
fn the_reorder_timeout_is_never_compared_against_an_age() {
    let shortest = depth_after_time_passes(1);
    let longest = depth_after_time_passes(u64::MAX);

    assert_eq!(
        shortest, longest,
        "a one-microsecond timeout and an unbounded one behave identically"
    );
    assert_eq!(
        shortest,
        (3, 0),
        "nothing expired and nothing was counted as a timeout"
    );
}

#[test]
fn the_window_is_the_sibling_limit_that_does_bite() {
    // Control, same struct and same code path: a limit that is enforced changes
    // the outcome, so the assertions above are not passing because this test
    // cannot see a limit at all.
    let mut narrow = ReplicationPipeline::new(2, 1, limits(1, 1));
    narrow
        .receive_out_of_order(&entry(3))
        .expect("the first entry fits the window");
    assert!(
        narrow.receive_out_of_order(&entry(5)).is_err(),
        "the second does not"
    );
    assert_eq!(narrow.status().reorder_entries_rejected, 1);
    assert_eq!(narrow.status().reorder_queue_depth, 1);

    let mut wide = ReplicationPipeline::new(2, 1, limits(1, 8));
    for index in [3, 5] {
        wide.receive_out_of_order(&entry(index))
            .expect("both fit the wider window");
    }
    assert_eq!(wide.status().reorder_entries_rejected, 0);
    assert_eq!(wide.status().reorder_queue_depth, 2);
}

#[test]
fn only_an_explicit_call_drops_them_and_it_is_counted_as_a_timeout() {
    let mut pipeline = ReplicationPipeline::new(2, 1, limits(1, 8));
    for index in [3, 5, 7] {
        pipeline
            .receive_out_of_order(&entry(index))
            .expect("queues past next_index");
    }

    assert_eq!(pipeline.expire_reorder_queue(), 3);
    let status = pipeline.status();
    assert_eq!(status.reorder_queue_depth, 0);
    assert_eq!(
        status.reorder_entry_timeouts, 3,
        "the counter named for timeouts is moved by the operator-s call"
    );
    assert_eq!(status.reorder_dropped_packages, 3);
}
