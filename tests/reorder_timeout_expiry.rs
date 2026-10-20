// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The reorder timeout is compared against an age.
//!
//! It was not. This file replaces `reorder_timeout_is_inert.rs`, whose name
//! was the claim: the queue was a `BTreeSet<LogIndex>` that recorded no
//! arrival time, so `reorder_timeout_us` had nothing to be measured against
//! and 1 behaved exactly like 5,000,000. The only thing that dropped an
//! out-of-order append was an operator calling `expire_reorder_queue`, which
//! drops **all** of them -- and that call is what moved the counter named for
//! timeouts.
//!
//! The queue carries an age per entry now and `tick_reorder_queue` compares
//! the two. The old file's control is kept, because the reason it was there is
//! unchanged: a test that cannot see a limit bite proves nothing about the one
//! that does not.

use matrixraft::{
    Config, LogEntry, LogId, Peer, PipelineLimits, RaftCluster, ReplicaRole, ReplicationPipeline,
};

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

fn queued(reorder_timeout_us: u64) -> ReplicationPipeline {
    let mut pipeline = ReplicationPipeline::new(2, 1, limits(reorder_timeout_us, 8));
    for index in [3, 5, 7] {
        pipeline
            .receive_out_of_order(&entry(index))
            .expect("queues past next_index");
    }
    assert_eq!(pipeline.status().reorder_queue_depth, 3);
    pipeline
}

#[test]
fn an_entry_that_waits_longer_than_the_timeout_is_dropped() {
    let mut pipeline = queued(1_000);

    assert_eq!(
        pipeline.tick_reorder_queue(400),
        0,
        "400us of a 1,000us timeout is not long enough"
    );
    assert_eq!(pipeline.status().reorder_queue_depth, 3);
    assert_eq!(pipeline.status().reorder_entry_timeouts, 0);

    assert_eq!(
        pipeline.tick_reorder_queue(400),
        0,
        "and neither is 800 -- the age accumulates across ticks rather than \
         resetting, but it has not arrived yet"
    );

    assert_eq!(
        pipeline.tick_reorder_queue(400),
        3,
        "1,200us has, and every entry queued at the same moment goes together"
    );
    let status = pipeline.status();
    assert_eq!(status.reorder_queue_depth, 0);
    assert_eq!(status.reorder_entry_timeouts, 3);
    assert_eq!(status.reorder_dropped_packages, 3);
}

#[test]
fn the_timeout_magnitude_decides_the_outcome() {
    // The assertion the old file made in reverse: a one-microsecond timeout and
    // an unbounded one behaved identically, and now they do not.
    let mut shortest = queued(1);
    let mut longest = queued(u64::MAX);

    assert_eq!(shortest.tick_reorder_queue(1_000), 3);
    assert_eq!(longest.tick_reorder_queue(1_000), 0);
    assert_eq!(shortest.status().reorder_queue_depth, 0);
    assert_eq!(longest.status().reorder_queue_depth, 3);
}

#[test]
fn a_timeout_of_zero_is_off() {
    // The convention the byte and task limits in the same file use. Without it
    // a default-constructed limit set would expire every out-of-order append
    // on the first tick.
    let mut pipeline = queued(0);
    assert_eq!(pipeline.tick_reorder_queue(u64::MAX), 0);
    assert_eq!(pipeline.status().reorder_queue_depth, 3);
    assert_eq!(pipeline.status().reorder_entry_timeouts, 0);
}

#[test]
fn nothing_expires_without_a_tick() {
    // The pipeline keeps no clock. Time passing in the world is not time
    // passing here, which is the same contract the leader lease has.
    let pipeline = queued(1);
    assert_eq!(pipeline.status().reorder_queue_depth, 3);
    assert_eq!(pipeline.status().reorder_entry_timeouts, 0);
}

#[test]
fn an_entry_queued_later_is_not_swept_out_by_its_neighbours_waiting() {
    let mut pipeline = queued(1_000);
    assert_eq!(pipeline.tick_reorder_queue(900), 0);

    // This one arrives with 900us already on the clock for the others.
    pipeline
        .receive_out_of_order(&entry(9))
        .expect("queues past next_index");
    assert_eq!(pipeline.status().reorder_queue_depth, 4);

    assert_eq!(
        pipeline.tick_reorder_queue(200),
        3,
        "the three that had waited 1,100us go; the newcomer has waited 200"
    );
    assert_eq!(pipeline.status().reorder_queue_depth, 1);

    assert_eq!(
        pipeline.tick_reorder_queue(800),
        1,
        "and it goes on its own schedule"
    );
    assert_eq!(pipeline.status().reorder_queue_depth, 0);
    assert_eq!(pipeline.status().reorder_entry_timeouts, 4);
}

#[test]
fn a_drained_entry_is_not_a_timeout() {
    // Convergence is the other way out of the queue, and it must not be
    // counted as an expiry: the counter is what an operator alerts on.
    let mut pipeline = ReplicationPipeline::new(2, 1, limits(1_000, 8));
    for index in [2, 3] {
        pipeline
            .receive_out_of_order(&entry(index))
            .expect("queues past next_index");
    }
    assert_eq!(pipeline.tick_reorder_queue(500), 0);

    // Filling the hole drains both, ages and all.
    pipeline
        .receive_out_of_order(&entry(1))
        .expect("the hole is filled");
    let status = pipeline.status();
    assert_eq!(status.reorder_queue_depth, 0);
    assert_eq!(status.reorder_entries_converged, 2);
    assert_eq!(
        status.reorder_entry_timeouts, 0,
        "they converged; nothing timed out"
    );
    assert_eq!(status.next_index, 4);
}

#[test]
fn the_window_is_the_sibling_limit_that_also_bites() {
    // Control, kept from the file this replaces: a limit that is enforced
    // changes the outcome, so the assertions above are not passing because
    // this test cannot see a limit at all.
    let mut narrow = ReplicationPipeline::new(2, 1, limits(1_000, 1));
    narrow
        .receive_out_of_order(&entry(3))
        .expect("the first entry fits the window");
    assert!(
        narrow.receive_out_of_order(&entry(5)).is_err(),
        "the second does not"
    );
    assert_eq!(narrow.status().reorder_entries_rejected, 1);
    assert_eq!(narrow.status().reorder_queue_depth, 1);

    let mut wide = ReplicationPipeline::new(2, 1, limits(1_000, 8));
    for index in [3, 5] {
        wide.receive_out_of_order(&entry(index))
            .expect("both fit the wider window");
    }
    assert_eq!(wide.status().reorder_entries_rejected, 0);
    assert_eq!(wide.status().reorder_queue_depth, 2);
}

#[test]
fn the_operators_hammer_still_drops_everything() {
    // `expire_reorder_queue` is unchanged: a policy and an override are
    // different things, and the override does not consult the age.
    let mut pipeline = queued(u64::MAX);
    assert_eq!(pipeline.tick_reorder_queue(1_000), 0);
    assert_eq!(pipeline.expire_reorder_queue(), 3);
    let status = pipeline.status();
    assert_eq!(status.reorder_queue_depth, 0);
    assert_eq!(status.reorder_entry_timeouts, 3);
    assert_eq!(status.reorder_dropped_packages, 3);
}

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 81_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 82_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

/// A started cluster with one out-of-order append waiting on peer 2.
///
/// **The timeout is the default and cannot be set here.** `RaftCluster` builds
/// every pipeline with `PipelineLimits::default()` at four sites, and `Config`
/// carries no reorder fields at all -- so the in-process cluster's reorder
/// timeout is not configurable. That is a separate thing to fix and this test
/// does not pretend otherwise: it asserts the default it depends on, so it
/// fails loudly if the default moves rather than quietly testing nothing.
fn cluster_with_a_queued_append() -> RaftCluster {
    assert_eq!(
        PipelineLimits::default().reorder_timeout_us,
        DEFAULT_REORDER_TIMEOUT_US,
        "this test's arithmetic is built on the default timeout"
    );
    let members = (1..=3u64).map(peer).collect::<Vec<_>>();
    let mut cluster = RaftCluster::new(91, Config::default(), members).expect("cluster");
    cluster.start().expect("start");

    // Far enough past the peer's next index to leave a hole.
    cluster
        .receive_out_of_order_append_for(2, entry(500))
        .expect("queues past next_index");
    assert_eq!(
        cluster
            .peer_pipeline_status(2)
            .expect("pipeline status")
            .reorder_queue_depth,
        1,
        "the cluster has to have something queued for this to be about anything"
    );
    cluster
}

const DEFAULT_REORDER_TIMEOUT_US: u64 = 5_000_000;

#[test]
fn the_cluster_ticks_the_queue_it_holds() {
    // The wiring, which the pipeline tests above cannot see: a plant that made
    // `tick_reorder_queues` skip every pipeline survived the suite until this
    // test existed.
    let mut cluster = cluster_with_a_queued_append();
    assert_eq!(
        cluster.tick_reorder_queues(DEFAULT_REORDER_TIMEOUT_US - 1),
        0,
        "one microsecond short"
    );
    assert_eq!(
        cluster
            .peer_pipeline_status(2)
            .expect("status")
            .reorder_queue_depth,
        1
    );

    assert_eq!(cluster.tick_reorder_queues(1), 1, "and now it has waited");
    let status = cluster.peer_pipeline_status(2).expect("status");
    assert_eq!(status.reorder_queue_depth, 0);
    assert_eq!(status.reorder_entry_timeouts, 1);
}

#[test]
fn the_heartbeat_form_converts_milliseconds_to_microseconds() {
    // The driver loop has an interval in milliseconds and the timeout is in
    // microseconds. The conversion is the cluster's so that it can be tested:
    // a plant that dropped the factor of a thousand from the driver loop
    // survived, because nothing tests a driver loop's arithmetic.
    let mut cluster = cluster_with_a_queued_append();
    let timeout_ms = DEFAULT_REORDER_TIMEOUT_US / 1_000;

    assert_eq!(
        cluster.tick_reorder_queues_for_heartbeat(timeout_ms - 1),
        0,
        "one millisecond short of the timeout"
    );
    assert_eq!(
        cluster.tick_reorder_queues_for_heartbeat(1),
        1,
        "one more reaches it -- and if this read milliseconds as microseconds, \
         the whole timeout would be five thousand microseconds of ticking away"
    );
    assert_eq!(
        cluster
            .peer_pipeline_status(2)
            .expect("status")
            .reorder_entry_timeouts,
        1
    );
}

#[test]
fn a_tick_on_an_empty_queue_costs_nothing_and_counts_nothing() {
    let mut pipeline = ReplicationPipeline::new(2, 1, limits(1, 8));
    assert_eq!(pipeline.tick_reorder_queue(u64::MAX), 0);
    assert_eq!(pipeline.status().reorder_entry_timeouts, 0);
}
