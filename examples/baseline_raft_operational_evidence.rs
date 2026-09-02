// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    matrixraft_baseline_raft_operational_evidence_bundle,
    matrixraft_validate_baseline_raft_operational_evidence_bundle, PeerProgress, PipelineLimits,
    WalLifecycleStatus,
};

fn main() {
    let pipeline_limits = PipelineLimits::production_default();
    let bundle = matrixraft_baseline_raft_operational_evidence_bundle(
        replication_pipeline_peers(pipeline_limits),
        pipeline_limits,
        snapshot_lifecycle_peers(pipeline_limits),
        1_000,
        1,
        wal_lifecycle_status(),
    );
    let validation = matrixraft_validate_baseline_raft_operational_evidence_bundle(&bundle);
    assert!(
        validation.valid,
        "BaselineRaft operational evidence bundle failed validation: {validation:#?}"
    );
    println!("{}", serde_json::to_string_pretty(&bundle).unwrap());
}

fn replication_pipeline_peers(limits: PipelineLimits) -> Vec<PeerProgress> {
    let mut peer_2 = PeerProgress::new(2, 105, limits);
    peer_2.append_queue_depth = limits.max_inflights_replicate;
    peer_2.append_queue_max_depth = limits.max_inflights_replicate;
    peer_2.apply_queue_max_depth = limits.max_inflights_apply_task;
    peer_2.memory_backpressure_rejections = 1;
    peer_2.oversized_log_rejections = 1;
    peer_2.stale_term_rejections = 1;

    let mut peer_3 = PeerProgress::new(3, 105, limits);
    peer_3.reorder_queue_depth = 1;
    peer_3.reorder_entry_timeouts = 1;
    peer_3.reorder_dropped_packages = 1;
    peer_3.out_of_order_append_rejections = 1;
    peer_3.packet_loss_events = 2;
    peer_3.network_error_probe_transitions = 1;

    vec![peer_2, peer_3]
}

fn snapshot_lifecycle_peers(limits: PipelineLimits) -> Vec<PeerProgress> {
    let mut sender = PeerProgress::new(2, 105, limits);
    sender.snapshot_sending = true;
    sender.snapshot_send_attempts = 2;
    sender.snapshot_install_total_chunks = 8;
    sender.snapshot_install_progress_per_mille = 250;
    sender.snapshot_backpressure_rejections = 1;
    sender.snapshot_chunk_retry_count = 1;
    sender.snapshot_send_timeouts = 1;
    sender.snapshot_rate_limit_rejections = 1;
    sender.snapshot_during_membership_change = true;

    let mut installer = PeerProgress::new(3, 105, limits);
    installer.snapshot_installing = true;
    installer.snapshot_install_total_chunks = 4;
    installer.snapshot_install_progress_per_mille = 750;
    installer.snapshot_installed_index = 128;
    installer.snapshot_install_rolled_back = 1;
    installer.snapshot_rejoin_after_compacted_log = true;

    vec![sender, installer]
}

fn wal_lifecycle_status() -> WalLifecycleStatus {
    WalLifecycleStatus {
        segment_count: 3,
        active_segment_id: 7,
        first_retained_segment_id: 5,
        last_retained_segment_id: 7,
        total_bytes: 64 * 1024,
        active_segment_bytes: 8 * 1024,
        total_records: 128,
        first_sequence: 42,
        last_sequence: 169,
        first_log_index: 101,
        last_log_index: 228,
        released_segment_count: 4,
        slow_fsync_backpressure_observed: true,
        fsync_count: 0,
        slow_fsync_threshold_ms: 10,
        slow_fsync_count: 2,
        consecutive_slow_fsync_count: 1,
        max_fsync_elapsed_ms: 42,
        compacted_after_slow_fsync_count: 2,
    }
}
