// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    AppendEntriesRequest, Config, LogEntry, LogId, Peer, PipelineLimits, RaftCluster, ReplicaRole,
};

fn peer(node_id: u64, role: ReplicaRole) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 9_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 10_000 + node_id),
        role,
        auto_promote: false,
    }
}

fn cluster(limits: PipelineLimits) -> RaftCluster {
    RaftCluster::new(
        7,
        Config::default().with_pipeline_limits(limits),
        vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
        ],
    )
    .expect("valid cluster")
}

// The queue is a private field, but RaftCluster derives Serialize, so the cached
// state is observable without changing production code.
fn cached(cluster: &RaftCluster) -> serde_json::Value {
    let v: serde_json::Value =
        serde_json::from_str(&serde_json::to_string(cluster).expect("cluster serializes"))
            .expect("json");
    v.get("reorder_queues").cloned().expect("field present")
}

fn depth(cluster: &RaftCluster, node: u64) -> usize {
    cached(cluster)
        .get(node.to_string())
        .and_then(|q| q.as_object().map(|m| m.len()))
        .unwrap_or(0)
}

fn bytes(cluster: &RaftCluster) -> usize {
    serde_json::to_string(&cached(cluster)).expect("json").len()
}

fn keys(cluster: &RaftCluster, node: u64) -> Vec<u64> {
    let mut keys: Vec<_> = cached(cluster)
        .get(node.to_string())
        .and_then(serde_json::Value::as_object)
        .into_iter()
        .flat_map(|queue| queue.keys())
        .map(|key| key.parse().expect("numeric log index"))
        .collect();
    keys.sort_unstable();
    keys
}

fn start_with_base(limits: PipelineLimits) -> RaftCluster {
    let mut c = cluster(limits);
    c.start().expect("starts");
    let base = c
        .append_entries_to(
            2,
            AppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![LogEntry {
                    log_id: LogId { term: 1, index: 1 },
                    payload: b"one".to_vec(),
                    is_command: true,
                }],
                leader_commit: 1,
                lease_epoch: 0,
            },
        )
        .expect("base append");
    assert!(base.success);
    assert_eq!(depth(&c, 2), 0, "nothing cached while appends are in order");
    c
}

fn reject_ahead(c: &mut RaftCluster, prev: u64, payload_bytes: usize) {
    let rejected = c
        .append_entries_to(
            2,
            AppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: Some(LogId {
                    term: 1,
                    index: prev,
                }),
                entries: vec![LogEntry {
                    log_id: LogId {
                        term: 1,
                        index: prev + 1,
                    },
                    payload: vec![b'x'; payload_bytes],
                    is_command: true,
                }],
                leader_commit: prev + 1,
                lease_epoch: 0,
            },
        )
        .expect("append is answered");
    assert!(!rejected.success, "an append past match_index is rejected");
    assert_eq!(
        rejected.rejection_hint,
        Some(2),
        "the sender is still told exactly which index is missing"
    );
}

#[test]
fn rejected_append_heap_stays_inside_the_replication_memory_budget() {
    const AHEAD: u64 = 64;
    const PAYLOAD: usize = 4096;
    const RETAINED_PER_REQUEST: usize = PAYLOAD + std::mem::size_of::<LogEntry>();
    let limits = PipelineLimits {
        reorder_window_size: 8,
        max_memory_replicate_log_bytes: (2 * RETAINED_PER_REQUEST) as u64,
        ..PipelineLimits::default()
    };
    let mut c = start_with_base(limits);
    let empty = bytes(&c);

    for k in 0..AHEAD {
        reject_ahead(&mut c, 2 + k * 2, PAYLOAD);
    }

    let grown = bytes(&c);
    assert_eq!(
        depth(&c, 2),
        2,
        "two payloads consume the entire configured byte budget"
    );
    assert_eq!(keys(&c, 2), vec![2, 4], "the nearest requests survive");
    assert!(
        grown - empty < 4 * PAYLOAD,
        "serialized retained state must stay proportional to the two cached payloads"
    );

    let fill = c
        .append_entries_to(
            2,
            AppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: Some(LogId { term: 1, index: 1 }),
                entries: vec![LogEntry {
                    log_id: LogId { term: 1, index: 2 },
                    payload: b"two".to_vec(),
                    is_command: true,
                }],
                leader_commit: 2,
                lease_epoch: 0,
            },
        )
        .expect("append that fills the hole");
    assert!(fill.success);
    assert_eq!(
        keys(&c, 2),
        vec![4],
        "filling the first hole drains the cached request it releases"
    );

    let expired = c.expire_peer_reorder_queue(2).expect("peer exists");
    assert_eq!(
        expired, 0,
        "expire_peer_reorder_queue reports the pipeline queue, which is empty here"
    );
    assert_eq!(
        depth(&c, 2),
        1,
        "the pipeline queue control does not alter this separately bounded cache"
    );
}

#[test]
fn zero_payload_entries_still_count_against_the_memory_budget() {
    let limits = PipelineLimits {
        reorder_window_size: 8,
        max_memory_replicate_log_bytes: (2 * std::mem::size_of::<LogEntry>()) as u64,
        ..PipelineLimits::default()
    };
    let mut c = start_with_base(limits);
    let prev = 20;
    let entries = (1..=8)
        .map(|offset| LogEntry {
            log_id: LogId {
                term: 1,
                index: prev + offset,
            },
            payload: Vec::new(),
            is_command: true,
        })
        .collect();
    let rejected = c
        .append_entries_to(
            2,
            AppendEntriesRequest {
                group_id: 7,
                term: 1,
                leader_id: 1,
                prev_log_id: Some(LogId {
                    term: 1,
                    index: prev,
                }),
                entries,
                leader_commit: prev + 8,
                lease_epoch: 0,
            },
        )
        .expect("append is answered");
    assert!(!rejected.success);
    assert_eq!(
        depth(&c, 2),
        0,
        "entry storage exceeds the budget even when every payload is empty"
    );
}

#[test]
fn a_full_count_window_keeps_the_requests_nearest_the_hole() {
    let limits = PipelineLimits {
        reorder_window_size: 3,
        max_memory_replicate_log_bytes: u64::MAX,
        ..PipelineLimits::default()
    };
    let mut c = start_with_base(limits);
    for prev in [100, 200, 300, 400] {
        reject_ahead(&mut c, prev, 1);
    }
    assert_eq!(keys(&c, 2), vec![100, 200, 300]);

    reject_ahead(&mut c, 50, 1);
    assert_eq!(
        keys(&c, 2),
        vec![50, 100, 200],
        "a nearer request displaces the farthest retained request"
    );
}

#[test]
fn a_restored_queue_rebuilds_its_payload_budget() {
    const PAYLOAD: usize = 4096;
    const RETAINED_PER_REQUEST: usize = PAYLOAD + std::mem::size_of::<LogEntry>();
    let limits = PipelineLimits {
        reorder_window_size: 8,
        max_memory_replicate_log_bytes: (2 * RETAINED_PER_REQUEST) as u64,
        ..PipelineLimits::default()
    };
    let mut c = start_with_base(limits);
    reject_ahead(&mut c, 2, PAYLOAD);
    reject_ahead(&mut c, 4, PAYLOAD);
    assert_eq!(keys(&c, 2), vec![2, 4]);

    let encoded = serde_json::to_string(&c).expect("serialize cluster");
    let mut restored: RaftCluster = serde_json::from_str(&encoded).expect("restore cluster");
    reject_ahead(&mut restored, 3, PAYLOAD);
    let retained = keys(&restored, 2);
    assert!(
        retained.contains(&2),
        "the request nearest the hole must survive"
    );
    assert!(
        !retained.contains(&4) && retained.len() <= 2,
        "the restored byte total must evict the farther payload and refuse growth: {retained:?}"
    );
}

#[test]
fn disabling_reordering_keeps_rejected_requests_out_of_the_cache() {
    let limits = PipelineLimits {
        enable_reorder_queue: false,
        ..PipelineLimits::default()
    };
    let mut c = start_with_base(limits);
    reject_ahead(&mut c, 2, 4096);
    assert_eq!(depth(&c, 2), 0);
    assert_eq!(cached(&c), serde_json::json!({}));
}
