// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{AppendEntriesRequest, Config, LogEntry, LogId, Peer, RaftCluster, ReplicaRole};

fn peer(node_id: u64, role: ReplicaRole) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 9_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 10_000 + node_id),
        role,
        auto_promote: false,
    }
}

fn cluster() -> RaftCluster {
    RaftCluster::new(
        7,
        Config::default(),
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

#[test]
fn the_cluster_reorder_queue_grows_once_per_distinct_prev_index() {
    let mut c = cluster();
    c.start().expect("starts");

    // One accepted entry, so match_index == 1 and anything past it is out of order.
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

    const AHEAD: u64 = 64;
    const PAYLOAD: usize = 4096;
    let empty = bytes(&c);

    for k in 0..AHEAD {
        let prev = 2 + k * 2; // distinct, all beyond match_index
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
                        payload: vec![b'x'; PAYLOAD],
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
            "every rejection names the same next index, so a leader that honours \
             the hint never reaches this queue"
        );
    }

    let grown = bytes(&c);
    eprintln!(
        "cached={} appends  state={} bytes  ({} bytes per cached append, payload {})",
        depth(&c, 2),
        grown - empty,
        (grown - empty) / AHEAD as usize,
        PAYLOAD
    );
    assert_eq!(
        depth(&c, 2),
        AHEAD as usize,
        "one cached append per distinct prev index -- the queue has no window"
    );
    assert!(
        grown - empty > AHEAD as usize * PAYLOAD,
        "the cache keeps whole payloads, not just indexes: {} bytes of state for \
         {} rejected appends",
        grown - empty,
        AHEAD
    );

    // A successful append drains only the contiguous run, so the rest stays cached.
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
    let after_drain = depth(&c, 2);
    eprintln!(
        "after filling one hole: {} of {} cached appends remain",
        after_drain, AHEAD
    );
    assert_eq!(
        after_drain,
        AHEAD as usize - 1,
        "filling one hole drains the one cached append it unblocks; the other {} stay, because their prev indexes are still ahead of match_index",
        AHEAD - 1
    );

    // The operator lever that names this queue drains a different one.
    let expired = c.expire_peer_reorder_queue(2).expect("peer exists");
    assert_eq!(
        expired, 0,
        "expire_peer_reorder_queue reports the pipeline queue, which is empty here"
    );
    assert_eq!(
        depth(&c, 2),
        after_drain,
        "and it leaves the cluster-level queue untouched"
    );
}
