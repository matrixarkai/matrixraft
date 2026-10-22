// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A bridge partition: both halves can reach a quorum, only one wins.
//!
//! The topology, rather than the rule. B reaches A and C; **A and C cannot reach
//! each other**. In a three-voter group that leaves each outer node able to count
//! two of three -- itself plus the bridge -- so connectivity does not decide the
//! election. The bridge's one-vote-per-term rule does, and
//! `one_vote_per_term_is_what_makes_a_bridge_safe.rs` pins that rule in
//! isolation.
//!
//! This builds the topology it protects, which nothing did before: every other
//! partition in the suite goes through `set_node_healthy`, which is **per-node
//! health** and cannot express "A cannot reach C but B can reach both".
//!
//! How it is built: three `RaftCluster`s, each carrying the **whole membership**
//! but driving only its own local node -- which is how a real deployment looks --
//! wired together with one `InMemoryRaftTransport` per node. A transport only
//! routes to the peers registered in it, so the reachability is asymmetric by
//! construction rather than by a flag.

use matrixraft::{
    ClusterRaftTransport, InMemoryRaftTransport, LogId, Peer, RaftCluster, ReplicaRole, Transport,
    VoteRequest,
};
use std::sync::{Arc, Mutex};

const GROUP: u64 = 97;
const A: u64 = 1;
const BRIDGE: u64 = 2;
const C: u64 = 3;
/// Above the term the groups settle on, so a vote is not answered `known_leader`.
const TERM: u64 = 7;
/// Two of three voters.
const QUORUM: usize = 2;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 54_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 55_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

/// One process's view: the whole membership, started.
fn node_process() -> Arc<Mutex<RaftCluster>> {
    let cluster = RaftCluster::new(
        GROUP,
        Default::default(),
        vec![peer(A), peer(BRIDGE), peer(C)],
    )
    .expect("build a process cluster");
    let cluster = Arc::new(Mutex::new(cluster));
    cluster.lock().expect("lock").start().expect("start");
    cluster
}

fn vote_request(candidate: u64) -> VoteRequest {
    VoteRequest {
        group_id: GROUP,
        term: TERM,
        candidate_id: candidate,
        last_log_id: Some(LogId { term: 1, index: 1 }),
        pre_vote: false,
        force: false,
    }
}

struct Bridge {
    a: Arc<Mutex<RaftCluster>>,
    c: Arc<Mutex<RaftCluster>>,
    /// A's view of the network: the bridge only.
    from_a: InMemoryRaftTransport,
    /// C's view: the bridge only.
    from_c: InMemoryRaftTransport,
    /// The bridge's view: both outer nodes.
    from_bridge: InMemoryRaftTransport,
}

fn bridge_topology() -> Bridge {
    let (a, b, c) = (node_process(), node_process(), node_process());
    let handler_a: Arc<dyn Transport + Send + Sync> =
        Arc::new(ClusterRaftTransport::new(Arc::clone(&a)));
    let handler_b: Arc<dyn Transport + Send + Sync> =
        Arc::new(ClusterRaftTransport::new(Arc::clone(&b)));
    let handler_c: Arc<dyn Transport + Send + Sync> =
        Arc::new(ClusterRaftTransport::new(Arc::clone(&c)));

    let from_a = InMemoryRaftTransport::new();
    from_a
        .register_handler(BRIDGE, Arc::clone(&handler_b))
        .expect("A reaches the bridge");

    let from_c = InMemoryRaftTransport::new();
    from_c
        .register_handler(BRIDGE, Arc::clone(&handler_b))
        .expect("C reaches the bridge");

    let from_bridge = InMemoryRaftTransport::new();
    from_bridge
        .register_handler(A, handler_a)
        .expect("the bridge reaches A");
    from_bridge
        .register_handler(C, handler_c)
        .expect("the bridge reaches C");

    Bridge {
        a,
        c,
        from_a,
        from_c,
        from_bridge,
    }
}

#[test]
fn the_topology_is_actually_a_bridge() {
    // The premise, and without it the rest proves nothing: the outer nodes can
    // each reach the bridge and NOT each other, while the bridge reaches both.
    let net = bridge_topology();

    assert!(
        net.from_a.vote(BRIDGE, vote_request(A)).is_ok(),
        "A must reach the bridge"
    );
    assert!(
        net.from_c.vote(BRIDGE, vote_request(C)).is_ok(),
        "C must reach the bridge"
    );
    assert!(
        net.from_a.vote(C, vote_request(A)).is_err(),
        "A must NOT reach C -- that is what makes this a bridge and not a mesh"
    );
    assert!(
        net.from_c.vote(A, vote_request(C)).is_err(),
        "nor C reach A"
    );
    assert!(
        net.from_bridge.vote(A, vote_request(BRIDGE)).is_ok()
            && net.from_bridge.vote(C, vote_request(BRIDGE)).is_ok(),
        "while the bridge reaches both halves"
    );
}

#[test]
fn only_one_half_collects_a_quorum_in_a_term() {
    let net = bridge_topology();

    // A campaigns: its own vote, plus the bridge's, which it can reach.
    let a_self = net
        .a
        .lock()
        .expect("lock A")
        .vote_to(A, vote_request(A))
        .expect("A answers itself");
    let a_bridge = net
        .from_a
        .vote(BRIDGE, vote_request(A))
        .expect("the bridge answers A");
    let a_votes = [a_self.vote_granted, a_bridge.vote_granted]
        .iter()
        .filter(|granted| **granted)
        .count();

    // C campaigns in the SAME term, reaching the same bridge.
    let c_self = net
        .c
        .lock()
        .expect("lock C")
        .vote_to(C, vote_request(C))
        .expect("C answers itself");
    let c_bridge = net
        .from_c
        .vote(BRIDGE, vote_request(C))
        .expect("the bridge answers C");
    let c_votes = [c_self.vote_granted, c_bridge.vote_granted]
        .iter()
        .filter(|granted| **granted)
        .count();

    assert_eq!(
        a_bridge.reason, "vote_granted",
        "the bridge votes for whoever asks first"
    );
    assert_eq!(
        c_bridge.reason, "already_voted",
        "and refuses the other half in the same term -- the whole safety argument"
    );
    assert_eq!(a_votes, QUORUM, "A reaches a quorum");
    assert!(
        c_votes < QUORUM,
        "C does not, despite being able to reach a quorum's worth of nodes: {c_votes}"
    );
}
