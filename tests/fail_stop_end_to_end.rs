// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A node whose durable write fails is taken out of service, end to end.
//!
//! `after_durable_write` takes the write's **outcome** as an argument rather
//! than being driven by a real failure, and the comment on it says why: the
//! write could not be made to fail from a test. That was wrong twice over.
//!
//! First, a segment **roll** creates and opens a new file, so with descriptors
//! exhausted a real append fails -- which `tests/wal_failed_append.rs`
//! establishes at the WAL level. Second, I thought reaching a roll through a
//! `NodeRuntime` needed a thousand proposals, because the record count per
//! segment is hardcoded. It is not: the roll also triggers on **bytes**
//! (`wal_runtime.rs:699`), and `max_segment_bytes` comes from `Config`. One
//! byte, and the second proposal rolls.
//!
//! So this is the demonstration that was missing: a proposal lands, the
//! descriptors go, the next proposal's durable write fails for real, and the
//! node takes itself out of the quorum.
//!
//! Alone in its own binary, as the other descriptor test is: the limit is per
//! process. Descriptors are released before anything is asserted, so a failing
//! assertion can still write its message.

use matrixraft::{Config, NodeOptions, NodeRuntime, Peer, ReplicaRole};
use std::fs::File;
use std::time::{SystemTime, UNIX_EPOCH};

const GROUP_ID: u64 = 111;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 35_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 35_500 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

#[test]
fn a_node_whose_durable_write_fails_leaves_the_quorum() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a clock")
        .as_nanos();
    let base = std::env::temp_dir().join(format!(
        "matrixraft-failstop-{}-{nonce}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).expect("a base directory");

    let mut runtime = NodeRuntime::create(NodeOptions {
        group_id: GROUP_ID,
        node_id: 1,
        raft_addr: "127.0.0.1:35001".to_string(),
        snapshot_addr: "127.0.0.1:35501".to_string(),
        wal_dir: base.join("wal").to_string_lossy().into_owned(),
        snapshot_dir: base.join("snapshot").to_string_lossy().into_owned(),
        role: ReplicaRole::Voter,
        config: Config {
            // One byte a segment, so the second record has to roll -- and a
            // roll is the one durable write that can be made to fail.
            max_segment_bytes: 1,
            ..Config::default()
        },
        peers: vec![peer(1), peer(2), peer(3)],
    })
    .expect("create the group");
    runtime.start().expect("start it");

    // The first proposal lands: a roll needs a record already in the segment.
    runtime.propose(b"first".to_vec()).expect("the first write");
    let healthy_before = runtime
        .status()
        .expect("status")
        .peer_runtime
        .iter()
        .find(|peer| peer.node_id == 1)
        .map(|peer| peer.healthy);

    let probe_file = base.join("probe");
    File::create(&probe_file).expect("a file to hold open");
    let mut held: Vec<File> = Vec::new();
    while let Ok(file) = File::open(&probe_file) {
        held.push(file);
        if held.len() > 1_000_000 {
            break;
        }
    }
    let exhausted = held.len();

    // This one has to roll, and the roll cannot open a file.
    let refused = runtime.propose(b"second".to_vec());
    let healthy_after = runtime
        .status()
        .expect("status after")
        .peer_runtime
        .iter()
        .find(|peer| peer.node_id == 1)
        .map(|peer| peer.healthy);

    drop(held);

    assert!(
        exhausted > 16,
        "only {exhausted} descriptors were available, so nothing here can fail \
         for the reason this test is about"
    );
    assert_eq!(
        healthy_before,
        Some(true),
        "the node starts healthy, or the comparison below means nothing"
    );
    assert!(
        refused.is_err(),
        "the proposal's durable write had to fail: {refused:?}"
    );
    assert_eq!(
        healthy_after,
        Some(false),
        "a node whose durable write failed must stop counting as a serving \
         member -- this is `after_durable_write` firing for a real failure \
         rather than for an argument handed to it"
    );

    let _ = runtime.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}
