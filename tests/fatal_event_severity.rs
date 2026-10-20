// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A node that refuses a request is not reporting a fatal event.
//!
//! `record_runtime_result` raised a **fatal** blocker for every errored step
//! whose operation allowed one, and `fatal_on_step_error` allows one for
//! everything except install-snapshot, read-index and membership. So the most
//! ordinary client mistake there is -- a write sent to a follower, which comes
//! back `NotLeader` -- was recorded with severity `Fatal`. Measured on the
//! unfixed runtime, node 2 of a three-voter group, 25 such writes:
//!
//! ```text
//! before: ready=true  fatal=false blockers=0  fatal_count=0
//! after:  ready=false fatal=true  blockers=25 fatal_count=25
//!    first blocker: id=propose:node 1 is not the leader severity=Fatal
//! ```
//!
//! There is a `RustRaftFatalEvents` alert in `src/metrics.rs` watching that
//! count, so the signal pages somebody for a client sending a write to the
//! wrong node.
//!
//! **Severity is all that moves here.** The refusal is still recorded as an
//! ordinary blocker and the error still reaches the caller unchanged; nothing
//! is hidden. What this file does not change is the other half of the
//! measurement above: `blockers` is appended to for the life of the group and
//! never emptied, so `ready` -- which is `blockers.is_empty()` -- stays false
//! once anything has been refused, and the vector grows without bound while
//! being cloned whole on every `status()` call. That is a question about what
//! the report means, between a history and a current state, and it is left
//! open deliberately rather than answered here. The assertions below state the
//! retained count explicitly so that staying open is visible rather than
//! implied.

use matrixraft::{Config, NodeOptions, NodeRuntime, Peer, ReplicaRole};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const GROUP_ID: u64 = 77;
const REFUSED_WRITES: usize = 25;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 12_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 13_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "matrixraft-fatal-severity-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn options(base: &Path, node_id: u64) -> NodeOptions {
    NodeOptions {
        group_id: GROUP_ID,
        node_id,
        raft_addr: format!("127.0.0.1:{}", 12_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 13_000 + node_id),
        wal_dir: base.join("wal").to_string_lossy().into_owned(),
        snapshot_dir: base.join("snapshot").to_string_lossy().into_owned(),
        role: ReplicaRole::Voter,
        config: Config::default(),
        peers: vec![peer(1), peer(2), peer(3)],
    }
}

#[test]
fn writes_sent_to_a_follower_are_not_fatal_events() {
    let base = dir("follower");
    let _ = std::fs::remove_dir_all(&base);
    // Node 2 of a three-voter group, so it is not the leader and every
    // proposal it is handed comes back `NotLeader`.
    let mut runtime = NodeRuntime::create(options(&base, 2)).expect("create the group");
    runtime.start().expect("start it");

    let before = runtime.status().expect("status").fatal_blocker_report;
    assert!(
        !before.fatal && before.fatal_count == 0,
        "a freshly started group has nothing fatal about it: {before:?}"
    );

    let mut refused = 0_usize;
    for _ in 0..REFUSED_WRITES {
        if runtime.propose(b"to-a-follower".to_vec()).is_err() {
            refused += 1;
        }
    }
    assert_eq!(
        refused, REFUSED_WRITES,
        "every one of these has to be refused for the rest to say anything"
    );

    let after = runtime.status().expect("status").fatal_blocker_report;
    assert_eq!(
        after.fatal_count,
        0,
        "a write sent to a follower is the client addressing the wrong node, not the \
         group failing: {:?}",
        after
            .blockers
            .iter()
            .map(|blocker| (blocker.id.clone(), blocker.severity))
            .collect::<Vec<_>>()
    );
    assert!(
        !after.fatal,
        "and so the report must not be flagged fatal, because an alert watches that"
    );

    // Still recorded, and the caller was still told: severity is the only
    // thing this change moves.
    assert_eq!(
        after.blocker_count, REFUSED_WRITES as u64,
        "the refusals are still recorded as ordinary blockers, all {REFUSED_WRITES} of them"
    );
    assert!(
        after
            .blockers
            .iter()
            .all(|blocker| blocker.id.contains("is not the leader")),
        "and they still say what was refused"
    );
    let _ = std::fs::remove_dir_all(&base);
}
