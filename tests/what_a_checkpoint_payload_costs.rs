// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a checkpoint snapshot's payload costs, and what reads it.
//!
//! `RaftCluster::checkpoint_snapshot` fills a snapshot's payload with
//! `serde_json::to_vec(&node.log)` -- the whole log, as JSON. Measured:
//!
//! ```text
//!  commands   bytes each   command bytes   payload bytes   per command byte
//!        10          9 B              90           1,065            11.83x
//!       100          9 B             900           9,977            11.09x
//!       100        300 B          30,000         126,377             4.21x
//!        10       4096 B          40,960         164,545             4.02x
//! ```
//!
//! **4.0x asymptotically, and 11.1x for small commands.** Two costs, each with
//! a known cause and a known alternative already in this crate:
//!
//! - `serde_json` writes a `Vec<u8>` as decimal numbers, so a payload byte of
//!   value 120 spells `120,`. That is the 4.0x, and `wal.rs`'s own
//!   `costs_a_third_of_what_a_number_array_costs` measures the same thing from
//!   the other side. The `wal_entry_payloads` codec beside it costs ~1.37x and
//!   is applied to the WAL record and to `AppendEntriesRequest.entries` -- and
//!   not here.
//! - A JSON entry envelope of about 64 bytes per entry, which is what takes
//!   9-byte commands to 11.1x. The WAL record's protobuf entry is 10 bytes.
//!
//! So a protobuf log would be roughly 2.1x for small commands and 1.0x for
//! large ones, against 11.1x and 4.0x.
//!
//! ## And the payload is opaque: nothing reads it back
//!
//! This is the part that decides what the figures above mean, so it is asserted
//! rather than described. `a_snapshot_installs_with_a_payload_that_is_not_a_log`
//! installs a snapshot whose payload is a short ASCII string and shows the
//! install succeeds and advances the receiver -- because `install_snapshot`
//! uses `meta.last_log_id` and never looks at the bytes.
//!
//! Checked across both repositories when this was written: no decoder exists
//! for a snapshot payload in matrixraft, and none in the consumer either. The
//! payload is sliced into chunks, carried, length-checked against
//! `meta.payload_bytes`, and dropped.
//!
//! That is a reasonable contract for a library that carries a state machine it
//! does not own -- a real consumer supplies its own image. What this file pins
//! is the consequence: `checkpoint_snapshot` pays 4.0x to 11.1x filling that
//! payload with matrixraft's own log, and nothing in matrixraft reads it.
//! Whether that wants a cheaper encoding or a doc comment saying what the
//! payload is for is a decision about a public API, not a measurement, so this
//! file does not take it.
//!
//! ## What is not claimed
//!
//! - That the payload is unused by every consumer. It is public, undocumented,
//!   and a caller could be decoding it; that is exactly why the encoding is not
//!   changed here.
//! - Anything about compression, which nothing on this path does.

use matrixraft::{
    ApplySnapshotFence, Config, GenericSnapshot, LogId, Peer, RaftCluster, ReplicaRole,
    SnapshotMetadata,
};

const GROUP: u64 = 9;

/// Measured, exact: the same proposals always produce the same payload.
const PAYLOAD_10_X_9: usize = 1_065;
const PAYLOAD_100_X_9: usize = 9_977;
const PAYLOAD_100_X_300: usize = 126_377;
const PAYLOAD_10_X_4096: usize = 164_545;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 7000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 8000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn started() -> (RaftCluster, u64) {
    let mut cluster = RaftCluster::new(GROUP, Config::default(), vec![peer(1), peer(2), peer(3)])
        .expect("the cluster builds");
    cluster.start().expect("the cluster starts");
    let leader = cluster.leader_id().expect("a leader after start");
    (cluster, leader)
}

/// The payload a checkpoint produces after `count` proposals of `size` bytes.
fn payload_bytes(count: usize, size: usize) -> usize {
    let (mut cluster, leader) = started();
    for index in 0..count {
        cluster
            .propose(vec![b'x'; size])
            .unwrap_or_else(|err| panic!("proposal {index}: {err}"));
    }
    cluster
        .checkpoint_snapshot(leader, "cost-1")
        .expect("the checkpoint is produced")
        .payload
        .len()
}

#[test]
fn a_checkpoint_payload_costs_four_times_its_commands_and_eleven_for_small_ones() {
    let small_few = payload_bytes(10, 9);
    let small_many = payload_bytes(100, 9);
    let medium = payload_bytes(100, 300);
    let large = payload_bytes(10, 4_096);

    assert_eq!(small_few, PAYLOAD_10_X_9, "the 10 x 9 point moved");
    assert_eq!(small_many, PAYLOAD_100_X_9, "the 100 x 9 point moved");
    assert_eq!(medium, PAYLOAD_100_X_300, "the 100 x 300 point moved");
    assert_eq!(large, PAYLOAD_10_X_4096, "the 10 x 4096 point moved");

    // The asymptote, where the per-entry envelope is small beside the payload:
    // 4.0x, which is what a JSON number array costs.
    assert_eq!(
        large * 100 / (10 * 4_096),
        401,
        "a large command should cost about 4.01x in the payload, got {:.4}x",
        large as f64 / (10.0 * 4_096.0)
    );

    // And the small-command figure, where the envelope dominates.
    assert_eq!(
        small_many * 100 / (100 * 9),
        1_108,
        "a 9-byte command should cost about 11.09x in the payload, got {:.4}x",
        small_many as f64 / 900.0
    );

    // The claim that survives any re-pinning: a number array cannot cost less
    // than about four times its bytes, so the payload is never near 1x. If this
    // ever fails, the encoding changed and every figure above needs re-reading.
    assert!(
        large > 10 * 4_096 * 7 / 2,
        "a 40,960-byte log produced a {large}-byte payload, under 3.5x, so it is no longer a number array"
    );
}

/// The payload is opaque: a snapshot installs with bytes that are not a log.
///
/// The assertion the figures above depend on. If the payload were decoded into
/// a log, this would fail -- and the cost of encoding it would be the price of
/// something. It is not decoded: `install_snapshot` uses `meta.last_log_id` and
/// never looks at the bytes.
///
/// Two details copied from `record_builders_agree`, which hit both: the index
/// must be ABOVE what the group has already committed, because an install
/// refuses a snapshot that is not newer, and the fence has to continue the
/// snapshot rather than overlap it.
#[test]
fn a_snapshot_installs_with_a_payload_that_is_not_a_log() {
    let (mut cluster, leader) = started();
    for _ in 0..5 {
        cluster.propose(b"set a=1".to_vec()).expect("propose");
    }

    let committed = cluster
        .wal_record_for(leader)
        .expect("a record to read the committed index from")
        .hard_state
        .committed
        .expect("a committed index")
        .index;
    let snapshot_index = committed + 1;

    let payload = b"this is not a serialized log".to_vec();
    cluster
        .install_snapshot_to(
            leader,
            GenericSnapshot {
                group_id: GROUP,
                meta: SnapshotMetadata {
                    snapshot_id: "nonsense-1".to_string(),
                    last_log_id: LogId {
                        term: 1,
                        index: snapshot_index,
                    },
                    membership: vec![1, 2, 3],
                    members: Vec::new(),
                    payload_bytes: payload.len() as u64,
                },
                payload,
            },
            ApplySnapshotFence {
                applied_index: snapshot_index,
                commit_index: snapshot_index,
                installed_snapshot_index: snapshot_index,
                first_retained_log_index: snapshot_index + 1,
            },
        )
        .unwrap_or_else(|err| {
            panic!("installing a snapshot whose payload is not a log was refused: {err}")
        });

    // And it took effect, so the install was real rather than a no-op that
    // happened not to look at the payload because it did nothing at all.
    let record = cluster
        .wal_record_for(leader)
        .expect("a record after the install");
    assert_eq!(
        record.apply_snapshot_fence.installed_snapshot_index, snapshot_index,
        "the install did not take, so this proves nothing about the payload: {:?}",
        record.apply_snapshot_fence
    );
    assert_eq!(
        record
            .installed_snapshot
            .as_ref()
            .map(|meta| meta.snapshot_id.as_str()),
        Some("nonsense-1"),
        "the installed snapshot on the record is not the one with the nonsense payload"
    );
}
