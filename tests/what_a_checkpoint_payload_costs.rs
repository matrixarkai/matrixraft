// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a checkpoint snapshot's payload costs, and what reads it.
//!
//! `RaftCluster::checkpoint_snapshot` fills a snapshot's payload with the
//! node's log. It did that as JSON, and now does it as protobuf. Measured:
//!
//! ```text
//!  commands   bytes each   command bytes      as JSON   as protobuf
//!        10          9 B              90        1,065           225
//!       100          9 B             900        9,977         2,115
//!       100        300 B          30,000      126,377        31,415
//!        10       4096 B          40,960      164,545        41,115
//! ```
//!
//! Per command byte, **11.09x -> 2.35x** for small commands and
//! **4.02x -> 1.004x** for large ones.
//!
//! Both JSON costs had a cause and an alternative already in this crate:
//!
//! - `serde_json` writes a `Vec<u8>` as decimal numbers, so a payload byte of
//!   value 120 spelled `120,`. That was the 4.0x, and `wal.rs`'s own
//!   `costs_a_third_of_what_a_number_array_costs` measures the same thing from
//!   the other side.
//! - A JSON entry envelope of about 90 bytes, which is what took 9-byte
//!   commands to 11.1x.
//!
//! An entry now costs exactly **21 bytes at a 9-byte payload** -- 9 of payload
//! and 12 of envelope -- which the marginal identity below asserts rather than
//! divides for, since it is the difference between two measured points.
//!
//! **The payload is also decodable now**, which it was not: `matrixraft_decode_log`
//! reads back what `matrixraft_encode_log` wrote. The JSON form offered that in
//! principle and nothing took it up; this makes the bytes a format rather than
//! a cost.
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
    matrixraft_decode_log, ApplySnapshotFence, Config, GenericSnapshot, LogId, Peer, RaftCluster,
    ReplicaRole, SnapshotMetadata,
};

const GROUP: u64 = 9;

/// Measured, exact: the same proposals always produce the same payload.
const PAYLOAD_10_X_9: usize = 225;
const PAYLOAD_100_X_9: usize = 2_115;
const PAYLOAD_100_X_300: usize = 31_415;
const PAYLOAD_10_X_4096: usize = 41_115;

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
fn a_checkpoint_payload_costs_its_commands_and_twelve_bytes_an_entry() {
    let small_few = payload_bytes(10, 9);
    let small_many = payload_bytes(100, 9);
    let medium = payload_bytes(100, 300);
    let large = payload_bytes(10, 4_096);

    assert_eq!(small_few, PAYLOAD_10_X_9, "the 10 x 9 point moved");
    assert_eq!(small_many, PAYLOAD_100_X_9, "the 100 x 9 point moved");
    assert_eq!(medium, PAYLOAD_100_X_300, "the 100 x 300 point moved");
    assert_eq!(large, PAYLOAD_10_X_4096, "the 10 x 4096 point moved");

    // The asymptote, where the per-entry envelope is small beside the payload:
    // a payload byte costs a payload byte.
    assert_eq!(
        large * 100 / (10 * 4_096),
        100,
        "a large command should cost about 1.00x in the payload, got {:.4}x",
        large as f64 / (10.0 * 4_096.0)
    );

    // And the small-command figure, where the envelope still shows.
    assert_eq!(
        small_many * 100 / (100 * 9),
        235,
        "a 9-byte command should cost about 2.35x in the payload, got {:.4}x",
        small_many as f64 / 900.0
    );

    // The marginal cost of an entry, as a difference between two measured
    // points rather than a division -- so it is exact, and it is the figure
    // that moves if the entry's shape changes rather than its payload's.
    let marginal = (small_many - small_few) / 90;
    assert_eq!(
        marginal, 21,
        "an entry with a 9-byte payload costs {marginal} bytes, not the 9 of payload and 12 of envelope this file describes"
    );

    // The claim that survives any re-pinning: the payload is now within a
    // small multiple of the bytes it carries, where a JSON number array could
    // not be under about four times them. If this fails, the encoding went
    // back.
    assert!(
        large < 10 * 4_096 * 3 / 2,
        "a 40,960-byte log produced a {large}-byte payload, over 1.5x, which is the number-array regime this left"
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

/// The payload reads back as the log it was written from.
///
/// New with the protobuf form. The JSON payload was decodable in principle and
/// nothing decoded it; this makes the bytes a format rather than only a cost,
/// and asserts it for every byte value and either `is_command`, since those are
/// where an encoder gets a field wrong and a readable fixture cannot see it.
#[test]
fn a_checkpoint_payload_reads_back_as_the_log_it_came_from() {
    let (mut cluster, leader) = started();
    let mut sent: Vec<Vec<u8>> = Vec::new();
    for length in [0usize, 1, 2, 3, 127, 128, 255, 256] {
        let command: Vec<u8> = (0..length).map(|index| (index % 256) as u8).collect();
        cluster.propose(command.clone()).expect("propose");
        sent.push(command);
    }

    let snapshot = cluster
        .checkpoint_snapshot(leader, "roundtrip-1")
        .expect("the checkpoint is produced");
    let entries = matrixraft_decode_log(&snapshot.payload).expect("the payload decodes");

    let commands: Vec<Vec<u8>> = entries
        .iter()
        .filter(|entry| entry.is_command)
        .map(|entry| entry.payload.clone())
        .collect();
    assert_eq!(
        commands, sent,
        "the commands did not survive the round trip through the payload"
    );

    // And the log ids are intact and in order, which a payload that merely
    // happened to decode could still get wrong.
    let indices: Vec<u64> = entries.iter().map(|entry| entry.log_id.index).collect();
    let mut sorted = indices.clone();
    sorted.sort_unstable();
    assert_eq!(indices, sorted, "the entries came back out of order");
    assert!(
        indices.windows(2).all(|pair| pair[0] != pair[1]),
        "the payload holds a duplicated index: {indices:?}"
    );
}
