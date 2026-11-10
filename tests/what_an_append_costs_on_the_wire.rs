// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What an AppendEntries costs on the wire, now that the wire is protobuf.
//!
//! The transport wrote a newline-delimited JSON envelope with entry payloads as
//! base64. It writes a framed protobuf message. Measured, for the same request:
//!
//! ```text
//!  entries   payload     as JSON    as framed   JSON/framed
//!        0       0 B         193           34         5.68x
//!      100       0 B       6,586        1,039         6.34x
//!      100       9 B       7,786        2,139         3.64x
//!      100     300 B      46,586       31,442         1.48x
//!      100    3000 B     406,586      301,442         1.35x
//! ```
//!
//! Three figures carry it.
//!
//! **A payload byte costs exactly one byte.** Between the 300-byte and
//! 3000-byte points the extra wire bytes and the extra payload are both
//! 270,000 -- asserted as an integer identity, where the JSON form was 4/3 of
//! every byte because the payloads were base64.
//!
//! **An entry's envelope is 10 bytes**, against 63.93 as JSON. That 63.93 was
//! about what the whole protobuf WAL record weighs, for a log entry carrying a
//! log id, a payload and a flag.
//!
//! **A request with no entries is 34 bytes**, against 193.
//!
//! So a leader replicating 9-byte commands sends **3.64x less** than it did.
//! The win is largest where the traffic is smallest -- a heartbeat-shaped
//! append with no entries is 6.3x cheaper -- and smallest on bulk payloads,
//! where there was never much envelope to remove and base64's 4/3 is the whole
//! of it.
//!
//! ## Why the framing changed with the encoding
//!
//! A JSON object is newline-safe and a protobuf message is not: its bytes can
//! be any value, `0x0A` among them. A reader cannot look for a newline, so the
//! frame carries its own length -- four bytes of magic, a little-endian `u32`
//! length, a little-endian `u32` CRC-32C over the body, then the body. That is
//! the same frame shape the WAL record and the checkpoint use.
//!
//! ## The order a cluster upgrades in
//!
//! A receiver reads **either** shape, keyed on the first byte: a frame opens
//! with `M` and a JSON object with `{`. And it **answers in the shape it was
//! asked**, which is what makes the staged upgrade work -- replying framed to a
//! JSON request would break every old sender the moment a receiver was
//! upgraded.
//!
//! So: **receivers first, then senders.** `transport_contract`'s
//! `a_peer_that_sends_no_cluster_id_is_served_as_any_cluster` is an old sender
//! against the current server, on the wire, and it passes unchanged.
//!
//! ## What is not measured here
//!
//! - The durable peer path, which was already prost.
//! - Compression and TLS framing; nothing here applies either.
//! - Every figure is for `is_command: true`. The WAL's entry projection skips
//!   that field when false, so these are per-command figures rather than
//!   per-entry ones -- the same field that once made one WAL measurement read
//!   29 bytes where another read 48.

use matrixraft::{
    matrixraft_decode_rpc_envelope, matrixraft_decode_rpc_response, matrixraft_encode_rpc_envelope,
    matrixraft_encode_rpc_response, AppendEntriesRequest, AppendEntriesResponse,
    InstallSnapshotRequest, InstallSnapshotResponse, LogEntry, LogId, Peer, ReadIndexRequest,
    ReadIndexResponse, ReplicaRole, SnapshotChunk, SnapshotMetadata, SnapshotState,
    TcpRaftRpcResult, TcpRaftTransportEnvelope, TcpRaftTransportRequest, TcpRaftTransportResponse,
    VoteRequest, VoteResponse,
};

/// Measured, exact: the same request always produces the same bytes.
const FRAMED_NO_ENTRIES: usize = 34;
const FRAMED_100_EMPTY: usize = 1_039;
const FRAMED_100_NINE: usize = 2_139;
const FRAMED_100_300: usize = 31_442;
const FRAMED_100_3000: usize = 301_442;

/// The JSON the wire used to carry, kept as the comparison this file is about.
const JSON_100_NINE: usize = 7_786;

const ENTRIES: u64 = 100;

fn entry(index: u64, payload_len: usize) -> LogEntry {
    LogEntry {
        log_id: LogId { term: 3, index },
        payload: vec![b'x'; payload_len],
        is_command: true,
    }
}

fn append(count: u64, payload_len: usize) -> AppendEntriesRequest {
    AppendEntriesRequest {
        group_id: 9,
        term: 3,
        leader_id: 1,
        prev_log_id: Some(LogId { term: 3, index: 0 }),
        entries: (1..=count).map(|index| entry(index, payload_len)).collect(),
        leader_commit: count,
        lease_epoch: 7,
    }
}

fn envelope(count: u64, payload_len: usize) -> TcpRaftTransportEnvelope {
    TcpRaftTransportEnvelope {
        cluster_id: 1,
        network_epoch: 0,
        user_payload: None,
        request: TcpRaftTransportRequest::AppendEntries {
            target: 2,
            request: append(count, payload_len),
        },
    }
}

/// What the transport puts on the socket for this append.
fn framed_bytes(count: u64, payload_len: usize) -> usize {
    matrixraft_encode_rpc_envelope(&envelope(count, payload_len))
        .expect("the envelope encodes")
        .len()
}

/// What it used to put there: JSON and a newline.
fn json_bytes(count: u64, payload_len: usize) -> usize {
    serde_json::to_vec(&envelope(count, payload_len))
        .expect("the envelope encodes as JSON")
        .len()
        + 1
}

#[test]
fn an_append_costs_a_small_envelope_and_exactly_its_payload() {
    let empty = framed_bytes(0, 0);
    let hundred_empty = framed_bytes(ENTRIES, 0);
    let hundred_nine = framed_bytes(ENTRIES, 9);
    let at_300 = framed_bytes(ENTRIES, 300);
    let at_3000 = framed_bytes(ENTRIES, 3_000);

    assert_eq!(empty, FRAMED_NO_ENTRIES, "the empty point moved");
    assert_eq!(hundred_empty, FRAMED_100_EMPTY, "the 100-entry point moved");
    assert_eq!(hundred_nine, FRAMED_100_NINE, "the 9-byte point moved");
    assert_eq!(at_300, FRAMED_100_300, "the 300-byte point moved");
    assert_eq!(at_3000, FRAMED_100_3000, "the 3000-byte point moved");

    // A payload byte costs a payload byte. An integer identity, where the JSON
    // form could only ever be 4/3 of one because the payloads were base64.
    let extra_wire = at_3000 - at_300;
    let extra_payload = (3_000 - 300) * ENTRIES as usize;
    assert_eq!(
        extra_wire, extra_payload,
        "a payload byte must cost exactly one byte: {extra_wire} wire bytes for {extra_payload} payload bytes is {:.6}, not 1.0",
        extra_wire as f64 / extra_payload as f64
    );

    // The per-entry envelope, as a difference between two MEASURED points
    // rather than between two of this file's constants -- which would subtract
    // at compile time and assert nothing about the code.
    let per_entry = (hundred_empty - empty) / ENTRIES as usize;
    assert_eq!(
        per_entry, 10,
        "an entry's envelope is {per_entry} bytes, not the 10 this file describes"
    );
}

#[test]
fn the_wire_carries_three_and_a_half_times_less_than_the_json_did() {
    let framed = framed_bytes(ENTRIES, 9);
    let json = json_bytes(ENTRIES, 9);
    assert_eq!(json, JSON_100_NINE, "the JSON comparison point moved");
    assert_eq!(framed, FRAMED_100_NINE, "the framed point moved");

    // 3.64x, as hundredths so it is an integer.
    assert_eq!(
        json * 100 / framed,
        364,
        "100 appends of a 9-byte command should be 3.64x smaller framed, got {:.2}x",
        json as f64 / framed as f64
    );

    // And the shape of the win: biggest where there is least to send. A
    // heartbeat-shaped append with no entries is the extreme, and bulk payloads
    // the other end, where base64's 4/3 is all there was to remove.
    let heartbeat = json_bytes(0, 0) * 100 / framed_bytes(0, 0);
    let bulk = json_bytes(ENTRIES, 3_000) * 100 / framed_bytes(ENTRIES, 3_000);
    assert!(
        heartbeat > bulk,
        "the saving should be larger on an empty append ({heartbeat}) than on a bulk one ({bulk})"
    );
    assert_eq!(heartbeat, 567, "the empty-append ratio moved");
    assert_eq!(bulk, 134, "the bulk ratio moved");
}

/// Every request and response variant survives the round trip.
///
/// Written out rather than sampled, because a oneof arm that is never encoded
/// is an arm whose tag nobody has checked -- and a wrong tag is invisible until
/// a peer sends that variant.
#[test]
fn every_rpc_variant_survives_the_round_trip() {
    let meta = SnapshotMetadata {
        snapshot_id: "snap-1".to_string(),
        last_log_id: LogId { term: 3, index: 7 },
        membership: vec![1, 2, 3],
        members: vec![Peer {
            node_id: 2,
            raft_addr: "127.0.0.1:1".to_string(),
            snapshot_addr: "127.0.0.1:2".to_string(),
            role: ReplicaRole::Witness,
            auto_promote: true,
        }],
        payload_bytes: 4,
    };

    let requests = vec![
        TcpRaftTransportRequest::AppendEntries {
            target: 2,
            request: append(3, 9),
        },
        TcpRaftTransportRequest::Vote {
            target: 3,
            request: VoteRequest {
                group_id: 9,
                term: 4,
                candidate_id: 1,
                last_log_id: Some(LogId { term: 3, index: 9 }),
                pre_vote: true,
                force: true,
            },
        },
        TcpRaftTransportRequest::InstallSnapshot {
            target: 4,
            request: InstallSnapshotRequest {
                group_id: 9,
                term: 5,
                leader_id: 1,
                chunk: SnapshotChunk {
                    meta: meta.clone(),
                    offset: 64,
                    data: vec![0, 1, 2, 255],
                    done: true,
                },
            },
        },
        TcpRaftTransportRequest::ReadIndex {
            target: 5,
            request: ReadIndexRequest {
                group_id: 9,
                requester_id: 1,
                min_commit_index: 12,
                allow_lease_read: true,
            },
        },
    ];

    for request in &requests {
        let value = TcpRaftTransportEnvelope {
            cluster_id: 7,
            network_epoch: 11,
            user_payload: Some("hello".to_string()),
            request: request.clone(),
        };
        let framed = matrixraft_encode_rpc_envelope(&value).expect("encodes");
        let back = matrixraft_decode_rpc_envelope(&framed).expect("decodes");
        assert_eq!(back, value, "a request variant did not survive");
    }

    // And a batch, which is the recursive arm -- including one nested inside
    // another, since a flat batch would not exercise the recursion.
    let batch = TcpRaftTransportEnvelope {
        cluster_id: 7,
        network_epoch: 11,
        user_payload: None,
        request: TcpRaftTransportRequest::Batch {
            requests: vec![
                requests[0].clone(),
                TcpRaftTransportRequest::Batch {
                    requests: vec![requests[1].clone(), requests[3].clone()],
                },
                requests[2].clone(),
            ],
        },
    };
    let framed = matrixraft_encode_rpc_envelope(&batch).expect("encodes");
    assert_eq!(
        matrixraft_decode_rpc_envelope(&framed).expect("decodes"),
        batch,
        "a nested batch did not survive"
    );

    let responses = vec![
        TcpRaftTransportResponse::AppendEntries(TcpRaftRpcResult {
            ok: Some(AppendEntriesResponse {
                term: 2,
                success: true,
                match_index: 9,
                rejection_hint: Some(3),
                rejected_index: Some(4),
                require_snapshot: Some(5),
                snapshot_state: SnapshotState::Receiving,
                lease_confirmation_epoch: 6,
                lease_duration_ms: 7,
            }),
            error: None,
        }),
        TcpRaftTransportResponse::Vote(TcpRaftRpcResult {
            ok: Some(VoteResponse {
                term: 4,
                vote_granted: false,
                reason: "already voted".to_string(),
            }),
            error: None,
        }),
        TcpRaftTransportResponse::InstallSnapshot(TcpRaftRpcResult {
            ok: Some(InstallSnapshotResponse {
                term: 5,
                accepted: true,
                next_offset: 128,
                committed_index: 11,
                reason: String::new(),
                throttled: true,
            }),
            error: None,
        }),
        TcpRaftTransportResponse::ReadIndex(TcpRaftRpcResult {
            ok: Some(ReadIndexResponse {
                safe: true,
                read_index: 12,
                lease_read: true,
                reason: String::new(),
            }),
            error: None,
        }),
        // An error arm, since a refusal travels this way and `ok` is then None.
        TcpRaftTransportResponse::Vote(TcpRaftRpcResult {
            ok: None,
            error: Some("refused".to_string()),
        }),
    ];

    for response in &responses {
        let framed = matrixraft_encode_rpc_response(response).expect("encodes");
        let back = matrixraft_decode_rpc_response(&framed).expect("decodes");
        assert_eq!(&back, response, "a response variant did not survive");
    }

    let nested = TcpRaftTransportResponse::Batch(vec![
        responses[0].clone(),
        TcpRaftTransportResponse::Batch(vec![responses[1].clone(), responses[4].clone()]),
    ]);
    let framed = matrixraft_encode_rpc_response(&nested).expect("encodes");
    assert_eq!(
        matrixraft_decode_rpc_response(&framed).expect("decodes"),
        nested,
        "a nested response batch did not survive"
    );
}

/// Every snapshot state round trips, and an unknown one is refused.
///
/// `SnapshotState::default()` is `None`, so defaulting an unrecognised value
/// would turn a receiver that is mid-transfer into one that looks idle. The
/// same argument the WAL's `role_from_wire` makes, where a witness read as a
/// voter changes what a quorum is.
#[test]
fn an_unknown_snapshot_state_is_refused_rather_than_defaulted() {
    for state in [
        SnapshotState::None,
        SnapshotState::Creating,
        SnapshotState::Receiving,
        SnapshotState::Received,
        SnapshotState::NotReady,
    ] {
        let response = TcpRaftTransportResponse::AppendEntries(TcpRaftRpcResult {
            ok: Some(AppendEntriesResponse {
                term: 2,
                success: true,
                match_index: 0,
                rejection_hint: None,
                rejected_index: None,
                require_snapshot: None,
                snapshot_state: state,
                lease_confirmation_epoch: 0,
                lease_duration_ms: 0,
            }),
            error: None,
        });
        let framed = matrixraft_encode_rpc_response(&response).expect("encodes");
        assert_eq!(
            matrixraft_decode_rpc_response(&framed).expect("decodes"),
            response,
            "snapshot state {state:?} did not survive"
        );
    }

    // A state this build does not know. Encoded by hand, because the enum has
    // no variant for it -- which is the point: a future build could send one.
    let response = TcpRaftTransportResponse::AppendEntries(TcpRaftRpcResult {
        ok: Some(AppendEntriesResponse {
            term: 2,
            success: true,
            match_index: 0,
            rejection_hint: None,
            rejected_index: None,
            require_snapshot: None,
            snapshot_state: SnapshotState::NotReady,
            lease_confirmation_epoch: 0,
            lease_duration_ms: 0,
        }),
        error: None,
    });
    let mut framed = matrixraft_encode_rpc_response(&response).expect("encodes");
    // `snapshot_state` is field 7 of the append response, a varint, and
    // `NotReady` is 4. Find that byte and make it something unknown.
    let body_start = 12;
    let position = framed[body_start..]
        .windows(2)
        .position(|pair| pair == [0x38, 0x04])
        .expect("the snapshot state field is on the wire as tag 7 holding 4");
    framed[body_start + position + 1] = 0x25;
    // The digest covers the body, so it has to be recomputed or the frame is
    // refused for being torn instead of for the value this test is about.
    let body = framed[body_start..].to_vec();
    let digest = crc32c_of(&body);
    framed[8..12].copy_from_slice(&digest.to_le_bytes());

    let error = matrixraft_decode_rpc_response(&framed)
        .expect_err("an unknown snapshot state should be refused");
    assert!(
        error.to_string().contains("does not recognise"),
        "refused with {error}, which does not say the state was unrecognised"
    );
}

/// CRC-32C, so this file can repair a frame it damaged on purpose.
///
/// Hand-rolled here rather than exported from the crate: a test needing it is
/// not a reason to widen the public surface, and the published check value pins
/// it the same way the crate's own copy is pinned.
fn crc32c_of(bytes: &[u8]) -> u32 {
    const POLYNOMIAL: u32 = 0x82F6_3B78;
    let mut crc = u32::MAX;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ POLYNOMIAL
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

#[test]
fn the_test_helper_crc_is_the_same_one_the_frame_uses() {
    assert_eq!(
        crc32c_of(b"123456789"),
        0xE306_9283,
        "this file's CRC is not CRC-32C, so repairing a frame with it would make every damage test meaningless"
    );
}

/// A damaged frame is refused, and a JSON line is not mistaken for one.
#[test]
fn a_damaged_frame_is_refused_and_json_is_not_taken_for_a_frame() {
    let good = matrixraft_encode_rpc_envelope(&envelope(3, 9)).expect("encodes");
    assert_eq!(
        &good[..4],
        b"MRRP",
        "the frame does not open with its magic"
    );

    for (label, index) in [
        ("a payload byte", 40usize),
        ("a length byte", 5),
        ("a digest byte", 9),
    ] {
        let mut damaged = good.clone();
        damaged[index] ^= 0xFF;
        assert!(
            matrixraft_decode_rpc_envelope(&damaged).is_err(),
            "{label} flipped and the frame was still accepted"
        );
    }

    // Truncated.
    assert!(
        matrixraft_decode_rpc_envelope(&good[..good.len() - 1]).is_err(),
        "a truncated frame was accepted"
    );

    // And a JSON line, which a receiver tells apart by the first byte: `{`
    // against `M`. Decoding one AS a frame has to fail rather than produce
    // something, since that is the branch a receiver takes for the other shape.
    let json = serde_json::to_vec(&envelope(3, 9)).expect("encodes as JSON");
    assert_eq!(json[0], b'{', "a JSON envelope should start with a brace");
    assert_ne!(
        json[0], b'M',
        "a JSON envelope must not start with the frame magic's first byte, or a receiver could not tell the two apart"
    );
    assert!(
        matrixraft_decode_rpc_envelope(&json).is_err(),
        "a JSON envelope decoded as a frame"
    );
}
