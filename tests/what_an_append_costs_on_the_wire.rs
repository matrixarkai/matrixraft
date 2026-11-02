// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What one AppendEntries costs on the wire.
//!
//! The durable side of this crate is protobuf now -- a WAL record weighs 64
//! bytes and a snapshot checkpoint costs 1.000 bytes per payload byte. The wire
//! is not. `TcpRaftTransport` writes a newline-delimited JSON envelope, with
//! entry payloads as base64, which is the shape the WAL had before it changed.
//!
//! Measured:
//!
//! ```text
//!  entries   payload   wire bytes   per entry   per payload byte
//!        0       0 B          193           -                  -
//!      100       0 B        6,586       63.93                  -
//!      100       9 B        7,786       77.86              8.65x
//!      100     300 B       46,586      465.86              1.55x
//!      100    3000 B      406,586     4065.86             1.355x
//! ```
//!
//! Two figures carry the whole story.
//!
//! **A payload byte costs 4/3 of a byte**, exactly: between the 300-byte and
//! 3000-byte points the extra wire bytes and the extra payload are 360,000 and
//! 270,000, which is base64 and nothing else. The durable paths left that
//! behind; this one has not.
//!
//! **An entry's envelope is 63.93 bytes** -- and that is about what the whole
//! protobuf WAL record now weighs, 64 bytes, while carrying a frame, a
//! checksum, a hard state, a membership and an apply fence that an RPC entry
//! does not. So the JSON spent on one log entry here is roughly the entire
//! budget of a durable record there. An entry as protobuf measures 10 bytes in
//! the WAL's own field attribution.
//!
//! ## What converting it would be worth, and why this file does not do it
//!
//! Taking the envelope from ~64 to ~10 and the payload slope from 4/3 to 1
//! would put 100 appends of a 9-byte command at roughly 2,000 wire bytes
//! against 7,786 -- about **3.9x** on replication traffic for small commands.
//! That is the largest remaining amplification in this crate.
//!
//! It is also a **cross-node** wire, which makes it a rollout decision rather
//! than a change. Two things in the current code say so clearly. The envelope
//! uses `flatten` specifically so a peer predating `cluster_id` reads and is
//! read by one that has it, in both directions. And the base64 move documented
//! its own order: an old sender can talk to a new receiver but not the reverse,
//! so a mixed-version cluster upgrades receivers first. A protobuf wire needs
//! that story designed, not inherited.
//!
//! So this file pins the cost instead, for the same reason the sibling suites
//! do: so the figure is a number rather than an impression when somebody comes
//! to decide, and so nothing quietly makes it worse first.
//!
//! ## What is not measured here
//!
//! - The durable peer path. `peer_runtime` is prost throughout -- 40 encode
//!   sites and no `serde_json` -- so this is the facade transport's cost and
//!   not "the wire" in general.
//! - Compression and TLS framing. Nothing here applies either.
//! - The response direction, which is small: a response carries no entries.

use matrixraft::{
    AppendEntriesRequest, LogEntry, LogId, TcpRaftTransportEnvelope, TcpRaftTransportRequest,
};

/// Measured, exact: the same request always produces the same bytes.
const WIRE_AT_NO_ENTRIES: usize = 193;
const WIRE_AT_100_EMPTY: usize = 6_586;
const WIRE_AT_100_NINE: usize = 7_786;
const WIRE_AT_100_300: usize = 46_586;
const WIRE_AT_100_3000: usize = 406_586;

const ENTRIES: u64 = 100;

fn entry(index: u64, payload_len: usize) -> LogEntry {
    LogEntry {
        log_id: LogId { term: 3, index },
        payload: vec![b'x'; payload_len],
        is_command: true,
    }
}

/// What the transport would put on the socket for this append.
///
/// Goes through the same `serde_json` the transport uses, plus the one newline
/// it appends -- it writes body and newline in a single write, so the newline is
/// part of the frame rather than a separate packet.
fn wire_bytes(count: u64, payload_len: usize) -> usize {
    let envelope = TcpRaftTransportEnvelope {
        cluster_id: 1,
        user_payload: None,
        request: TcpRaftTransportRequest::AppendEntries {
            target: 2,
            request: AppendEntriesRequest {
                group_id: 9,
                term: 3,
                leader_id: 1,
                prev_log_id: Some(LogId { term: 3, index: 0 }),
                entries: (1..=count).map(|index| entry(index, payload_len)).collect(),
                leader_commit: count,
                lease_epoch: 7,
            },
        },
    };
    serde_json::to_vec(&envelope)
        .expect("the envelope encodes")
        .len()
        + 1
}

#[test]
fn an_append_costs_a_fixed_envelope_and_four_thirds_of_its_payload() {
    assert_eq!(
        wire_bytes(0, 0),
        WIRE_AT_NO_ENTRIES,
        "the empty point moved"
    );
    assert_eq!(
        wire_bytes(ENTRIES, 0),
        WIRE_AT_100_EMPTY,
        "the 100-entry empty-payload point moved"
    );
    assert_eq!(
        wire_bytes(ENTRIES, 300),
        WIRE_AT_100_300,
        "the 300-byte point moved"
    );
    assert_eq!(
        wire_bytes(ENTRIES, 3_000),
        WIRE_AT_100_3000,
        "the 3000-byte point moved"
    );

    // Both payloads are multiples of three, so base64 adds no padding and the
    // slope is exact. An integer identity rather than a float comparison, which
    // would need a tolerance nobody can justify.
    let extra_wire = WIRE_AT_100_3000 - WIRE_AT_100_300;
    let extra_payload = (3_000 - 300) * ENTRIES as usize;
    assert_eq!(
        extra_wire * 3,
        extra_payload * 4,
        "a payload byte must cost exactly 4/3 on the wire: {extra_wire} wire bytes for {extra_payload} payload bytes is {:.6}, not {:.6}",
        extra_wire as f64 / extra_payload as f64,
        4.0 / 3.0
    );
}

/// An entry's envelope is about what a whole durable record now weighs.
///
/// The comparison this file exists to make. A WAL record is 64 bytes of
/// protobuf carrying a frame, a checksum, a hard state, a membership and an
/// apply fence; one JSON log entry on this wire costs about the same while
/// carrying a log id, a payload and a flag.
///
/// Asserted as the difference between two measured points rather than as a
/// division, so it is exact: the request envelope is paid once and every entry
/// after it is the per-entry cost.
#[test]
fn an_entry_costs_sixty_four_bytes_of_envelope_before_its_payload() {
    let entries_only = WIRE_AT_100_EMPTY - WIRE_AT_NO_ENTRIES;
    assert_eq!(
        entries_only, 6_393,
        "100 empty entries cost {entries_only} bytes of envelope, not 6,393"
    );

    // 63.93 a piece. Bounded rather than pinned to two decimals, because the
    // index inside each entry is spelled in decimal and so the per-entry figure
    // depends on how many of them have three digits.
    assert!(
        (60..=70).contains(&(entries_only / ENTRIES as usize)),
        "the per-entry envelope is {} bytes, outside the 60..=70 this file describes",
        entries_only / ENTRIES as usize
    );
}

/// A small command pays about nine times its own size on the wire.
///
/// The figure to compare against the durable one: the same 9-byte command costs
/// 8.33x on disk and 8.65x here, which is a coincidence of two different
/// formats rather than a relationship -- the disk figure is protobuf plus a
/// frame, and this one is JSON plus base64.
#[test]
fn a_small_command_pays_about_nine_times_its_own_size_on_the_wire() {
    let bytes = wire_bytes(ENTRIES, 9);
    assert_eq!(bytes, WIRE_AT_100_NINE, "the 9-byte point moved");

    let payload_total = 9 * ENTRIES as usize;
    assert_eq!(
        bytes * 100 / payload_total,
        865,
        "a 9-byte command should cost 8.65x on the wire, got {:.2}x",
        bytes as f64 / payload_total as f64
    );
}

/// The payload is base64 on the wire, not a number array and not raw.
///
/// The byte counts above would be met by another encoding of the same width, so
/// this looks at the bytes. It is also what pins which of the two JSON
/// spellings is in use: the number-array form cost about 3.9 per payload byte
/// and the decoder still accepts it, so only the encoder's choice is visible
/// from a size.
#[test]
fn the_payload_travels_as_base64_and_not_as_numbers_or_raw() {
    let envelope = TcpRaftTransportEnvelope {
        cluster_id: 1,
        user_payload: None,
        request: TcpRaftTransportRequest::AppendEntries {
            target: 2,
            request: AppendEntriesRequest {
                group_id: 9,
                term: 3,
                leader_id: 1,
                prev_log_id: Some(LogId { term: 3, index: 0 }),
                entries: vec![entry(1, 3_072)],
                leader_commit: 1,
                lease_epoch: 7,
            },
        },
    };
    let text = serde_json::to_string(&envelope).expect("encodes");

    assert!(
        text.contains("eHh4"),
        "the payload is not base64 of `x` repeated"
    );
    assert!(
        !text.contains("120,120,120"),
        "the payload is a number array, which costs about 3.9 per payload byte"
    );
    assert!(
        !text.contains("xxxxxxxxxx"),
        "the payload is raw in a JSON string, which would not survive a byte that is not valid UTF-8"
    );
}
