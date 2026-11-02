// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a host-facing message costs when it stops naming the fields it is not
//! using.
//!
//! `MatrixRaftMessage` has 31 fields and `MatrixRaftAdminCommand` has 30,
//! because each is one flat struct covering every message or command kind
//! rather than a union of the shapes it can take. Any one message fills a
//! handful of them.
//!
//! Every optional field carried `#[serde(default)]`, which decides what happens
//! when a field is **absent on decode** and says nothing about **encode**. So
//! the encoder wrote them all, and an append-entries message spelled out
//! 22 fields as `null`:
//!
//! ```text
//!   "raft_addr":null,"snapshot_addr":null,"vote_request":null, ... 19 more
//! ```
//!
//! Measured, that was **494 of 709 bytes -- 69.7%** of an empty one. The admin
//! command spent 414 of 643, or 64.4%.
//!
//! Adding `skip_serializing_if = "Option::is_none"` beside each `default`:
//!
//! | | before | after | |
//! |---|---:|---:|---:|
//! | an append-entries message, no entries | 709 | **215** | 3.30x |
//!
//! | an admin command | 643 | **229** | 2.81x |
//!
//! ## Why this is safe in both directions, which is unusual
//!
//! Most encoding changes here are one-way: an old sender can talk to a new
//! receiver and not the reverse, so a cluster upgrades receivers first. This one
//! is safe both ways.
//!
//! - A **new** message omits a field. Any receiver, old or new, decodes an
//!   absent field as `None`.
//! - An **old** message spells the field out as `null`. A new receiver decodes
//!   `null` into `Option` as `None`, which is what it always did.
//!
//! The first direction is carried by the field being an `Option`, and **not** by
//! the `#[serde(default)]` beside it -- serde treats a missing `Option` field as
//! `None` whether or not that attribute is there. Removing it from a field
//! leaves this suite green, which is how that was established rather than
//! assumed. The attribute is left in place because it is what the rest of the
//! file does, but the compatibility claim does not rest on it, and it would hold
//! for a build old enough to predate it.
//!
//! The second direction is the one worth testing rather than reasoning about,
//! because nothing writes that shape any more -- the round-trip tests in
//! `matrixraft_protocol_compat` now encode without nulls, so they would not
//! notice if decoding them broke. `a_message_that_spells_out_every_null_still_decodes`
//! holds it with a literal, which is the only form that cannot drift with the
//! encoder.
//!
//! What that literal does **not** defend is a field changing type so that `null`
//! stops being a legal value for it. Turning one `Option<u64>` into a `u64` does
//! not redden this suite -- it fails to compile, which is the stronger of the
//! two outcomes and the reason no test here tries to cover it.
//!
//! ## What is left, and what this does not touch
//!
//! The envelope is no longer where the bytes are. At 100 entries the message is
//! 22,010 bytes, so about 220 per entry, and the nulls were only 494 of the
//! 22,504 it used to be -- 2.2%. Per-entry cost is a field-naming and number-
//! formatting problem, which is a codec change and not an attribute, so it is
//! not addressed here.
//!
//! The nested types keep their nulls for now. They were measured and left: the
//! saving is in the two outer structs because those are the ones with 25 and 27
//! unused optional fields.

use matrixraft::{
    AppendEntriesRequest, LogEntry, LogId, MatrixRaftAdminCommand, MatrixRaftAdminCommandType,
    MatrixRaftMessage, MatrixRaftMessageType,
};

/// Measured, exact.
const EMPTY_MESSAGE: usize = 215;
const ADMIN_COMMAND: usize = 229;
const AT_100_ENTRIES: usize = 22_010;

/// What the same two cost before, kept as the comparison this file is about.
const EMPTY_MESSAGE_WITH_NULLS: usize = 709;
const ADMIN_COMMAND_WITH_NULLS: usize = 643;

fn entry(index: u64, payload_len: usize) -> LogEntry {
    LogEntry {
        log_id: LogId { term: 3, index },
        payload: vec![b'x'; payload_len],
        is_command: true,
    }
}

fn request(count: u64, payload_len: usize) -> AppendEntriesRequest {
    AppendEntriesRequest {
        group_id: 9,
        term: 5,
        leader_id: 1,
        prev_log_id: Some(LogId { term: 4, index: 0 }),
        entries: (1..=count).map(|i| entry(i, payload_len)).collect(),
        leader_commit: count,
        lease_epoch: 7,
    }
}

fn message(count: u64, payload_len: usize) -> MatrixRaftMessage {
    MatrixRaftMessage::append_entries(1, 2, &request(count, payload_len))
}

fn encoded(value: &MatrixRaftMessage) -> String {
    String::from_utf8(value.to_wire_bytes().expect("the message encodes"))
        .expect("the encoding is UTF-8")
}

/// How many of an object's own fields are written as `null`.
fn nulls(json: &str) -> Vec<String> {
    let value: serde_json::Value = serde_json::from_str(json).expect("the encoding is JSON");
    value
        .as_object()
        .expect("a message encodes as an object")
        .iter()
        .filter(|(_, held)| held.is_null())
        .map(|(name, _)| name.clone())
        .collect()
}

#[test]
fn a_host_message_no_longer_names_the_fields_it_is_not_using() {
    let text = encoded(&message(0, 0));
    assert_eq!(text.len(), EMPTY_MESSAGE, "the empty message moved");

    let written_as_null = nulls(&text);
    assert!(
        written_as_null.is_empty(),
        "{} fields are still written as null: {written_as_null:?}",
        written_as_null.len()
    );

    // The fields it does fill are all still there -- a missing field would also
    // produce a smaller message, and this is the assertion that tells the two
    // apart.
    let back = MatrixRaftMessage::from_wire_bytes(text.as_bytes()).expect("decodes");
    assert_eq!(back, message(0, 0), "the message did not survive encoding");
    assert_eq!(
        back.message_type,
        MatrixRaftMessageType::AppendEntriesRequest
    );
    assert_eq!(back.from, Some(1));
    assert_eq!(back.to, Some(2));
    assert_eq!(back.term, Some(5));
    assert!(
        back.append_entries_request.is_some(),
        "the one field this message is about was dropped"
    );
}

#[test]
fn an_admin_command_no_longer_names_the_fields_it_is_not_using() {
    let command = MatrixRaftAdminCommand::begin_snapshot_send(2, "snapshot-13", 13, 3);
    let bytes = command.to_wire_bytes().expect("encodes");
    assert_eq!(bytes.len(), ADMIN_COMMAND, "the admin command moved");

    let text = String::from_utf8(bytes).expect("UTF-8");
    let written_as_null = nulls(&text);
    assert!(
        written_as_null.is_empty(),
        "{} fields are still written as null: {written_as_null:?}",
        written_as_null.len()
    );

    let back = MatrixRaftAdminCommand::from_wire_bytes(text.as_bytes()).expect("decodes");
    assert_eq!(back, command, "the command did not survive encoding");
    assert_eq!(
        back.command_type,
        MatrixRaftAdminCommandType::BeginSnapshotSend
    );
    assert_eq!(back.snapshot_id.as_deref(), Some("snapshot-13"));
}

/// The saving, measured against the shape that was being written.
///
/// Rebuilding the old form rather than quoting a constant at it: the null
/// fields are put back one by one, so this measures what was removed instead of
/// restating a number from a commit message.
#[test]
fn the_nulls_were_most_of_an_empty_message() {
    let value = message(0, 0);
    let text = encoded(&value);
    let mut object: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(&text).expect("JSON object");

    // Every optional field of the struct, as the encoder used to name them.
    const ONCE_WRITTEN_AS_NULL: [&str; 22] = [
        "raft_addr",
        "snapshot_addr",
        "vote_request",
        "vote_response",
        "config_change",
        "membership_operation",
        "propose",
        "entry",
        "append_entries_response",
        "install_snapshot_request",
        "install_snapshot_response",
        "read_index_request",
        "read_index_response",
        "old_snapshot_finish",
        "timestamp",
        "snapshot_state",
        "snapshot",
        "snapshot_progress",
        "require_snapshot",
        "lease_request",
        "lease_response",
        "command",
    ];

    for name in ONCE_WRITTEN_AS_NULL {
        assert!(
            !object.contains_key(name),
            "{name} is still being written, so this list is stale"
        );
        object.insert(name.to_string(), serde_json::Value::Null);
    }

    let with_nulls = serde_json::to_string(&object).expect("re-encodes");
    assert_eq!(
        with_nulls.len(),
        EMPTY_MESSAGE_WITH_NULLS,
        "rebuilding the old shape gave {} bytes, not the {EMPTY_MESSAGE_WITH_NULLS} that was measured",
        with_nulls.len()
    );

    // In hundredths, so the assertion is over integers: 709/215 is 3.2976,
    // which truncates to 329 and is not the 330 that rounding it would give.
    assert_eq!(
        with_nulls.len() * 100 / text.len(),
        329,
        "the saving is now {}/100x ({} bytes against {})",
        with_nulls.len() * 100 / text.len(),
        with_nulls.len(),
        text.len()
    );

    // And that shape still decodes, which is what makes dropping it safe.
    let back = MatrixRaftMessage::from_wire_bytes(with_nulls.as_bytes())
        .expect("the old shape must still decode");
    assert_eq!(back, value, "the old shape decoded to something else");
}

/// A message with a field left out entirely decodes as `None`.
///
/// The forward direction of the compatibility claim. It is what every other
/// test in this file relies on without saying so -- the encoder now omits 22
/// fields, so each round trip already depends on an absent field decoding --
/// but stating it once makes the pair legible beside the literal below.
#[test]
fn a_message_with_a_field_left_out_entirely_decodes_as_none() {
    const WITHOUT_VOTE_RESPONSE: &str = r#"{"message_type":"append_entries_request","from":1,"to":2,"term":5,"committed_index":0,"append_entries_request":{"prev_term":4,"prev_index":0,"entries":[]},"to_conf_state":"voter","auto_promote":false,"bytes_size":0}"#;

    let back = MatrixRaftMessage::from_wire_bytes(WITHOUT_VOTE_RESPONSE.as_bytes())
        .expect("a message that omits a field decodes");
    assert!(back.vote_response.is_none());
    assert!(back.raft_addr.is_none());
    assert_eq!(back, message(0, 0));
}

/// A message with every null spelled out, as a literal.
///
/// The round-trip tests cannot hold this any more: the encoder no longer writes
/// nulls, so they would stay green if decoding them broke. Only a literal
/// pins it.
#[test]
fn a_message_that_spells_out_every_null_still_decodes() {
    const OLD_SHAPE: &str = r#"{"message_type":"append_entries_request","from":1,"raft_addr":null,"snapshot_addr":null,"to":2,"term":5,"committed_index":0,"vote_request":null,"vote_response":null,"config_change":null,"membership_operation":null,"propose":null,"entry":null,"append_entries_request":{"prev_term":4,"prev_index":0,"entries":[]},"append_entries_response":null,"install_snapshot_request":null,"install_snapshot_response":null,"read_index_request":null,"read_index_response":null,"old_snapshot_finish":null,"timestamp":null,"snapshot_state":null,"snapshot":null,"snapshot_progress":null,"require_snapshot":null,"to_conf_state":"voter","auto_promote":false,"lease_request":null,"lease_response":null,"bytes_size":0,"command":null}"#;

    assert_eq!(
        OLD_SHAPE.len(),
        EMPTY_MESSAGE_WITH_NULLS,
        "this literal is not the message that was measured"
    );
    let back =
        MatrixRaftMessage::from_wire_bytes(OLD_SHAPE.as_bytes()).expect("the old shape decodes");
    assert_eq!(
        back,
        message(0, 0),
        "an old message decoded to something else"
    );

    // An old command, likewise.
    const OLD_COMMAND: &str = r#"{"command_type":"begin_snapshot_send","request_id":null,"member_id":null,"node_id":null,"peer_id":null,"term":null,"index":null,"snapshot_peer_id":2,"snapshot_id":"snapshot-13","snapshot_index":13,"snapshot_total_chunks":3,"snapshot_chunk_index":null,"snapshot_bytes":null,"elapsed_ms":null}"#;
    let command = MatrixRaftAdminCommand::from_wire_bytes(OLD_COMMAND.as_bytes())
        .expect("the old command decodes");
    assert_eq!(
        command,
        MatrixRaftAdminCommand::begin_snapshot_send(2, "snapshot-13", 13, 3),
        "an old command decoded to something else"
    );
}

/// Where the bytes are now: the entries, not the envelope.
///
/// Stated here so the next change is aimed at the right thing. A reader who
/// sees 3.30x on an empty message should also see that at 100 entries the same
/// change is worth 2.2%.
#[test]
fn the_envelope_is_no_longer_where_a_loaded_message_spends_its_bytes() {
    let loaded = encoded(&message(100, 9));
    assert_eq!(loaded.len(), AT_100_ENTRIES, "the 100-entry figure moved");

    let empty = encoded(&message(0, 0)).len();
    let per_entry = (loaded.len() - empty) / 100;
    assert!(
        per_entry > 200,
        "an entry costs {per_entry} bytes here; if that has come down, this file's closing claim is stale"
    );

    // The nulls, against a loaded message rather than an empty one: 494 bytes
    // of a former 22,504, which is why this change is an envelope change.
    let saving = EMPTY_MESSAGE_WITH_NULLS - EMPTY_MESSAGE;
    let former_total = loaded.len() + saving;
    assert_eq!(saving, 494, "the per-message saving moved");
    assert_eq!(
        former_total * 100 / saving / 100,
        45,
        "the nulls were about 1/45th of a loaded message; got 1/{}",
        former_total / saving
    );
}

#[test]
fn the_admin_command_saving_is_two_point_eight_times() {
    let command = MatrixRaftAdminCommand::begin_snapshot_send(2, "snapshot-13", 13, 3);
    let now = command.to_wire_bytes().expect("encodes").len();
    assert_eq!(now, ADMIN_COMMAND);
    assert_eq!(
        ADMIN_COMMAND_WITH_NULLS * 100 / now,
        280,
        "the saving is now {}/100x ({ADMIN_COMMAND_WITH_NULLS} bytes against {now})",
        ADMIN_COMMAND_WITH_NULLS * 100 / now
    );
}
