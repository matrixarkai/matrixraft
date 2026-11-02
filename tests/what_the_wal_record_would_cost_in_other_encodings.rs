// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What the WAL record would cost in other encodings.
//!
//! A sibling suite pins what an append costs: 2 bytes per segment file, 73
//! bytes per record, plus the payload, so a 9-byte command costs about nine
//! times its own size. It cost forty-seven times under the JSON record this
//! file prices, roughly three quarters of which was its own field names.
//!
//! The JSON column moved from 437 to 408 when the record stopped writing an
//! empty `learners` and `witnesses` on every append, and the three computed
//! columns did NOT move -- correctly, and that is worth saying out loud. A
//! binary encoding writes a zero-length prefix for an empty list, one byte as a
//! varint and four fixed-width, so it never paid the 29 bytes JSON was spending
//! on `"learners":[],"witnesses":[],`. The saving closes a little of the
//! distance to binary rather than changing what binary costs.
//! The obvious reaction is to shorten the names. **Measured, that is the worst
//! of the three alternatives**, and this file exists so that conclusion is a
//! number rather than an opinion.
//!
//! | encoding | envelope | 9-byte command | large payloads |
//! |---|---|---|---|
//! | JSON, as it was written until this change | 408 B | 46.7x | 1.333x |
//! | JSON with one-character keys | 183 B | 21.7x | 1.333x |
//! | binary, every integer eight bytes | 169 B | 19.8x | 1.000x |
//! | binary, varint integers | **35 B** | **4.9x** | 1.000x |
//! | **protobuf, as written today, measured** | **73 B** | **9.3x** | 1.000x |
//!
//! Two things fall out of that table.
//!
//! **Short keys are dominated.** They land at 183 bytes, which is within 8% of
//! what fixed-width binary reaches at 169 -- so they would give up the WAL's
//! readability, which is presumably why JSON was chosen, for a saving binary
//! gets anyway, while still paying base64's 4/3 on every payload byte.
//!
//! **The envelope is small integers written as text under long names.** This
//! record's integers are a term of 3, an index of 1, a node of 1, an epoch of
//! 3: each costs one byte as a varint and up to twenty-six characters as
//! `"installed_snapshot_index":0`. Collapsing both at once is what takes 408
//! bytes to 35 on this file's arithmetic -- and to 73 in the format that
//! actually landed -- and it is also what moves the payload slope from 4/3 to
//! 1, because a binary record has no reason to base64 anything.
//!
//! ## The decision this file priced has been taken, and the price was wrong
//!
//! This was a pricing rather than a proposal, and it said so: moving the WAL to
//! a binary record was a durable-format decision with a migration to design,
//! and not this file's to make. That decision has since been made -- the WAL
//! stores prost messages, and reads JSON segments written by earlier builds the
//! same way it already read pre-base64 number arrays.
//!
//! So the varint row stopped being hypothetical, and it can be checked. It is
//! **wrong by 2.1x**: the landed envelope is 73 bytes, not 35. The direction
//! held and the magnitude did not, and the three reasons are the lesson worth
//! keeping, because they are what pricing a format from its field widths
//! leaves out:
//!
//! - **No record frame was priced.** A record on disk carries 8 bytes in front
//!   of it -- a length and a digest of the body -- so a torn tail can be found
//!   and cut. Arithmetic over field widths has no reason to invent that.
//! - **The checksum was priced as the `u64` it encodes**, at 8 bytes. It is
//!   still a 16-character hex `String`, which costs 18 on the wire. That is
//!   +10, and it is the one line in `varint_bytes` below that says what it
//!   assumed out loud: "the raw u64 the hex text already encodes".
//! - **Tags and nested lengths were not priced.** protobuf pays a tag for each
//!   field present and a tag plus a length for each nested message, and
//!   `hard_state`, `membership`, `apply_snapshot_fence` and every entry are
//!   nested messages.
//!
//! The first two are exact. The remaining 20 bytes are attributed to the third
//! as a **remainder**, not measured field by field, and the identity below
//! holds the three terms together so that changing one without re-reading the
//! others goes red.
//!
//! ## How the figures are produced, which differs per column
//!
//! The first column is **measured**: `serde_json` encoding the real record.
//! The other three are **computed from the record's declared shape** -- field
//! by field, by the two functions at the bottom -- not produced by a codec
//! that exists. That is exact arithmetic rather than an estimate, because the
//! widths of `u64`, a length prefix and a varint are all known, but it is
//! arithmetic over a hypothetical and is labelled as such. No dependency was
//! added to produce it; adding one to price an option would be a poor trade on
//! a crate with a gating licence-and-advisory check.
//!
//! The assertions are equalities on whole numbers because all four quantities
//! are deterministic. Their value is not as a regression guard on behaviour --
//! nothing here runs in production -- but as a tripwire on the **comparison**.
//!
//! Be precise about how that tripwire works, because only one column is live.
//! Add or rename a field and the **JSON figure moves on its own**, because it
//! is measured, and this file goes red. The three computed columns do **not**
//! move by themselves: the functions at the bottom enumerate the record's
//! fields by hand, so they keep answering for the old shape until someone
//! updates them. That is the point of failing loudly rather than drifting --
//! the red JSON assertion is what forces the other three to be brought back
//! into line, and a reader who trusts the computed columns without checking
//! that this file is green is reading a stale price. Verified by planting a
//! field rename, which reddens the envelope assertion as intended.

use matrixraft::{
    matrixraft_wal_checksum, ApplySnapshotFence, HardState, LogEntry, LogId, Membership, WalRecord,
};

/// The common case this prices: a delta record carrying one new entry.
fn record(payload_len: usize) -> WalRecord {
    let mut record = WalRecord {
        entries_are_delta: true,
        group_id: 9,
        node_id: 1,
        hard_state: HardState {
            current_term: 3,
            voted_for: Some(1),
            committed: Some(LogId { term: 3, index: 1 }),
        },
        membership: Membership {
            group_id: 9,
            voters: vec![1, 2, 3],
            learners: Vec::new(),
            witnesses: Vec::new(),
            epoch: 3,
        },
        entries: vec![LogEntry {
            log_id: LogId { term: 3, index: 1 },
            payload: vec![b'x'; payload_len],
            is_command: true,
        }],
        installed_snapshot: None,
        apply_snapshot_fence: ApplySnapshotFence {
            applied_index: 1,
            commit_index: 1,
            installed_snapshot_index: 0,
            first_retained_log_index: 1,
        },
        checksum: String::new(),
    };
    record.checksum = matrixraft_wal_checksum(&record);
    record
}

#[test]
fn the_envelope_was_408_bytes_as_json_and_is_73_as_protobuf() {
    let empty = record(0);
    assert_eq!(
        serde_json::to_string(&empty).expect("encode").len(),
        408,
        "the JSON envelope moved, so every figure in this file needs re-pricing"
    );
    assert_eq!(short_key_json_len(&empty), 183, "short-key envelope moved");
    assert_eq!(fixed_width_bytes(&empty), 169, "fixed-width envelope moved");
    assert_eq!(varint_bytes(&empty), 35, "varint envelope moved");
}

#[test]
fn shortening_the_keys_is_the_worst_of_the_three_alternatives() {
    let empty = record(0);
    let short = short_key_json_len(&empty);
    let fixed = fixed_width_bytes(&empty);
    let varint = varint_bytes(&empty);

    // Within 10% of what binary reaches without giving up readability's whole
    // point -- that is the trade being refused.
    assert!(
        short < fixed * 110 / 100 && short > fixed,
        "short keys ({short} B) are expected to land just above fixed-width binary ({fixed} B); if that stopped holding, the recommendation in this file needs rechecking"
    );
    assert!(
        short > varint * 5,
        "varints should beat short keys by well over fivefold: {short} B against {varint} B"
    );
}

#[test]
fn a_binary_record_also_removes_the_payloads_four_thirds() {
    // base64 is only needed because the record is text. The slope is the other
    // half of the win and it is invisible in the envelope column.
    let big = record(3_000);
    let json = serde_json::to_string(&big).expect("encode").len();
    let varint = varint_bytes(&big);
    assert_eq!(json, 4_408);
    assert_eq!(varint, 3_036);
    assert!(
        varint < 3_000 + 50,
        "a varint record should carry a 3000-byte payload in about 3000 bytes, got {varint}"
    );
    assert!(
        json > 3_000 * 4 / 3,
        "the JSON form must still be paying base64 on the payload: {json}"
    );
}

/// Length of the same record with one-character keys and the same base64
/// payload. Built as text because the point is the character count.
fn short_key_json_len(record: &WalRecord) -> usize {
    let entries: Vec<String> = record
        .entries
        .iter()
        .map(|entry| {
            format!(
                r#"{{"l":{{"t":{},"i":{}}},"p":"{}","c":{}}}"#,
                entry.log_id.term,
                entry.log_id.index,
                "A".repeat(entry.payload.len().div_ceil(3) * 4),
                entry.is_command
            )
        })
        .collect();
    let fence = &record.apply_snapshot_fence;
    format!(
        r#"{{"g":{},"n":{},"h":{{"t":{},"v":{},"c":{{"t":{},"i":{}}}}},"m":{{"g":{},"v":[1,2,3],"e":{}}},"e":[{}],"d":true,"f":{{"a":{},"c":{},"s":{},"r":{}}},"k":"{}"}}"#,
        record.group_id,
        record.node_id,
        record.hard_state.current_term,
        record.hard_state.voted_for.unwrap_or(0),
        3,
        1,
        record.membership.group_id,
        record.membership.epoch,
        entries.join(","),
        fence.applied_index,
        fence.commit_index,
        fence.installed_snapshot_index,
        fence.first_retained_log_index,
        record.checksum,
    )
    .len()
}

/// Every integer eight bytes, every length prefix four, every tag one.
fn fixed_width_bytes(record: &WalRecord) -> usize {
    let mut bytes = 8 + 8; // group_id, node_id
    bytes += 8 + 1 + 8 + 1 + 16; // hard_state
    bytes += 8 + 4 + record.membership.voters.len() * 8 + 4 + 4 + 8; // membership
    bytes += 1 + 1; // entries_are_delta, installed_snapshot tag
    bytes += 4 * 8; // apply_snapshot_fence
    bytes += 8; // checksum as a u64 rather than sixteen hex characters
    bytes += 4; // entries length
    for entry in &record.entries {
        bytes += 16 + 4 + entry.payload.len() + 1;
    }
    bytes
}

/// The same, with integers varint-encoded: one byte per seven bits.
fn varint_bytes(record: &WalRecord) -> usize {
    fn width(value: u64) -> usize {
        let mut bytes = 1;
        let mut rest = value >> 7;
        while rest > 0 {
            bytes += 1;
            rest >>= 7;
        }
        bytes
    }

    let mut bytes = width(record.group_id) + width(record.node_id);
    bytes += width(record.hard_state.current_term);
    bytes += 1 + record.hard_state.voted_for.map_or(0, width);
    bytes += 1 + record
        .hard_state
        .committed
        .as_ref()
        .map_or(0, |id| width(id.term) + width(id.index));
    bytes += width(record.membership.group_id)
        + width(record.membership.voters.len() as u64)
        + record
            .membership
            .voters
            .iter()
            .map(|id| width(*id))
            .sum::<usize>()
        + width(record.membership.learners.len() as u64)
        + width(record.membership.witnesses.len() as u64)
        + width(record.membership.epoch);
    bytes += 1 + 1; // entries_are_delta, installed_snapshot tag
    let fence = &record.apply_snapshot_fence;
    bytes += width(fence.applied_index)
        + width(fence.commit_index)
        + width(fence.installed_snapshot_index)
        + width(fence.first_retained_log_index);
    bytes += 8; // checksum, the raw u64 the hex text already encodes
    bytes += width(record.entries.len() as u64);
    for entry in &record.entries {
        bytes += width(entry.log_id.term)
            + width(entry.log_id.index)
            + width(entry.payload.len() as u64)
            + entry.payload.len()
            + 1;
    }
    bytes
}

/// What this file's arithmetic missed, held together as an identity.
///
/// The computed varint column is 35 bytes and the landed format measures 73.
/// The difference is not a mystery and it is not fully attributed either: two
/// terms are exact and the third is a remainder. Writing it as an equality
/// means nobody can revise one term and leave the other two telling a story
/// that no longer adds up -- which is the failure this whole file is a guard
/// against, one level up.
///
/// Worth stating plainly rather than burying: the remainder is the **largest**
/// of the three terms. The single biggest thing field-width arithmetic misses
/// about protobuf is protobuf's own framing, not the frame around the record
/// and not the checksum. That is an observation and not a guard, so it is
/// written here -- as an `assert!` it would compare three constants, be folded
/// at compile time and check nothing.
#[test]
fn the_computed_varint_price_missed_the_landed_format_by_a_frame_a_checksum_and_its_tags() {
    /// Measured by the sibling suite, from two directions: 100 appends weighing
    /// 7,302 bytes, and one stored record written alone weighing 73.
    const LANDED: usize = 73;
    /// A length and a digest, in front of every record on disk.
    const FRAME: usize = 8;
    /// 16 hex characters, a tag and a length, where this file priced the `u64`
    /// they encode at 8 bytes.
    const CHECKSUM_EXCESS: usize = 18 - 8;
    /// A tag per present field, and a tag and a length per nested message. Not
    /// measured field by field: this is what is left over.
    const TAGS_AND_NESTED_LENGTHS: usize = 20;

    let computed = varint_bytes(&record(0));
    assert_eq!(
        computed, 35,
        "the computed varint envelope moved, so the identity below is pricing something else"
    );
    assert_eq!(
        computed + FRAME + CHECKSUM_EXCESS + TAGS_AND_NESTED_LENGTHS,
        LANDED,
        "the computed price plus what it left out no longer reaches the landed envelope"
    );
}
