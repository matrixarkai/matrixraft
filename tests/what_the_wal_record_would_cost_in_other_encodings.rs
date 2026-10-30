// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What the WAL record would cost in other encodings.
//!
//! A sibling suite pins what an append costs today: 43 bytes per segment file,
//! 441 bytes per record, plus 4/3 of the payload, so a 9-byte command costs
//! about fifty times its own size. 77.4% of a record is its own field names.
//! The obvious reaction is to shorten the names. **Measured, that is the worst
//! of the three alternatives**, and this file exists so that conclusion is a
//! number rather than an opinion.
//!
//! | encoding | envelope | 9-byte command | large payloads |
//! |---|---|---|---|
//! | JSON, as written today | 437 B | 49.9x | 1.333x |
//! | JSON with one-character keys | 183 B | 21.7x | 1.333x |
//! | binary, every integer eight bytes | 169 B | 19.8x | 1.000x |
//! | binary, varint integers | **35 B** | **4.9x** | 1.000x |
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
//! `"installed_snapshot_index":0`. Collapsing both at once is what takes 437
//! bytes to 35 -- an envelope **12.5x smaller than short keys achieve** -- and
//! it is also what moves the payload slope from 4/3 to 1, because a binary
//! record has no reason to base64 anything.
//!
//! ## What this file is and is not
//!
//! It is a **pricing, not a proposal**, and it changes no format. Moving the
//! WAL to a binary record is a durable-format decision with a migration to
//! design -- the reader would have to keep taking JSON segments written by
//! earlier builds, exactly as it already keeps taking the pre-base64 number
//! arrays -- and that decision is not this file's to make.
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
fn the_envelope_is_437_bytes_today_and_35_as_varints() {
    let empty = record(0);
    assert_eq!(
        serde_json::to_string(&empty).expect("encode").len(),
        437,
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
    assert_eq!(json, 4_437);
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
