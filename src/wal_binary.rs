// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The WAL record as protobuf, and the frame that carries it.
//!
//! The record used to be written as JSON, one newline-terminated object per
//! append. Measured, that envelope was 412 bytes around a nine-byte command --
//! about forty-six times the payload -- and roughly three quarters of it was the
//! record's own field names, written out in full on every append. A sibling
//! suite priced the alternatives on one record: 408 B as JSON, 183 B with
//! one-character keys, 169 B as fixed-width binary, **35 B as varints**. Short
//! keys are dominated; varints are the whole win, and they also drop base64's
//! 4/3 on every payload byte, because a binary record has no reason to encode
//! bytes as text.
//!
//! ## What is mirrored, and why these are separate types
//!
//! prost derives live on mirror structs here rather than on `WalRecord` and its
//! nine nested types. Those types are also serialized for RPC and for the JSON
//! debug surfaces, and `GenericLogEntry` is generic over its payload -- the same
//! line the base64 payload encoding was kept on, for the same reason. Confining
//! prost to this module means the logical types keep one owner.
//!
//! ## The frame
//!
//! ```text
//! segment = MAGIC(4) || record*
//! record  = length(4, LE) || crc32(4, LE) || prost bytes
//! ```
//!
//! Eight bytes a record, which is what the design being followed also spends
//! (`length` then `crc32`, little-endian, around a protobuf message). The
//! four-byte file magic is what lets a reader tell a binary segment from a
//! newline-delimited JSON one written by an earlier build: a JSON segment's
//! first byte is `{`, which `MAGIC` is chosen not to collide with.
//!
//! Length-prefixing is not a preference here, it is forced: a JSON segment is
//! framed by newlines, and a binary payload can contain `0x0A`.

use crate::{
    ApplySnapshotFence, HardState, LogEntry, LogId, Membership, Peer, RaftError, ReplicaRole,
    SnapshotMetadata, WalRecord,
};

/// Marks a segment file as binary. Deliberately not starting with `{`, so the
/// first byte alone separates this format from the JSON one.
pub(crate) const SEGMENT_MAGIC: &[u8; 4] = b"MRWB";

/// Bytes of frame around each record: a little-endian length and a CRC32.
pub(crate) const FRAME_BYTES: usize = 8;

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct LogIdProto {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(uint64, tag = "2")]
    pub index: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct HardStateProto {
    #[prost(uint64, tag = "1")]
    pub current_term: u64,
    /// Optional on the wire as well as in the type: a node that has not voted
    /// must not be read as having voted for node zero.
    #[prost(uint64, optional, tag = "2")]
    pub voted_for: Option<u64>,
    #[prost(message, optional, tag = "3")]
    pub committed: Option<LogIdProto>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct MembershipProto {
    #[prost(uint64, tag = "1")]
    pub group_id: u64,
    #[prost(uint64, repeated, tag = "2")]
    pub voters: Vec<u64>,
    #[prost(uint64, repeated, tag = "3")]
    pub learners: Vec<u64>,
    #[prost(uint64, repeated, tag = "4")]
    pub witnesses: Vec<u64>,
    #[prost(uint64, tag = "5")]
    pub epoch: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct LogEntryProto {
    #[prost(message, optional, tag = "1")]
    pub log_id: Option<LogIdProto>,
    /// `bytes`, not a string: this is what retires base64 and its 4/3.
    #[prost(bytes = "vec", tag = "2")]
    pub payload: Vec<u8>,
    #[prost(bool, tag = "3")]
    pub is_command: bool,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct ApplySnapshotFenceProto {
    #[prost(uint64, tag = "1")]
    pub applied_index: u64,
    #[prost(uint64, tag = "2")]
    pub commit_index: u64,
    #[prost(uint64, tag = "3")]
    pub installed_snapshot_index: u64,
    #[prost(uint64, tag = "4")]
    pub first_retained_log_index: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct PeerProto {
    #[prost(uint64, tag = "1")]
    pub node_id: u64,
    #[prost(string, tag = "2")]
    pub raft_addr: String,
    #[prost(string, tag = "3")]
    pub snapshot_addr: String,
    /// The role as its own small integer. Read back through `role_from_wire`,
    /// which REFUSES an unknown value rather than defaulting it -- a witness
    /// silently read as a voter would change what a quorum is.
    #[prost(int32, tag = "4")]
    pub role: i32,
    #[prost(bool, tag = "5")]
    pub auto_promote: bool,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct SnapshotMetadataProto {
    #[prost(string, tag = "1")]
    pub snapshot_id: String,
    #[prost(message, optional, tag = "2")]
    pub last_log_id: Option<LogIdProto>,
    #[prost(uint64, repeated, tag = "3")]
    pub membership: Vec<u64>,
    #[prost(message, repeated, tag = "4")]
    pub members: Vec<PeerProto>,
    #[prost(uint64, tag = "5")]
    pub payload_bytes: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct WalRecordProto {
    #[prost(uint64, tag = "1")]
    pub group_id: u64,
    #[prost(uint64, tag = "2")]
    pub node_id: u64,
    #[prost(message, optional, tag = "3")]
    pub hard_state: Option<HardStateProto>,
    #[prost(message, optional, tag = "4")]
    pub membership: Option<MembershipProto>,
    #[prost(message, repeated, tag = "5")]
    pub entries: Vec<LogEntryProto>,
    #[prost(bool, tag = "6")]
    pub entries_are_delta: bool,
    #[prost(message, optional, tag = "7")]
    pub installed_snapshot: Option<SnapshotMetadataProto>,
    #[prost(message, optional, tag = "8")]
    pub apply_snapshot_fence: Option<ApplySnapshotFenceProto>,
    #[prost(string, tag = "9")]
    pub checksum: String,
}

fn role_to_wire(role: ReplicaRole) -> i32 {
    match role {
        ReplicaRole::Voter => 0,
        ReplicaRole::Learner => 1,
        ReplicaRole::Witness => 2,
    }
}

/// An unknown role is an error, not a default.
///
/// `ReplicaRole::default()` is `Voter`, and defaulting an unrecognised value to
/// it would turn a witness or a learner into a voter on recovery -- which
/// changes the quorum this node believes in. A record carrying a role this
/// build does not know is a record this build cannot safely act on.
fn role_from_wire(value: i32) -> Result<ReplicaRole, RaftError> {
    match value {
        0 => Ok(ReplicaRole::Voter),
        1 => Ok(ReplicaRole::Learner),
        2 => Ok(ReplicaRole::Witness),
        other => Err(RaftError::Storage(format!(
            "WAL record carries an unknown replica role {other}"
        ))),
    }
}

fn missing(field: &str) -> RaftError {
    RaftError::Storage(format!("WAL record is missing {field}"))
}

impl From<&LogId> for LogIdProto {
    fn from(value: &LogId) -> Self {
        Self {
            term: value.term,
            index: value.index,
        }
    }
}

impl From<&LogIdProto> for LogId {
    fn from(value: &LogIdProto) -> Self {
        Self {
            term: value.term,
            index: value.index,
        }
    }
}

impl From<&WalRecord> for WalRecordProto {
    fn from(record: &WalRecord) -> Self {
        Self {
            group_id: record.group_id,
            node_id: record.node_id,
            hard_state: Some(HardStateProto {
                current_term: record.hard_state.current_term,
                voted_for: record.hard_state.voted_for,
                committed: record.hard_state.committed.as_ref().map(LogIdProto::from),
            }),
            membership: Some(MembershipProto {
                group_id: record.membership.group_id,
                voters: record.membership.voters.clone(),
                learners: record.membership.learners.clone(),
                witnesses: record.membership.witnesses.clone(),
                epoch: record.membership.epoch,
            }),
            entries: record
                .entries
                .iter()
                .map(|entry| LogEntryProto {
                    log_id: Some(LogIdProto::from(&entry.log_id)),
                    payload: entry.payload.clone(),
                    is_command: entry.is_command,
                })
                .collect(),
            entries_are_delta: record.entries_are_delta,
            installed_snapshot: record.installed_snapshot.as_ref().map(|snapshot| {
                SnapshotMetadataProto {
                    snapshot_id: snapshot.snapshot_id.clone(),
                    last_log_id: Some(LogIdProto::from(&snapshot.last_log_id)),
                    membership: snapshot.membership.clone(),
                    members: snapshot
                        .members
                        .iter()
                        .map(|peer| PeerProto {
                            node_id: peer.node_id,
                            raft_addr: peer.raft_addr.clone(),
                            snapshot_addr: peer.snapshot_addr.clone(),
                            role: role_to_wire(peer.role),
                            auto_promote: peer.auto_promote,
                        })
                        .collect(),
                    payload_bytes: snapshot.payload_bytes,
                }
            }),
            apply_snapshot_fence: Some(ApplySnapshotFenceProto {
                applied_index: record.apply_snapshot_fence.applied_index,
                commit_index: record.apply_snapshot_fence.commit_index,
                installed_snapshot_index: record.apply_snapshot_fence.installed_snapshot_index,
                first_retained_log_index: record.apply_snapshot_fence.first_retained_log_index,
            }),
            checksum: record.checksum.clone(),
        }
    }
}

impl TryFrom<WalRecordProto> for WalRecord {
    type Error = RaftError;

    fn try_from(proto: WalRecordProto) -> Result<Self, Self::Error> {
        let hard_state = proto.hard_state.ok_or_else(|| missing("its hard state"))?;
        let membership = proto.membership.ok_or_else(|| missing("its membership"))?;
        let fence = proto
            .apply_snapshot_fence
            .ok_or_else(|| missing("its apply snapshot fence"))?;

        let mut entries = Vec::with_capacity(proto.entries.len());
        for entry in proto.entries {
            let log_id = entry.log_id.ok_or_else(|| missing("an entry's log id"))?;
            entries.push(LogEntry {
                log_id: LogId::from(&log_id),
                payload: entry.payload,
                is_command: entry.is_command,
            });
        }

        let installed_snapshot = match proto.installed_snapshot {
            None => None,
            Some(snapshot) => {
                let last_log_id = snapshot
                    .last_log_id
                    .ok_or_else(|| missing("the installed snapshot's last log id"))?;
                let mut members = Vec::with_capacity(snapshot.members.len());
                for peer in snapshot.members {
                    members.push(Peer {
                        node_id: peer.node_id,
                        raft_addr: peer.raft_addr,
                        snapshot_addr: peer.snapshot_addr,
                        role: role_from_wire(peer.role)?,
                        auto_promote: peer.auto_promote,
                    });
                }
                Some(SnapshotMetadata {
                    snapshot_id: snapshot.snapshot_id,
                    last_log_id: LogId::from(&last_log_id),
                    membership: snapshot.membership,
                    members,
                    payload_bytes: snapshot.payload_bytes,
                })
            }
        };

        Ok(WalRecord {
            group_id: proto.group_id,
            node_id: proto.node_id,
            hard_state: HardState {
                current_term: hard_state.current_term,
                voted_for: hard_state.voted_for,
                committed: hard_state.committed.as_ref().map(LogId::from),
            },
            membership: Membership {
                group_id: membership.group_id,
                voters: membership.voters,
                learners: membership.learners,
                witnesses: membership.witnesses,
                epoch: membership.epoch,
            },
            entries,
            entries_are_delta: proto.entries_are_delta,
            installed_snapshot,
            apply_snapshot_fence: ApplySnapshotFence {
                applied_index: fence.applied_index,
                commit_index: fence.commit_index,
                installed_snapshot_index: fence.installed_snapshot_index,
                first_retained_log_index: fence.first_retained_log_index,
            },
            checksum: proto.checksum,
        })
    }
}

/// FNV-1a over the record bytes, as the frame's integrity check.
///
/// Not CRC32 despite the field's name in the design being followed: this crate
/// adds no dependency to get one, and the WAL's own record checksum is already
/// FNV-1a. The frame check answers a different question from that one -- it
/// catches a torn or shredded frame before the bytes are handed to a decoder,
/// where the record checksum catches a record whose CONTENT disagrees with
/// itself.
pub(crate) fn frame_digest(bytes: &[u8]) -> u32 {
    let mut hash = 2_166_136_261_u32;
    for byte in bytes {
        hash ^= *byte as u32;
        hash = hash.wrapping_mul(16_777_619);
    }
    hash
}

/// One record, framed: length, digest, then the encoded message.
pub(crate) fn encode_framed(record: &WalRecord) -> Result<Vec<u8>, RaftError> {
    let proto = WalRecordProto::from(record);
    let body = prost::Message::encode_to_vec(&proto);
    let length = u32::try_from(body.len())
        .map_err(|_| RaftError::Storage("WAL record exceeds a frame length".to_string()))?;
    let mut out = Vec::with_capacity(FRAME_BYTES + body.len());
    out.extend_from_slice(&length.to_le_bytes());
    out.extend_from_slice(&frame_digest(&body).to_le_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

/// What a framed record decodes to, and how many bytes it consumed.
///
/// `Ok(None)` means the frame is incomplete or its digest disagrees -- a torn
/// tail, which the caller truncates to the last whole record. That is the same
/// answer the JSON reader gave for a half-written line, and it must stay the
/// same answer: a crash between a write and its barrier is ordinary.
pub(crate) fn decode_framed(bytes: &[u8]) -> Result<Option<(WalRecord, usize)>, RaftError> {
    if bytes.len() < FRAME_BYTES {
        return Ok(None);
    }
    let length = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let digest = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    let end = FRAME_BYTES
        .checked_add(length)
        .ok_or_else(|| RaftError::Storage("WAL frame length overflows".to_string()))?;
    if bytes.len() < end {
        return Ok(None);
    }
    let body = &bytes[FRAME_BYTES..end];
    if frame_digest(body) != digest {
        return Ok(None);
    }
    let Ok(proto) = <WalRecordProto as prost::Message>::decode(body) else {
        return Ok(None);
    };
    let record = WalRecord::try_from(proto)?;
    Ok(Some((record, end)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matrixraft_wal_checksum;

    fn record(payload_len: usize, is_command: bool) -> WalRecord {
        let mut value = WalRecord {
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
                is_command,
            }],
            entries_are_delta: true,
            installed_snapshot: None,
            apply_snapshot_fence: ApplySnapshotFence {
                applied_index: 1,
                commit_index: 1,
                installed_snapshot_index: 0,
                first_retained_log_index: 1,
            },
            checksum: String::new(),
        };
        value.checksum = matrixraft_wal_checksum(&value);
        value
    }

    #[test]
    fn a_record_survives_the_round_trip_unchanged() {
        for payload in [0usize, 1, 9, 300, 3_000] {
            for is_command in [false, true] {
                let original = record(payload, is_command);
                let framed = encode_framed(&original).expect("encodes");
                let (read, used) = decode_framed(&framed).expect("decodes").expect("whole");
                assert_eq!(read, original, "payload {payload}, command {is_command}");
                assert_eq!(used, framed.len(), "the frame must report its own width");
            }
        }
    }

    #[test]
    fn a_snapshot_and_its_peers_survive_too() {
        // The widest arm: the one nested message with its own repeated message
        // inside it, and the role enum that must not be defaulted.
        let mut original = record(9, false);
        original.installed_snapshot = Some(SnapshotMetadata {
            snapshot_id: "snap-1".to_string(),
            last_log_id: LogId { term: 3, index: 1 },
            membership: vec![1, 2, 3],
            members: vec![
                Peer {
                    node_id: 1,
                    raft_addr: "a:1".to_string(),
                    snapshot_addr: "a:2".to_string(),
                    role: ReplicaRole::Voter,
                    auto_promote: false,
                },
                Peer {
                    node_id: 2,
                    raft_addr: "b:1".to_string(),
                    snapshot_addr: "b:2".to_string(),
                    role: ReplicaRole::Witness,
                    auto_promote: true,
                },
                Peer {
                    node_id: 3,
                    raft_addr: "c:1".to_string(),
                    snapshot_addr: "c:2".to_string(),
                    role: ReplicaRole::Learner,
                    auto_promote: false,
                },
            ],
            payload_bytes: 4_096,
        });
        original.checksum = matrixraft_wal_checksum(&original);

        let framed = encode_framed(&original).expect("encodes");
        let (read, _) = decode_framed(&framed).expect("decodes").expect("whole");
        assert_eq!(read, original);
        assert_eq!(
            read.installed_snapshot.as_ref().unwrap().members[1].role,
            ReplicaRole::Witness
        );
    }

    #[test]
    fn an_unknown_role_is_refused_rather_than_defaulted() {
        // The inverse of the arm above. Defaulting would read a witness as a
        // voter and change what this node believes a quorum is.
        assert!(role_from_wire(0).is_ok());
        assert!(role_from_wire(2).is_ok());
        let refused = role_from_wire(7);
        assert!(
            refused.is_err(),
            "an unrecognised role must not resolve to the default"
        );
    }

    #[test]
    fn a_torn_frame_reads_as_incomplete_rather_than_as_a_record() {
        let framed = encode_framed(&record(9, false)).expect("encodes");
        for cut in [0usize, 1, 4, 7, FRAME_BYTES, framed.len() - 1] {
            assert!(
                decode_framed(&framed[..cut])
                    .expect("no hard error")
                    .is_none(),
                "a frame cut at {cut} must read as incomplete"
            );
        }
        // And a whole frame whose body was altered fails its digest.
        let mut shredded = framed.clone();
        let last = shredded.len() - 1;
        shredded[last] ^= 0xff;
        assert!(
            decode_framed(&shredded).expect("no hard error").is_none(),
            "a frame whose body changed must fail its digest"
        );
    }

    #[test]
    fn the_frame_is_eight_bytes_and_the_magic_cannot_be_json() {
        assert_eq!(FRAME_BYTES, 8, "length plus digest, nothing else");
        assert_ne!(
            SEGMENT_MAGIC[0], b'{',
            "the magic must not collide with a JSON segment's first byte"
        );
        // The envelope this replaces was 412 bytes; state the new one as an
        // arithmetic over the frame plus what prost spends, not as a literal.
        let framed = encode_framed(&record(0, false)).expect("encodes");
        assert!(
            framed.len() < 100,
            "an empty-payload record should now be well under a hundred bytes, got {}",
            framed.len()
        );
    }
    #[test]
    fn the_envelope_is_about_seventy_five_bytes_and_the_payload_slope_is_one() {
        // Measured: 73 B at an empty payload, 75 at nine bytes, 77 at three
        // hundred and at three thousand. Bounded rather than pinned exactly,
        // because prost's own field framing is its business and not a contract
        // this suite should freeze -- but the ORDER of magnitude is the point,
        // against 412 B as JSON.
        let mut envelopes = Vec::new();
        for payload in [0usize, 9, 300, 3_000] {
            let framed = encode_framed(&record(payload, true)).expect("encodes");
            assert!(
                framed.len() >= payload,
                "a record cannot be smaller than its payload"
            );
            envelopes.push(framed.len() - payload);
        }
        for (index, envelope) in envelopes.iter().enumerate() {
            assert!(
                (60..=90).contains(envelope),
                "envelope {index} should sit near 75 bytes, got {envelope}"
            );
        }

        // The other half of the win, and the one the envelope column hides: a
        // binary record has no reason to base64 anything, so a payload byte
        // costs a payload byte. As JSON it cost 4/3 of one.
        let small = encode_framed(&record(300, true)).expect("encodes").len();
        let large = encode_framed(&record(3_000, true)).expect("encodes").len();
        assert_eq!(
            large - small,
            2_700,
            "2700 more payload bytes must cost exactly 2700 more on the wire"
        );
    }
}
