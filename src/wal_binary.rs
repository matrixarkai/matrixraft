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
// ## One frame shape, three policies on a bad frame
//
// Every durable and on-wire format in this crate carries the same frame: four
// bytes of magic, a little-endian `u32` length, a little-endian `u32` CRC-32C
// over the body, then the body. What differs is what a bad frame *means*, and
// that is decided at the call site rather than in the frame:
//
// | magic | stream | a bad frame means |
// |---|---|---|
// | `MRWC` | a WAL segment | stop reading here. The recovery reader truncates at that point |
// | `MRCP` | a checkpoint | damage, always. The file is staged and renamed, so a rename is atomic and a half-written one is never under its final name |
// | `MRSD` | a checkpoint descriptor | a hint that cannot be used: fall back to the checkpoint itself |
// | `MRRP` | an RPC message | a corrupt or desynchronised stream: refuse and drop the connection |
//
// So a shared shape does not imply a shared failure model, and the two are
// worth keeping separate on purpose. A reader reaching here to make the
// policies agree should know they were chosen to differ.
//
// ### The WAL row is coarser than it looks, and deliberately so
//
// `decode_framed` returns `Ok(None)` for a short frame, a short body, a failed
// digest and an undecodable payload alike. It has no notion of *where* in the
// segment it is, so it cannot say "a torn tail here, damage there".
//
// That is not an oversight, and the obvious refinement does not work: to learn
// whether valid records follow a bad one, a reader has to step over the bad one
// by its **declared length** -- the single field a failed digest gives it no
// reason to trust. Too large and it runs off the end exactly as a torn tail
// does; too small and it resyncs onto a boundary inside a record. Position is
// not knowable from a frame you have already failed to validate.
//
// What the digest buys, then, is not a tail-versus-damage verdict. It is the
// knowledge that the frame is invalid *at all* -- which a length-only frame
// cannot provide, because a corrupted length field produces a frame that is
// structurally plausible and simply wrong.
//
// ### An open consequence, not yet addressed
//
// Because a failed digest stops the read wherever it sits, a record whose bytes
// are **all present** but whose digest fails is discarded along with everything
// after it. Such a record was written completely, and therefore acknowledged, so
// this is loss of acknowledged data rather than repair of a torn tail.
//
// Shortness is the acknowledgement test: a short record was never completed, so
// truncating it is correct. A failed digest is not that test. A stricter reader
// would refuse rather than truncate in that case. This is recorded here because
// it is a property of the recovery policy rather than of the frame, and
// changing it is a behaviour change that wants its own measurement.
//
// ### Byte order does not travel
//
// The length and digest here are little-endian, as they are in every other
// frame in this crate.
//
// That is this crate's convention and not a shared one. A repository that pins
// this crate is known to use a different byte order in at least one of its own
// durable frames, and to have more than one framing besides. Which stream uses
// which is not verified here and is not this crate's business -- that is
// exactly the point. Two frames looking alike is no evidence that their fields
// agree, and the only thing that would differ is the byte order of two
// multi-byte integers, which fails silently rather than loudly. Read the other
// format before assuming it matches, and do not "harmonise" either into the
// other on the strength of the resemblance.
pub(crate) const SEGMENT_MAGIC: &[u8; 4] = b"MRWC";

/// The first framed shape, still read.
///
/// `MRWB` segments carry an FNV-1a-32 frame digest; `MRWC` carry a CRC-32C one.
/// The magic is what tells them apart, so no reader has to guess which function
/// a frame was written with. A segment never holds both shapes: the active one
/// is converted before the first append, exactly as a JSON segment is, and a
/// sealed one is rewritten the next time compaction touches it.
pub(crate) const SEGMENT_MAGIC_FNV: &[u8; 4] = b"MRWB";

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
    /// Only for a checksum that is not sixteen lowercase hex characters: a
    /// record written by hand, or one being carried across from a JSON
    /// segment. Everything this crate writes goes in `checksum_digest`.
    #[prost(string, tag = "9")]
    pub checksum: String,
    /// The `u64` the checksum spells, 9 bytes on the wire against the 18 the
    /// hex text costs.
    ///
    /// `optional` rather than bare, because an absent digest must not be
    /// confusable with a digest of zero -- a record carrying no checksum would
    /// otherwise read back as sixteen zeros, which is a valid-looking checksum
    /// that nothing computed.
    #[prost(fixed64, optional, tag = "10")]
    pub checksum_digest: Option<u64>,
}

impl TryFrom<LogEntryProto> for LogEntry {
    type Error = RaftError;

    fn try_from(proto: LogEntryProto) -> Result<Self, Self::Error> {
        let log_id = proto.log_id.ok_or_else(|| missing("an entry's log id"))?;
        Ok(Self {
            log_id: LogId::from(&log_id),
            payload: proto.payload,
            is_command: proto.is_command,
        })
    }
}

impl From<&LogEntry> for LogEntryProto {
    fn from(entry: &LogEntry) -> Self {
        Self {
            log_id: Some(LogIdProto::from(&entry.log_id)),
            payload: entry.payload.clone(),
            is_command: entry.is_command,
        }
    }
}

impl From<&Peer> for PeerProto {
    fn from(peer: &Peer) -> Self {
        Self {
            node_id: peer.node_id,
            raft_addr: peer.raft_addr.clone(),
            snapshot_addr: peer.snapshot_addr.clone(),
            role: role_to_wire(peer.role),
            auto_promote: peer.auto_promote,
        }
    }
}

impl TryFrom<PeerProto> for Peer {
    type Error = RaftError;

    fn try_from(proto: PeerProto) -> Result<Self, Self::Error> {
        Ok(Self {
            node_id: proto.node_id,
            raft_addr: proto.raft_addr,
            snapshot_addr: proto.snapshot_addr,
            role: role_from_wire(proto.role)?,
            auto_promote: proto.auto_promote,
        })
    }
}

impl From<&SnapshotMetadata> for SnapshotMetadataProto {
    fn from(meta: &SnapshotMetadata) -> Self {
        Self {
            snapshot_id: meta.snapshot_id.clone(),
            last_log_id: Some(LogIdProto::from(&meta.last_log_id)),
            membership: meta.membership.clone(),
            members: meta.members.iter().map(PeerProto::from).collect(),
            payload_bytes: meta.payload_bytes,
        }
    }
}

impl TryFrom<SnapshotMetadataProto> for SnapshotMetadata {
    type Error = RaftError;

    fn try_from(proto: SnapshotMetadataProto) -> Result<Self, Self::Error> {
        let last_log_id = proto
            .last_log_id
            .ok_or_else(|| missing("the snapshot's last log id"))?;
        let mut members = Vec::with_capacity(proto.members.len());
        for peer in proto.members {
            members.push(Peer::try_from(peer)?);
        }
        Ok(Self {
            snapshot_id: proto.snapshot_id,
            last_log_id: LogId::from(&last_log_id),
            membership: proto.membership,
            members,
            payload_bytes: proto.payload_bytes,
        })
    }
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
        let (text, digest) = checksum_to_wire(&record.checksum);
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
            installed_snapshot: record
                .installed_snapshot
                .as_ref()
                .map(SnapshotMetadataProto::from),
            apply_snapshot_fence: Some(ApplySnapshotFenceProto {
                applied_index: record.apply_snapshot_fence.applied_index,
                commit_index: record.apply_snapshot_fence.commit_index,
                installed_snapshot_index: record.apply_snapshot_fence.installed_snapshot_index,
                first_retained_log_index: record.apply_snapshot_fence.first_retained_log_index,
            }),
            checksum: text,
            checksum_digest: digest,
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

        let installed_snapshot = proto
            .installed_snapshot
            .map(SnapshotMetadata::try_from)
            .transpose()?;

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
            checksum: checksum_from_wire(proto.checksum, proto.checksum_digest),
        })
    }
}

/// What a snapshot checkpoint file holds.
#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct CheckpointProto {
    #[prost(uint64, tag = "1")]
    pub group_id: u64,
    #[prost(message, optional, tag = "2")]
    pub meta: Option<SnapshotMetadataProto>,
    #[prost(bytes = "vec", tag = "3")]
    pub payload: Vec<u8>,
}

/// A log, as the bytes a checkpoint's payload carries.
/// A checkpoint descriptor, without the byte count.
///
/// `StoredSnapshot` has a `bytes` field and this does not, deliberately. Every
/// reader of a descriptor overwrites that field with the length of the
/// checkpoint file itself -- a sidecar that disagrees with the disk would make
/// the byte totals fiction, and `describe` has said so since it was written.
/// A durable copy that every reader discards is weight, so the binary form
/// does not carry one, and `decode_descriptor` returns zero for it.
///
/// The JSON form still has the field, because files already on disk have it
/// written and the decoder still reads them.
#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct DescriptorProto {
    #[prost(string, tag = "1")]
    pub snapshot_id: String,
    #[prost(uint64, tag = "2")]
    pub group_id: u64,
    #[prost(message, optional, tag = "3")]
    pub last_log_id: Option<LogIdProto>,
}

/// Encodes a descriptor into a framed protobuf record.
pub(crate) fn encode_descriptor(
    snapshot_id: &str,
    group_id: u64,
    last_log_id: &LogId,
) -> Result<Vec<u8>, RaftError> {
    let proto = DescriptorProto {
        snapshot_id: snapshot_id.to_string(),
        group_id,
        last_log_id: Some(LogIdProto::from(last_log_id)),
    };
    let mut body = Vec::with_capacity(prost::Message::encoded_len(&proto));
    prost::Message::encode(&proto, &mut body).map_err(|err| {
        RaftError::Storage(format!("failed to encode a snapshot descriptor: {err}"))
    })?;

    let mut out = Vec::with_capacity(DESCRIPTOR_MAGIC.len() + FRAME_BYTES + body.len());
    out.extend_from_slice(DESCRIPTOR_MAGIC);
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&FrameDigest::Crc32c.of(&body).to_le_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

/// Whether these bytes are a framed descriptor rather than the JSON one.
pub(crate) fn is_framed_descriptor(bytes: &[u8]) -> bool {
    bytes.starts_with(DESCRIPTOR_MAGIC)
}

/// Decodes a framed descriptor, returning the snapshot id, group and log id.
///
/// The byte count is not returned because it is not stored -- see
/// [`DescriptorProto`].
pub(crate) fn decode_descriptor(bytes: &[u8]) -> Result<(String, u64, LogId), RaftError> {
    if !is_framed_descriptor(bytes) {
        return Err(RaftError::Storage(
            "a snapshot descriptor does not open with its magic".to_string(),
        ));
    }
    let rest = &bytes[DESCRIPTOR_MAGIC.len()..];
    if rest.len() < FRAME_BYTES {
        return Err(RaftError::Storage(
            "a snapshot descriptor is shorter than its frame".to_string(),
        ));
    }
    let length = u32::from_le_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;
    let digest = u32::from_le_bytes([rest[4], rest[5], rest[6], rest[7]]);
    let end = FRAME_BYTES.checked_add(length).ok_or_else(|| {
        RaftError::Storage("a snapshot descriptor's length overflows".to_string())
    })?;
    if rest.len() < end {
        return Err(RaftError::Storage(format!(
            "a snapshot descriptor declares {length} bytes and carries {}",
            rest.len() - FRAME_BYTES
        )));
    }
    let body = &rest[FRAME_BYTES..end];
    if FrameDigest::Crc32c.of(body) != digest {
        return Err(RaftError::Storage(
            "a snapshot descriptor fails its frame digest".to_string(),
        ));
    }
    let proto: DescriptorProto = prost::Message::decode(body).map_err(|err| {
        RaftError::Storage(format!("failed to decode a snapshot descriptor: {err}"))
    })?;
    let last_log_id = proto.last_log_id.ok_or_else(|| {
        RaftError::Storage("a snapshot descriptor carries no last log id".to_string())
    })?;
    Ok((proto.snapshot_id, proto.group_id, LogId::from(&last_log_id)))
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct LogProto {
    #[prost(message, repeated, tag = "1")]
    pub entries: Vec<LogEntryProto>,
}

/// Encodes a log for a checkpoint's payload.
///
/// This replaced `serde_json::to_vec(&node.log)`, which cost 4.0 bytes per
/// command byte asymptotically and 11.1x for small commands -- decimal number
/// arrays for the payloads, and about 64 bytes of JSON envelope per entry.
/// A protobuf entry is 10 bytes and a payload byte is a byte.
pub fn matrixraft_encode_log(entries: &[LogEntry]) -> Vec<u8> {
    let proto = LogProto {
        entries: entries.iter().map(LogEntryProto::from).collect(),
    };
    prost::Message::encode_to_vec(&proto)
}

/// The log a checkpoint's payload carries.
///
/// The counterpart of [`matrixraft_encode_log`]. A payload written by an
/// earlier build is JSON, and this does not read it -- a checkpoint payload is
/// opaque to this crate (nothing decodes it; a snapshot whose payload is not a
/// log installs perfectly well), so there is no recovery path that needs the
/// older shape. A caller that stored one and wants it back still has
/// `serde_json`.
pub fn matrixraft_decode_log(bytes: &[u8]) -> Result<Vec<LogEntry>, RaftError> {
    let proto = <LogProto as prost::Message>::decode(bytes)
        .map_err(|err| RaftError::Storage(format!("failed to decode a log: {err}")))?;
    let mut entries = Vec::with_capacity(proto.entries.len());
    for entry in proto.entries {
        entries.push(LogEntry::try_from(entry)?);
    }
    Ok(entries)
}

/// `CheckpointProto`'s payload field number, named because the encoder writes
/// that field by hand and the two must agree.
const CHECKPOINT_PAYLOAD_TAG: u32 = 3;

/// The four bytes that open a framed checkpoint.
///
/// Distinct from the WAL segment's magic on purpose: the two files live in
/// different directories and hold different messages, and a magic that
/// identified only "some framed thing" would let a decoder try a WAL record
/// against a checkpoint and report a torn file rather than the wrong file.
pub(crate) const CHECKPOINT_MAGIC: &[u8; 4] = b"MRCP";

/// A checkpoint's descriptor sidecar.
///
/// `S` for sidecar, and distinct from `MRCP` so a descriptor handed to the
/// checkpoint decoder is refused by its magic rather than by its contents.
pub(crate) const DESCRIPTOR_MAGIC: &[u8; 4] = b"MRSD";

/// One checkpoint, framed exactly as a WAL record is.
///
/// The payload is a protobuf `bytes` field, so it costs a tag, a length varint
/// and the bytes -- 1.000 per payload byte against the 1.333 base64 costs and
/// the 4.000 a JSON number array cost before that.
///
/// Known cost, not hidden: the payload is cloned into the message. The JSON
/// path it replaces also built a complete copy, and a four-times-larger one,
/// so this is strictly better -- but for a large snapshot it is still a full
/// copy, and avoiding it means appending the payload's own tag and length to an
/// otherwise-encoded message by hand. That is a change worth making on its own
/// rather than inside a format change.
pub(crate) fn encode_checkpoint(snapshot: &crate::RaftSnapshot) -> Result<Vec<u8>, RaftError> {
    use prost::bytes::BufMut;

    // The payload is appended by hand rather than put in the message, and the
    // message is encoded straight into the output buffer. Both are about
    // copies, not bytes: the result is identical to what a filled-in
    // `CheckpointProto` encodes, which `the_hand_written_payload_field_matches_what_prost_would_write`
    // asserts for a range of shapes.
    //
    // Measured before this, saving a checkpoint allocated 3.0x the payload --
    // a clone into the message, an encode buffer, and a second copy of that
    // buffer into the framed output. The last of those was introduced with the
    // frame itself. Only the output buffer is unavoidable, so this is 1.0x, and
    // for a snapshot of real state that is the difference between holding one
    // copy and three.
    let head = CheckpointProto {
        group_id: snapshot.group_id,
        meta: Some(SnapshotMetadataProto::from(&snapshot.meta)),
        // Left empty on purpose: proto3 omits an empty `bytes` field entirely,
        // so it contributes nothing here and the real one is appended below.
        // It has the highest tag, and prost writes fields in tag order, so
        // appending it is where prost would have put it.
        payload: Vec::new(),
    };

    let payload = snapshot.payload.as_slice();
    // An EMPTY payload contributes nothing, because proto3 omits an empty
    // `bytes` field and writing a zero-length one by hand would produce two
    // bytes prost does not. That difference is invisible to a byte count on any
    // realistic snapshot and was caught by comparing against prost at a 0-byte
    // payload.
    let payload_len = if payload.is_empty() {
        0
    } else {
        prost::encoding::key_len(CHECKPOINT_PAYLOAD_TAG)
            + prost::encoding::encoded_len_varint(payload.len() as u64)
            + payload.len()
    };
    let body_len = prost::Message::encoded_len(&head) + payload_len;
    let length = u32::try_from(body_len).map_err(|_| {
        RaftError::Storage("snapshot checkpoint exceeds a frame length".to_string())
    })?;

    let mut out = Vec::with_capacity(CHECKPOINT_MAGIC.len() + FRAME_BYTES + body_len);
    out.extend_from_slice(CHECKPOINT_MAGIC);
    // The frame's two words go down as zeroes and are patched once the body is
    // written, because the digest cannot be known before the bytes it covers.
    out.extend_from_slice(&[0u8; FRAME_BYTES]);
    let body_start = out.len();

    prost::Message::encode(&head, &mut out)
        .map_err(|err| RaftError::Storage(format!("failed to encode snapshot: {err}")))?;
    if !payload.is_empty() {
        prost::encoding::encode_key(
            CHECKPOINT_PAYLOAD_TAG,
            prost::encoding::WireType::LengthDelimited,
            &mut out,
        );
        prost::encoding::encode_varint(payload.len() as u64, &mut out);
        out.put_slice(payload);
    }

    debug_assert_eq!(
        out.len() - body_start,
        body_len,
        "the body was not the length this reserved for it"
    );
    let digest = FrameDigest::Crc32c.of(&out[body_start..]);
    let frame = CHECKPOINT_MAGIC.len();
    out[frame..frame + 4].copy_from_slice(&length.to_le_bytes());
    out[frame + 4..frame + 8].copy_from_slice(&digest.to_le_bytes());
    Ok(out)
}

/// Whether these bytes are a framed checkpoint rather than the JSON an earlier
/// build wrote.
pub(crate) fn is_framed_checkpoint(bytes: &[u8]) -> bool {
    bytes.starts_with(CHECKPOINT_MAGIC)
}

/// The checkpoint a framed file holds.
///
/// Every refusal begins "failed to decode snapshot", because callers match on
/// that phrase -- `snapshot_staging` asserts a checkpoint torn from outside the
/// store is refused by looking for it. The specific reason follows it.
///
/// Errors rather than reporting absence, which is the opposite of what
/// [`decode_framed`] does for a WAL record, and deliberately. A WAL's last
/// record can be half-written because a crash between a write and its barrier
/// is ordinary, so a bad digest there means "cut the tail". A checkpoint is
/// written to a staging name and renamed into place, and a rename is atomic --
/// so a reader sees a whole file or no file, and a bad digest means the bytes
/// are damaged. Treating that as absence would turn a corrupt checkpoint into a
/// missing one and lose the only signal that something is wrong.
pub(crate) fn decode_checkpoint(bytes: &[u8]) -> Result<crate::RaftSnapshot, RaftError> {
    if !is_framed_checkpoint(bytes) {
        return Err(RaftError::Storage(
            "failed to decode snapshot: the checkpoint does not open with its magic".to_string(),
        ));
    }
    let framed = &bytes[CHECKPOINT_MAGIC.len()..];
    if framed.len() < FRAME_BYTES {
        return Err(RaftError::Storage(
            "failed to decode snapshot: the checkpoint is shorter than its frame".to_string(),
        ));
    }
    let length = u32::from_le_bytes([framed[0], framed[1], framed[2], framed[3]]) as usize;
    let digest = u32::from_le_bytes([framed[4], framed[5], framed[6], framed[7]]);
    let end = FRAME_BYTES.checked_add(length).ok_or_else(|| {
        RaftError::Storage(
            "failed to decode snapshot: the checkpoint's length overflows".to_string(),
        )
    })?;
    if framed.len() < end {
        return Err(RaftError::Storage(format!(
            "failed to decode snapshot: the checkpoint declares {length} bytes and carries {}",
            framed.len() - FRAME_BYTES
        )));
    }
    let body = &framed[FRAME_BYTES..end];
    if FrameDigest::Crc32c.of(body) != digest {
        return Err(RaftError::Storage(
            "failed to decode snapshot: the checkpoint fails its frame digest".to_string(),
        ));
    }
    let proto = <CheckpointProto as prost::Message>::decode(body)
        .map_err(|err| RaftError::Storage(format!("failed to decode snapshot: {err}")))?;
    let meta = SnapshotMetadata::try_from(
        proto
            .meta
            .ok_or_else(|| missing("the checkpoint's metadata"))?,
    )?;
    Ok(crate::GenericSnapshot {
        group_id: proto.group_id,
        meta,
        payload: proto.payload,
    })
}

/// Splits a record's checksum into the two fields the wire carries.
///
/// A checksum this crate computed is always sixteen lowercase hex characters,
/// so it travels as a `fixed64` and costs 9 bytes instead of 18. Anything else
/// -- a value written by hand, or one carried across from a JSON segment --
/// travels as text, because the point of storing it is to hand back exactly
/// what was stored.
fn checksum_to_wire(checksum: &str) -> (String, Option<u64>) {
    match checksum_as_u64(checksum) {
        Some(value) => (String::new(), Some(value)),
        None => (checksum.to_string(), None),
    }
}

/// The inverse. The digest wins when it is present, since nothing sets both.
fn checksum_from_wire(text: String, digest: Option<u64>) -> String {
    match digest {
        Some(value) => format!("{value:016x}"),
        None => text,
    }
}

/// The `u64` that a sixteen-character lowercase hex string spells.
///
/// Lowercase only, deliberately. `u64::from_str_radix` would take `AB` as
/// happily as `ab`, and re-encoding would hand back the lowercase spelling --
/// so accepting uppercase here would make the round trip change the bytes it
/// was asked to preserve.
fn checksum_as_u64(checksum: &str) -> Option<u64> {
    if checksum.len() != 16 {
        return None;
    }
    if !checksum
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    u64::from_str_radix(checksum, 16).ok()
}

/// Which function checks a frame's digest, decided by the segment's magic.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum FrameDigest {
    /// `MRWB`, the first framed shape.
    Fnv1a32,
    /// `MRWC`. What this build writes.
    Crc32c,
}

impl FrameDigest {
    /// The variant a segment's leading bytes call for.
    ///
    /// `None` means the bytes are not a framed segment's magic at all, which is
    /// a JSON segment and the caller's legacy arm. Returning an Option rather
    /// than defaulting is the point: a framed segment read with the wrong
    /// digest looks exactly like a shredded one, so every record in it would be
    /// discarded as a torn tail.
    pub(crate) fn from_segment(bytes: &[u8]) -> Option<Self> {
        if bytes.starts_with(SEGMENT_MAGIC) {
            Some(Self::Crc32c)
        } else if bytes.starts_with(SEGMENT_MAGIC_FNV) {
            Some(Self::Fnv1a32)
        } else {
            None
        }
    }

    pub(crate) fn of(self, bytes: &[u8]) -> u32 {
        match self {
            Self::Fnv1a32 => fnv1a32(bytes),
            Self::Crc32c => crc32c(bytes),
        }
    }
}

/// CRC-32C (Castagnoli) over a record's bytes, as the frame's integrity check.
///
/// A length and a 32-bit CRC in front of each record, both little-endian, with
/// the CRC covering the serialized message and nothing else. That is the
/// ordinary shape for a log record frame, and CRC-32C is the ordinary choice of
/// function for it -- standardised for iSCSI in RFC 3720 and for SCTP in
/// RFC 4960, and carried in hardware since SSE4.2.
///
/// It replaced an FNV-1a-32 here, which was the wrong tool rather than a slow
/// one. A frame digest exists to catch a torn or rotted record before its bytes
/// reach a decoder, and a CRC is an error-DETECTING code built for exactly that,
/// with a guarantee: every burst error up to 32 bits. FNV-1a is a hash
/// function and offers no such guarantee -- it is built to spread keys over
/// buckets, which is a different job.
///
/// Hand-rolled for the same reason the base64 in `wal.rs` is: this crate adds
/// no dependency, and a gating licence-and-advisory check makes taking one for
/// thirty lines a poor trade. Bit-at-a-time rather than table-driven, because
/// the table is the part that is easy to get silently wrong and a WAL record is
/// tens to thousands of bytes. Pinned against the PUBLISHED check value rather
/// than against its own output, since a hand-rolled CRC that is wrong still
/// looks like a hash.
pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    /// 0x1EDC_6F41 bit-reversed, which is the form a right-shifting
    /// implementation needs.
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

/// FNV-1a-32, which `MRWB` segments carry.
///
/// Kept only to read them. The frame check answers a different question from
/// the record's own checksum -- it catches a torn or shredded frame before the
/// bytes are handed to a decoder, where the record checksum catches a record
/// whose CONTENT disagrees with itself.
pub(crate) fn fnv1a32(bytes: &[u8]) -> u32 {
    let mut hash = 2_166_136_261_u32;
    for byte in bytes {
        hash ^= *byte as u32;
        hash = hash.wrapping_mul(16_777_619);
    }
    hash
}

/// One record, framed: length, digest, then the encoded message.
pub(crate) fn encode_framed(record: &WalRecord) -> Result<Vec<u8>, RaftError> {
    let mut out = Vec::new();
    encode_framed_into(record, &mut out)?;
    Ok(out)
}

pub(crate) fn encode_framed_into(record: &WalRecord, out: &mut Vec<u8>) -> Result<(), RaftError> {
    let checksum_digest = checksum_as_u64(&record.checksum);
    let body_len = wal_record_encoded_len(record, checksum_digest);
    let length = u32::try_from(body_len)
        .map_err(|_| RaftError::Storage("WAL record exceeds a frame length".to_string()))?;
    out.clear();
    out.reserve(FRAME_BYTES + body_len);
    out.extend_from_slice(&length.to_le_bytes());
    out.extend_from_slice(&[0; 4]);
    encode_wal_record(record, checksum_digest, out);
    let digest = FrameDigest::Crc32c.of(&out[FRAME_BYTES..]);
    out[4..FRAME_BYTES].copy_from_slice(&digest.to_le_bytes());
    Ok(())
}

fn wal_record_encoded_len(record: &WalRecord, checksum_digest: Option<u64>) -> usize {
    nonzero_u64_len(1, record.group_id)
        + nonzero_u64_len(2, record.node_id)
        + message_len(3, hard_state_encoded_len(&record.hard_state))
        + message_len(4, membership_encoded_len(&record.membership))
        + record
            .entries
            .iter()
            .map(|entry| message_len(5, log_entry_encoded_len(entry)))
            .sum::<usize>()
        + nonzero_bool_len(6, record.entries_are_delta)
        + record.installed_snapshot.as_ref().map_or(0, |snapshot| {
            message_len(7, snapshot_metadata_encoded_len(snapshot))
        })
        + message_len(8, fence_encoded_len(&record.apply_snapshot_fence))
        + match checksum_digest {
            Some(value) => prost::encoding::fixed64::encoded_len(10, &value),
            None if !record.checksum.is_empty() => {
                prost::encoding::string::encoded_len(9, &record.checksum)
            }
            None => 0,
        }
}

fn encode_wal_record(record: &WalRecord, checksum_digest: Option<u64>, out: &mut Vec<u8>) {
    encode_nonzero_u64(1, record.group_id, out);
    encode_nonzero_u64(2, record.node_id, out);
    encode_message(3, hard_state_encoded_len(&record.hard_state), out, |out| {
        encode_hard_state(&record.hard_state, out)
    });
    encode_message(4, membership_encoded_len(&record.membership), out, |out| {
        encode_membership(&record.membership, out)
    });
    for entry in &record.entries {
        encode_message(5, log_entry_encoded_len(entry), out, |out| {
            encode_log_entry(entry, out)
        });
    }
    encode_nonzero_bool(6, record.entries_are_delta, out);
    if let Some(snapshot) = &record.installed_snapshot {
        encode_message(7, snapshot_metadata_encoded_len(snapshot), out, |out| {
            encode_snapshot_metadata(snapshot, out)
        });
    }
    encode_message(
        8,
        fence_encoded_len(&record.apply_snapshot_fence),
        out,
        |out| encode_fence(&record.apply_snapshot_fence, out),
    );
    match checksum_digest {
        Some(value) => prost::encoding::fixed64::encode(10, &value, out),
        None if !record.checksum.is_empty() => {
            prost::encoding::string::encode(9, &record.checksum, out)
        }
        None => {}
    }
}

fn hard_state_encoded_len(state: &HardState) -> usize {
    nonzero_u64_len(1, state.current_term)
        + state
            .voted_for
            .map_or(0, |value| prost::encoding::uint64::encoded_len(2, &value))
        + state
            .committed
            .as_ref()
            .map_or(0, |log_id| message_len(3, log_id_encoded_len(log_id)))
}

fn encode_hard_state(state: &HardState, out: &mut Vec<u8>) {
    encode_nonzero_u64(1, state.current_term, out);
    if let Some(value) = state.voted_for {
        prost::encoding::uint64::encode(2, &value, out);
    }
    if let Some(log_id) = &state.committed {
        encode_message(3, log_id_encoded_len(log_id), out, |out| {
            encode_log_id(log_id, out)
        });
    }
}

fn membership_encoded_len(membership: &Membership) -> usize {
    nonzero_u64_len(1, membership.group_id)
        + prost::encoding::uint64::encoded_len_packed(2, &membership.voters)
        + prost::encoding::uint64::encoded_len_packed(3, &membership.learners)
        + prost::encoding::uint64::encoded_len_packed(4, &membership.witnesses)
        + nonzero_u64_len(5, membership.epoch)
}

fn encode_membership(membership: &Membership, out: &mut Vec<u8>) {
    encode_nonzero_u64(1, membership.group_id, out);
    prost::encoding::uint64::encode_packed(2, &membership.voters, out);
    prost::encoding::uint64::encode_packed(3, &membership.learners, out);
    prost::encoding::uint64::encode_packed(4, &membership.witnesses, out);
    encode_nonzero_u64(5, membership.epoch, out);
}

fn log_entry_encoded_len(entry: &LogEntry) -> usize {
    message_len(1, log_id_encoded_len(&entry.log_id))
        + bytes_len(2, &entry.payload)
        + nonzero_bool_len(3, entry.is_command)
}

fn encode_log_entry(entry: &LogEntry, out: &mut Vec<u8>) {
    encode_message(1, log_id_encoded_len(&entry.log_id), out, |out| {
        encode_log_id(&entry.log_id, out)
    });
    encode_bytes(2, &entry.payload, out);
    encode_nonzero_bool(3, entry.is_command, out);
}

fn snapshot_metadata_encoded_len(snapshot: &SnapshotMetadata) -> usize {
    string_len(1, &snapshot.snapshot_id)
        + message_len(2, log_id_encoded_len(&snapshot.last_log_id))
        + prost::encoding::uint64::encoded_len_packed(3, &snapshot.membership)
        + snapshot
            .members
            .iter()
            .map(|peer| message_len(4, peer_encoded_len(peer)))
            .sum::<usize>()
        + nonzero_u64_len(5, snapshot.payload_bytes)
}

fn encode_snapshot_metadata(snapshot: &SnapshotMetadata, out: &mut Vec<u8>) {
    encode_string(1, &snapshot.snapshot_id, out);
    encode_message(2, log_id_encoded_len(&snapshot.last_log_id), out, |out| {
        encode_log_id(&snapshot.last_log_id, out)
    });
    prost::encoding::uint64::encode_packed(3, &snapshot.membership, out);
    for peer in &snapshot.members {
        encode_message(4, peer_encoded_len(peer), out, |out| encode_peer(peer, out));
    }
    encode_nonzero_u64(5, snapshot.payload_bytes, out);
}

fn peer_encoded_len(peer: &Peer) -> usize {
    nonzero_u64_len(1, peer.node_id)
        + string_len(2, &peer.raft_addr)
        + string_len(3, &peer.snapshot_addr)
        + nonzero_i32_len(4, role_to_wire(peer.role))
        + nonzero_bool_len(5, peer.auto_promote)
}

fn encode_peer(peer: &Peer, out: &mut Vec<u8>) {
    encode_nonzero_u64(1, peer.node_id, out);
    encode_string(2, &peer.raft_addr, out);
    encode_string(3, &peer.snapshot_addr, out);
    encode_nonzero_i32(4, role_to_wire(peer.role), out);
    encode_nonzero_bool(5, peer.auto_promote, out);
}

fn fence_encoded_len(fence: &ApplySnapshotFence) -> usize {
    nonzero_u64_len(1, fence.applied_index)
        + nonzero_u64_len(2, fence.commit_index)
        + nonzero_u64_len(3, fence.installed_snapshot_index)
        + nonzero_u64_len(4, fence.first_retained_log_index)
}

fn encode_fence(fence: &ApplySnapshotFence, out: &mut Vec<u8>) {
    encode_nonzero_u64(1, fence.applied_index, out);
    encode_nonzero_u64(2, fence.commit_index, out);
    encode_nonzero_u64(3, fence.installed_snapshot_index, out);
    encode_nonzero_u64(4, fence.first_retained_log_index, out);
}

fn log_id_encoded_len(log_id: &LogId) -> usize {
    nonzero_u64_len(1, log_id.term) + nonzero_u64_len(2, log_id.index)
}

fn encode_log_id(log_id: &LogId, out: &mut Vec<u8>) {
    encode_nonzero_u64(1, log_id.term, out);
    encode_nonzero_u64(2, log_id.index, out);
}

fn message_len(tag: u32, body_len: usize) -> usize {
    prost::encoding::key_len(tag) + prost::encoding::encoded_len_varint(body_len as u64) + body_len
}

fn encode_message(
    tag: u32,
    body_len: usize,
    out: &mut Vec<u8>,
    encode_body: impl FnOnce(&mut Vec<u8>),
) {
    prost::encoding::encode_key(tag, prost::encoding::WireType::LengthDelimited, out);
    prost::encoding::encode_varint(body_len as u64, out);
    encode_body(out);
}

fn nonzero_u64_len(tag: u32, value: u64) -> usize {
    if value == 0 {
        0
    } else {
        prost::encoding::uint64::encoded_len(tag, &value)
    }
}

fn encode_nonzero_u64(tag: u32, value: u64, out: &mut Vec<u8>) {
    if value != 0 {
        prost::encoding::uint64::encode(tag, &value, out);
    }
}

fn nonzero_i32_len(tag: u32, value: i32) -> usize {
    if value == 0 {
        0
    } else {
        prost::encoding::int32::encoded_len(tag, &value)
    }
}

fn encode_nonzero_i32(tag: u32, value: i32, out: &mut Vec<u8>) {
    if value != 0 {
        prost::encoding::int32::encode(tag, &value, out);
    }
}

fn nonzero_bool_len(tag: u32, value: bool) -> usize {
    if value {
        prost::encoding::bool::encoded_len(tag, &value)
    } else {
        0
    }
}

fn encode_nonzero_bool(tag: u32, value: bool, out: &mut Vec<u8>) {
    if value {
        prost::encoding::bool::encode(tag, &value, out);
    }
}

fn string_len(tag: u32, value: &str) -> usize {
    bytes_len(tag, value.as_bytes())
}

fn encode_string(tag: u32, value: &str, out: &mut Vec<u8>) {
    encode_bytes(tag, value.as_bytes(), out);
}

fn bytes_len(tag: u32, value: &[u8]) -> usize {
    if value.is_empty() {
        0
    } else {
        prost::encoding::key_len(tag)
            + prost::encoding::encoded_len_varint(value.len() as u64)
            + value.len()
    }
}

fn encode_bytes(tag: u32, value: &[u8], out: &mut Vec<u8>) {
    if value.is_empty() {
        return;
    }
    prost::encoding::encode_key(tag, prost::encoding::WireType::LengthDelimited, out);
    prost::encoding::encode_varint(value.len() as u64, out);
    out.extend_from_slice(value);
}

/// What a framed record decodes to, and how many bytes it consumed.
///
/// `Ok(None)` means the frame is incomplete or its digest disagrees -- a torn
/// tail, which the caller truncates to the last whole record. That is the same
/// answer the JSON reader gave for a half-written line, and it must stay the
/// same answer: a crash between a write and its barrier is ordinary.
///
/// Takes the digest the segment's magic calls for, rather than assuming the
/// current one. Every caller reads a segment file and therefore knows it.
pub(crate) fn decode_framed(
    bytes: &[u8],
    digest_kind: FrameDigest,
) -> Result<Option<(WalRecord, usize)>, RaftError> {
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
    if digest_kind.of(body) != digest {
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
                let (read, used) = decode_framed(&framed, FrameDigest::Crc32c)
                    .expect("decodes")
                    .expect("whole");
                assert_eq!(read, original, "payload {payload}, command {is_command}");
                assert_eq!(used, framed.len(), "the frame must report its own width");
            }
        }
    }

    #[test]
    fn a_reused_frame_buffer_drops_the_previous_tail() {
        let mut reused = Vec::new();
        encode_framed_into(&record(3_000, true), &mut reused).expect("large frame encodes");
        let retained_capacity = reused.capacity();

        let small = record(9, false);
        let expected = encode_framed(&small).expect("small frame encodes");
        encode_framed_into(&small, &mut reused).expect("small frame reuses buffer");

        assert_eq!(reused, expected);
        assert_eq!(reused.capacity(), retained_capacity);
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
        let (read, _) = decode_framed(&framed, FrameDigest::Crc32c)
            .expect("decodes")
            .expect("whole");
        assert_eq!(read, original);
        assert_eq!(
            read.installed_snapshot.as_ref().unwrap().members[1].role,
            ReplicaRole::Witness
        );
    }

    #[test]
    fn direct_record_encoding_matches_the_protobuf_mirror() {
        let mut wide = record(19, true);
        wide.membership.learners = vec![4, 5];
        wide.membership.witnesses = vec![6];
        wide.installed_snapshot = Some(SnapshotMetadata {
            snapshot_id: "snap-wide".to_string(),
            last_log_id: LogId { term: 7, index: 11 },
            membership: vec![1, 2, 3, 6],
            members: vec![
                Peer {
                    node_id: 1,
                    raft_addr: "a:1".to_string(),
                    snapshot_addr: "a:2".to_string(),
                    role: ReplicaRole::Voter,
                    auto_promote: false,
                },
                Peer {
                    node_id: 4,
                    raft_addr: "b:1".to_string(),
                    snapshot_addr: "b:2".to_string(),
                    role: ReplicaRole::Learner,
                    auto_promote: true,
                },
                Peer {
                    node_id: 6,
                    raft_addr: "c:1".to_string(),
                    snapshot_addr: "c:2".to_string(),
                    role: ReplicaRole::Witness,
                    auto_promote: false,
                },
            ],
            payload_bytes: 8_192,
        });
        wide.checksum = matrixraft_wal_checksum(&wide);

        let mut unusual_checksum = record(0, false);
        unusual_checksum.checksum = "keep-this-verbatim".to_string();
        for value in [&wide, &unusual_checksum] {
            let framed = encode_framed(value).expect("encodes");
            let mirror = prost::Message::encode_to_vec(&WalRecordProto::from(value));
            assert_eq!(&framed[FRAME_BYTES..], mirror.as_slice());
        }
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
                decode_framed(&framed[..cut], FrameDigest::Crc32c)
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
            decode_framed(&shredded, FrameDigest::Crc32c)
                .expect("no hard error")
                .is_none(),
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
    fn the_envelope_is_tens_of_bytes_not_hundreds_and_the_payload_slope_is_one() {
        // Measured: 64 B at an empty payload, 66 at nine bytes, 68 at three
        // hundred and at three thousand. Bounded rather than pinned exactly,
        // because prost's own field framing is its business and not a contract
        // this suite should freeze -- but the ORDER of magnitude is the point,
        // against 412 B as JSON.
        //
        // The window is wide on purpose, and was widened once: it used to be
        // 60..=90 around a 73-byte envelope, which left four bytes of headroom
        // below, so narrowing the checksum nearly reddened a bound that exists
        // to catch a return to JSON rather than to pin a figure. The exact
        // bytes are pinned by `what_one_wal_append_costs_on_disk`, and that is
        // the file to change when they move.
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
                (24..=120).contains(envelope),
                "envelope {index} should be tens of bytes, not hundreds like the 412 JSON cost, and not so few that it cannot be carrying a frame, a digest and the record's integers: got {envelope}"
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
    /// The `fixed64` path is actually taken, which the byte count depends on.
    ///
    /// A positive control. Every test below checks that some checksum survives
    /// the round trip, and all of them would pass with every checksum going
    /// through the text field and the nine-byte saving being imaginary -- so
    /// this one asserts the hex characters are NOT on the wire.
    #[test]
    fn a_real_checksum_travels_as_eight_bytes_and_not_as_its_hex_text() {
        let value = record(0, true);
        assert_eq!(
            value.checksum.len(),
            16,
            "the fixture's checksum is not the sixteen hex characters this test is about"
        );
        let framed = encode_framed(&value).expect("encodes");
        let hex = value.checksum.as_bytes();
        assert!(
            !framed.windows(hex.len()).any(|window| window == hex),
            "the checksum's hex text is still on the wire, so it is being carried as a string and the nine-byte figure is wrong"
        );

        let proto = WalRecordProto::from(&value);
        assert!(
            proto.checksum.is_empty(),
            "the text field is populated as well, so the checksum is being written twice"
        );
        assert!(
            proto.checksum_digest.is_some(),
            "the digest field is empty, so nothing is carrying the checksum"
        );
    }

    /// An empty checksum comes back empty, not as sixteen zeros.
    ///
    /// This is what `optional` on the digest buys. A bare `fixed64` cannot tell
    /// "no checksum" from "a checksum of zero", so a record carrying none would
    /// read back carrying `0000000000000000` -- a well-formed checksum that
    /// nothing ever computed, which `matrixraft_wal_checksum_valid` would then
    /// reject for a reason that has nothing to do with the stored bytes.
    #[test]
    fn a_record_with_no_checksum_does_not_acquire_one_in_transit() {
        let mut value = record(0, true);
        value.checksum = String::new();
        let framed = encode_framed(&value).expect("encodes");
        let (back, _) = decode_framed(&framed, FrameDigest::Crc32c)
            .expect("decodes")
            .expect("whole");
        assert_eq!(
            back.checksum, "",
            "an absent checksum came back as {:?}, so the wire cannot tell absent from zero",
            back.checksum
        );
    }

    /// A checksum of zero is a real value and survives as one.
    ///
    /// The other side of the case above: sixteen zeros is what a `u64` of zero
    /// spells, and it has to round trip as itself rather than being read as
    /// "no checksum".
    #[test]
    fn a_checksum_of_zero_is_a_value_and_not_an_absence() {
        let mut value = record(0, true);
        value.checksum = "0000000000000000".to_string();
        let framed = encode_framed(&value).expect("encodes");
        let (back, _) = decode_framed(&framed, FrameDigest::Crc32c)
            .expect("decodes")
            .expect("whole");
        assert_eq!(back.checksum, "0000000000000000");
        assert_eq!(
            WalRecordProto::from(&value).checksum_digest,
            Some(0),
            "a checksum of zero should travel as a digest of zero, not as text"
        );
    }

    /// Anything that is not sixteen lowercase hex characters travels verbatim.
    ///
    /// Two callers depend on this. A test that damages a checksum to see
    /// recovery refuse the record writes a value like the one below, and it has
    /// to survive encoding so that what fails is the checksum CHECK and not the
    /// parse. And a record carried across from a JSON segment can hold whatever
    /// that file held.
    #[test]
    fn a_checksum_that_is_not_sixteen_hex_characters_survives_verbatim() {
        for odd in [
            "not-the-right-checksum",
            "",
            "0",
            "d01ebf93ae8ef00",
            "d01ebf93ae8ef0066",
            "D01EBF93AE8EF006",
            "zzzzzzzzzzzzzzzz",
            "d01ebf93ae8ef00 ",
        ] {
            let mut value = record(0, true);
            value.checksum = odd.to_string();
            let framed = encode_framed(&value).expect("encodes");
            let (back, _) = decode_framed(&framed, FrameDigest::Crc32c)
                .expect("decodes")
                .expect("whole");
            assert_eq!(
                back.checksum, odd,
                "the checksum {odd:?} did not survive the round trip"
            );
        }
    }

    /// Uppercase hex is deliberately not accepted into the digest.
    ///
    /// `u64::from_str_radix` takes `AB` as happily as `ab`, so accepting it
    /// would make the round trip hand back the lowercase spelling of a value
    /// the caller wrote in uppercase -- a silent rewrite of a field whose whole
    /// job is to be compared for equality.
    #[test]
    fn uppercase_hex_is_not_rewritten_into_lowercase() {
        let mut value = record(0, true);
        value.checksum = "D01EBF93AE8EF006".to_string();
        let proto = WalRecordProto::from(&value);
        assert!(
            proto.checksum_digest.is_none(),
            "uppercase hex went into the digest, so decoding would rewrite it as lowercase"
        );
        assert_eq!(proto.checksum, "D01EBF93AE8EF006");
    }
    /// The CRC matches its published check value, not its own output.
    ///
    /// The whole risk with a hand-rolled CRC is that it computes *a* function
    /// of the bytes -- stable, plausible, and not the one it claims. Pinning it
    /// against a value this code did not produce is the only check that
    /// catches that, so these two came from published test vectors rather than
    /// from running the function and writing down the answer.
    ///
    /// Note what is NOT asserted here: 0xCBF43926 is the check value for
    /// CRC-32/ISO-HDLC, a different CRC, and test suites have been seen using
    /// it for Castagnoli by mistake. If this arm is ever "fixed" by swapping in
    /// that constant, the fix is the bug.
    ///
    /// Measured, not assumed: planting the UNREFLECTED polynomial
    /// (`0x1EDC_6F41` where a right-shifting implementation needs
    /// `0x82F6_3B78`) reddens **this arm and no other** -- 15 of the 16 in this
    /// module still pass. A wrong CRC round trips perfectly, because the
    /// encoder and the decoder agree with each other; only a value this code
    /// did not produce can tell them both apart from the format they claim to
    /// be. That is the entire argument for a published check value over a
    /// self-consistency test.
    #[test]
    fn the_crc_matches_its_published_check_value() {
        assert_eq!(
            crc32c(b"123456789"),
            0xE306_9283,
            "CRC-32C of the standard check string is wrong, so this is some other function of the bytes"
        );
        assert_eq!(
            crc32c(b""),
            0,
            "CRC-32C of nothing should be zero, which also pins the init and the final inversion against each other"
        );
        assert_ne!(
            crc32c(b"123456789"),
            0xCBF4_3926,
            "that is CRC-32/ISO-HDLC's check value: this is computing the wrong CRC"
        );
    }

    /// A one-bit change moves the digest, which is the property it exists for.
    ///
    /// Weak as a test of a CRC and strong as a test of the wiring: a digest
    /// function that ignored its input, or hashed a length rather than bytes,
    /// would pass the check values above only by coincidence but cannot pass
    /// this.
    #[test]
    fn flipping_one_bit_of_a_record_moves_its_frame_digest() {
        let framed = encode_framed(&record(64, true)).expect("encodes");
        let body = &framed[FRAME_BYTES..];
        let clean = crc32c(body);
        for bit in [0usize, 1, 7, 8, 63] {
            let mut damaged = body.to_vec();
            damaged[bit / 8] ^= 1 << (bit % 8);
            assert_ne!(
                crc32c(&damaged),
                clean,
                "flipping bit {bit} left the digest unchanged"
            );
        }
    }

    /// The magic says which digest a segment's frames carry, and reading one
    /// with the other discards every record in it.
    ///
    /// This is why `FrameDigest::from_segment` returns an Option instead of
    /// defaulting to the current shape. A frame checked against the wrong
    /// function fails its digest, which the reader cannot tell from a shredded
    /// frame -- so it is treated as a torn tail and the file is truncated. A
    /// default would therefore not be a lenient fallback; it would be silent
    /// data loss on every segment written by an earlier build.
    #[test]
    fn a_frame_read_with_the_wrong_digest_looks_torn_rather_than_valid() {
        let framed = encode_framed(&record(9, true)).expect("encodes");

        let (whole, used) = decode_framed(&framed, FrameDigest::Crc32c)
            .expect("no hard error")
            .expect("the right digest reads it whole");
        assert_eq!(used, framed.len());
        assert_eq!(whole.entries.len(), 1);

        assert!(
            decode_framed(&framed, FrameDigest::Fnv1a32)
                .expect("no hard error")
                .is_none(),
            "a CRC-32C frame read as FNV-1a passed its digest check, so the two functions agree on this body and the magic would not need a version"
        );
    }

    /// The two magics are distinct, four bytes, and neither can be mistaken for
    /// the JSON a reader might still meet.
    #[test]
    fn the_two_segment_magics_are_distinct_and_are_not_json() {
        assert_ne!(SEGMENT_MAGIC, SEGMENT_MAGIC_FNV);
        assert_eq!(SEGMENT_MAGIC.len(), 4);
        assert_eq!(SEGMENT_MAGIC_FNV.len(), 4);
        for magic in [SEGMENT_MAGIC, SEGMENT_MAGIC_FNV] {
            assert_ne!(magic[0], b'{', "a segment could be mistaken for JSON");
            assert!(
                FrameDigest::from_segment(magic).is_some(),
                "a magic this crate writes is not recognised by the reader that dispatches on it"
            );
        }
        assert!(
            FrameDigest::from_segment(b"{\"hard_state\":").is_none(),
            "a JSON segment was taken for a framed one"
        );
        assert!(
            FrameDigest::from_segment(b"").is_none(),
            "an empty file was taken for a framed segment"
        );
    }
    /// A segment written by the previous build is read, and then converted.
    ///
    /// The one test in this file that is about data rather than about bytes. A
    /// frame checked against the wrong digest fails, and a failed digest is
    /// indistinguishable from a shredded frame -- so getting this wrong does
    /// not raise an error, it silently truncates every segment an earlier build
    /// wrote and reports a WAL that recovered cleanly to nothing.
    ///
    /// The fixture is built by hand rather than by an older binary, which is
    /// the honest limit of it: it asserts this build reads what it believes the
    /// older shape to be. What makes that more than circular is that the shape
    /// is pinned independently -- `SEGMENT_MAGIC_FNV`, an 8-byte frame, and
    /// FNV-1a-32 over the body -- and those three were the format, not a
    /// description of it.
    #[test]
    fn a_segment_written_with_the_older_digest_is_read_and_then_converted() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "rustraft-crc-migrate-{}-{nonce}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create the wal dir");
        let path = dir.join(format!("{:020}.wal", 0));

        // Two records in the shape the writer produces: the first whole,
        // because it opens the segment, and the second a delta extending it.
        let mut first = record(0, true);
        first.entries_are_delta = false;
        first.checksum = matrixraft_wal_checksum(&first);

        let mut second = record(0, true);
        second.entries[0].log_id.index = 2;
        second.hard_state.committed = Some(LogId { term: 3, index: 2 });
        second.apply_snapshot_fence.applied_index = 2;
        second.apply_snapshot_fence.commit_index = 2;
        second.checksum = matrixraft_wal_checksum(&second);

        let mut bytes = Vec::new();
        bytes.extend_from_slice(SEGMENT_MAGIC_FNV);
        for value in [&first, &second] {
            let body = prost::Message::encode_to_vec(&WalRecordProto::from(value));
            let length = u32::try_from(body.len()).expect("fits");
            bytes.extend_from_slice(&length.to_le_bytes());
            // The older digest, which is the whole point of the fixture.
            bytes.extend_from_slice(&fnv1a32(&body).to_le_bytes());
            bytes.extend_from_slice(&body);
        }
        std::fs::write(&path, &bytes).expect("write the older segment");

        // The reader recognises the shape rather than defaulting to the current
        // one, and an inspector does not touch the file.
        assert_eq!(
            FrameDigest::from_segment(&bytes),
            Some(FrameDigest::Fnv1a32),
            "the older magic was not recognised"
        );
        let seen = crate::matrixraft_read_wal_segment_file(&path).expect("reads the older shape");
        assert_eq!(
            seen,
            vec![first.clone(), second.clone()],
            "the records did not come back from the older shape"
        );
        assert_eq!(
            std::fs::read(&path).expect("re-read"),
            bytes,
            "reading the segment modified it, which an inspector must never do"
        );

        // Opening the WAL converts it, and the records survive the conversion.
        let options = crate::PersistentRaftWalOptions {
            dir: dir.clone(),
            max_records_per_segment: 1_000_000,
            max_segment_bytes: u64::MAX,
            min_keep_segments: 1,
            fsync_on_append: false,
        };
        let mut wal = crate::PersistentRaftWal::open(options).expect("the WAL opens");
        let report = wal.recover().expect("it recovers");
        assert_eq!(
            report
                .recovered
                .as_ref()
                .and_then(|value| value.entries.last())
                .map(|entry| entry.log_id.index),
            Some(2),
            "recovery lost the entries the older segment held"
        );

        // One append is what triggers the conversion, since only the active
        // segment is ever appended to.
        let mut third = record(0, true);
        third.entries[0].log_id.index = 3;
        third.hard_state.committed = Some(LogId { term: 3, index: 3 });
        third.apply_snapshot_fence.applied_index = 3;
        third.apply_snapshot_fence.commit_index = 3;
        third.checksum = matrixraft_wal_checksum(&third);
        wal.append(third).expect("appends after the conversion");
        drop(wal);

        let after = std::fs::read(&path).expect("read the converted segment");
        assert_eq!(
            &after[..4],
            SEGMENT_MAGIC,
            "the segment was appended to without being converted, so it now holds two shapes and nothing can read it whole"
        );
        let converted =
            crate::matrixraft_read_wal_segment_file(&path).expect("reads the converted shape");
        assert_eq!(
            converted.len(),
            3,
            "the conversion did not keep every record it read"
        );
        assert_eq!(
            converted
                .last()
                .and_then(|value| value.entries.last())
                .map(|entry| entry.log_id.index),
            Some(3)
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
    /// The hand-written payload field is byte-identical to prost's own.
    ///
    /// `encode_checkpoint` leaves the message's payload empty and appends that
    /// field itself, to avoid cloning a potentially enormous payload into a
    /// message that is about to be copied again. That is only safe if the bytes
    /// are the same, so this builds the filled-in message the obvious way and
    /// compares, across the shapes where an encoder gets it wrong: an empty
    /// payload, lengths either side of a varint boundary, and every byte value.
    #[test]
    fn the_hand_written_payload_field_matches_what_prost_would_write() {
        for length in [0usize, 1, 2, 127, 128, 129, 16_383, 16_384, 16_385] {
            let mut value = checkpoint(0);
            value.payload = (0..length).map(|index| (index % 256) as u8).collect();
            value.meta.payload_bytes = length as u64;

            let ours = encode_checkpoint(&value).expect("encodes");

            // The obvious way: one message with the payload in it.
            let whole = CheckpointProto {
                group_id: value.group_id,
                meta: Some(SnapshotMetadataProto::from(&value.meta)),
                payload: value.payload.clone(),
            };
            let body = prost::Message::encode_to_vec(&whole);
            let mut expected = Vec::new();
            expected.extend_from_slice(CHECKPOINT_MAGIC);
            expected.extend_from_slice(&(body.len() as u32).to_le_bytes());
            expected.extend_from_slice(&FrameDigest::Crc32c.of(&body).to_le_bytes());
            expected.extend_from_slice(&body);

            assert_eq!(
                ours, expected,
                "the hand-written field differs from prost's at a {length}-byte payload"
            );

            // And it still round trips, which the equality above implies but is
            // cheap to state outright.
            let back = decode_checkpoint(&ours).expect("decodes");
            assert_eq!(back.payload, value.payload, "at {length} bytes");
            assert_eq!(back.meta, value.meta, "at {length} bytes");
        }
    }

    /// The reserved capacity is exactly what gets written.
    ///
    /// The encoder computes the body length before writing, to size the buffer
    /// and to fill in the frame. If that arithmetic and the encoder disagreed,
    /// the frame would declare a length the body does not have -- which the
    /// decoder refuses, so the failure would be a refusal to read back what was
    /// just written rather than anything subtler. A `debug_assert!` in the
    /// encoder catches it there; this catches it here, in a release build too.
    #[test]
    fn the_reserved_body_length_is_what_the_encoder_writes() {
        for length in [0usize, 9, 300, 16_384] {
            let mut value = checkpoint(0);
            value.payload = vec![b'x'; length];
            value.meta.payload_bytes = length as u64;
            let framed = encode_checkpoint(&value).expect("encodes");

            let frame = CHECKPOINT_MAGIC.len();
            let declared = u32::from_le_bytes([
                framed[frame],
                framed[frame + 1],
                framed[frame + 2],
                framed[frame + 3],
            ]) as usize;
            let actual = framed.len() - frame - FRAME_BYTES;
            assert_eq!(
                declared, actual,
                "the frame declares {declared} body bytes and the file carries {actual} at a {length}-byte payload"
            );
        }
    }

    fn checkpoint(payload_len: usize) -> crate::RaftSnapshot {
        crate::GenericSnapshot {
            group_id: 9,
            meta: SnapshotMetadata {
                snapshot_id: "cp-1".to_string(),
                last_log_id: LogId { term: 3, index: 7 },
                membership: vec![1, 2, 3],
                members: vec![Peer {
                    node_id: 2,
                    raft_addr: "127.0.0.1:1".to_string(),
                    snapshot_addr: "127.0.0.1:2".to_string(),
                    role: ReplicaRole::Learner,
                    auto_promote: true,
                }],
                payload_bytes: payload_len as u64,
            },
            payload: vec![b'x'; payload_len],
        }
    }
}
