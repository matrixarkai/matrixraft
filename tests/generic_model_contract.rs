// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    RustRaftGenericApplyRequest, RustRaftGenericApplyResponse, RustRaftGenericLogEntry,
    RustRaftGenericSnapshot, RustRaftGenericSnapshotChunk, RustRaftGroupId, RustRaftLogEntry,
    RustRaftLogId, RustRaftLogIndex, RustRaftNodeId, RustRaftPayload, RustRaftSnapshotMeta,
    RustRaftSnapshotPayload, RustRaftTerm,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TestGroupId(String);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TestPayload {
    bytes: Vec<u8>,
}

#[test]
fn core_id_aliases_are_public_contract_types() {
    let node_id: RustRaftNodeId = 7;
    let group_id: RustRaftGroupId = 42;
    let term: RustRaftTerm = 3;
    let index: RustRaftLogIndex = 9;

    assert_eq!(node_id, 7);
    assert_eq!(group_id, 42);
    assert_eq!(term, 3);
    assert_eq!(index, 9);
}

#[test]
fn default_log_and_snapshot_models_use_byte_payloads() {
    let payload: RustRaftPayload = vec![1, 2, 3];
    let entry = RustRaftLogEntry {
        log_id: RustRaftLogId { term: 1, index: 1 },
        payload,
        is_command: true,
    };

    let snapshot_payload: RustRaftSnapshotPayload = vec![4, 5, 6];
    let snapshot_chunk = GenericSnapshotChunkAlias {
        meta: RustRaftSnapshotMeta {
            snapshot_id: "snap-1".to_string(),
            last_log_id: entry.log_id.clone(),
            membership: vec![1, 2, 3],
            members: Vec::new(),
        },
        offset: 0,
        data: snapshot_payload,
        done: true,
    };

    assert_eq!(entry.payload, vec![1, 2, 3]);
    assert_eq!(snapshot_chunk.data, vec![4, 5, 6]);
}

#[test]
fn generic_models_accept_domain_payload_and_group_ids() {
    let group_id = TestGroupId("tenant-a/table-7".to_string());
    let payload = TestPayload {
        bytes: b"domain command bytes".to_vec(),
    };

    let entry = RustRaftGenericLogEntry {
        log_id: RustRaftLogId { term: 8, index: 13 },
        payload: payload.clone(),
        is_command: true,
    };
    let apply_request = RustRaftGenericApplyRequest {
        group_id: group_id.clone(),
        log_id: entry.log_id.clone(),
        payload: payload.clone(),
    };
    let apply_response = RustRaftGenericApplyResponse {
        applied_index: 13,
        response: payload.clone(),
    };
    let snapshot = RustRaftGenericSnapshot {
        group_id,
        meta: RustRaftSnapshotMeta {
            snapshot_id: "snap-generic".to_string(),
            last_log_id: entry.log_id,
            membership: vec![11, 12, 13],
            members: Vec::new(),
        },
        payload: payload.clone(),
    };

    assert_eq!(apply_request.payload, payload);
    assert_eq!(apply_response.applied_index, 13);
    assert_eq!(snapshot.payload.bytes, b"domain command bytes");
}

#[test]
fn rustraft_core_does_not_export_temporalstore_command_or_shard_names() {
    let lib_rs = include_str!("../src/lib.rs");

    assert!(!lib_rs.contains("pub struct Command"));
    assert!(!lib_rs.contains("pub enum Command"));
    assert!(!lib_rs.contains("pub type Command"));
    assert!(!lib_rs.contains("pub struct TemporalCommand"));
    assert!(!lib_rs.contains("pub enum TemporalCommand"));
    assert!(!lib_rs.contains("ShardId"));
}

type GenericSnapshotChunkAlias = RustRaftGenericSnapshotChunk<RustRaftSnapshotPayload>;
