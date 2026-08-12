// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    matrixraft_flexible_apply_with_store, matrixraft_flexible_apply_with_store_report,
    rustraft_apply_entry, EntryPayload, MatrixRaftCheckpoint, MatrixRaftConfigurationApplied,
    MatrixRaftFsm, MatrixRaftFsmEntry, MatrixRaftFsmEntryKind, MatrixRaftStoreFsm, RaftApply,
    RaftApplyRequest, RaftApplyResponse, RaftLogEntry, RaftStateMachine, RustRaftApplyRequest,
    RustRaftApplyResponse, RustRaftLogId, RustRaftNodeId, RustRaftSnapshotChunk,
    RustRaftSnapshotMeta, RustRaftStateMachine,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct DataShardPayload {
    key: String,
    value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetaPayload {
    assignment: String,
}

#[derive(Default)]
struct DataShardStateMachine {
    applied: Vec<(String, Vec<u8>)>,
}

impl RaftApply<String, DataShardPayload> for DataShardStateMachine {
    type Response = Vec<u8>;

    fn apply(
        &mut self,
        request: RaftApplyRequest<String, DataShardPayload>,
    ) -> Result<RaftApplyResponse<Self::Response>, matrixraft::RaftError> {
        assert_eq!(request.group_id, "tenant-a/shard-7");
        self.applied
            .push((request.payload.key, request.payload.value.clone()));
        Ok(RaftApplyResponse {
            applied_index: request.log_id.index,
            response: request.payload.value,
        })
    }
}

impl RaftStateMachine<String, DataShardPayload> for DataShardStateMachine {
    type Snapshot = Vec<(String, Vec<u8>)>;

    fn snapshot(&self, _group_id: String) -> Result<Self::Snapshot, matrixraft::RaftError> {
        Ok(self.applied.clone())
    }

    fn install_snapshot(&mut self, snapshot: Self::Snapshot) -> Result<(), matrixraft::RaftError> {
        self.applied = snapshot;
        Ok(())
    }
}

#[derive(Default)]
struct MetaStateMachine {
    assignments: Vec<String>,
}

impl RaftApply<u64, MetaPayload> for MetaStateMachine {
    type Response = String;

    fn apply(
        &mut self,
        request: RaftApplyRequest<u64, MetaPayload>,
    ) -> Result<RaftApplyResponse<Self::Response>, matrixraft::RaftError> {
        assert_eq!(request.group_id, 42);
        self.assignments.push(request.payload.assignment.clone());
        Ok(RaftApplyResponse {
            applied_index: request.log_id.index,
            response: request.payload.assignment,
        })
    }
}

#[derive(Default)]
struct OpaqueBytesStateMachine {
    applied: Vec<EntryPayload>,
}

#[derive(Default)]
struct MatrixStyleStateMachine {
    opened: bool,
    closed: bool,
    applied: Vec<(u64, Vec<u8>)>,
    following: Vec<(u64, RustRaftNodeId)>,
    stopped_following: Vec<(u64, RustRaftNodeId)>,
    leader_terms: Vec<u64>,
    stopped_leader_terms: Vec<u64>,
    configs: Vec<MatrixRaftConfigurationApplied>,
    snapshots: Vec<String>,
}

impl MatrixRaftFsm for MatrixStyleStateMachine {
    fn open(&mut self) -> Result<(), matrixraft::RaftError> {
        self.opened = true;
        Ok(())
    }

    fn close(&mut self) -> Result<(), matrixraft::RaftError> {
        self.closed = true;
        Ok(())
    }

    fn apply(&mut self, index: u64, data: &[u8]) -> Result<(), matrixraft::RaftError> {
        self.applied.push((index, data.to_vec()));
        Ok(())
    }

    fn on_start_following(
        &mut self,
        cur_leader_term: u64,
        cur_leader_id: RustRaftNodeId,
    ) -> Result<(), matrixraft::RaftError> {
        self.following.push((cur_leader_term, cur_leader_id));
        Ok(())
    }

    fn on_stop_following(
        &mut self,
        prev_leader_term: u64,
        prev_leader_id: RustRaftNodeId,
    ) -> Result<(), matrixraft::RaftError> {
        self.stopped_following
            .push((prev_leader_term, prev_leader_id));
        Ok(())
    }

    fn on_leader_start(&mut self, term: u64) -> Result<(), matrixraft::RaftError> {
        self.leader_terms.push(term);
        Ok(())
    }

    fn on_leader_stop(&mut self, term: u64) -> Result<(), matrixraft::RaftError> {
        self.stopped_leader_terms.push(term);
        Ok(())
    }

    fn checkpoint(&mut self, path: &str) -> Result<MatrixRaftCheckpoint, matrixraft::RaftError> {
        Ok(MatrixRaftCheckpoint {
            path: path.to_string(),
            applied_index: self.flushed_index(),
        })
    }

    fn on_snapshot_load(&mut self, snapshot_path: &str) -> Result<(), matrixraft::RaftError> {
        self.snapshots.push(snapshot_path.to_string());
        Ok(())
    }

    fn on_configuration_applied(&mut self, config: MatrixRaftConfigurationApplied) {
        self.configs.push(config);
    }

    fn flushed_index(&self) -> u64 {
        self.applied
            .last()
            .map(|(index, _)| *index)
            .unwrap_or_default()
    }
}

#[derive(Default)]
struct MatrixStyleStore {
    next_batch_id: isize,
    begun: Vec<isize>,
    committed: Vec<isize>,
}

impl MatrixRaftStoreFsm for MatrixStyleStore {
    fn begin(&mut self) -> Result<isize, matrixraft::RaftError> {
        self.next_batch_id += 1;
        self.begun.push(self.next_batch_id);
        Ok(self.next_batch_id)
    }

    fn commit(&mut self, batch_id: isize) -> Result<(), matrixraft::RaftError> {
        self.committed.push(batch_id);
        Ok(())
    }
}

impl RustRaftStateMachine for OpaqueBytesStateMachine {
    fn apply(
        &mut self,
        request: RustRaftApplyRequest,
    ) -> Result<RustRaftApplyResponse, matrixraft::RustRaftError> {
        self.applied.push(request.payload.clone());
        Ok(RustRaftApplyResponse {
            applied_index: request.log_id.index,
            response: request.payload,
        })
    }

    fn snapshot(&self) -> Result<RustRaftSnapshotChunk, matrixraft::RustRaftError> {
        Ok(RustRaftSnapshotChunk {
            meta: RustRaftSnapshotMeta {
                snapshot_id: "opaque".to_string(),
                last_log_id: RustRaftLogId { term: 1, index: 1 },
                membership: vec![1, 2, 3],
                members: Vec::new(),
            },
            offset: 0,
            data: self.applied.concat(),
            done: true,
        })
    }

    fn install_snapshot(
        &mut self,
        chunk: RustRaftSnapshotChunk,
    ) -> Result<(), matrixraft::RustRaftError> {
        self.applied = vec![chunk.data];
        Ok(())
    }
}

#[test]
fn generic_apply_trait_accepts_temporalstore_data_shard_payloads() {
    let mut state_machine = DataShardStateMachine::default();
    let response = rustraft_apply_entry(
        &mut state_machine,
        "tenant-a/shard-7".to_string(),
        RaftLogEntry {
            log_id: RustRaftLogId { term: 3, index: 11 },
            payload: DataShardPayload {
                key: "temperature".to_string(),
                value: b"72".to_vec(),
            },
            is_command: true,
        },
    )
    .expect("apply entry");

    assert_eq!(response.applied_index, 11);
    assert_eq!(response.response, b"72");
    assert_eq!(
        state_machine
            .snapshot("tenant-a/shard-7".to_string())
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn generic_apply_trait_accepts_temporalstore_meta_payloads() {
    let mut state_machine = MetaStateMachine::default();
    let response = rustraft_apply_entry(
        &mut state_machine,
        42,
        RaftLogEntry {
            log_id: RustRaftLogId { term: 4, index: 8 },
            payload: MetaPayload {
                assignment: "shard-7 -> node-2".to_string(),
            },
            is_command: true,
        },
    )
    .expect("apply entry");

    assert_eq!(response.applied_index, 8);
    assert_eq!(response.response, "shard-7 -> node-2");
    assert_eq!(state_machine.assignments.len(), 1);
}

#[test]
fn opaque_bytes_state_machine_still_uses_compatibility_trait() {
    let mut state_machine = OpaqueBytesStateMachine::default();
    let response = rustraft_apply_entry(
        &mut state_machine,
        7,
        RaftLogEntry {
            log_id: RustRaftLogId { term: 1, index: 2 },
            payload: b"opaque temporalstore command bytes".to_vec(),
            is_command: true,
        },
    )
    .expect("apply entry");

    assert_eq!(response.applied_index, 2);
    assert_eq!(response.response, b"opaque temporalstore command bytes");
}

#[test]
fn matrix_style_flexible_apply_reports_mixed_meta_and_data_entries() {
    let mut fsm = MatrixStyleStateMachine::default();
    let mut store = MatrixStyleStore::default();
    let report = matrixraft_flexible_apply_with_store_report(
        &mut fsm,
        &mut store,
        vec![
            MatrixRaftFsmEntry::meta(4, 2, b"meta-shard-route".to_vec()),
            MatrixRaftFsmEntry::data(5, 2, b"put temperature=72".to_vec()),
            MatrixRaftFsmEntry::noop(6, 2),
            MatrixRaftFsmEntry::config_change(7, 2, b"add data-node-3".to_vec()),
            MatrixRaftFsmEntry::data(8, 2, b"put humidity=40".to_vec()),
        ],
    )
    .expect("flexible apply report");

    assert_eq!(report.batch_id, 1);
    assert_eq!(report.attempted, 5);
    assert_eq!(report.applied, 2);
    assert_eq!(report.skipped_meta, 1);
    assert_eq!(report.skipped_noop, 1);
    assert_eq!(report.skipped_config_change, 1);
    assert_eq!(
        report.first_log_id,
        Some(RustRaftLogId { term: 2, index: 4 })
    );
    assert_eq!(
        report.last_log_id,
        Some(RustRaftLogId { term: 2, index: 8 })
    );
    assert_eq!(report.applied_through, 8);
    assert_eq!(report.next_index, 9);
    assert_eq!(
        fsm.applied,
        vec![
            (5, b"put temperature=72".to_vec()),
            (8, b"put humidity=40".to_vec())
        ]
    );
    assert_eq!(store.begun, vec![1]);
    assert_eq!(store.committed, vec![1]);

    let next_batch = matrixraft_flexible_apply_with_store(
        &mut fsm,
        &mut store,
        vec![MatrixRaftFsmEntry {
            batch_id: 99,
            log_id: RustRaftLogId { term: 3, index: 9 },
            data: b"put pressure=30".to_vec(),
            kind: MatrixRaftFsmEntryKind::Data,
        }],
    )
    .expect("compat batch apply");
    assert_eq!(next_batch, 2);
    assert_eq!(store.committed, vec![1, 2]);
    assert_eq!(fsm.applied.last(), Some(&(9, b"put pressure=30".to_vec())));
}
