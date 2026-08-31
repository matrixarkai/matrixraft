// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    matrixraft_flexible_apply_with_store, MatrixRaftBatchId, MatrixRaftCheckpoint,
    MatrixRaftConfigurationApplied, MatrixRaftFsm, MatrixRaftFsmEntry, MatrixRaftFsmEntryKind,
    MatrixRaftFsmIterator, MatrixRaftFsmRuntimeBinding, MatrixRaftNodeId, MatrixRaftStatus,
    MatrixRaftStoreFsm, Membership, RaftError, StateRole,
};

#[derive(Debug, Default)]
struct CompatFsm {
    opened: bool,
    closed: bool,
    applied: Vec<(u64, Vec<u8>)>,
    events: Vec<String>,
    configs: Vec<MatrixRaftConfigurationApplied>,
    loaded_snapshots: Vec<String>,
}

impl MatrixRaftFsm for CompatFsm {
    fn open(&mut self) -> Result<(), RaftError> {
        self.opened = true;
        Ok(())
    }

    fn close(&mut self) -> Result<(), RaftError> {
        self.closed = true;
        Ok(())
    }

    fn apply(&mut self, index: u64, data: &[u8]) -> Result<(), RaftError> {
        self.applied.push((index, data.to_vec()));
        Ok(())
    }

    fn flexible_apply(&mut self, iterator: &mut MatrixRaftFsmIterator) -> Result<(), RaftError> {
        while iterator.valid() {
            let index = iterator.index().expect("valid index");
            let data = iterator.data().expect("valid data").to_vec();
            match iterator.kind().expect("valid kind") {
                MatrixRaftFsmEntryKind::Data
                | MatrixRaftFsmEntryKind::ConfigChange
                | MatrixRaftFsmEntryKind::Meta => self.apply(index, &data)?,
                MatrixRaftFsmEntryKind::NoOp => {}
            }
            iterator.next();
        }
        Ok(())
    }

    fn on_start_following(
        &mut self,
        cur_leader_term: u64,
        cur_leader_id: u64,
    ) -> Result<(), RaftError> {
        self.events
            .push(format!("start_following:{cur_leader_term}:{cur_leader_id}"));
        Ok(())
    }

    fn on_stop_following(
        &mut self,
        prev_leader_term: u64,
        prev_leader_id: u64,
    ) -> Result<(), RaftError> {
        self.events.push(format!(
            "stop_following:{prev_leader_term}:{prev_leader_id}"
        ));
        Ok(())
    }

    fn on_leader_start(&mut self, term: u64) -> Result<(), RaftError> {
        self.events.push(format!("leader_start:{term}"));
        Ok(())
    }

    fn on_leader_stop(&mut self, term: u64) -> Result<(), RaftError> {
        self.events.push(format!("leader_stop:{term}"));
        Ok(())
    }

    fn checkpoint(&mut self, path: &str) -> Result<MatrixRaftCheckpoint, RaftError> {
        Ok(MatrixRaftCheckpoint {
            path: path.to_string(),
            applied_index: self.flushed_index(),
        })
    }

    fn on_snapshot_load(&mut self, snapshot_path: &str) -> Result<(), RaftError> {
        self.loaded_snapshots.push(snapshot_path.to_string());
        Ok(())
    }

    fn on_configuration_applied(&mut self, config: MatrixRaftConfigurationApplied) {
        self.configs.push(config);
    }

    fn flushed_index(&self) -> u64 {
        self.applied.last().map(|(index, _)| *index).unwrap_or(0)
    }
}

#[derive(Debug, Default)]
struct CompatStoreFsm {
    next_batch_id: MatrixRaftBatchId,
    begun: Vec<MatrixRaftBatchId>,
    committed: Vec<MatrixRaftBatchId>,
}

impl MatrixRaftStoreFsm for CompatStoreFsm {
    fn begin(&mut self) -> Result<MatrixRaftBatchId, RaftError> {
        self.next_batch_id += 1;
        self.begun.push(self.next_batch_id);
        Ok(self.next_batch_id)
    }

    fn commit(&mut self, batch_id: MatrixRaftBatchId) -> Result<(), RaftError> {
        self.committed.push(batch_id);
        Ok(())
    }
}

fn node(peer_id: u64) -> MatrixRaftNodeId {
    MatrixRaftNodeId {
        peer_id,
        raft_addr: format!("127.0.0.1:{}", 51_000 + peer_id),
        snapshot_addr: format!("127.0.0.1:{}", 52_000 + peer_id),
    }
}

fn status(role: StateRole, term: u64, leader_id: Option<u64>) -> MatrixRaftStatus {
    MatrixRaftStatus {
        node_id: 1,
        group_id: 44,
        role,
        term,
        leader_id,
        leader_lease_valid: true,
        commit_index: 0,
        applied_index: 0,
        last_log_index: 0,
        membership: Membership {
            group_id: 44,
            voters: vec![1, 2, 3],
            learners: Vec::new(),
            witnesses: Vec::new(),
            epoch: 0,
        },
    }
}

#[test]
fn matrixraft_fsm_contract_exposes_iterator_callbacks_and_store_batches() {
    let mut fsm = CompatFsm::default();
    fsm.open().expect("open");
    fsm.on_start_following(3, 11).expect("start following");
    fsm.on_stop_following(3, 11).expect("stop following");
    fsm.on_leader_start(4).expect("leader start");
    fsm.on_leader_stop(4).expect("leader stop");

    let mut iterator = MatrixRaftFsmIterator::new(vec![
        MatrixRaftFsmEntry::data(1, 4, b"data".to_vec()).with_batch_id(77),
        MatrixRaftFsmEntry::noop(2, 4).with_batch_id(77),
        MatrixRaftFsmEntry::config_change(3, 4, b"config".to_vec()).with_batch_id(77),
        MatrixRaftFsmEntry::meta(4, 4, b"meta".to_vec()).with_batch_id(77),
    ]);
    assert!(iterator.valid());
    assert_eq!(iterator.batch_id(), 77);
    assert_eq!(iterator.index(), Some(1));
    assert_eq!(iterator.term(), Some(4));
    assert_eq!(iterator.kind(), Some(MatrixRaftFsmEntryKind::Data));
    assert_eq!(iterator.remaining().len(), 4);
    fsm.flexible_apply(&mut iterator).expect("flexible apply");
    assert!(!iterator.valid());
    assert_eq!(
        fsm.applied,
        vec![
            (1, b"data".to_vec()),
            (3, b"config".to_vec()),
            (4, b"meta".to_vec())
        ]
    );

    fsm.on_configuration_applied(MatrixRaftConfigurationApplied {
        old_config: vec![node(1), node(2), node(3)],
        new_config: vec![node(1), node(2), node(4)],
    });
    assert_eq!(fsm.configs.len(), 1);
    assert_eq!(fsm.flushed_index(), 4);
    let checkpoint = fsm.checkpoint("/tmp/matrixraft-fsm").expect("checkpoint");
    assert_eq!(checkpoint.applied_index, 4);
    fsm.on_snapshot_load("/tmp/matrixraft-fsm")
        .expect("snapshot load");
    assert_eq!(fsm.loaded_snapshots, vec!["/tmp/matrixraft-fsm"]);
    fsm.close().expect("close");
    assert!(fsm.opened);
    assert!(fsm.closed);
    assert_eq!(
        fsm.events,
        vec![
            "start_following:3:11",
            "stop_following:3:11",
            "leader_start:4",
            "leader_stop:4"
        ]
    );

    let mut store = CompatStoreFsm::default();
    let batch_id = matrixraft_flexible_apply_with_store(
        &mut fsm,
        &mut store,
        vec![MatrixRaftFsmEntry::data(5, 4, b"batched".to_vec())],
    )
    .expect("transactional flexible apply");
    assert_eq!(batch_id, 1);
    assert_eq!(store.begun, vec![1]);
    assert_eq!(store.committed, vec![1]);
    assert_eq!(fsm.flushed_index(), 5);
}

#[test]
fn matrixraft_fsm_runtime_binding_invokes_hooks_from_status_and_membership_changes() {
    let mut binding = MatrixRaftFsmRuntimeBinding::new(CompatFsm::default());
    let first = binding
        .observe_status(
            &status(StateRole::Follower, 3, Some(11)),
            vec![node(1), node(2), node(3)],
        )
        .expect("first follower status");
    assert!(first.opened);
    assert!(first.following_started);
    assert!(!first.configuration_applied);

    let same = binding
        .observe_status(
            &status(StateRole::Follower, 3, Some(11)),
            vec![node(1), node(2), node(3)],
        )
        .expect("same follower status");
    assert!(!same.opened);
    assert!(!same.following_started);
    assert!(!same.following_stopped);

    let leader = binding
        .observe_status(
            &status(StateRole::Leader, 4, Some(1)),
            vec![node(1), node(2), node(3)],
        )
        .expect("leader status");
    assert!(leader.following_stopped);
    assert!(leader.leader_started);

    let follower_with_new_config = binding
        .observe_status(
            &status(StateRole::Follower, 5, Some(22)),
            vec![node(1), node(2), node(4)],
        )
        .expect("follower with new config");
    assert!(follower_with_new_config.leader_stopped);
    assert!(follower_with_new_config.following_started);
    assert!(follower_with_new_config.configuration_applied);
    assert_eq!(binding.fsm().configs.len(), 1);
    assert_eq!(
        binding.fsm().events,
        vec![
            "start_following:3:11",
            "stop_following:3:11",
            "leader_start:4",
            "leader_stop:4",
            "start_following:5:22",
        ]
    );

    let closed = binding.close().expect("close binding");
    assert!(closed.closed);
    assert!(closed.following_stopped);
    assert!(binding.fsm().closed);
}
