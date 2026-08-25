// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Generic state-machine apply, replay, and checkpoint helpers.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::marker::PhantomData;

use crate::{
    EntryPayload, RaftApplyRequest, RaftApplyResponse, RaftError, RaftLogEntry,
    RustRaftApplyRequest, RustRaftApplyResponse, RustRaftError, RustRaftGroupId, RustRaftLogId,
    RustRaftLogIndex, RustRaftNodeId, RustRaftRole, RustRaftSnapshotChunk, RustRaftTerm,
};

pub fn matrixraft_apply_entry<S, G, P>(
    state_machine: &mut S,
    group_id: G,
    entry: RaftLogEntry<P>,
) -> Result<RaftApplyResponse<S::Response>, RaftError>
where
    S: RaftApply<G, P>,
{
    state_machine.apply(RaftApplyRequest {
        group_id,
        log_id: entry.log_id,
        payload: entry.payload,
    })
}

pub trait RaftApply<G = RustRaftGroupId, P = EntryPayload> {
    type Response;

    fn apply(
        &mut self,
        request: RaftApplyRequest<G, P>,
    ) -> Result<RaftApplyResponse<Self::Response>, RaftError>;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftFsmApplyOutcome<R> {
    pub response: RaftApplyResponse<R>,
    pub applied: bool,
    pub replayed: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftFsmReplayReport {
    pub attempted: u64,
    pub applied: u64,
    pub skipped_replay: u64,
    pub last_applied: RustRaftLogIndex,
    pub idempotent: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RaftFsmApplyEntryKind {
    Data,
    NoOp,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftFsmBatchApplyReport {
    pub attempted: u64,
    pub applied: u64,
    pub skipped_noop: u64,
    pub skipped_replay: u64,
    pub deferred: u64,
    pub first_log_id: Option<RustRaftLogId>,
    pub last_log_id: Option<RustRaftLogId>,
    pub applied_through: RustRaftLogIndex,
    pub next_index: RustRaftLogIndex,
    pub hit_batch_limit: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftFlexibleApplyReport {
    pub batch_id: MatrixRaftBatchId,
    pub attempted: u64,
    pub applied: u64,
    pub skipped_noop: u64,
    pub skipped_config_change: u64,
    pub skipped_meta: u64,
    pub first_log_id: Option<RustRaftLogId>,
    pub last_log_id: Option<RustRaftLogId>,
    pub applied_through: RustRaftLogIndex,
    pub next_index: RustRaftLogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftFsmCheckpoint<G, S> {
    pub group_id: G,
    pub last_applied: RustRaftLogIndex,
    pub applied_log_ids: Vec<RustRaftLogId>,
    pub snapshot: S,
}

pub trait RaftStateMachine<G = RustRaftGroupId, P = EntryPayload>: RaftApply<G, P> {
    type Snapshot;

    fn snapshot(&self, group_id: G) -> Result<Self::Snapshot, RaftError>;
    fn install_snapshot(&mut self, snapshot: Self::Snapshot) -> Result<(), RaftError>;
}

pub type MatrixRaftBatchId = isize;
pub const MATRIXRAFT_NON_BATCH: MatrixRaftBatchId = 0;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MatrixRaftFsmEntryKind {
    Data,
    NoOp,
    ConfigChange,
    Meta,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftFsmEntry {
    pub batch_id: MatrixRaftBatchId,
    pub log_id: RustRaftLogId,
    pub data: EntryPayload,
    pub kind: MatrixRaftFsmEntryKind,
}

impl MatrixRaftFsmEntry {
    pub fn data(index: RustRaftLogIndex, term: RustRaftTerm, data: EntryPayload) -> Self {
        Self {
            batch_id: MATRIXRAFT_NON_BATCH,
            log_id: RustRaftLogId { term, index },
            data,
            kind: MatrixRaftFsmEntryKind::Data,
        }
    }

    pub fn noop(index: RustRaftLogIndex, term: RustRaftTerm) -> Self {
        Self {
            batch_id: MATRIXRAFT_NON_BATCH,
            log_id: RustRaftLogId { term, index },
            data: Vec::new(),
            kind: MatrixRaftFsmEntryKind::NoOp,
        }
    }

    pub fn config_change(index: RustRaftLogIndex, term: RustRaftTerm, data: EntryPayload) -> Self {
        Self {
            batch_id: MATRIXRAFT_NON_BATCH,
            log_id: RustRaftLogId { term, index },
            data,
            kind: MatrixRaftFsmEntryKind::ConfigChange,
        }
    }

    pub fn meta(index: RustRaftLogIndex, term: RustRaftTerm, data: EntryPayload) -> Self {
        Self {
            batch_id: MATRIXRAFT_NON_BATCH,
            log_id: RustRaftLogId { term, index },
            data,
            kind: MatrixRaftFsmEntryKind::Meta,
        }
    }

    pub fn with_batch_id(mut self, batch_id: MatrixRaftBatchId) -> Self {
        self.batch_id = batch_id;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftFsmIterator {
    entries: Vec<MatrixRaftFsmEntry>,
    position: usize,
}

impl MatrixRaftFsmIterator {
    pub fn new(entries: Vec<MatrixRaftFsmEntry>) -> Self {
        Self {
            entries,
            position: 0,
        }
    }

    pub fn batch_id(&self) -> MatrixRaftBatchId {
        self.current()
            .map(|entry| entry.batch_id)
            .unwrap_or(MATRIXRAFT_NON_BATCH)
    }

    pub fn index(&self) -> Option<RustRaftLogIndex> {
        self.current().map(|entry| entry.log_id.index)
    }

    pub fn term(&self) -> Option<RustRaftTerm> {
        self.current().map(|entry| entry.log_id.term)
    }

    pub fn data(&self) -> Option<&[u8]> {
        self.current().map(|entry| entry.data.as_slice())
    }

    pub fn kind(&self) -> Option<MatrixRaftFsmEntryKind> {
        self.current().map(|entry| entry.kind)
    }

    pub fn next(&mut self) {
        if self.valid() {
            self.position += 1;
        }
    }

    pub fn valid(&self) -> bool {
        self.position < self.entries.len()
    }

    pub fn current(&self) -> Option<&MatrixRaftFsmEntry> {
        self.entries.get(self.position)
    }

    pub fn remaining(&self) -> &[MatrixRaftFsmEntry] {
        &self.entries[self.position.min(self.entries.len())..]
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftCheckpoint {
    pub path: String,
    pub applied_index: RustRaftLogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftConfigurationApplied {
    pub old_config: Vec<crate::MatrixRaftNodeId>,
    pub new_config: Vec<crate::MatrixRaftNodeId>,
}

pub trait MatrixRaftStoreFsm {
    fn begin(&mut self) -> Result<MatrixRaftBatchId, RaftError>;
    fn commit(&mut self, batch_id: MatrixRaftBatchId) -> Result<(), RaftError>;
}

pub trait MatrixRaftFsm {
    fn open(&mut self) -> Result<(), RaftError>;
    fn close(&mut self) -> Result<(), RaftError>;

    fn apply(&mut self, index: RustRaftLogIndex, data: &[u8]) -> Result<(), RaftError> {
        let _ = (index, data);
        Err(RaftError::InvalidRequest(
            "unimplemented MatrixRaftFsm::apply".to_string(),
        ))
    }

    fn flexible_apply(&mut self, iterator: &mut MatrixRaftFsmIterator) -> Result<(), RaftError> {
        while let Some(entry) = iterator.current().cloned() {
            if matches!(entry.kind, MatrixRaftFsmEntryKind::Data) {
                self.apply(entry.log_id.index, &entry.data)?;
            }
            iterator.next();
        }
        Ok(())
    }

    fn on_start_following(
        &mut self,
        cur_leader_term: RustRaftTerm,
        cur_leader_id: RustRaftNodeId,
    ) -> Result<(), RaftError>;

    fn on_stop_following(
        &mut self,
        prev_leader_term: RustRaftTerm,
        prev_leader_id: RustRaftNodeId,
    ) -> Result<(), RaftError>;

    fn on_leader_start(&mut self, term: RustRaftTerm) -> Result<(), RaftError>;
    fn on_leader_stop(&mut self, term: RustRaftTerm) -> Result<(), RaftError>;
    fn checkpoint(&mut self, path: &str) -> Result<MatrixRaftCheckpoint, RaftError>;
    fn on_snapshot_load(&mut self, snapshot_path: &str) -> Result<(), RaftError>;
    fn on_configuration_applied(&mut self, config: MatrixRaftConfigurationApplied);

    fn flushed_index(&self) -> RustRaftLogIndex {
        0
    }
}

pub fn matrixraft_flexible_apply_with_store<F, S>(
    fsm: &mut F,
    store: &mut S,
    entries: Vec<MatrixRaftFsmEntry>,
) -> Result<MatrixRaftBatchId, RaftError>
where
    F: MatrixRaftFsm,
    S: MatrixRaftStoreFsm,
{
    Ok(matrixraft_flexible_apply_with_store_report(fsm, store, entries)?.batch_id)
}

pub fn matrixraft_flexible_apply_with_store_report<F, S>(
    fsm: &mut F,
    store: &mut S,
    entries: Vec<MatrixRaftFsmEntry>,
) -> Result<MatrixRaftFlexibleApplyReport, RaftError>
where
    F: MatrixRaftFsm,
    S: MatrixRaftStoreFsm,
{
    let batch_id = store.begin()?;
    let entries = entries
        .into_iter()
        .map(|entry| entry.with_batch_id(batch_id))
        .collect::<Vec<_>>();
    let mut report = MatrixRaftFlexibleApplyReport {
        batch_id,
        attempted: entries.len() as u64,
        applied: 0,
        skipped_noop: 0,
        skipped_config_change: 0,
        skipped_meta: 0,
        first_log_id: entries.first().map(|entry| entry.log_id.clone()),
        last_log_id: entries.last().map(|entry| entry.log_id.clone()),
        applied_through: 0,
        next_index: entries
            .last()
            .map(|entry| entry.log_id.index.saturating_add(1))
            .unwrap_or(0),
    };
    let mut iterator = MatrixRaftFsmIterator::new(entries.clone());
    fsm.flexible_apply(&mut iterator)?;
    for entry in entries {
        match entry.kind {
            MatrixRaftFsmEntryKind::Data => {
                report.applied += 1;
                report.applied_through = report.applied_through.max(entry.log_id.index);
            }
            MatrixRaftFsmEntryKind::NoOp => report.skipped_noop += 1,
            MatrixRaftFsmEntryKind::ConfigChange => report.skipped_config_change += 1,
            MatrixRaftFsmEntryKind::Meta => report.skipped_meta += 1,
        }
    }
    store.commit(batch_id)?;
    Ok(report)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MatrixRaftFsmRuntimeHookReport {
    pub opened: bool,
    pub closed: bool,
    pub leader_started: bool,
    pub leader_stopped: bool,
    pub following_started: bool,
    pub following_stopped: bool,
    pub configuration_applied: bool,
    pub term: RustRaftTerm,
    pub leader_id: Option<RustRaftNodeId>,
    pub role: RustRaftRole,
}

#[derive(Debug, Clone)]
pub struct MatrixRaftFsmRuntimeBinding<F>
where
    F: MatrixRaftFsm,
{
    fsm: F,
    opened: bool,
    role: Option<RustRaftRole>,
    term: RustRaftTerm,
    leader_id: Option<RustRaftNodeId>,
    membership: Vec<crate::MatrixRaftNodeId>,
}

impl<F> MatrixRaftFsmRuntimeBinding<F>
where
    F: MatrixRaftFsm,
{
    pub fn new(fsm: F) -> Self {
        Self {
            fsm,
            opened: false,
            role: None,
            term: 0,
            leader_id: None,
            membership: Vec::new(),
        }
    }

    pub fn fsm(&self) -> &F {
        &self.fsm
    }

    pub fn fsm_mut(&mut self) -> &mut F {
        &mut self.fsm
    }

    pub fn into_inner(self) -> F {
        self.fsm
    }

    pub fn open(&mut self) -> Result<MatrixRaftFsmRuntimeHookReport, RaftError> {
        if !self.opened {
            self.fsm.open()?;
            self.opened = true;
            Ok(self.report(true, false))
        } else {
            Ok(self.report(false, false))
        }
    }

    pub fn close(&mut self) -> Result<MatrixRaftFsmRuntimeHookReport, RaftError> {
        let mut report = self.report(false, false);
        if self.role == Some(RustRaftRole::Leader) {
            self.fsm.on_leader_stop(self.term)?;
            report.leader_stopped = true;
        }
        if self.following() {
            self.fsm
                .on_stop_following(self.term, self.leader_id.unwrap_or_default())?;
            report.following_stopped = true;
        }
        if self.opened {
            self.fsm.close()?;
            self.opened = false;
            report.closed = true;
        }
        self.role = None;
        self.leader_id = None;
        Ok(report)
    }

    pub fn observe_status(
        &mut self,
        status: &crate::MatrixRaftStatus,
        membership: Vec<crate::MatrixRaftNodeId>,
    ) -> Result<MatrixRaftFsmRuntimeHookReport, RaftError> {
        let mut report = self.report(false, false);
        report.term = status.term;
        report.leader_id = status.leader_id;
        report.role = status.role;

        if !self.opened {
            self.fsm.open()?;
            self.opened = true;
            report.opened = true;
        }

        let old_follow = self.following();
        let old_term = self.term;
        let old_leader = self.leader_id;
        let new_follow = status.role == RustRaftRole::Follower && status.leader_id.is_some();
        if old_follow && (!new_follow || old_leader != status.leader_id || old_term != status.term)
        {
            self.fsm
                .on_stop_following(old_term, old_leader.unwrap_or_default())?;
            report.following_stopped = true;
        }

        let was_leader = self.role == Some(RustRaftRole::Leader);
        let is_leader = status.role == RustRaftRole::Leader;
        if was_leader && !is_leader {
            self.fsm.on_leader_stop(self.term)?;
            report.leader_stopped = true;
        }
        if !was_leader && is_leader {
            self.fsm.on_leader_start(status.term)?;
            report.leader_started = true;
        }

        if new_follow && (!old_follow || old_leader != status.leader_id || old_term != status.term)
        {
            self.fsm
                .on_start_following(status.term, status.leader_id.unwrap_or_default())?;
            report.following_started = true;
        }

        if !self.membership.is_empty() && self.membership != membership {
            self.fsm
                .on_configuration_applied(MatrixRaftConfigurationApplied {
                    old_config: self.membership.clone(),
                    new_config: membership.clone(),
                });
            report.configuration_applied = true;
        }
        self.membership = membership;
        self.role = Some(status.role);
        self.term = status.term;
        self.leader_id = status.leader_id;
        Ok(report)
    }

    fn following(&self) -> bool {
        self.role == Some(RustRaftRole::Follower) && self.leader_id.is_some()
    }

    fn report(&self, opened: bool, closed: bool) -> MatrixRaftFsmRuntimeHookReport {
        MatrixRaftFsmRuntimeHookReport {
            opened,
            closed,
            leader_started: false,
            leader_stopped: false,
            following_started: false,
            following_stopped: false,
            configuration_applied: false,
            term: self.term,
            leader_id: self.leader_id,
            role: self.role.unwrap_or(RustRaftRole::Follower),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RaftFsmAdapter<S, G = RustRaftGroupId, P = EntryPayload>
where
    S: RaftStateMachine<G, P>,
{
    group_id: G,
    state_machine: S,
    applied: BTreeMap<RustRaftLogIndex, RustRaftTerm>,
    responses: BTreeMap<RustRaftLogIndex, RaftApplyResponse<S::Response>>,
    last_applied: RustRaftLogIndex,
    _payload: PhantomData<P>,
}

impl<S, G, P> RaftFsmAdapter<S, G, P>
where
    S: RaftStateMachine<G, P>,
    G: Clone,
    P: Clone,
    S::Response: Clone,
{
    pub fn new(group_id: G, state_machine: S) -> Self {
        Self {
            group_id,
            state_machine,
            applied: BTreeMap::new(),
            responses: BTreeMap::new(),
            last_applied: 0,
            _payload: PhantomData,
        }
    }

    pub fn apply_entry(
        &mut self,
        entry: RaftLogEntry<P>,
    ) -> Result<RaftFsmApplyOutcome<S::Response>, RaftError> {
        if let Some(term) = self.applied.get(&entry.log_id.index) {
            if *term != entry.log_id.term {
                return Err(RaftError::InvalidRequest(format!(
                    "FSM replay conflict at index {}: existing term {}, replay term {}",
                    entry.log_id.index, term, entry.log_id.term
                )));
            }
            let response = self
                .responses
                .get(&entry.log_id.index)
                .cloned()
                .ok_or_else(|| {
                    RaftError::Storage(format!(
                        "FSM replay response missing for applied index {}",
                        entry.log_id.index
                    ))
                })?;
            return Ok(RaftFsmApplyOutcome {
                response,
                applied: false,
                replayed: true,
                reason: "duplicate_log_id_replayed_idempotently".to_string(),
            });
        }

        let response = self.state_machine.apply(RaftApplyRequest {
            group_id: self.group_id.clone(),
            log_id: entry.log_id.clone(),
            payload: entry.payload,
        })?;
        self.last_applied = self.last_applied.max(response.applied_index);
        self.applied.insert(entry.log_id.index, entry.log_id.term);
        self.responses.insert(entry.log_id.index, response.clone());
        Ok(RaftFsmApplyOutcome {
            response,
            applied: true,
            replayed: false,
            reason: "applied_new_log_id".to_string(),
        })
    }

    pub fn replay_entries<I>(&mut self, entries: I) -> Result<RaftFsmReplayReport, RaftError>
    where
        I: IntoIterator<Item = RaftLogEntry<P>>,
    {
        let mut report = RaftFsmReplayReport {
            attempted: 0,
            applied: 0,
            skipped_replay: 0,
            last_applied: self.last_applied,
            idempotent: true,
        };
        for entry in entries {
            report.attempted += 1;
            let outcome = self.apply_entry(entry)?;
            if outcome.applied {
                report.applied += 1;
            }
            if outcome.replayed {
                report.skipped_replay += 1;
            }
        }
        report.last_applied = self.last_applied;
        Ok(report)
    }

    pub fn apply_batch(
        &mut self,
        entries: &[RaftLogEntry<P>],
        max_entries: usize,
    ) -> Result<RaftFsmBatchApplyReport, RaftError>
    where
        S::Response: Default,
    {
        let apply_count = entries.len().min(max_entries.max(1));
        let mut report = RaftFsmBatchApplyReport {
            attempted: 0,
            applied: 0,
            skipped_noop: 0,
            skipped_replay: 0,
            deferred: entries.len().saturating_sub(apply_count) as u64,
            first_log_id: entries.first().map(|entry| entry.log_id.clone()),
            last_log_id: entries.last().map(|entry| entry.log_id.clone()),
            applied_through: self.last_applied,
            next_index: entries
                .get(apply_count)
                .map(|entry| entry.log_id.index)
                .or_else(|| {
                    entries
                        .last()
                        .map(|entry| entry.log_id.index.saturating_add(1))
                })
                .unwrap_or(self.last_applied),
            hit_batch_limit: entries.len() > apply_count,
        };

        for entry in &entries[..apply_count] {
            report.attempted += 1;
            match matrixraft_fsm_entry_kind(entry) {
                RaftFsmApplyEntryKind::Data => {
                    let outcome = self.apply_entry(entry.clone())?;
                    if outcome.applied {
                        report.applied += 1;
                    }
                    if outcome.replayed {
                        report.skipped_replay += 1;
                    }
                }
                RaftFsmApplyEntryKind::NoOp => {
                    if let Some(term) = self.applied.get(&entry.log_id.index) {
                        if *term != entry.log_id.term {
                            return Err(RaftError::InvalidRequest(format!(
                                "FSM replay conflict at index {}: existing term {}, replay term {}",
                                entry.log_id.index, term, entry.log_id.term
                            )));
                        }
                        report.skipped_replay += 1;
                    } else {
                        self.applied.insert(entry.log_id.index, entry.log_id.term);
                        self.responses.insert(
                            entry.log_id.index,
                            RaftApplyResponse {
                                applied_index: entry.log_id.index,
                                response: S::Response::default(),
                            },
                        );
                        report.applied += 1;
                    }
                    self.last_applied = self.last_applied.max(entry.log_id.index);
                    report.skipped_noop += 1;
                }
            }
            report.applied_through = self.last_applied;
        }

        Ok(report)
    }

    pub fn checkpoint(&self) -> Result<RaftFsmCheckpoint<G, S::Snapshot>, RaftError> {
        Ok(RaftFsmCheckpoint {
            group_id: self.group_id.clone(),
            last_applied: self.last_applied,
            applied_log_ids: self
                .applied
                .iter()
                .map(|(index, term)| RustRaftLogId {
                    term: *term,
                    index: *index,
                })
                .collect(),
            snapshot: self.state_machine.snapshot(self.group_id.clone())?,
        })
    }

    pub fn install_checkpoint(
        &mut self,
        checkpoint: RaftFsmCheckpoint<G, S::Snapshot>,
    ) -> Result<(), RaftError> {
        self.state_machine.install_snapshot(checkpoint.snapshot)?;
        self.last_applied = checkpoint.last_applied;
        self.applied = checkpoint
            .applied_log_ids
            .into_iter()
            .map(|log_id| (log_id.index, log_id.term))
            .collect();
        self.responses.clear();
        Ok(())
    }

    pub fn last_applied(&self) -> RustRaftLogIndex {
        self.last_applied
    }

    pub fn applied_log_count(&self) -> usize {
        self.applied.len()
    }

    pub fn inner(&self) -> &S {
        &self.state_machine
    }

    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.state_machine
    }
}

pub fn matrixraft_fsm_entry_kind<P>(entry: &RaftLogEntry<P>) -> RaftFsmApplyEntryKind {
    if entry.is_command {
        RaftFsmApplyEntryKind::Data
    } else {
        RaftFsmApplyEntryKind::NoOp
    }
}

pub trait RustRaftStateMachine {
    fn apply(
        &mut self,
        request: RustRaftApplyRequest,
    ) -> Result<RustRaftApplyResponse, RustRaftError>;
    fn snapshot(&self) -> Result<RustRaftSnapshotChunk, RustRaftError>;
    fn install_snapshot(&mut self, chunk: RustRaftSnapshotChunk) -> Result<(), RustRaftError>;
}

impl<T> RaftApply<RustRaftGroupId, EntryPayload> for T
where
    T: RustRaftStateMachine,
{
    type Response = EntryPayload;

    fn apply(
        &mut self,
        request: RaftApplyRequest<RustRaftGroupId, EntryPayload>,
    ) -> Result<RaftApplyResponse<Self::Response>, RaftError> {
        let response = RustRaftStateMachine::apply(
            self,
            RustRaftApplyRequest {
                group_id: request.group_id,
                log_id: request.log_id,
                payload: request.payload,
            },
        )?;
        Ok(RaftApplyResponse {
            applied_index: response.applied_index,
            response: response.response,
        })
    }
}
