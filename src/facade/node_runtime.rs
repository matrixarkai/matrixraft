// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// stoppable node runtime worker and command loop.
// Split from src/lib.rs to keep the crate facade small and focused.

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RaftNodeRuntimeState {
    Created,
    Running,
    Stopped,
    Shutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftNodeRuntimeStatus {
    pub node_id: RustRaftNodeId,
    pub group_id: RustRaftGroupId,
    pub state: RaftNodeRuntimeState,
    pub restart_count: u64,
    pub worker_running: bool,
    pub cluster_status: Option<RaftClusterStatusReport>,
    pub wal_lifecycle_status: Option<RustRaftWalLifecycleStatus>,
    pub wal_recovery_report: Option<RaftWalRecoveryReport>,
    pub snapshot_trigger_status: RustRaftSnapshotTriggerStatus,
    pub timer_status: RaftRuntimeTimerStatus,
    pub peer_runtime: Vec<RaftPeerRuntimeState>,
    pub fatal_blocker_report: RustRaftFatalBlockerReport,
}

enum RaftNodeRuntimeOp {
    Start(mpsc::Sender<Result<(), RaftError>>),
    Stop(mpsc::Sender<Result<(), RaftError>>),
    Status(mpsc::Sender<Result<RaftNodeRuntimeStatus, RaftError>>),
    WalLifecycleStatus(mpsc::Sender<Result<RustRaftWalLifecycleStatus, RaftError>>),
    WalRecoveryReport(mpsc::Sender<Result<Option<RaftWalRecoveryReport>, RaftError>>),
    Step(
        RustRaftMessage,
        mpsc::Sender<Result<RustRaftStepResult, RaftError>>,
    ),
    StepBatch(
        Vec<RustRaftMessage>,
        mpsc::Sender<Result<Vec<RustRaftStepResult>, RaftError>>,
    ),
    ReadIndex(
        RustRaftLogIndex,
        mpsc::Sender<Result<ReadIndexResponse, RaftError>>,
    ),
    BoundedStaleReadIndex(
        RustRaftLogIndex,
        RustRaftLogIndex,
        mpsc::Sender<Result<RustRaftReadPathReport, RaftError>>,
    ),
    MembershipWorkflowWithRollback(
        Vec<RaftMembershipOperation>,
        mpsc::Sender<Result<Vec<RaftMembershipExecutionReport>, RaftError>>,
    ),
    MembershipReports(mpsc::Sender<Result<Vec<RaftMembershipExecutionReport>, RaftError>>),
    InstallSnapshot(
        RustRaftNodeId,
        RaftSnapshot,
        RustRaftApplySnapshotFence,
        mpsc::Sender<Result<(), RaftError>>,
    ),
    PeerPipelineStatus(
        RustRaftNodeId,
        mpsc::Sender<Result<RaftPeerPipelineState, RaftError>>,
    ),
    PeerPipelineStatuses(mpsc::Sender<Result<Vec<RaftPeerPipelineState>, RaftError>>),
    IsBusy(mpsc::Sender<Result<bool, RaftError>>),
    LeaderTransferState(mpsc::Sender<Result<Option<RaftLeaderTransferState>, RaftError>>),
    Shutdown(mpsc::Sender<Result<(), RaftError>>),
}

#[derive(Debug)]
pub struct RaftNodeRuntime {
    node_id: RustRaftNodeId,
    group_id: RustRaftGroupId,
    command_tx: Option<mpsc::Sender<RaftNodeRuntimeOp>>,
    worker: Option<thread::JoinHandle<()>>,
    restart_count: u64,
    state: RaftNodeRuntimeState,
}

impl RaftNodeRuntime {
    pub fn create(options: RustRaftNodeOptions) -> Result<Self, RaftError> {
        let node_id = options.node_id;
        let group_id = options.group_id;
        let (command_tx, command_rx) = mpsc::channel();
        let worker = thread::Builder::new()
            .name(format!("rustraft-node-{group_id}-{node_id}"))
            .spawn(move || raft_node_runtime_loop(options, command_rx))
            .map_err(|err| RaftError::Transport(format!("failed to spawn raft node: {err}")))?;
        Ok(Self {
            node_id,
            group_id,
            command_tx: Some(command_tx),
            worker: Some(worker),
            restart_count: 0,
            state: RaftNodeRuntimeState::Created,
        })
    }

    pub fn start(&mut self) -> Result<(), RaftError> {
        self.send_unit(RaftNodeRuntimeOp::Start)?;
        self.state = RaftNodeRuntimeState::Running;
        Ok(())
    }

    pub fn stop(&mut self) -> Result<(), RaftError> {
        self.send_unit(RaftNodeRuntimeOp::Stop)?;
        self.state = RaftNodeRuntimeState::Stopped;
        Ok(())
    }

    pub fn restart(&mut self) -> Result<(), RaftError> {
        if self.state == RaftNodeRuntimeState::Shutdown {
            return Err(RaftError::InvalidRequest(
                "cannot restart a shutdown raft node runtime".to_string(),
            ));
        }
        if self.state == RaftNodeRuntimeState::Running {
            self.stop()?;
        }
        self.restart_count += 1;
        self.start()
    }

    pub fn shutdown(&mut self) -> Result<(), RaftError> {
        if self.state == RaftNodeRuntimeState::Shutdown {
            return Ok(());
        }
        let sender = self.command_tx.take().ok_or_else(|| {
            RaftError::InvalidRequest("raft node runtime channel is closed".to_string())
        })?;
        let (reply_tx, reply_rx) = mpsc::channel();
        sender
            .send(RaftNodeRuntimeOp::Shutdown(reply_tx))
            .map_err(|err| RaftError::Transport(format!("failed to shutdown raft node: {err}")))?;
        let result = recv_runtime_reply(reply_rx)?;
        if let Some(worker) = self.worker.take() {
            worker.join().map_err(|_| {
                RaftError::Transport("raft node worker panicked during shutdown".to_string())
            })?;
        }
        self.state = RaftNodeRuntimeState::Shutdown;
        result
    }

    pub fn propose(&self, payload: RustRaftPayload) -> Result<RustRaftLogId, RaftError> {
        self.propose_with_options(payload, RustRaftProposeOptions::default())
    }

    pub fn propose_with_options(
        &self,
        payload: RustRaftPayload,
        options: RustRaftProposeOptions,
    ) -> Result<RustRaftLogId, RaftError> {
        match self.step(RustRaftMessage::Propose { payload, options })? {
            RustRaftStepResult::Proposed(log_id) => Ok(log_id),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected propose result: {other:?}"
            ))),
        }
    }

    pub fn step(&self, message: RustRaftMessage) -> Result<RustRaftStepResult, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(RaftNodeRuntimeOp::Step(message, reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!("failed to send step to raft node: {err}"))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn step_batch(
        &self,
        messages: Vec<RustRaftMessage>,
    ) -> Result<Vec<RustRaftStepResult>, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(RaftNodeRuntimeOp::StepBatch(messages, reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!("failed to send step batch to raft node: {err}"))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn append_entries_to(
        &self,
        target: RustRaftNodeId,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        match self.step(RustRaftMessage::AppendEntries { target, request })? {
            RustRaftStepResult::AppendEntries(response) => Ok(response),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected append-entries result: {other:?}"
            ))),
        }
    }

    pub fn vote_to(
        &self,
        target: RustRaftNodeId,
        request: VoteRequest,
    ) -> Result<VoteResponse, RaftError> {
        match self.step(RustRaftMessage::Vote { target, request })? {
            RustRaftStepResult::Vote(response) => Ok(response),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected vote result: {other:?}"
            ))),
        }
    }

    pub fn handle_vote_response(
        &self,
        local_node_id: RustRaftNodeId,
        response: VoteResponse,
        pre_vote: bool,
    ) -> Result<(), RaftError> {
        match self.step(RustRaftMessage::VoteResponse {
            local_node_id,
            peer_id: None,
            response,
            pre_vote,
        })? {
            RustRaftStepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected vote response result: {other:?}"
            ))),
        }
    }

    pub fn read_index(
        &self,
        min_commit_index: RustRaftLogIndex,
    ) -> Result<ReadIndexResponse, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(RaftNodeRuntimeOp::ReadIndex(min_commit_index, reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!("failed to send read-index to raft node: {err}"))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn read_index_request(
        &self,
        request: ReadIndexRequest,
    ) -> Result<ReadIndexResponse, RaftError> {
        match self.step(RustRaftMessage::ReadIndex { request })? {
            RustRaftStepResult::ReadIndex(response) => Ok(response),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected read-index result: {other:?}"
            ))),
        }
    }

    pub fn bounded_stale_read_index(
        &self,
        min_commit_index: RustRaftLogIndex,
        max_stale_index_lag: RustRaftLogIndex,
    ) -> Result<RustRaftReadPathReport, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(RaftNodeRuntimeOp::BoundedStaleReadIndex(
                min_commit_index,
                max_stale_index_lag,
                reply_tx,
            ))
            .map_err(|err| {
                RaftError::Transport(format!(
                    "failed to send bounded-stale read-index to raft node: {err}"
                ))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn execute_membership_operation(
        &self,
        operation: RaftMembershipOperation,
    ) -> Result<RaftMembershipExecutionReport, RaftError> {
        match self.step(RustRaftMessage::Membership { operation })? {
            RustRaftStepResult::Membership(report) => Ok(report),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected membership operation result: {other:?}"
            ))),
        }
    }

    pub fn execute_membership_workflow_with_rollback<I>(
        &self,
        operations: I,
    ) -> Result<Vec<RaftMembershipExecutionReport>, RaftError>
    where
        I: IntoIterator<Item = RaftMembershipOperation>,
    {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(RaftNodeRuntimeOp::MembershipWorkflowWithRollback(
                operations.into_iter().collect(),
                reply_tx,
            ))
            .map_err(|err| {
                RaftError::Transport(format!(
                    "failed to send membership workflow to raft node: {err}"
                ))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn membership_execution_reports(
        &self,
    ) -> Result<Vec<RaftMembershipExecutionReport>, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(RaftNodeRuntimeOp::MembershipReports(reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!(
                    "failed to query membership reports from raft node: {err}"
                ))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn install_snapshot_to(
        &self,
        target: RustRaftNodeId,
        snapshot: RaftSnapshot,
        fence: RustRaftApplySnapshotFence,
    ) -> Result<(), RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(RaftNodeRuntimeOp::InstallSnapshot(
                target, snapshot, fence, reply_tx,
            ))
            .map_err(|err| {
                RaftError::Transport(format!(
                    "failed to send snapshot install to raft node: {err}"
                ))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn install_snapshot_chunk_to(
        &self,
        target: RustRaftNodeId,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        match self.step(RustRaftMessage::InstallSnapshot { target, request })? {
            RustRaftStepResult::InstallSnapshot(response) => Ok(response),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected snapshot chunk install result: {other:?}"
            ))),
        }
    }

    pub fn begin_snapshot_send_to(
        &self,
        peer_id: RustRaftNodeId,
        snapshot_id: impl Into<String>,
        snapshot_index: RustRaftLogIndex,
        total_chunks: u64,
    ) -> Result<(), RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::BeginSnapshotSend {
                peer_id,
                snapshot_id: snapshot_id.into(),
                snapshot_index,
                total_chunks,
            },
        })? {
            RustRaftStepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected begin snapshot send result: {other:?}"
            ))),
        }
    }

    pub fn record_snapshot_chunk_sent_to(
        &self,
        peer_id: RustRaftNodeId,
        bytes: u64,
    ) -> Result<(), RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::RecordSnapshotChunkSent { peer_id, bytes },
        })? {
            RustRaftStepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected snapshot chunk-sent result: {other:?}"
            ))),
        }
    }

    pub fn acknowledge_snapshot_chunk_to(&self, peer_id: RustRaftNodeId) -> Result<(), RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::AcknowledgeSnapshotChunk { peer_id },
        })? {
            RustRaftStepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected snapshot chunk acknowledgement result: {other:?}"
            ))),
        }
    }

    pub fn retry_snapshot_chunk_to(&self, peer_id: RustRaftNodeId) -> Result<(), RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::RetrySnapshotChunk { peer_id },
        })? {
            RustRaftStepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected snapshot chunk retry result: {other:?}"
            ))),
        }
    }

    pub fn cancel_snapshot_send_to(&self, peer_id: RustRaftNodeId) -> Result<(), RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::CancelSnapshotSend { peer_id },
        })? {
            RustRaftStepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected snapshot send cancel result: {other:?}"
            ))),
        }
    }

    pub fn begin_snapshot_install_from(
        &self,
        peer_id: RustRaftNodeId,
        snapshot_id: impl Into<String>,
        snapshot_index: RustRaftLogIndex,
        total_chunks: u64,
    ) -> Result<(), RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::BeginSnapshotInstall {
                peer_id,
                snapshot_id: snapshot_id.into(),
                snapshot_index,
                total_chunks,
            },
        })? {
            RustRaftStepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected begin snapshot install result: {other:?}"
            ))),
        }
    }

    pub fn receive_snapshot_chunk_from(
        &self,
        peer_id: RustRaftNodeId,
        bytes: u64,
        done: bool,
    ) -> Result<(), RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::ReceiveSnapshotChunk {
                peer_id,
                bytes,
                done,
            },
        })? {
            RustRaftStepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected snapshot receive progress result: {other:?}"
            ))),
        }
    }

    pub fn rollback_snapshot_install_from(&self, peer_id: RustRaftNodeId) -> Result<(), RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::RollbackSnapshotInstall { peer_id },
        })? {
            RustRaftStepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected snapshot install rollback result: {other:?}"
            ))),
        }
    }

    pub fn catch_up_peer(
        &self,
        peer_id: RustRaftNodeId,
    ) -> Result<RaftLearnerCatchUpLoopReport, RaftError> {
        match self.step(RustRaftMessage::CatchUpPeer { peer_id })? {
            RustRaftStepResult::CatchUpPeer(report) => Ok(report),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected peer catch-up result: {other:?}"
            ))),
        }
    }

    pub fn auto_promote_learner(
        &self,
        learner_id: RustRaftNodeId,
    ) -> Result<RaftLearnerAutoPromoteReport, RaftError> {
        match self.step(RustRaftMessage::AutoPromoteLearner { learner_id })? {
            RustRaftStepResult::AutoPromoteLearner(report) => Ok(report),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected learner auto-promotion result: {other:?}"
            ))),
        }
    }

    pub fn receive_out_of_order_append_for(
        &self,
        peer_id: RustRaftNodeId,
        entry: RustRaftLogEntry,
    ) -> Result<(), RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::ReceiveOutOfOrderAppend { peer_id, entry },
        })? {
            RustRaftStepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected out-of-order append result: {other:?}"
            ))),
        }
    }

    pub fn expire_peer_reorder_queue(&self, peer_id: RustRaftNodeId) -> Result<u64, RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::ExpirePeerReorderQueue { peer_id },
        })? {
            RustRaftStepResult::CompactedLogs(expired) => Ok(expired),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected reorder queue expiration result: {other:?}"
            ))),
        }
    }

    pub fn record_network_error_for(&self, peer_id: RustRaftNodeId) -> Result<(), RaftError> {
        match self.step(RustRaftMessage::NetworkError { peer_id })? {
            RustRaftStepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected network error result: {other:?}"
            ))),
        }
    }

    pub fn peer_pipeline_status(
        &self,
        peer_id: RustRaftNodeId,
    ) -> Result<RaftPeerPipelineState, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(RaftNodeRuntimeOp::PeerPipelineStatus(peer_id, reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!(
                    "failed to send peer pipeline status request to raft node: {err}"
                ))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn peer_pipeline_statuses(&self) -> Result<Vec<RaftPeerPipelineState>, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(RaftNodeRuntimeOp::PeerPipelineStatuses(reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!(
                    "failed to send peer pipeline statuses request to raft node: {err}"
                ))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn is_busy(&self) -> Result<bool, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(RaftNodeRuntimeOp::IsBusy(reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!("failed to send is-busy to raft node: {err}"))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn compact_logs_through(&self, log_index: RustRaftLogIndex) -> Result<u64, RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::CompactLogsThrough { log_index },
        })? {
            RustRaftStepResult::CompactedLogs(compacted) => Ok(compacted),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected log compaction result: {other:?}"
            ))),
        }
    }

    pub fn release_memory(&self) -> Result<bool, RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::ReleaseMemory,
        })? {
            RustRaftStepResult::ReleasedMemory(released) => Ok(released),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected release-memory result: {other:?}"
            ))),
        }
    }

    pub fn compact_logs_with_storage_fence(
        &self,
        log_index: RustRaftLogIndex,
        fence: RustRaftStorageApplyFence,
    ) -> Result<RaftWalCompactionReport, RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::CompactLogsWithStorageFence { log_index, fence },
        })? {
            RustRaftStepResult::FencedCompaction(report) => Ok(report),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected fenced log compaction result: {other:?}"
            ))),
        }
    }

    pub fn checkpoint_snapshot(
        &self,
        node_id: RustRaftNodeId,
        snapshot_id: impl Into<String>,
    ) -> Result<RaftSnapshot, RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::CheckpointSnapshot {
                target: node_id,
                snapshot_id: snapshot_id.into(),
            },
        })? {
            RustRaftStepResult::CheckpointedSnapshot(snapshot) => Ok(snapshot),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected checkpoint snapshot result: {other:?}"
            ))),
        }
    }

    pub fn set_node_healthy(
        &self,
        node_id: RustRaftNodeId,
        healthy: bool,
    ) -> Result<(), RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::SetNodeHealthy { node_id, healthy },
        })? {
            RustRaftStepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected health update result: {other:?}"
            ))),
        }
    }

    pub fn partition_peer(&self, node_id: RustRaftNodeId) -> Result<(), RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::PartitionPeer { peer_id: node_id },
        })? {
            RustRaftStepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected peer partition result: {other:?}"
            ))),
        }
    }

    pub fn heal_peer(
        &self,
        node_id: RustRaftNodeId,
    ) -> Result<RaftLearnerCatchUpLoopReport, RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::HealPeer { peer_id: node_id },
        })? {
            RustRaftStepResult::CatchUpPeer(report) => Ok(report),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected peer heal result: {other:?}"
            ))),
        }
    }

    pub fn set_leader_lease_valid(&self, valid: bool) -> Result<(), RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::SetLeaderLeaseValid { valid },
        })? {
            RustRaftStepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected lease update result: {other:?}"
            ))),
        }
    }

    pub fn set_ignore_witness(&self, ignore_witness: bool) -> Result<(), RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::IgnoreWitness {
                ignore: ignore_witness,
            },
        })? {
            RustRaftStepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected witness policy update result: {other:?}"
            ))),
        }
    }

    pub fn fire_fatal_event(
        &self,
        node_id: RustRaftNodeId,
        reason: impl Into<String>,
    ) -> Result<Option<RustRaftNodeId>, RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::FireFatalEvent {
                node_id,
                reason: reason.into(),
            },
        })? {
            RustRaftStepResult::FatalEvent(target) => Ok(target),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected fatal event result: {other:?}"
            ))),
        }
    }

    pub fn witness_quorum_report<I>(
        &self,
        acknowledgements: I,
    ) -> Result<RaftWitnessQuorumReport, RaftError>
    where
        I: IntoIterator<Item = RustRaftNodeId>,
    {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::WitnessQuorum {
                acknowledgements: acknowledgements.into_iter().collect(),
            },
        })? {
            RustRaftStepResult::WitnessQuorum(report) => Ok(report),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected witness quorum result: {other:?}"
            ))),
        }
    }

    pub fn set_prohibits_election(&self, prohibits_election: bool) -> Result<(), RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::ProhibitsElection {
                prohibits: prohibits_election,
            },
        })? {
            RustRaftStepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected election-prohibit update result: {other:?}"
            ))),
        }
    }

    pub fn transfer_leader(&self, target: RustRaftNodeId) -> Result<(), RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::TransferLeader { target },
        })? {
            RustRaftStepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected leader transfer step result: {other:?}"
            ))),
        }
    }

    pub fn timeout_now(
        &self,
        from: RustRaftNodeId,
        target: RustRaftNodeId,
    ) -> Result<TimeoutNowResponse, RaftError> {
        match self.step(RustRaftMessage::TimeoutNow { from, target })? {
            RustRaftStepResult::TimeoutNow(response) => Ok(response),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected timeout-now step result: {other:?}"
            ))),
        }
    }

    pub fn try_complete_leader_transfer(&self) -> Result<bool, RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::CompleteLeaderTransfer,
        })? {
            RustRaftStepResult::LeaderTransferCompleted(completed) => Ok(completed),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected leader transfer completion result: {other:?}"
            ))),
        }
    }

    pub fn abort_leader_transfer(&self, reason: impl Into<String>) -> Result<bool, RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::AbortLeaderTransfer {
                reason: reason.into(),
            },
        })? {
            RustRaftStepResult::LeaderTransferAborted(aborted) => Ok(aborted),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected leader transfer abort result: {other:?}"
            ))),
        }
    }

    pub fn leader_transfer_state(&self) -> Result<Option<RaftLeaderTransferState>, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(RaftNodeRuntimeOp::LeaderTransferState(reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!(
                    "failed to query leader transfer state from raft node: {err}"
                ))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn step_down(
        &self,
        transferee: Option<RustRaftNodeId>,
    ) -> Result<Option<RustRaftNodeId>, RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::StepDown { transferee },
        })? {
            RustRaftStepResult::StepDown(transferee) => Ok(transferee),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected step-down result: {other:?}"
            ))),
        }
    }

    pub fn resign_leader(&self, reason: impl Into<String>) -> Result<bool, RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::Resign {
                reason: reason.into(),
            },
        })? {
            RustRaftStepResult::LeaderResigned(resigned) => Ok(resigned),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected leader resign result: {other:?}"
            ))),
        }
    }

    pub fn trigger_snapshot(&self) -> Result<RustRaftSnapshotMeta, RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::TriggerSnapshot,
        })? {
            RustRaftStepResult::SnapshotTriggered(snapshot) => Ok(snapshot),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected snapshot trigger step result: {other:?}"
            ))),
        }
    }

    pub fn mark_snapshot_ready(&self, snapshot_id: &str, success: bool) -> Result<(), RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::SnapshotReady {
                snapshot_id: snapshot_id.to_string(),
                success,
            },
        })? {
            RustRaftStepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected snapshot-ready step result: {other:?}"
            ))),
        }
    }

    pub fn complete_snapshot_trigger(&self, snapshot_id: &str) -> Result<(), RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::SnapshotApplied {
                snapshot_id: snapshot_id.to_string(),
            },
        })? {
            RustRaftStepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected snapshot completion step result: {other:?}"
            ))),
        }
    }

    pub fn pre_vote(&self) -> Result<VoteResponse, RaftError> {
        match self.step(RustRaftMessage::PreVote {
            candidate_id: self.node_id,
        })? {
            RustRaftStepResult::PreVote(response) => Ok(response),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected pre-vote step result: {other:?}"
            ))),
        }
    }

    pub fn campaign(&self, forced: bool) -> Result<(), RaftError> {
        match self.step(RustRaftMessage::Admin {
            command: RustRaftAdminCommand::Campaign {
                candidate_id: self.node_id,
                forced,
            },
        })? {
            RustRaftStepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected campaign step result: {other:?}"
            ))),
        }
    }

    pub fn status(&self) -> Result<RaftNodeRuntimeStatus, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(RaftNodeRuntimeOp::Status(reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!("failed to send status to raft node: {err}"))
            })?;
        let mut status = recv_runtime_reply(reply_rx)??;
        status.restart_count = self.restart_count;
        status.state = self.state;
        Ok(status)
    }

    pub fn wal_lifecycle_status(&self) -> Result<RustRaftWalLifecycleStatus, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(RaftNodeRuntimeOp::WalLifecycleStatus(reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!(
                    "failed to query WAL lifecycle status from raft node: {err}"
                ))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn wal_recovery_report(&self) -> Result<Option<RaftWalRecoveryReport>, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(RaftNodeRuntimeOp::WalRecoveryReport(reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!(
                    "failed to query WAL recovery report from raft node: {err}"
                ))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn state(&self) -> RaftNodeRuntimeState {
        self.state
    }

    pub fn node_id(&self) -> RustRaftNodeId {
        self.node_id
    }

    pub fn group_id(&self) -> RustRaftGroupId {
        self.group_id
    }

    pub fn restart_count(&self) -> u64 {
        self.restart_count
    }

    fn send_unit(
        &self,
        command: fn(mpsc::Sender<Result<(), RaftError>>) -> RaftNodeRuntimeOp,
    ) -> Result<(), RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?.send(command(reply_tx)).map_err(|err| {
            RaftError::Transport(format!(
                "failed to send lifecycle command to raft node: {err}"
            ))
        })?;
        recv_runtime_reply(reply_rx)?
    }

    fn sender(&self) -> Result<&mpsc::Sender<RaftNodeRuntimeOp>, RaftError> {
        self.command_tx
            .as_ref()
            .ok_or_else(|| RaftError::InvalidRequest("raft node runtime is shut down".to_string()))
    }
}

impl Drop for RaftNodeRuntime {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn is_leader_transfer_step_message(message: &RustRaftMessage) -> bool {
    matches!(
        message,
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::TransferLeader { .. }
                | RustRaftAdminCommand::StepDown { .. },
        }
    )
}

fn runtime_step_operation_name(message: &RustRaftMessage) -> &'static str {
    match message {
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::TransferLeader { .. },
        } => "transfer_leader",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::CompleteLeaderTransfer,
        } => "try_complete_leader_transfer",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::AbortLeaderTransfer { .. },
        } => "abort_leader_transfer",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::FireFatalEvent { .. },
        } => "fire_fatal_event",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::StepDown { .. },
        } => "step_down",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::Campaign { .. },
        } => "campaign",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::Resign { .. },
        } => "resign_leader",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::SetNodeHealthy { .. },
        } => "set_node_healthy",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::PartitionPeer { .. },
        } => "partition_peer",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::HealPeer { .. },
        } => "heal_peer",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::SetLeaderLeaseValid { .. },
        } => "set_leader_lease_valid",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::IgnoreWitness { .. },
        } => "set_ignore_witness",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::ProhibitsElection { .. },
        } => "set_prohibits_election",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::CompactLogsThrough { .. },
        } => "compact_logs",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::ReleaseMemory,
        } => "release_memory",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::CompactLogsWithStorageFence { .. },
        } => "compact_logs_with_storage_fence",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::CheckpointSnapshot { .. },
        } => "checkpoint_snapshot",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::WitnessQuorum { .. },
        } => "witness_quorum_report",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::BeginSnapshotSend { .. },
        } => "begin_snapshot_send",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::RecordSnapshotChunkSent { .. },
        } => "record_snapshot_chunk_sent",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::AcknowledgeSnapshotChunk { .. },
        } => "acknowledge_snapshot_chunk",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::RetrySnapshotChunk { .. },
        } => "retry_snapshot_chunk",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::CancelSnapshotSend { .. },
        } => "cancel_snapshot_send",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::BeginSnapshotInstall { .. },
        } => "begin_snapshot_install",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::ReceiveSnapshotChunk { .. },
        } => "receive_snapshot_chunk",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::RollbackSnapshotInstall { .. },
        } => "rollback_snapshot_install",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::ReceiveOutOfOrderAppend { .. },
        } => "receive_out_of_order_append",
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::ExpirePeerReorderQueue { .. },
        } => "expire_peer_reorder_queue",
        RustRaftMessage::CatchUpPeer { .. } => "catch_up_peer",
        RustRaftMessage::AutoPromoteLearner { .. } => "auto_promote_learner",
        RustRaftMessage::NetworkError { .. } => "record_network_error",
        RustRaftMessage::AppendEntries { .. } => "append_entries",
        RustRaftMessage::Vote { .. } => "vote",
        RustRaftMessage::VoteResponse { .. } => "handle_vote_response",
        RustRaftMessage::InstallSnapshot { .. } => "install_snapshot_chunk",
        RustRaftMessage::ReadIndex { .. } => "read_index_request",
        RustRaftMessage::PreVote { .. } => "pre_vote",
        RustRaftMessage::TimeoutNow { .. } => "timeout_now",
        RustRaftMessage::Propose { .. } => "propose",
        _ => "step",
    }
}

fn runtime_step_message(
    cluster: &mut RaftCluster,
    wal: &mut Option<PersistentRaftWal>,
    membership_executor: &mut RaftMembershipExecutor,
    node_id: RustRaftNodeId,
    message: RustRaftMessage,
) -> Result<RustRaftStepResult, RaftError> {
    match message {
        RustRaftMessage::Propose { payload, options } => {
            if cluster.leader_id() != Some(node_id) {
                return Err(RaftError::NotLeader(
                    cluster.leader_id().unwrap_or_default(),
                ));
            }
            let log_id = cluster.propose_with_options(payload, options)?;
            if let Some(wal) = wal.as_mut() {
                wal.append(cluster.wal_record_for(node_id)?)?;
            }
            Ok(RustRaftStepResult::Proposed(log_id))
        }
        RustRaftMessage::Membership { operation } => membership_executor
            .execute(cluster, operation)
            .map(RustRaftStepResult::Membership),
        RustRaftMessage::Admin {
            command: RustRaftAdminCommand::CompactLogsWithStorageFence { log_index, fence },
        } => {
            let report = wal
                .as_mut()
                .ok_or_else(|| RaftError::Storage("WAL is not available".to_string()))?
                .compact_through_with_fence(log_index, &fence)?;
            if report.fence_valid {
                let _ = cluster.compact_logs_through(log_index);
            }
            Ok(RustRaftStepResult::FencedCompaction(report))
        }
        RustRaftMessage::Admin {
            command:
                RustRaftAdminCommand::CheckpointSnapshot {
                    target,
                    snapshot_id,
                },
        } => cluster
            .checkpoint_snapshot(target, snapshot_id)
            .map(RustRaftStepResult::CheckpointedSnapshot),
        other => cluster.step(other),
    }
}

fn raft_node_runtime_loop(
    options: RustRaftNodeOptions,
    command_rx: mpsc::Receiver<RaftNodeRuntimeOp>,
) {
    let node_id = options.node_id;
    let group_id = options.group_id;
    let mut peers = options.peers.clone();
    if !peers.iter().any(|peer| peer.node_id == node_id) {
        peers.push(RustRaftPeer {
            node_id,
            raft_addr: options.raft_addr,
            snapshot_addr: options.snapshot_addr,
            role: options.role,
            auto_promote: false,
        });
    }
    let mut cluster = match RaftCluster::new(group_id, options.config.clone(), peers) {
        Ok(cluster) => cluster,
        Err(error) => {
            while let Ok(command) = command_rx.recv() {
                if respond_runtime_error(command, error.clone()) {
                    break;
                }
            }
            return;
        }
    };
    let mut last_wal_recovery_report = None;
    let mut wal = match PersistentRaftWal::open(PersistentRaftWalOptions {
        dir: PathBuf::from(&options.wal_dir),
        max_records_per_segment: 10_000,
        max_segment_bytes: options.config.max_segment_bytes,
        min_keep_segments: options.config.min_keep_segment_num as usize,
        fsync_on_append: true,
    }) {
        Ok(mut wal) => {
            if let Ok(report) = wal.recover() {
                if let Some(record) = report.recovered.clone() {
                    let _ = cluster.restore_wal_record(record);
                }
                last_wal_recovery_report = Some(report);
            }
            Some(wal)
        }
        Err(error) => {
            while let Ok(command) = command_rx.recv() {
                if respond_runtime_error(command, error.clone()) {
                    break;
                }
            }
            return;
        }
    };
    let mut state = RaftNodeRuntimeState::Created;
    let heartbeat_interval_ms = options.config.heartbeat_interval_ms.max(1);
    let election_timeout_ms = options
        .config
        .election_timeout_ms
        .max(heartbeat_interval_ms);
    let leader_lease_timeout_ms = options.config.leader_lease_ms.max(1);
    let heartbeat_interval = Duration::from_millis(heartbeat_interval_ms);
    let mut heartbeat_ticks = 0;
    let mut election_ticks = 0;
    let mut election_elapsed_ms: u64 = 0;
    let mut pre_vote_executions = 0;
    let mut campaign_executions = 0;
    let mut leader_transfer_executions = 0_u64;
    let mut last_tick_reason = "runtime_created".to_string();
    let mut blockers = Vec::<String>::new();
    let mut fatal_blockers = Vec::<String>::new();
    let mut membership_executor = RaftMembershipExecutor::new();
    loop {
        let command = match command_rx.recv_timeout(heartbeat_interval) {
            Ok(command) => command,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if state == RaftNodeRuntimeState::Running {
                    heartbeat_ticks += 1;
                    election_elapsed_ms = election_elapsed_ms.saturating_add(heartbeat_interval_ms);
                    let _ = cluster.tick_leader_lease(heartbeat_interval_ms);
                    cluster.tick_follower_lease(heartbeat_interval_ms);
                    let _ = cluster.mark_peer_active(node_id);
                    if cluster.leader_id() == Some(node_id) {
                        for peer_id in cluster.tick_peer_liveness(heartbeat_interval_ms) {
                            blockers.push(format!("peer_offline_timeout:{peer_id}"));
                        }
                    }
                    let received_live_leader_heartbeat =
                        cluster.leader_id().is_some_and(|leader_id| {
                            leader_id != node_id
                                && cluster
                                    .nodes
                                    .get(&leader_id)
                                    .map(|node| node.healthy)
                                    .unwrap_or(false)
                                && cluster
                                    .nodes
                                    .get(&node_id)
                                    .map(|node| node.healthy)
                                    .unwrap_or(false)
                        });
                    if let Err(error) = cluster.broadcast_heartbeat() {
                        blockers.push(format!("broadcast_heartbeat:{error}"));
                    }
                    if received_live_leader_heartbeat {
                        election_elapsed_ms = 0;
                    }
                    if !cluster.leader_lease_valid && cluster.step_down_leader_if_lost_quorum() {
                        blockers.push("lost_quorum_step_down".to_string());
                    }
                    last_tick_reason = "heartbeat_tick".to_string();
                    if cluster.tick_snapshot_trigger() {
                        let snapshot_id = cluster
                            .snapshot_trigger_status()
                            .snapshot_id
                            .unwrap_or_else(|| "unknown".to_string());
                        blockers.push(format!("snapshot_trigger_timeout:{snapshot_id}"));
                    }
                    let lagging_peers = cluster
                        .nodes
                        .iter()
                        .filter(|(peer_id, node)| {
                            Some(**peer_id) != cluster.leader_id
                                && node.healthy
                                && node.replica_role.can_serve_data()
                                && node.match_index() < cluster.last_log_index
                        })
                        .map(|(peer_id, _)| *peer_id)
                        .collect::<Vec<_>>();
                    for peer_id in lagging_peers {
                        if let Err(error) = cluster.catch_up_peer(peer_id) {
                            blockers.push(format!("peer_catchup:{peer_id}:{error}"));
                        }
                    }
                    if let Err(error) = cluster.broadcast_commit_index_to_old_paused_peers() {
                        blockers.push(format!("old_paused_commit_broadcast:{error}"));
                    }
                    if cluster.leader_transfer_state().is_some() {
                        match cluster.try_complete_leader_transfer() {
                            Ok(true) => {
                                leader_transfer_executions =
                                    leader_transfer_executions.saturating_add(1);
                            }
                            Ok(false) => {
                                if cluster.tick_leader_transfer() {
                                    blockers.push("leader_transfer_timeout".to_string());
                                }
                            }
                            Err(error) => blockers.push(format!("leader_transfer:{error}")),
                        }
                    }
                    if election_elapsed_ms >= election_timeout_ms {
                        election_ticks += 1;
                        election_elapsed_ms = 0;
                        last_tick_reason = "election_tick".to_string();
                        let local_replica_role =
                            cluster.nodes.get(&node_id).map(|node| node.replica_role);
                        let lease_expired = !cluster.follower_lease_valid();
                        let local_can_campaign = local_replica_role
                            .is_some_and(RustRaftReplicaRole::can_be_leader)
                            && cluster
                                .nodes
                                .get(&node_id)
                                .map(|node| node.healthy)
                                .unwrap_or(false)
                            && !cluster.prohibits_election();
                        if lease_expired && local_can_campaign {
                            pre_vote_executions += 1;
                            match cluster.pre_vote(node_id) {
                                Ok(vote) if vote.vote_granted => {
                                    campaign_executions += 1;
                                    let result = record_runtime_result(
                                        "election_tick_campaign",
                                        cluster.campaign(node_id, false),
                                        &mut blockers,
                                        &mut fatal_blockers,
                                        false,
                                    );
                                    let _ = result;
                                }
                                Ok(vote) => blockers.push(format!("pre_vote:{}", vote.reason)),
                                Err(error) => blockers.push(format!("pre_vote:{error}")),
                            }
                        } else if lease_expired
                            && (local_replica_role == Some(RustRaftReplicaRole::Witness)
                                || cluster.prohibits_election())
                        {
                            cluster.leader_id = None;
                            cluster.clear_election_responses();
                        }
                    }
                }
                continue;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };
        match command {
            RaftNodeRuntimeOp::Start(reply) => {
                let result = cluster.start();
                if result.is_ok() {
                    state = RaftNodeRuntimeState::Running;
                    election_elapsed_ms = 0;
                }
                let _ = reply.send(record_runtime_result(
                    "start",
                    result,
                    &mut blockers,
                    &mut fatal_blockers,
                    false,
                ));
            }
            RaftNodeRuntimeOp::Stop(reply) => {
                let result = cluster.stop();
                if result.is_ok() {
                    state = RaftNodeRuntimeState::Stopped;
                }
                let _ = reply.send(record_runtime_result(
                    "stop",
                    result,
                    &mut blockers,
                    &mut fatal_blockers,
                    false,
                ));
            }
            RaftNodeRuntimeOp::Status(reply) => {
                let status = RaftNodeRuntimeStatus {
                    node_id,
                    group_id,
                    state,
                    restart_count: 0,
                    worker_running: state != RaftNodeRuntimeState::Shutdown,
                    cluster_status: cluster.cluster_status_report().ok(),
                    wal_lifecycle_status: wal.as_ref().map(PersistentRaftWal::status),
                    wal_recovery_report: last_wal_recovery_report.clone(),
                    snapshot_trigger_status: cluster.snapshot_trigger_status(),
                    timer_status: RaftRuntimeTimerStatus {
                        heartbeat_interval_ms,
                        election_timeout_ms,
                        leader_lease_timeout_ms,
                        leader_lease_elapsed_ms: cluster.leader_lease_elapsed_ms,
                        leader_lease_valid: cluster.leader_lease_valid,
                        heartbeat_ticks,
                        election_ticks,
                        pre_vote_executions,
                        campaign_executions,
                        leader_transfer_executions,
                        last_tick_reason: last_tick_reason.clone(),
                    },
                    peer_runtime: raft_peer_runtime_states(
                        &cluster,
                        election_elapsed_ms,
                        heartbeat_ticks > 0,
                        pre_vote_executions > 0,
                    ),
                    fatal_blocker_report: rustraft_fatal_blocker_report(
                        "raft_node_runtime",
                        blockers.clone(),
                        fatal_blockers.clone(),
                    ),
                };
                let _ = reply.send(Ok(status));
            }
            RaftNodeRuntimeOp::WalLifecycleStatus(reply) => {
                let result = wal
                    .as_ref()
                    .map(PersistentRaftWal::status)
                    .ok_or_else(|| RaftError::Storage("WAL is not available".to_string()));
                let _ = reply.send(result);
            }
            RaftNodeRuntimeOp::WalRecoveryReport(reply) => {
                let _ = reply.send(Ok(last_wal_recovery_report.clone()));
            }
            RaftNodeRuntimeOp::Step(message, reply) => {
                let operation_name = runtime_step_operation_name(&message);
                if matches!(&message, RustRaftMessage::PreVote { .. }) {
                    pre_vote_executions += 1;
                }
                if is_leader_transfer_step_message(&message) {
                    leader_transfer_executions = leader_transfer_executions.saturating_add(1);
                }
                if matches!(&message, RustRaftMessage::TimeoutNow { .. }) {
                    campaign_executions = campaign_executions.saturating_add(1);
                }
                let campaign_message = matches!(
                    &message,
                    RustRaftMessage::Admin {
                        command: RustRaftAdminCommand::Campaign { .. },
                    }
                );
                if campaign_message {
                    campaign_executions = campaign_executions.saturating_add(1);
                }
                let fatal_event = match &message {
                    RustRaftMessage::Admin {
                        command: RustRaftAdminCommand::FireFatalEvent { node_id, reason },
                    } => Some((*node_id, reason.clone())),
                    _ => None,
                };
                if let Some((node_id, reason)) = &fatal_event {
                    let blocker = format!("fatal_event:{node_id}:{reason}");
                    blockers.push(blocker.clone());
                    fatal_blockers.push(blocker);
                }
                let fatal_on_step_error = !matches!(
                    &message,
                    RustRaftMessage::InstallSnapshot { .. }
                        | RustRaftMessage::ReadIndex { .. }
                        | RustRaftMessage::Membership { .. }
                );
                let result = runtime_step_message(
                    &mut cluster,
                    &mut wal,
                    &mut membership_executor,
                    node_id,
                    message,
                );
                if result
                    .as_ref()
                    .map(|step| matches!(step, RustRaftStepResult::FatalEvent(Some(_))))
                    .unwrap_or(false)
                {
                    leader_transfer_executions = leader_transfer_executions.saturating_add(1);
                }
                let _ = reply.send(record_runtime_result(
                    operation_name,
                    result,
                    &mut blockers,
                    &mut fatal_blockers,
                    fatal_on_step_error,
                ));
            }
            RaftNodeRuntimeOp::StepBatch(messages, reply) => {
                pre_vote_executions += messages
                    .iter()
                    .filter(|message| matches!(message, RustRaftMessage::PreVote { .. }))
                    .count() as u64;
                leader_transfer_executions = leader_transfer_executions.saturating_add(
                    messages
                        .iter()
                        .filter(|message| is_leader_transfer_step_message(message))
                        .count() as u64,
                );
                campaign_executions = campaign_executions.saturating_add(
                    messages
                        .iter()
                        .filter(|message| matches!(message, RustRaftMessage::TimeoutNow { .. }))
                        .count() as u64,
                );
                let campaign_message_count = messages
                    .iter()
                    .filter(|message| {
                        matches!(
                            message,
                            RustRaftMessage::Admin {
                                command: RustRaftAdminCommand::Campaign { .. },
                            }
                        )
                    })
                    .count() as u64;
                campaign_executions = campaign_executions.saturating_add(campaign_message_count);
                let result: Result<Vec<RustRaftStepResult>, RaftError> = messages
                    .into_iter()
                    .map(|message| {
                        runtime_step_message(
                            &mut cluster,
                            &mut wal,
                            &mut membership_executor,
                            node_id,
                            message,
                        )
                    })
                    .collect();
                let _ = reply.send(record_runtime_result(
                    "step_batch",
                    result,
                    &mut blockers,
                    &mut fatal_blockers,
                    true,
                ));
            }
            RaftNodeRuntimeOp::ReadIndex(min_commit_index, reply) => {
                let result = if cluster.leader_id() == Some(node_id) {
                    let request = ReadIndexRequest {
                        group_id,
                        requester_id: node_id,
                        min_commit_index,
                        allow_lease_read: true,
                    };
                    cluster.read_index(request)
                } else {
                    Err(RaftError::NotLeader(
                        cluster.leader_id().unwrap_or_default(),
                    ))
                };
                let _ = reply.send(record_runtime_result(
                    "read_index",
                    result,
                    &mut blockers,
                    &mut fatal_blockers,
                    false,
                ));
            }
            RaftNodeRuntimeOp::BoundedStaleReadIndex(
                min_commit_index,
                max_stale_index_lag,
                reply,
            ) => {
                let request = ReadIndexRequest {
                    group_id,
                    requester_id: node_id,
                    min_commit_index,
                    allow_lease_read: false,
                };
                let _ = reply.send(record_runtime_result(
                    "bounded_stale_read_index",
                    cluster.read_path_report(request, max_stale_index_lag),
                    &mut blockers,
                    &mut fatal_blockers,
                    false,
                ));
            }
            RaftNodeRuntimeOp::MembershipWorkflowWithRollback(operations, reply) => {
                let _ = reply.send(record_runtime_result(
                    "membership_workflow_with_rollback",
                    membership_executor.execute_all_with_rollback(&mut cluster, operations),
                    &mut blockers,
                    &mut fatal_blockers,
                    false,
                ));
            }
            RaftNodeRuntimeOp::MembershipReports(reply) => {
                let _ = reply.send(Ok(membership_executor.reports().to_vec()));
            }
            RaftNodeRuntimeOp::InstallSnapshot(target, snapshot, fence, reply) => {
                let _ = reply.send(record_runtime_result(
                    "install_snapshot",
                    cluster.install_snapshot_to(target, snapshot, fence),
                    &mut blockers,
                    &mut fatal_blockers,
                    false,
                ));
            }
            RaftNodeRuntimeOp::PeerPipelineStatus(peer_id, reply) => {
                let _ = reply.send(record_runtime_result(
                    "peer_pipeline_status",
                    cluster.peer_pipeline_status(peer_id),
                    &mut blockers,
                    &mut fatal_blockers,
                    false,
                ));
            }
            RaftNodeRuntimeOp::PeerPipelineStatuses(reply) => {
                let _ = reply.send(record_runtime_result(
                    "peer_pipeline_statuses",
                    Ok(cluster.peer_pipeline_statuses()),
                    &mut blockers,
                    &mut fatal_blockers,
                    false,
                ));
            }
            RaftNodeRuntimeOp::IsBusy(reply) => {
                let _ = reply.send(record_runtime_result(
                    "is_busy",
                    Ok(cluster.is_busy()),
                    &mut blockers,
                    &mut fatal_blockers,
                    false,
                ));
            }
            RaftNodeRuntimeOp::LeaderTransferState(reply) => {
                let _ = reply.send(Ok(cluster.leader_transfer_state()));
            }
            RaftNodeRuntimeOp::Shutdown(reply) => {
                let result = cluster.stop();
                let _ = reply.send(record_runtime_result(
                    "shutdown",
                    result,
                    &mut blockers,
                    &mut fatal_blockers,
                    false,
                ));
                break;
            }
        }
    }
}

fn record_runtime_result<T>(
    operation: &str,
    result: Result<T, RaftError>,
    blockers: &mut Vec<String>,
    fatal_blockers: &mut Vec<String>,
    fatal_on_error: bool,
) -> Result<T, RaftError> {
    if let Err(error) = &result {
        let blocker = format!("{operation}:{error}");
        blockers.push(blocker.clone());
        if fatal_on_error {
            fatal_blockers.push(blocker);
        }
    }
    result
}

fn raft_peer_runtime_states(
    cluster: &RaftCluster,
    election_elapsed_ms: u64,
    heartbeat_due: bool,
    pre_vote_sent: bool,
) -> Vec<RaftPeerRuntimeState> {
    let leader_commit_index = cluster.commit_index;
    cluster
        .nodes
        .values()
        .map(|node| {
            let mut blockers = Vec::new();
            if !node.healthy {
                blockers.push("peer_unhealthy".to_string());
            }
            if node.match_index() < leader_commit_index {
                blockers.push("peer_lagging".to_string());
            }
            RaftPeerRuntimeState {
                node_id: node.id,
                role: node.raft_role,
                replica_role: node.replica_role,
                healthy: node.healthy,
                matched: node.match_index(),
                lag: leader_commit_index.saturating_sub(node.match_index()),
                heartbeat_due,
                election_elapsed_ms,
                pre_vote_sent,
                transfer_leader_target: cluster
                    .leader_transfer
                    .as_ref()
                    .map(|transfer| transfer.transferee_id == node.id)
                    .unwrap_or(false),
                blockers,
            }
        })
        .collect()
}

fn respond_runtime_error(command: RaftNodeRuntimeOp, error: RaftError) -> bool {
    match command {
        RaftNodeRuntimeOp::Start(reply) | RaftNodeRuntimeOp::Stop(reply) => {
            let _ = reply.send(Err(error));
            false
        }
        RaftNodeRuntimeOp::WalLifecycleStatus(reply) => {
            let _ = reply.send(Err(error));
            false
        }
        RaftNodeRuntimeOp::WalRecoveryReport(reply) => {
            let _ = reply.send(Err(error));
            false
        }
        RaftNodeRuntimeOp::LeaderTransferState(reply) => {
            let _ = reply.send(Err(error));
            false
        }
        RaftNodeRuntimeOp::Shutdown(reply) => {
            let _ = reply.send(Err(error));
            true
        }
        RaftNodeRuntimeOp::Status(reply) => {
            let _ = reply.send(Err(error));
            false
        }
        RaftNodeRuntimeOp::Step(_, reply) => {
            let _ = reply.send(Err(error));
            false
        }
        RaftNodeRuntimeOp::StepBatch(_, reply) => {
            let _ = reply.send(Err(error));
            false
        }
        RaftNodeRuntimeOp::ReadIndex(_, reply) => {
            let _ = reply.send(Err(error));
            false
        }
        RaftNodeRuntimeOp::BoundedStaleReadIndex(_, _, reply) => {
            let _ = reply.send(Err(error));
            false
        }
        RaftNodeRuntimeOp::MembershipWorkflowWithRollback(_, reply) => {
            let _ = reply.send(Err(error));
            false
        }
        RaftNodeRuntimeOp::MembershipReports(reply) => {
            let _ = reply.send(Err(error));
            false
        }
        RaftNodeRuntimeOp::InstallSnapshot(_, _, _, reply) => {
            let _ = reply.send(Err(error));
            false
        }
        RaftNodeRuntimeOp::PeerPipelineStatus(_, reply) => {
            let _ = reply.send(Err(error));
            false
        }
        RaftNodeRuntimeOp::PeerPipelineStatuses(reply) => {
            let _ = reply.send(Err(error));
            false
        }
        RaftNodeRuntimeOp::IsBusy(reply) => {
            let _ = reply.send(Err(error));
            false
        }
    }
}

fn recv_runtime_reply<T>(reply_rx: mpsc::Receiver<T>) -> Result<T, RaftError> {
    reply_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|err| RaftError::Transport(format!("raft node runtime did not reply: {err}")))
}

