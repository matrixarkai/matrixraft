// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// stoppable node runtime worker and command loop.
// Split from src/lib.rs to keep the crate facade small and focused.

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeRuntimeState {
    Created,
    Running,
    Stopped,
    Shutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeRuntimeStatus {
    pub node_id: NodeId,
    pub group_id: GroupId,
    pub state: NodeRuntimeState,
    pub restart_count: u64,
    pub worker_running: bool,
    pub cluster_status: Option<ClusterStatusReport>,
    pub wal_lifecycle_status: Option<WalLifecycleStatus>,
    pub wal_recovery_report: Option<WalRecoveryReport>,
    pub snapshot_trigger_status: SnapshotTriggerStatus,
    pub timer_status: RuntimeTimerStatus,
    pub peer_runtime: Vec<PeerRuntimeState>,
    pub fatal_blocker_report: FatalBlockerReport,
}

enum NodeRuntimeOp {
    Start(mpsc::Sender<Result<(), RaftError>>),
    Stop(mpsc::Sender<Result<(), RaftError>>),
    Status(mpsc::Sender<Result<NodeRuntimeStatus, RaftError>>),
    WalLifecycleStatus(mpsc::Sender<Result<WalLifecycleStatus, RaftError>>),
    WalRecoveryReport(mpsc::Sender<Result<Option<WalRecoveryReport>, RaftError>>),
    Step(
        Message,
        mpsc::Sender<Result<StepResult, RaftError>>,
    ),
    StepBatch(
        Vec<Message>,
        mpsc::Sender<Result<Vec<StepResult>, RaftError>>,
    ),
    ReadIndex(
        LogIndex,
        mpsc::Sender<Result<ReadIndexResponse, RaftError>>,
    ),
    BoundedStaleReadIndex(
        LogIndex,
        LogIndex,
        mpsc::Sender<Result<ReadPathReport, RaftError>>,
    ),
    MembershipWorkflowWithRollback(
        Vec<MembershipOperation>,
        mpsc::Sender<Result<Vec<MembershipExecutionReport>, RaftError>>,
    ),
    MembershipReports(mpsc::Sender<Result<Vec<MembershipExecutionReport>, RaftError>>),
    InstallSnapshot(
        NodeId,
        RaftSnapshot,
        ApplySnapshotFence,
        mpsc::Sender<Result<(), RaftError>>,
    ),
    PeerPipelineStatus(
        NodeId,
        mpsc::Sender<Result<PeerProgress, RaftError>>,
    ),
    PeerPipelineStatuses(mpsc::Sender<Result<Vec<PeerProgress>, RaftError>>),
    IsBusy(mpsc::Sender<Result<bool, RaftError>>),
    LeaderTransferState(mpsc::Sender<Result<Option<LeaderTransferState>, RaftError>>),
    Shutdown(mpsc::Sender<Result<(), RaftError>>),
}

#[derive(Debug)]
pub struct NodeRuntime {
    node_id: NodeId,
    group_id: GroupId,
    command_tx: Option<mpsc::Sender<NodeRuntimeOp>>,
    worker: Option<thread::JoinHandle<()>>,
    restart_count: u64,
    state: NodeRuntimeState,
}

impl NodeRuntime {
    pub fn create(options: NodeOptions) -> Result<Self, RaftError> {
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
            state: NodeRuntimeState::Created,
        })
    }

    pub fn start(&mut self) -> Result<(), RaftError> {
        self.send_unit(NodeRuntimeOp::Start)?;
        self.state = NodeRuntimeState::Running;
        Ok(())
    }

    pub fn stop(&mut self) -> Result<(), RaftError> {
        self.send_unit(NodeRuntimeOp::Stop)?;
        self.state = NodeRuntimeState::Stopped;
        Ok(())
    }

    pub fn restart(&mut self) -> Result<(), RaftError> {
        if self.state == NodeRuntimeState::Shutdown {
            return Err(RaftError::InvalidRequest(
                "cannot restart a shutdown raft node runtime".to_string(),
            ));
        }
        if self.state == NodeRuntimeState::Running {
            self.stop()?;
        }
        self.restart_count += 1;
        self.start()
    }

    pub fn shutdown(&mut self) -> Result<(), RaftError> {
        if self.state == NodeRuntimeState::Shutdown {
            return Ok(());
        }
        let sender = self.command_tx.take().ok_or_else(|| {
            RaftError::InvalidRequest("raft node runtime channel is closed".to_string())
        })?;
        let (reply_tx, reply_rx) = mpsc::channel();
        sender
            .send(NodeRuntimeOp::Shutdown(reply_tx))
            .map_err(|err| RaftError::Transport(format!("failed to shutdown raft node: {err}")))?;
        let result = recv_runtime_reply(reply_rx)?;
        if let Some(worker) = self.worker.take() {
            worker.join().map_err(|_| {
                RaftError::Transport("raft node worker panicked during shutdown".to_string())
            })?;
        }
        self.state = NodeRuntimeState::Shutdown;
        result
    }

    pub fn propose(&self, payload: Payload) -> Result<LogId, RaftError> {
        self.propose_with_options(payload, ProposeOptions::default())
    }

    pub fn propose_with_options(
        &self,
        payload: Payload,
        options: ProposeOptions,
    ) -> Result<LogId, RaftError> {
        match self.step(Message::Propose { payload, options })? {
            StepResult::Proposed(log_id) => Ok(log_id),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected propose result: {other:?}"
            ))),
        }
    }

    pub fn step(&self, message: Message) -> Result<StepResult, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(NodeRuntimeOp::Step(message, reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!("failed to send step to raft node: {err}"))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn step_batch(
        &self,
        messages: Vec<Message>,
    ) -> Result<Vec<StepResult>, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(NodeRuntimeOp::StepBatch(messages, reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!("failed to send step batch to raft node: {err}"))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn append_entries_to(
        &self,
        target: NodeId,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        match self.step(Message::AppendEntries { target, request })? {
            StepResult::AppendEntries(response) => Ok(response),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected append-entries result: {other:?}"
            ))),
        }
    }

    pub fn vote_to(
        &self,
        target: NodeId,
        request: VoteRequest,
    ) -> Result<VoteResponse, RaftError> {
        match self.step(Message::Vote { target, request })? {
            StepResult::Vote(response) => Ok(response),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected vote result: {other:?}"
            ))),
        }
    }

    pub fn handle_vote_response(
        &self,
        local_node_id: NodeId,
        response: VoteResponse,
        pre_vote: bool,
    ) -> Result<(), RaftError> {
        match self.step(Message::VoteResponse {
            local_node_id,
            peer_id: None,
            response,
            pre_vote,
        })? {
            StepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected vote response result: {other:?}"
            ))),
        }
    }

    pub fn read_index(
        &self,
        min_commit_index: LogIndex,
    ) -> Result<ReadIndexResponse, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(NodeRuntimeOp::ReadIndex(min_commit_index, reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!("failed to send read-index to raft node: {err}"))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn read_index_request(
        &self,
        request: ReadIndexRequest,
    ) -> Result<ReadIndexResponse, RaftError> {
        match self.step(Message::ReadIndex { request })? {
            StepResult::ReadIndex(response) => Ok(response),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected read-index result: {other:?}"
            ))),
        }
    }

    pub fn bounded_stale_read_index(
        &self,
        min_commit_index: LogIndex,
        max_stale_index_lag: LogIndex,
    ) -> Result<ReadPathReport, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(NodeRuntimeOp::BoundedStaleReadIndex(
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
        operation: MembershipOperation,
    ) -> Result<MembershipExecutionReport, RaftError> {
        match self.step(Message::Membership { operation })? {
            StepResult::Membership(report) => Ok(report),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected membership operation result: {other:?}"
            ))),
        }
    }

    pub fn execute_membership_workflow_with_rollback<I>(
        &self,
        operations: I,
    ) -> Result<Vec<MembershipExecutionReport>, RaftError>
    where
        I: IntoIterator<Item = MembershipOperation>,
    {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(NodeRuntimeOp::MembershipWorkflowWithRollback(
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
    ) -> Result<Vec<MembershipExecutionReport>, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(NodeRuntimeOp::MembershipReports(reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!(
                    "failed to query membership reports from raft node: {err}"
                ))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn install_snapshot_to(
        &self,
        target: NodeId,
        snapshot: RaftSnapshot,
        fence: ApplySnapshotFence,
    ) -> Result<(), RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(NodeRuntimeOp::InstallSnapshot(
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
        target: NodeId,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        match self.step(Message::InstallSnapshot { target, request })? {
            StepResult::InstallSnapshot(response) => Ok(response),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected snapshot chunk install result: {other:?}"
            ))),
        }
    }

    pub fn begin_snapshot_send_to(
        &self,
        peer_id: NodeId,
        snapshot_id: impl Into<String>,
        snapshot_index: LogIndex,
        total_chunks: u64,
    ) -> Result<(), RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::BeginSnapshotSend {
                peer_id,
                snapshot_id: snapshot_id.into(),
                snapshot_index,
                total_chunks,
            },
        })? {
            StepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected begin snapshot send result: {other:?}"
            ))),
        }
    }

    pub fn record_snapshot_chunk_sent_to(
        &self,
        peer_id: NodeId,
        bytes: u64,
    ) -> Result<(), RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::RecordSnapshotChunkSent { peer_id, bytes },
        })? {
            StepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected snapshot chunk-sent result: {other:?}"
            ))),
        }
    }

    pub fn acknowledge_snapshot_chunk_to(&self, peer_id: NodeId) -> Result<(), RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::AcknowledgeSnapshotChunk { peer_id },
        })? {
            StepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected snapshot chunk acknowledgement result: {other:?}"
            ))),
        }
    }

    pub fn retry_snapshot_chunk_to(&self, peer_id: NodeId) -> Result<(), RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::RetrySnapshotChunk { peer_id },
        })? {
            StepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected snapshot chunk retry result: {other:?}"
            ))),
        }
    }

    pub fn cancel_snapshot_send_to(&self, peer_id: NodeId) -> Result<(), RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::CancelSnapshotSend { peer_id },
        })? {
            StepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected snapshot send cancel result: {other:?}"
            ))),
        }
    }

    pub fn begin_snapshot_install_from(
        &self,
        peer_id: NodeId,
        snapshot_id: impl Into<String>,
        snapshot_index: LogIndex,
        total_chunks: u64,
    ) -> Result<(), RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::BeginSnapshotInstall {
                peer_id,
                snapshot_id: snapshot_id.into(),
                snapshot_index,
                total_chunks,
            },
        })? {
            StepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected begin snapshot install result: {other:?}"
            ))),
        }
    }

    pub fn receive_snapshot_chunk_from(
        &self,
        peer_id: NodeId,
        bytes: u64,
        done: bool,
    ) -> Result<(), RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::ReceiveSnapshotChunk {
                peer_id,
                bytes,
                done,
            },
        })? {
            StepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected snapshot receive progress result: {other:?}"
            ))),
        }
    }

    pub fn rollback_snapshot_install_from(&self, peer_id: NodeId) -> Result<(), RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::RollbackSnapshotInstall { peer_id },
        })? {
            StepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected snapshot install rollback result: {other:?}"
            ))),
        }
    }

    pub fn catch_up_peer(
        &self,
        peer_id: NodeId,
    ) -> Result<LearnerCatchUpLoopReport, RaftError> {
        match self.step(Message::CatchUpPeer { peer_id })? {
            StepResult::CatchUpPeer(report) => Ok(report),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected peer catch-up result: {other:?}"
            ))),
        }
    }

    pub fn auto_promote_learner(
        &self,
        learner_id: NodeId,
    ) -> Result<LearnerAutoPromoteReport, RaftError> {
        match self.step(Message::AutoPromoteLearner { learner_id })? {
            StepResult::AutoPromoteLearner(report) => Ok(report),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected learner auto-promotion result: {other:?}"
            ))),
        }
    }

    pub fn receive_out_of_order_append_for(
        &self,
        peer_id: NodeId,
        entry: LogEntry,
    ) -> Result<(), RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::ReceiveOutOfOrderAppend { peer_id, entry },
        })? {
            StepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected out-of-order append result: {other:?}"
            ))),
        }
    }

    pub fn expire_peer_reorder_queue(&self, peer_id: NodeId) -> Result<u64, RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::ExpirePeerReorderQueue { peer_id },
        })? {
            StepResult::CompactedLogs(expired) => Ok(expired),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected reorder queue expiration result: {other:?}"
            ))),
        }
    }

    pub fn record_network_error_for(&self, peer_id: NodeId) -> Result<(), RaftError> {
        match self.step(Message::NetworkError { peer_id })? {
            StepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected network error result: {other:?}"
            ))),
        }
    }

    pub fn peer_pipeline_status(
        &self,
        peer_id: NodeId,
    ) -> Result<PeerProgress, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(NodeRuntimeOp::PeerPipelineStatus(peer_id, reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!(
                    "failed to send peer pipeline status request to raft node: {err}"
                ))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn peer_pipeline_statuses(&self) -> Result<Vec<PeerProgress>, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(NodeRuntimeOp::PeerPipelineStatuses(reply_tx))
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
            .send(NodeRuntimeOp::IsBusy(reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!("failed to send is-busy to raft node: {err}"))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn compact_logs_through(&self, log_index: LogIndex) -> Result<u64, RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::CompactLogsThrough { log_index },
        })? {
            StepResult::CompactedLogs(compacted) => Ok(compacted),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected log compaction result: {other:?}"
            ))),
        }
    }

    pub fn release_memory(&self) -> Result<bool, RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::ReleaseMemory,
        })? {
            StepResult::ReleasedMemory(released) => Ok(released),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected release-memory result: {other:?}"
            ))),
        }
    }

    pub fn compact_logs_with_storage_fence(
        &self,
        log_index: LogIndex,
        fence: StorageApplyFence,
    ) -> Result<WalCompactionReport, RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::CompactLogsWithStorageFence { log_index, fence },
        })? {
            StepResult::FencedCompaction(report) => Ok(report),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected fenced log compaction result: {other:?}"
            ))),
        }
    }

    pub fn checkpoint_snapshot(
        &self,
        node_id: NodeId,
        snapshot_id: impl Into<String>,
    ) -> Result<RaftSnapshot, RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::CheckpointSnapshot {
                target: node_id,
                snapshot_id: snapshot_id.into(),
            },
        })? {
            StepResult::CheckpointedSnapshot(snapshot) => Ok(snapshot),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected checkpoint snapshot result: {other:?}"
            ))),
        }
    }

    pub fn set_node_healthy(
        &self,
        node_id: NodeId,
        healthy: bool,
    ) -> Result<(), RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::SetNodeHealthy { node_id, healthy },
        })? {
            StepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected health update result: {other:?}"
            ))),
        }
    }

    pub fn partition_peer(&self, node_id: NodeId) -> Result<(), RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::PartitionPeer { peer_id: node_id },
        })? {
            StepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected peer partition result: {other:?}"
            ))),
        }
    }

    pub fn heal_peer(
        &self,
        node_id: NodeId,
    ) -> Result<LearnerCatchUpLoopReport, RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::HealPeer { peer_id: node_id },
        })? {
            StepResult::CatchUpPeer(report) => Ok(report),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected peer heal result: {other:?}"
            ))),
        }
    }

    pub fn set_leader_lease_valid(&self, valid: bool) -> Result<(), RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::SetLeaderLeaseValid { valid },
        })? {
            StepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected lease update result: {other:?}"
            ))),
        }
    }

    /// Advances the leader-lease clock by `elapsed_ms`, reporting whether the
    /// lease has now expired.
    ///
    /// The runtime also ticks this clock on its own, but only from the timeout
    /// arm of its command loop -- so the automatic tick fires when the command
    /// channel has been *idle* for a whole heartbeat interval, and a caller
    /// that drives the runtime steadily starves it rather than accelerating it.
    /// This drives the same clock directly, which is what makes lease expiry
    /// something a caller can sequence rather than wait for.
    pub fn tick_leader_lease(&self, elapsed_ms: u64) -> Result<bool, RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::TickLeaderLease { elapsed_ms },
        })? {
            StepResult::LeaderLeaseExpired(expired) => Ok(expired),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected tick-leader-lease result: {other:?}"
            ))),
        }
    }

    /// Advances the follower-lease clock by `elapsed_ms`, reporting whether the
    /// lease has now expired. See [`Self::tick_leader_lease`] for why driving
    /// this explicitly is not the same as waiting.
    pub fn tick_follower_lease(&self, elapsed_ms: u64) -> Result<bool, RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::TickFollowerLease { elapsed_ms },
        })? {
            StepResult::FollowerLeaseExpired(expired) => Ok(expired),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected tick-follower-lease result: {other:?}"
            ))),
        }
    }

    pub fn set_ignore_witness(&self, ignore_witness: bool) -> Result<(), RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::IgnoreWitness {
                ignore: ignore_witness,
            },
        })? {
            StepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected witness policy update result: {other:?}"
            ))),
        }
    }

    pub fn fire_fatal_event(
        &self,
        node_id: NodeId,
        reason: impl Into<String>,
    ) -> Result<Option<NodeId>, RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::FireFatalEvent {
                node_id,
                reason: reason.into(),
            },
        })? {
            StepResult::FatalEvent(target) => Ok(target),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected fatal event result: {other:?}"
            ))),
        }
    }

    pub fn witness_quorum_report<I>(
        &self,
        acknowledgements: I,
    ) -> Result<WitnessQuorumReport, RaftError>
    where
        I: IntoIterator<Item = NodeId>,
    {
        match self.step(Message::Admin {
            command: AdminCommand::WitnessQuorum {
                acknowledgements: acknowledgements.into_iter().collect(),
            },
        })? {
            StepResult::WitnessQuorum(report) => Ok(report),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected witness quorum result: {other:?}"
            ))),
        }
    }

    pub fn set_prohibits_election(&self, prohibits_election: bool) -> Result<(), RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::ProhibitsElection {
                prohibits: prohibits_election,
            },
        })? {
            StepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected election-prohibit update result: {other:?}"
            ))),
        }
    }

    pub fn transfer_leader(&self, target: NodeId) -> Result<(), RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::TransferLeader { target },
        })? {
            StepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected leader transfer step result: {other:?}"
            ))),
        }
    }

    pub fn timeout_now(
        &self,
        from: NodeId,
        target: NodeId,
    ) -> Result<TimeoutNowResponse, RaftError> {
        match self.step(Message::TimeoutNow { from, target })? {
            StepResult::TimeoutNow(response) => Ok(response),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected timeout-now step result: {other:?}"
            ))),
        }
    }

    pub fn try_complete_leader_transfer(&self) -> Result<bool, RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::CompleteLeaderTransfer,
        })? {
            StepResult::LeaderTransferCompleted(completed) => Ok(completed),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected leader transfer completion result: {other:?}"
            ))),
        }
    }

    pub fn abort_leader_transfer(&self, reason: impl Into<String>) -> Result<bool, RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::AbortLeaderTransfer {
                reason: reason.into(),
            },
        })? {
            StepResult::LeaderTransferAborted(aborted) => Ok(aborted),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected leader transfer abort result: {other:?}"
            ))),
        }
    }

    pub fn leader_transfer_state(&self) -> Result<Option<LeaderTransferState>, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(NodeRuntimeOp::LeaderTransferState(reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!(
                    "failed to query leader transfer state from raft node: {err}"
                ))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn step_down(
        &self,
        transferee: Option<NodeId>,
    ) -> Result<Option<NodeId>, RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::StepDown { transferee },
        })? {
            StepResult::StepDown(transferee) => Ok(transferee),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected step-down result: {other:?}"
            ))),
        }
    }

    pub fn resign_leader(&self, reason: impl Into<String>) -> Result<bool, RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::Resign {
                reason: reason.into(),
            },
        })? {
            StepResult::LeaderResigned(resigned) => Ok(resigned),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected leader resign result: {other:?}"
            ))),
        }
    }

    pub fn trigger_snapshot(&self) -> Result<SnapshotMetadata, RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::TriggerSnapshot,
        })? {
            StepResult::SnapshotTriggered(snapshot) => Ok(snapshot),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected snapshot trigger step result: {other:?}"
            ))),
        }
    }

    pub fn mark_snapshot_ready(&self, snapshot_id: &str, success: bool) -> Result<(), RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::SnapshotReady {
                snapshot_id: snapshot_id.to_string(),
                success,
            },
        })? {
            StepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected snapshot-ready step result: {other:?}"
            ))),
        }
    }

    pub fn complete_snapshot_trigger(&self, snapshot_id: &str) -> Result<(), RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::SnapshotApplied {
                snapshot_id: snapshot_id.to_string(),
            },
        })? {
            StepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected snapshot completion step result: {other:?}"
            ))),
        }
    }

    pub fn pre_vote(&self) -> Result<VoteResponse, RaftError> {
        match self.step(Message::PreVote {
            candidate_id: self.node_id,
        })? {
            StepResult::PreVote(response) => Ok(response),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected pre-vote step result: {other:?}"
            ))),
        }
    }

    pub fn campaign(&self, forced: bool) -> Result<(), RaftError> {
        match self.step(Message::Admin {
            command: AdminCommand::Campaign {
                candidate_id: self.node_id,
                forced,
            },
        })? {
            StepResult::Handled => Ok(()),
            other => Err(RaftError::InvalidRequest(format!(
                "unexpected campaign step result: {other:?}"
            ))),
        }
    }

    pub fn status(&self) -> Result<NodeRuntimeStatus, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(NodeRuntimeOp::Status(reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!("failed to send status to raft node: {err}"))
            })?;
        let mut status = recv_runtime_reply(reply_rx)??;
        status.restart_count = self.restart_count;
        status.state = self.state;
        Ok(status)
    }

    pub fn wal_lifecycle_status(&self) -> Result<WalLifecycleStatus, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(NodeRuntimeOp::WalLifecycleStatus(reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!(
                    "failed to query WAL lifecycle status from raft node: {err}"
                ))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn wal_recovery_report(&self) -> Result<Option<WalRecoveryReport>, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(NodeRuntimeOp::WalRecoveryReport(reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!(
                    "failed to query WAL recovery report from raft node: {err}"
                ))
            })?;
        recv_runtime_reply(reply_rx)?
    }

    pub fn state(&self) -> NodeRuntimeState {
        self.state
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn group_id(&self) -> GroupId {
        self.group_id
    }

    pub fn restart_count(&self) -> u64 {
        self.restart_count
    }

    fn send_unit(
        &self,
        command: fn(mpsc::Sender<Result<(), RaftError>>) -> NodeRuntimeOp,
    ) -> Result<(), RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?.send(command(reply_tx)).map_err(|err| {
            RaftError::Transport(format!(
                "failed to send lifecycle command to raft node: {err}"
            ))
        })?;
        recv_runtime_reply(reply_rx)?
    }

    fn sender(&self) -> Result<&mpsc::Sender<NodeRuntimeOp>, RaftError> {
        self.command_tx
            .as_ref()
            .ok_or_else(|| RaftError::InvalidRequest("raft node runtime is shut down".to_string()))
    }
}

impl Drop for NodeRuntime {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn is_leader_transfer_step_message(message: &Message) -> bool {
    matches!(
        message,
        Message::Admin {
            command: AdminCommand::TransferLeader { .. }
                | AdminCommand::StepDown { .. },
        }
    )
}

fn runtime_step_operation_name(message: &Message) -> &'static str {
    match message {
        Message::Admin {
            command: AdminCommand::TransferLeader { .. },
        } => "transfer_leader",
        Message::Admin {
            command: AdminCommand::CompleteLeaderTransfer,
        } => "try_complete_leader_transfer",
        Message::Admin {
            command: AdminCommand::AbortLeaderTransfer { .. },
        } => "abort_leader_transfer",
        Message::Admin {
            command: AdminCommand::FireFatalEvent { .. },
        } => "fire_fatal_event",
        Message::Admin {
            command: AdminCommand::StepDown { .. },
        } => "step_down",
        Message::Admin {
            command: AdminCommand::Campaign { .. },
        } => "campaign",
        Message::Admin {
            command: AdminCommand::Resign { .. },
        } => "resign_leader",
        Message::Admin {
            command: AdminCommand::SetNodeHealthy { .. },
        } => "set_node_healthy",
        Message::Admin {
            command: AdminCommand::PartitionPeer { .. },
        } => "partition_peer",
        Message::Admin {
            command: AdminCommand::HealPeer { .. },
        } => "heal_peer",
        Message::Admin {
            command: AdminCommand::SetLeaderLeaseValid { .. },
        } => "set_leader_lease_valid",
        Message::Admin {
            command: AdminCommand::IgnoreWitness { .. },
        } => "set_ignore_witness",
        Message::Admin {
            command: AdminCommand::ProhibitsElection { .. },
        } => "set_prohibits_election",
        Message::Admin {
            command: AdminCommand::CompactLogsThrough { .. },
        } => "compact_logs",
        Message::Admin {
            command: AdminCommand::ReleaseMemory,
        } => "release_memory",
        Message::Admin {
            command: AdminCommand::CompactLogsWithStorageFence { .. },
        } => "compact_logs_with_storage_fence",
        Message::Admin {
            command: AdminCommand::CheckpointSnapshot { .. },
        } => "checkpoint_snapshot",
        Message::Admin {
            command: AdminCommand::WitnessQuorum { .. },
        } => "witness_quorum_report",
        Message::Admin {
            command: AdminCommand::BeginSnapshotSend { .. },
        } => "begin_snapshot_send",
        Message::Admin {
            command: AdminCommand::RecordSnapshotChunkSent { .. },
        } => "record_snapshot_chunk_sent",
        Message::Admin {
            command: AdminCommand::AcknowledgeSnapshotChunk { .. },
        } => "acknowledge_snapshot_chunk",
        Message::Admin {
            command: AdminCommand::RetrySnapshotChunk { .. },
        } => "retry_snapshot_chunk",
        Message::Admin {
            command: AdminCommand::CancelSnapshotSend { .. },
        } => "cancel_snapshot_send",
        Message::Admin {
            command: AdminCommand::BeginSnapshotInstall { .. },
        } => "begin_snapshot_install",
        Message::Admin {
            command: AdminCommand::ReceiveSnapshotChunk { .. },
        } => "receive_snapshot_chunk",
        Message::Admin {
            command: AdminCommand::RollbackSnapshotInstall { .. },
        } => "rollback_snapshot_install",
        Message::Admin {
            command: AdminCommand::ReceiveOutOfOrderAppend { .. },
        } => "receive_out_of_order_append",
        Message::Admin {
            command: AdminCommand::ExpirePeerReorderQueue { .. },
        } => "expire_peer_reorder_queue",
        Message::CatchUpPeer { .. } => "catch_up_peer",
        Message::AutoPromoteLearner { .. } => "auto_promote_learner",
        Message::NetworkError { .. } => "record_network_error",
        Message::AppendEntries { .. } => "append_entries",
        Message::Vote { .. } => "vote",
        Message::VoteResponse { .. } => "handle_vote_response",
        Message::InstallSnapshot { .. } => "install_snapshot_chunk",
        Message::ReadIndex { .. } => "read_index_request",
        Message::PreVote { .. } => "pre_vote",
        Message::TimeoutNow { .. } => "timeout_now",
        Message::Propose { .. } => "propose",
        _ => "step",
    }
}

fn runtime_step_message(
    cluster: &mut RaftCluster,
    wal: &mut Option<PersistentRaftWal>,
    membership_executor: &mut MembershipExecutor,
    node_id: NodeId,
    message: Message,
) -> Result<StepResult, RaftError> {
    match message {
        Message::Propose { payload, options } => {
            if cluster.leader_id() != Some(node_id) {
                return Err(RaftError::NotLeader(
                    cluster.leader_id().unwrap_or_default(),
                ));
            }
            let log_id = cluster.propose_with_options(payload, options)?;
            if let Some(wal) = wal.as_mut() {
                // Built against what the WAL already holds, so a proposal does
                // not copy and hash the whole log to write one entry.
                wal.append_built(|coverage| cluster.wal_record_for_coverage(node_id, coverage))?;
            }
            Ok(StepResult::Proposed(log_id))
        }
        Message::Membership { operation } => membership_executor
            .execute(cluster, operation)
            .map(StepResult::Membership),
        Message::Admin {
            command: AdminCommand::CompactLogsWithStorageFence { log_index, fence },
        } => {
            let report = wal
                .as_mut()
                .ok_or_else(|| RaftError::Storage("WAL is not available".to_string()))?
                .compact_through_with_fence(log_index, &fence)?;
            if report.fence_valid {
                let _ = cluster.compact_logs_through(log_index);
            }
            Ok(StepResult::FencedCompaction(report))
        }
        Message::Admin {
            command:
                AdminCommand::CheckpointSnapshot {
                    target,
                    snapshot_id,
                },
        } => cluster
            .checkpoint_snapshot(target, snapshot_id)
            .map(StepResult::CheckpointedSnapshot),
        other => cluster.step(other),
    }
}

fn raft_node_runtime_loop(
    options: NodeOptions,
    command_rx: mpsc::Receiver<NodeRuntimeOp>,
) {
    let node_id = options.node_id;
    let group_id = options.group_id;
    let mut peers = options.peers.clone();
    if !peers.iter().any(|peer| peer.node_id == node_id) {
        peers.push(Peer {
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
    let mut state = NodeRuntimeState::Created;
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
    let mut membership_executor = MembershipExecutor::new();
    loop {
        let command = match command_rx.recv_timeout(heartbeat_interval) {
            Ok(command) => command,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if state == NodeRuntimeState::Running {
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
                        let lease_expired = !cluster.is_follower_lease_valid();
                        let local_can_campaign = local_replica_role
                            .is_some_and(ReplicaRole::can_be_leader)
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
                            && (local_replica_role == Some(ReplicaRole::Witness)
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
            NodeRuntimeOp::Start(reply) => {
                let result = cluster.start();
                if result.is_ok() {
                    state = NodeRuntimeState::Running;
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
            NodeRuntimeOp::Stop(reply) => {
                let result = cluster.stop();
                if result.is_ok() {
                    state = NodeRuntimeState::Stopped;
                }
                let _ = reply.send(record_runtime_result(
                    "stop",
                    result,
                    &mut blockers,
                    &mut fatal_blockers,
                    false,
                ));
            }
            NodeRuntimeOp::Status(reply) => {
                let status = NodeRuntimeStatus {
                    node_id,
                    group_id,
                    state,
                    restart_count: 0,
                    worker_running: state != NodeRuntimeState::Shutdown,
                    cluster_status: cluster.cluster_status_report().ok(),
                    wal_lifecycle_status: wal.as_ref().map(PersistentRaftWal::status),
                    wal_recovery_report: last_wal_recovery_report.clone(),
                    snapshot_trigger_status: cluster.snapshot_trigger_status(),
                    timer_status: RuntimeTimerStatus {
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
                    fatal_blocker_report: matrixraft_fatal_blocker_report(
                        "raft_node_runtime",
                        blockers.clone(),
                        fatal_blockers.clone(),
                    ),
                };
                let _ = reply.send(Ok(status));
            }
            NodeRuntimeOp::WalLifecycleStatus(reply) => {
                let result = wal
                    .as_ref()
                    .map(PersistentRaftWal::status)
                    .ok_or_else(|| RaftError::Storage("WAL is not available".to_string()));
                let _ = reply.send(result);
            }
            NodeRuntimeOp::WalRecoveryReport(reply) => {
                let _ = reply.send(Ok(last_wal_recovery_report.clone()));
            }
            NodeRuntimeOp::Step(message, reply) => {
                let operation_name = runtime_step_operation_name(&message);
                if matches!(&message, Message::PreVote { .. }) {
                    pre_vote_executions += 1;
                }
                if is_leader_transfer_step_message(&message) {
                    leader_transfer_executions = leader_transfer_executions.saturating_add(1);
                }
                if matches!(&message, Message::TimeoutNow { .. }) {
                    campaign_executions = campaign_executions.saturating_add(1);
                }
                let campaign_message = matches!(
                    &message,
                    Message::Admin {
                        command: AdminCommand::Campaign { .. },
                    }
                );
                if campaign_message {
                    campaign_executions = campaign_executions.saturating_add(1);
                }
                let fatal_event = match &message {
                    Message::Admin {
                        command: AdminCommand::FireFatalEvent { node_id, reason },
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
                    Message::InstallSnapshot { .. }
                        | Message::ReadIndex { .. }
                        | Message::Membership { .. }
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
                    .map(|step| matches!(step, StepResult::FatalEvent(Some(_))))
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
            NodeRuntimeOp::StepBatch(messages, reply) => {
                pre_vote_executions += messages
                    .iter()
                    .filter(|message| matches!(message, Message::PreVote { .. }))
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
                        .filter(|message| matches!(message, Message::TimeoutNow { .. }))
                        .count() as u64,
                );
                let campaign_message_count = messages
                    .iter()
                    .filter(|message| {
                        matches!(
                            message,
                            Message::Admin {
                                command: AdminCommand::Campaign { .. },
                            }
                        )
                    })
                    .count() as u64;
                campaign_executions = campaign_executions.saturating_add(campaign_message_count);
                let result: Result<Vec<StepResult>, RaftError> = messages
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
            NodeRuntimeOp::ReadIndex(min_commit_index, reply) => {
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
            NodeRuntimeOp::BoundedStaleReadIndex(
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
            NodeRuntimeOp::MembershipWorkflowWithRollback(operations, reply) => {
                let _ = reply.send(record_runtime_result(
                    "membership_workflow_with_rollback",
                    membership_executor.execute_all_with_rollback(&mut cluster, operations),
                    &mut blockers,
                    &mut fatal_blockers,
                    false,
                ));
            }
            NodeRuntimeOp::MembershipReports(reply) => {
                let _ = reply.send(Ok(membership_executor.reports().to_vec()));
            }
            NodeRuntimeOp::InstallSnapshot(target, snapshot, fence, reply) => {
                let _ = reply.send(record_runtime_result(
                    "install_snapshot",
                    cluster.install_snapshot_to(target, snapshot, fence),
                    &mut blockers,
                    &mut fatal_blockers,
                    false,
                ));
            }
            NodeRuntimeOp::PeerPipelineStatus(peer_id, reply) => {
                let _ = reply.send(record_runtime_result(
                    "peer_pipeline_status",
                    cluster.peer_pipeline_status(peer_id),
                    &mut blockers,
                    &mut fatal_blockers,
                    false,
                ));
            }
            NodeRuntimeOp::PeerPipelineStatuses(reply) => {
                let _ = reply.send(record_runtime_result(
                    "peer_pipeline_statuses",
                    Ok(cluster.peer_pipeline_statuses()),
                    &mut blockers,
                    &mut fatal_blockers,
                    false,
                ));
            }
            NodeRuntimeOp::IsBusy(reply) => {
                let _ = reply.send(record_runtime_result(
                    "is_busy",
                    Ok(cluster.is_busy()),
                    &mut blockers,
                    &mut fatal_blockers,
                    false,
                ));
            }
            NodeRuntimeOp::LeaderTransferState(reply) => {
                let _ = reply.send(Ok(cluster.leader_transfer_state()));
            }
            NodeRuntimeOp::Shutdown(reply) => {
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
) -> Vec<PeerRuntimeState> {
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
            PeerRuntimeState {
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

fn respond_runtime_error(command: NodeRuntimeOp, error: RaftError) -> bool {
    match command {
        NodeRuntimeOp::Start(reply) | NodeRuntimeOp::Stop(reply) => {
            let _ = reply.send(Err(error));
            false
        }
        NodeRuntimeOp::WalLifecycleStatus(reply) => {
            let _ = reply.send(Err(error));
            false
        }
        NodeRuntimeOp::WalRecoveryReport(reply) => {
            let _ = reply.send(Err(error));
            false
        }
        NodeRuntimeOp::LeaderTransferState(reply) => {
            let _ = reply.send(Err(error));
            false
        }
        NodeRuntimeOp::Shutdown(reply) => {
            let _ = reply.send(Err(error));
            true
        }
        NodeRuntimeOp::Status(reply) => {
            let _ = reply.send(Err(error));
            false
        }
        NodeRuntimeOp::Step(_, reply) => {
            let _ = reply.send(Err(error));
            false
        }
        NodeRuntimeOp::StepBatch(_, reply) => {
            let _ = reply.send(Err(error));
            false
        }
        NodeRuntimeOp::ReadIndex(_, reply) => {
            let _ = reply.send(Err(error));
            false
        }
        NodeRuntimeOp::BoundedStaleReadIndex(_, _, reply) => {
            let _ = reply.send(Err(error));
            false
        }
        NodeRuntimeOp::MembershipWorkflowWithRollback(_, reply) => {
            let _ = reply.send(Err(error));
            false
        }
        NodeRuntimeOp::MembershipReports(reply) => {
            let _ = reply.send(Err(error));
            false
        }
        NodeRuntimeOp::InstallSnapshot(_, _, _, reply) => {
            let _ = reply.send(Err(error));
            false
        }
        NodeRuntimeOp::PeerPipelineStatus(_, reply) => {
            let _ = reply.send(Err(error));
            false
        }
        NodeRuntimeOp::PeerPipelineStatuses(reply) => {
            let _ = reply.send(Err(error));
            false
        }
        NodeRuntimeOp::IsBusy(reply) => {
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

