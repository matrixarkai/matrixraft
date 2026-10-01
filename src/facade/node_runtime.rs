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
    TransferLeaderOutcome(
        NodeId,
        mpsc::Sender<Result<crate::LeaderTransferOutcome, RaftError>>,
    ),
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
    command_tx: Option<CommandSender>,
    /// The group's own thread, when it has one.
    worker: Option<thread::JoinHandle<()>>,
    /// Where the group is hosted, when it is not on a thread of its own.
    shared: Option<(Arc<SharedGroupRuntime>, DriverGroupKey)>,
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
            command_tx: Some(CommandSender {
                tx: command_tx,
                shared: None,
            }),
            worker: Some(worker),
            shared: None,
            restart_count: 0,
            state: NodeRuntimeState::Created,
        })
    }

    /// Hosts this group on a shared runtime instead of a thread of its own.
    ///
    /// The group keeps its own mailbox and answers exactly the same commands;
    /// what changes is that its ticking comes from one shared ticker and its
    /// work runs on a fixed pool, so the process stops paying a thread and a
    /// context switch per interval for every group it holds. See
    /// [`SharedGroupRuntime`].
    pub fn create_on(
        options: NodeOptions,
        runtime: Arc<SharedGroupRuntime>,
    ) -> Result<Self, RaftError> {
        let node_id = options.node_id;
        let group_id = options.group_id;
        let tick_interval_ms = options.config.heartbeat_interval_ms.max(1);
        let key = DriverGroupKey::new(group_id, node_id);
        let (command_tx, command_rx) = mpsc::channel();
        // The error is reported rather than answered into the channel: no
        // command can have been sent yet, because this is what hands the
        // sender out.
        let core = NodeCore::new(options, command_rx).map_err(|(error, _rx)| error)?;
        runtime.host(key, core, tick_interval_ms)?;
        Ok(Self {
            node_id,
            group_id,
            command_tx: Some(CommandSender {
                tx: command_tx,
                shared: Some((Arc::clone(&runtime), key)),
            }),
            worker: None,
            shared: Some((runtime, key)),
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
        // A hosted group has no thread to join. It has to leave the ticker
        // and the pool instead, or the runtime goes on ticking a group that
        // has shut down.
        if let Some((runtime, key)) = self.shared.take() {
            runtime.release(key);
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

    /// Proposes several payloads and persists them together.
    ///
    /// A durable append is its fsync and very little else, so proposing one at
    /// a time caps a node at a few hundred entries per second however fast the
    /// rest of it is. This applies the whole batch and writes one WAL record
    /// for it, which is one fsync rather than one each.
    ///
    /// Every proposal is durable when this returns and none before, exactly as
    /// with [`Self::propose`]. Do not acknowledge any of them until it returns.
    pub fn propose_batch(&self, payloads: Vec<Payload>) -> Result<Vec<LogId>, RaftError> {
        self.propose_batch_with_options(payloads, ProposeOptions::default())
    }

    /// [`Self::propose_batch`], with the same options applied to every payload.
    pub fn propose_batch_with_options(
        &self,
        payloads: Vec<Payload>,
        options: ProposeOptions,
    ) -> Result<Vec<LogId>, RaftError> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        let messages: Vec<Message> = payloads
            .into_iter()
            .map(|payload| Message::Propose {
                payload,
                options: options.clone(),
            })
            .collect();
        self.step_batch(messages)?
            .into_iter()
            .map(|result| match result {
                StepResult::Proposed(log_id) => Ok(log_id),
                other => Err(RaftError::InvalidRequest(format!(
                    "unexpected propose result: {other:?}"
                ))),
            })
            .collect()
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

    /// Transfer leadership and report which of the three outcomes occurred.
    ///
    /// Prefer this to [`Self::transfer_leader`] when the caller needs to know
    /// whether leadership actually moved: `transfer_leader` returns `Ok` for an
    /// ignored request as well as a completed one.
    pub fn transfer_leader_outcome(
        &self,
        target: NodeId,
    ) -> Result<crate::LeaderTransferOutcome, RaftError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.sender()?
            .send(NodeRuntimeOp::TransferLeaderOutcome(target, reply_tx))
            .map_err(|err| {
                RaftError::Transport(format!(
                    "failed to transfer leadership through raft node: {err}"
                ))
            })?;
        recv_runtime_reply(reply_rx)?
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

    fn sender(&self) -> Result<&CommandSender, RaftError> {
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

/// Applies one message.
///
/// `persist_proposal` is false when the caller will persist for a whole batch
/// of messages instead. A WAL record describes the log as it stands, so one
/// record covers every proposal applied before it -- and one fsync makes them
/// all durable, rather than one fsync each.
fn runtime_step_message(
    cluster: &mut RaftCluster,
    wal: &mut Option<PersistentRaftWal>,
    membership_executor: &mut MembershipExecutor,
    node_id: NodeId,
    message: Message,
    persist_proposal: bool,
) -> Result<StepResult, RaftError> {
    match message {
        Message::Propose { payload, options } => {
            if cluster.leader_id() != Some(node_id) {
                return Err(RaftError::NotLeader(
                    cluster.leader_id().unwrap_or_default(),
                ));
            }
            let log_id = cluster.propose_with_options(payload, options)?;
            if let Some(wal) = wal.as_mut().filter(|_| persist_proposal) {
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

/// Everything one raft group's runtime owns.
///
/// This was twenty-odd locals inside `raft_node_runtime_loop`. State shaped
/// that way can only live on a thread dedicated to the group, which is why
/// each group costs one: a thread parked in `recv_timeout`, waking once every
/// heartbeat interval whether or not there is anything to do.
/// `examples/idle_tick_cost.rs` puts that at roughly one core per thousand
/// groups, of which about 85% is the wake-up rather than raft work.
///
/// Holding it in a struct does not by itself change any of that -- the loop
/// below still drives it exactly as before. What it changes is that the loop
/// is no longer the only thing that *could*.
struct NodeCore {
    node_id: NodeId,
    group_id: GroupId,
    cluster: RaftCluster,
    wal: Option<PersistentRaftWal>,
    last_wal_recovery_report: Option<WalRecoveryReport>,
    state: NodeRuntimeState,
    heartbeat_interval_ms: u64,
    election_timeout_ms: u64,
    leader_lease_timeout_ms: u64,
    heartbeat_interval: Duration,
    heartbeat_ticks: u64,
    election_ticks: u64,
    election_elapsed_ms: u64,
    pre_vote_executions: u64,
    campaign_executions: u64,
    leader_transfer_executions: u64,
    last_tick_reason: String,
    blockers: Vec<String>,
    fatal_blockers: Vec<String>,
    membership_executor: MembershipExecutor,
    /// A command drained by proposal coalescing that turned out not to be a
    /// proposal; processed first next time round, untouched.
    carried_command: Option<NodeRuntimeOp>,
    /// The group's mailbox.
    ///
    /// The core owns it rather than the thread, which is what lets a pool
    /// worker drain the group without one.
    command_rx: mpsc::Receiver<NodeRuntimeOp>,
}

impl NodeCore {
    /// Builds the group's runtime, or reports why it cannot be built.
    ///
    /// The caller answers outstanding commands with the error: a runtime that
    /// failed to open its WAL must say so to whoever asks, rather than drop
    /// the channel and look like a node that merely went quiet.
    /// Hands the receiver back on failure rather than draining it here.
    ///
    /// Draining inside this function would deadlock the pooled constructor:
    /// its sender is still alive in the caller's scope, so `recv` would never
    /// see a disconnect and would block forever. Only the thread-backed
    /// caller, which parks on that channel anyway, should answer the queue.
    #[allow(clippy::result_large_err)]
    fn new(
        options: NodeOptions,
        command_rx: mpsc::Receiver<NodeRuntimeOp>,
    ) -> Result<Self, (RaftError, mpsc::Receiver<NodeRuntimeOp>)> {
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
            return Err((error, command_rx));
        }
    };
    let mut last_wal_recovery_report = None;
    let wal = match PersistentRaftWal::open(PersistentRaftWalOptions {
        dir: PathBuf::from(&options.wal_dir),
        // See `PersistentRaftWalOptions::new`: this is now just how much the
        // node holds in memory, and smaller is strictly cheaper.
        max_records_per_segment: 1_000,
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
            return Err((error, command_rx));
        }
    };
    let state = NodeRuntimeState::Created;
    let heartbeat_interval_ms = options.config.heartbeat_interval_ms.max(1);
    let election_timeout_ms = options
        .config
        .election_timeout_ms
        .max(heartbeat_interval_ms);
    let leader_lease_timeout_ms = options.config.leader_lease_ms.max(1);
    let heartbeat_interval = Duration::from_millis(heartbeat_interval_ms);
    let heartbeat_ticks = 0;
    let election_ticks = 0;
    let election_elapsed_ms: u64 = 0;
    let pre_vote_executions = 0;
    let campaign_executions = 0;
    let leader_transfer_executions = 0_u64;
    let last_tick_reason = "runtime_created".to_string();
    let blockers = Vec::<String>::new();
    let fatal_blockers = Vec::<String>::new();
    let membership_executor = MembershipExecutor::new();
    // A command drained by proposal coalescing that turned out not to be a
    // proposal; processed first on the next iteration, untouched.
    let carried_command: Option<NodeRuntimeOp> = None;
    // When the next tick is due. The loop waits for what is left of the
    // interval rather than starting a fresh one per receive, so arriving
    // commands cannot hold the tick off indefinitely.
        Ok(Self {
            node_id,
            group_id,
            cluster,
            wal,
            last_wal_recovery_report,
            state,
            heartbeat_interval_ms,
            election_timeout_ms,
            leader_lease_timeout_ms,
            heartbeat_interval,
            heartbeat_ticks,
            election_ticks,
            election_elapsed_ms,
            pre_vote_executions,
            campaign_executions,
            leader_transfer_executions,
            last_tick_reason,
            blockers,
            fatal_blockers,
            membership_executor,
            carried_command,
            command_rx,
        })
    }

    /// One heartbeat interval of work: leases, peer liveness, the snapshot
    /// trigger, catch-up and the election clock.
    ///
    /// Gated on the running state exactly as it was in the loop, so a created
    /// or stopped node does no raft work when its interval comes round.
    fn tick(&mut self) {
        let Self {
            node_id,
            cluster,
            state,
            heartbeat_interval_ms,
            election_timeout_ms,
            heartbeat_ticks,
            election_ticks,
            election_elapsed_ms,
            pre_vote_executions,
            campaign_executions,
            leader_transfer_executions,
            last_tick_reason,
            blockers,
            fatal_blockers,
            ..
        } = self;
        let node_id = *node_id;
        let heartbeat_interval_ms = *heartbeat_interval_ms;
        let election_timeout_ms = *election_timeout_ms;
                if *state == NodeRuntimeState::Running {
                    *heartbeat_ticks += 1;
                    *election_elapsed_ms = election_elapsed_ms.saturating_add(heartbeat_interval_ms);
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
                        *election_elapsed_ms = 0;
                    }
                    if !cluster.leader_lease_valid && cluster.step_down_leader_if_lost_quorum() {
                        blockers.push("lost_quorum_step_down".to_string());
                    }
                    *last_tick_reason = "heartbeat_tick".to_string();
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
                                *leader_transfer_executions =
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
                    if *election_elapsed_ms >= election_timeout_ms {
                        *election_ticks += 1;
                        *election_elapsed_ms = 0;
                        *last_tick_reason = "election_tick".to_string();
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
                            *pre_vote_executions += 1;
                            match cluster.pre_vote(node_id) {
                                Ok(vote) if vote.vote_granted => {
                                    *campaign_executions += 1;
                                    let result = record_runtime_result(
                                        "election_tick_campaign",
                                        cluster.campaign(node_id, false),
                                        blockers,
                                        fatal_blockers,
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
    }

    /// The next command waiting, if any, without blocking.
    ///
    /// A command carried over from proposal coalescing comes first: it was
    /// taken out of the channel already and would otherwise be served out of
    /// order, behind commands that arrived after it.
    fn next_command(&mut self) -> Option<NodeRuntimeOp> {
        match self.carried_command.take() {
            Some(command) => Some(command),
            None => self.command_rx.try_recv().ok(),
        }
    }

    /// Handles one command, answering its reply channel.
    ///
    /// Takes the receiver because proposal coalescing drains whatever else is
    /// already queued and commits it under one fsync.
    ///
    /// `Break` means the runtime has shut down and must not be driven again.
    fn handle(&mut self, command: NodeRuntimeOp) -> ControlFlow<()> {
        let Self {
            node_id,
            group_id,
            cluster,
            wal,
            last_wal_recovery_report,
            state,
            heartbeat_interval_ms,
            election_timeout_ms,
            leader_lease_timeout_ms,
            heartbeat_ticks,
            election_ticks,
            election_elapsed_ms,
            pre_vote_executions,
            campaign_executions,
            leader_transfer_executions,
            last_tick_reason,
            blockers,
            fatal_blockers,
            membership_executor,
            carried_command,
            command_rx,
            ..
        } = self;
        let node_id = *node_id;
        let group_id = *group_id;
        let heartbeat_interval_ms = *heartbeat_interval_ms;
        let election_timeout_ms = *election_timeout_ms;
        let leader_lease_timeout_ms = *leader_lease_timeout_ms;
        // Auto group commit. A durable proposal is its fsync and very little
        // else, so a node answering concurrent proposers one at a time is
        // capped at a few hundred per second whatever else it does. While one
        // proposal's fsync runs, the next proposers queue; draining the queued
        // proposals here applies them together, persists them with one fsync,
        // and answers each sender individually. A lone proposal takes the
        // ordinary path below unchanged, and a drained command that turned out
        // not to be a proposal is carried into the next iteration.
        let command = match command {
            NodeRuntimeOp::Step(message @ Message::Propose { .. }, reply)
                if *state == NodeRuntimeState::Running =>
            {
                let mut pending = vec![(message, reply)];
                // Bounded so a saturated queue cannot starve ticks and other
                // commands indefinitely.
                while pending.len() < 128 {
                    match command_rx.try_recv() {
                        Ok(NodeRuntimeOp::Step(next @ Message::Propose { .. }, next_reply)) => {
                            pending.push((next, next_reply));
                        }
                        Ok(other) => {
                            *carried_command = Some(other);
                            break;
                        }
                        Err(_) => break,
                    }
                }
                if pending.len() == 1 {
                    let (message, reply) = pending.pop().expect("one pending proposal");
                    NodeRuntimeOp::Step(message, reply)
                } else {
                    let mut results = Vec::with_capacity(pending.len());
                    let mut replies = Vec::with_capacity(pending.len());
                    let mut any_applied = false;
                    for (message, reply) in pending {
                        let result = runtime_step_message(
                            cluster,
                            wal,
                            membership_executor,
                            node_id,
                            message,
                            false,
                        );
                        any_applied |= result.is_ok();
                        results.push(result);
                        replies.push(reply);
                    }
                    // One record covers every proposal applied above, exactly
                    // as step_batch persists. Nothing applied means nothing to
                    // persist.
                    let persisted: Result<(), RaftError> = if any_applied {
                        match wal.as_mut() {
                            Some(wal) => wal
                                .append_built(|coverage| {
                                    cluster.wal_record_for_coverage(node_id, coverage)
                                })
                                .map(|_| ()),
                            None => Ok(()),
                        }
                    } else {
                        Ok(())
                    };
                    for (result, reply) in results.into_iter().zip(replies) {
                        // A proposal that applied but did not persist is not
                        // durable, and must not be acknowledged as if it were.
                        let result = match (&persisted, result) {
                            (Err(error), Ok(_)) => Err(error.clone()),
                            (_, result) => result,
                        };
                        let _ = reply.send(record_runtime_result(
                            "propose",
                            result,
                            blockers,
                            fatal_blockers,
                            true,
                        ));
                    }
                    return ControlFlow::Continue(());
                }
            }
            other => other,
        };
        match command {
            NodeRuntimeOp::Start(reply) => {
                let result = cluster.start();
                if result.is_ok() {
                    *state = NodeRuntimeState::Running;
                    *election_elapsed_ms = 0;
                }
                let _ = reply.send(record_runtime_result(
                    "start",
                    result,
                    blockers,
                    fatal_blockers,
                    false,
                ));
            }
            NodeRuntimeOp::Stop(reply) => {
                let result = cluster.stop();
                if result.is_ok() {
                    *state = NodeRuntimeState::Stopped;
                }
                let _ = reply.send(record_runtime_result(
                    "stop",
                    result,
                    blockers,
                    fatal_blockers,
                    false,
                ));
            }
            NodeRuntimeOp::Status(reply) => {
                let status = NodeRuntimeStatus {
                    node_id,
                    group_id,
                    state: *state,
                    restart_count: 0,
                    worker_running: *state != NodeRuntimeState::Shutdown,
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
                        heartbeat_ticks: *heartbeat_ticks,
                        election_ticks: *election_ticks,
                        pre_vote_executions: *pre_vote_executions,
                        campaign_executions: *campaign_executions,
                        leader_transfer_executions: *leader_transfer_executions,
                        last_tick_reason: last_tick_reason.clone(),
                    },
                    peer_runtime: raft_peer_runtime_states(
                        cluster,
                        *election_elapsed_ms,
                        *heartbeat_ticks > 0,
                        *pre_vote_executions > 0,
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
                    *pre_vote_executions += 1;
                }
                if is_leader_transfer_step_message(&message) {
                    *leader_transfer_executions = leader_transfer_executions.saturating_add(1);
                }
                if matches!(&message, Message::TimeoutNow { .. }) {
                    *campaign_executions = campaign_executions.saturating_add(1);
                }
                let campaign_message = matches!(
                    &message,
                    Message::Admin {
                        command: AdminCommand::Campaign { .. },
                    }
                );
                if campaign_message {
                    *campaign_executions = campaign_executions.saturating_add(1);
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
                    cluster,
                    wal,
                    membership_executor,
                    node_id,
                    message,
                    true,
                );
                if result
                    .as_ref()
                    .map(|step| matches!(step, StepResult::FatalEvent(Some(_))))
                    .unwrap_or(false)
                {
                    *leader_transfer_executions = leader_transfer_executions.saturating_add(1);
                }
                let _ = reply.send(record_runtime_result(
                    operation_name,
                    result,
                    blockers,
                    fatal_blockers,
                    fatal_on_step_error,
                ));
            }
            NodeRuntimeOp::StepBatch(messages, reply) => {
                *pre_vote_executions += messages
                    .iter()
                    .filter(|message| matches!(message, Message::PreVote { .. }))
                    .count() as u64;
                *leader_transfer_executions = leader_transfer_executions.saturating_add(
                    messages
                        .iter()
                        .filter(|message| is_leader_transfer_step_message(message))
                        .count() as u64,
                );
                *campaign_executions = campaign_executions.saturating_add(
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
                *campaign_executions = campaign_executions.saturating_add(campaign_message_count);
                // A WAL record describes the log as it stands, so one record
                // covers every proposal in the batch. Persisting per proposal
                // made a batch of N cost N fsyncs, and an fsync is essentially
                // the whole cost of a durable append.
                let batch_has_proposal = messages
                    .iter()
                    .any(|message| matches!(message, Message::Propose { .. }));
                let stepped: Result<Vec<StepResult>, RaftError> = messages
                    .into_iter()
                    .map(|message| {
                        runtime_step_message(
                            cluster,
                            wal,
                            membership_executor,
                            node_id,
                            message,
                            false,
                        )
                    })
                    .collect();
                // Persisted even when a message failed, so what is on disk
                // still describes what the node applied.
                let persisted = if batch_has_proposal {
                    match wal.as_mut() {
                        Some(wal) => wal
                            .append_built(|coverage| {
                                cluster.wal_record_for_coverage(node_id, coverage)
                            })
                            .map(|_| ()),
                        None => Ok(()),
                    }
                } else {
                    Ok(())
                };
                let result: Result<Vec<StepResult>, RaftError> = match (stepped, persisted) {
                    (Ok(results), Ok(())) => Ok(results),
                    (Err(error), _) => Err(error),
                    (Ok(_), Err(error)) => Err(error),
                };
                let _ = reply.send(record_runtime_result(
                    "step_batch",
                    result,
                    blockers,
                    fatal_blockers,
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
                    blockers,
                    fatal_blockers,
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
                    blockers,
                    fatal_blockers,
                    false,
                ));
            }
            NodeRuntimeOp::MembershipWorkflowWithRollback(operations, reply) => {
                let _ = reply.send(record_runtime_result(
                    "membership_workflow_with_rollback",
                    membership_executor.execute_all_with_rollback(cluster, operations),
                    blockers,
                    fatal_blockers,
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
                    blockers,
                    fatal_blockers,
                    false,
                ));
            }
            NodeRuntimeOp::PeerPipelineStatus(peer_id, reply) => {
                let _ = reply.send(record_runtime_result(
                    "peer_pipeline_status",
                    cluster.peer_pipeline_status(peer_id),
                    blockers,
                    fatal_blockers,
                    false,
                ));
            }
            NodeRuntimeOp::PeerPipelineStatuses(reply) => {
                let _ = reply.send(record_runtime_result(
                    "peer_pipeline_statuses",
                    Ok(cluster.peer_pipeline_statuses()),
                    blockers,
                    fatal_blockers,
                    false,
                ));
            }
            NodeRuntimeOp::IsBusy(reply) => {
                let _ = reply.send(record_runtime_result(
                    "is_busy",
                    Ok(cluster.is_busy()),
                    blockers,
                    fatal_blockers,
                    false,
                ));
            }
            NodeRuntimeOp::LeaderTransferState(reply) => {
                let _ = reply.send(Ok(cluster.leader_transfer_state()));
            }
            NodeRuntimeOp::TransferLeaderOutcome(target, reply) => {
                // Performed and classified inside the runtime thread, so the
                // outcome cannot be invalidated between doing the transfer and
                // observing it.
                let _ = reply.send(cluster.transfer_leader_outcome(target));
            }
            NodeRuntimeOp::Shutdown(reply) => {
                let result = cluster.stop();
                let _ = reply.send(record_runtime_result(
                    "shutdown",
                    result,
                    blockers,
                    fatal_blockers,
                    false,
                ));
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    }
}

/// Answers every queued command with the reason the runtime does not exist.
///
/// Dropping the receiver instead would make a node that failed to open its
/// WAL indistinguishable from one that merely went quiet.
fn drain_with_error(command_rx: &mpsc::Receiver<NodeRuntimeOp>, error: &RaftError) {
    while let Ok(command) = command_rx.recv() {
        if respond_runtime_error(command, error.clone()) {
            break;
        }
    }
}

/// One group hosted on the shared runtime instead of on a thread.
struct PooledGroup {
    /// The group's whole runtime.
    ///
    /// One lock, not several: a worker running a command and a worker running
    /// a tick must not interleave inside raft.
    core: Mutex<NodeCore>,
    /// Heartbeat intervals that have come due and not yet been served.
    ///
    /// The ticker only ever increments this. It must not take `core`: a
    /// single ticker blocked on one slow group would stop ticking every other
    /// group in the process.
    ticks_due: AtomicU64,
}

impl std::fmt::Debug for PooledGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledGroup")
            .field("ticks_due", &self.ticks_due.load(Ordering::Relaxed))
            .finish()
    }
}

impl PooledGroup {
    /// Runs the tick here if the group is free, and says whether it did.
    ///
    /// `try_lock`, never `lock`: this is called on a ticker thread, and a
    /// ticker that blocked on one group would stop ticking every other group
    /// in its shard. A refusal is not a dropped tick -- `ticks_due` is
    /// already incremented, and the caller hands the group to a worker.
    ///
    /// Only the tick, never the mail. A tick is bounded work; a proposal is
    /// an fsync, and serving one here would stall the shard.
    fn try_run_tick(&self) -> bool {
        let Ok(mut core) = self.core.try_lock() else {
            return false;
        };
        if self.ticks_due.swap(0, Ordering::AcqRel) > 0 {
            core.tick();
        }
        true
    }

    /// Runs whatever the group has waiting: the tick first, then its mail.
    ///
    /// The tick goes first so a command is never served against a lease this
    /// interval should already have expired.
    ///
    /// Serves at most `max_commands` of them and answers whether more were
    /// left. On a thread of its own a group may drain its mailbox dry,
    /// because the thread is its own and has nothing else owed; on a shared
    /// worker that would let one busy group hold the worker while every
    /// other group it serves waits.
    fn run_pending(&self, max_commands: usize) -> bool {
        // A worker that panicked mid-command leaves the group poisoned. The
        // alternative to carrying on is a group that never ticks again, which
        // is worse than one whose counters are suspect.
        let mut core = match self.core.lock() {
            Ok(core) => core,
            Err(poisoned) => poisoned.into_inner(),
        };
        if self.ticks_due.swap(0, Ordering::AcqRel) > 0 {
            // Coalesced to one however far behind the group fell, the same
            // way the driver's heap pulls a late group forward rather than
            // firing its whole backlog at it.
            core.tick();
        }
        let mut more = true;
        for _ in 0..max_commands.max(1) {
            let Some(command) = core.next_command() else {
                more = false;
                break;
            };
            if core.handle(command).is_break() {
                more = false;
                break;
            }
        }
        // Re-read before releasing. The ticker does not post mail for a
        // group it found busy, so a tick that arrived after the read above
        // has no other way in than this.
        if self.ticks_due.swap(0, Ordering::AcqRel) > 0 {
            core.tick();
        }
        if !more {
            return false;
        }
        // Whether anything is actually left is only knowable by taking one,
        // so a spare visit is possible. That is far cheaper than the
        // alternative it replaces.
        true
    }
}

/// How a shared runtime's ticks were served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharedRuntimeStats {
    /// Ticks run on the ticker thread, because the group was free.
    ///
    /// This is the cheap path and should be nearly all of them: it costs no
    /// mail, and no worker wake-up.
    pub ticks_in_place: u64,
    /// Ticks handed to a worker, because the group's lock was held.
    ///
    /// Climbing steadily means groups are contended for longer than an
    /// interval, which is what too few workers looks like from outside.
    pub ticks_handed_over: u64,
    /// Groups the runtime holds.
    pub groups: usize,
    /// Threads it uses, whatever the group count.
    pub threads: usize,
}

#[derive(Debug, Default)]
struct TickCounters {
    in_place: AtomicU64,
    handed_over: AtomicU64,
}

/// Ticks a group, on this thread when it can.
struct PooledGroupTicker {
    group: Arc<PooledGroup>,
    pool: Arc<DriverWorkerPool<()>>,
    counters: Arc<TickCounters>,
    key: DriverGroupKey,
}

impl DriverTickReceiver for PooledGroupTicker {
    fn fire_tick(&self) {
        self.group.ticks_due.fetch_add(1, Ordering::Relaxed);
        // Do it here when the group is free, which is the common case and
        // costs nothing beyond this thread. Posting a mail for every group
        // every interval is what kept the switch count at half the
        // thread-per-group model instead of near zero.
        if self.group.try_run_tick() {
            self.counters.in_place.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.counters.handed_over.fetch_add(1, Ordering::Relaxed);
        // Busy: hand it over rather than wait. Best effort, because a
        // refused send means a wake is already queued, and one is enough.
        //
        // Do NOT remove this as redundant on the grounds that the lock
        // holder reads `ticks_due` before releasing. It is also, by
        // accident, the thing that re-arms a group every interval -- and
        // the pool's send path loses a wake-up often enough that without it
        // the runtime wedges: a command sent, every worker parked, and
        // nothing left to rouse them. Tried, measured, reverted.
        let _ = self.pool.send(self.key, MailPriority::Normal, ());
    }
}

/// Runs a group when a worker picks its wake-up out of the pool.
struct PooledGroupWorker {
    group: Arc<PooledGroup>,
    pool: Arc<DriverWorkerPool<()>>,
    key: DriverGroupKey,
    /// Most commands to serve for this group in one visit.
    max_each_visit: usize,
}

impl DriverMailHandler<()> for PooledGroupWorker {
    fn handle_mail(&self, _wakes: Vec<()>) {
        // The wakes carry nothing; how much there is to do is in the group.
        if self.group.run_pending(self.max_each_visit) {
            // Still more waiting. Go to the back of the queue rather than
            // hold the worker: the other groups it serves are owed a turn.
            let _ = self.pool.send(self.key, MailPriority::Normal, ());
        }
    }
}

/// Hosts many raft groups on a few tickers and a fixed pool of workers.
///
/// The alternative, and still the default, is a thread for each group parked
/// in `recv_timeout`. That costs one context switch per group per heartbeat
/// interval -- about one core per thousand groups, measured by
/// `examples/idle_tick_cost.rs`, of which roughly 85% is the wake-up rather
/// than raft work. Here the switching is a function of the shard count
/// rather than the group count: measured at about 3,400 a second whether the
/// runtime holds a thousand groups or four thousand.
///
/// # Sizing `worker_num`
///
/// It decides the shard count, and the shards carry every group's tick, so
/// this is no longer only a tuning preference -- it is what decides whether
/// the groups are ticked at all.
///
/// What has actually been measured, at a 10ms interval, every group a live
/// leader (`examples/idle_tick_cost.rs`):
///
/// ```text
///   groups  shards  per shard   cores   ticks delivered
///     1024       4        256   0.244            100.0%
///     1536       4        384   0.314            100.1%
///     2048       4        512   0.444            100.0%
///     3072       4        768   0.579            100.0%
///     4096       8        512   1.098            100.0%
/// ```
///
/// Cores rise linearly with the group count and the ticks all arrive, so
/// there is no sign of a ceiling in any of it. Starting is linear too, about
/// 83us a group, and no slower than giving each group a thread of its own.
///
/// **4096 groups on 4 shards is not in the table, and an earlier version of
/// this note said they "never finished starting". That was wrong.** A
/// backtrace of the stalled run shows `start_all` had completed and all 4096
/// nodes existed; what had stalled was the probe, sampling group statuses
/// afterwards. Each `NodeRuntime::status` waits up to five seconds, and
/// sixty-four of them that do not answer is five minutes of nothing.
///
/// So what is actually unexplained is narrower and worth stating as such: at
/// 1024 live groups per shard, a status command can take longer than five
/// seconds to come back. Ticking is fine; answering is not. That is where to
/// look next, and no formula is offered until it is understood.
///
/// Undersizing does not fail loudly. The tickers fall behind, the groups are
/// ticked more slowly than they were configured for, and because heartbeats,
/// leases and election timeouts are all counted in those ticks, leases begin
/// to outlive their configuration. Watch the `kept up` column of
/// `examples/idle_tick_cost.rs`, or [`SharedRuntimeStats::ticks_handed_over`]
/// on a running store.
pub struct SharedGroupRuntime {
    /// One ticker per shard, with groups hashed across them.
    ///
    /// A tick now runs on the ticker that owns the group rather than being
    /// posted to the pool, so a single ticker would carry every group's raft
    /// work -- about 2.3us a tick measured, which saturates one thread near
    /// four thousand groups at a 10ms interval. Sharding moves that out by
    /// the shard count, and keeps each due-time heap smaller as well.
    tickers: Vec<Driver>,
    pool: Arc<DriverWorkerPool<()>>,
    /// `max_messages_each_poll`, which bounds one group's turn on a worker.
    max_each_visit: usize,
    counters: Arc<TickCounters>,
    groups: Mutex<BTreeMap<DriverGroupKey, Arc<PooledGroup>>>,
}

impl std::fmt::Debug for SharedGroupRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedGroupRuntime")
            .field("tickers", &self.tickers.len())
            .field("hosted_groups", &self.group_count())
            .finish()
    }
}

impl SharedGroupRuntime {
    /// Starts the tickers and the worker pool.
    ///
    /// One ticker per worker: the tickers do the tick work now, so their
    /// number is what bounds how many groups can be ticked, and tying it to
    /// `worker_num` means one setting still describes the whole runtime.
    pub fn start(options: DriverOptions) -> Result<Self, RaftError> {
        let shards = options.worker_num.max(1);
        let mut tickers = Vec::with_capacity(shards);
        for _ in 0..shards {
            // One worker each: a ticker's own pool is never used, because
            // every group registers its mail with the shared pool below.
            tickers.push(Driver::start(DriverOptions {
                worker_num: 1,
                ..options
            })?);
        }
        Ok(Self {
            tickers,
            pool: Arc::new(DriverWorkerPool::start(options)?),
            max_each_visit: options.max_messages_each_poll.max(1),
            counters: Arc::new(TickCounters::default()),
            groups: Mutex::new(BTreeMap::new()),
        })
    }

    /// Which ticker owns a group.
    ///
    /// Hashed rather than taken modulo the group id.
    ///
    /// Consecutive ids would spread perfectly well under `%` -- it is a
    /// stride that defeats it. A store numbering its groups 4, 8, 12 with
    /// four shards puts every one of them on shard zero, and the runtime
    /// would look fixed-size while ticking on a single thread. Hashing has
    /// no such pattern to fall into.
    fn ticker_for(&self, key: DriverGroupKey) -> &Driver {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        &self.tickers[(hasher.finish() % self.tickers.len() as u64) as usize]
    }

    /// How many groups this runtime hosts.
    pub fn group_count(&self) -> usize {
        self.groups
            .lock()
            .map(|groups| groups.len())
            .unwrap_or(0)
    }

    /// How the runtime's ticks have been served, and what it holds.
    pub fn stats(&self) -> SharedRuntimeStats {
        SharedRuntimeStats {
            ticks_in_place: self.counters.in_place.load(Ordering::Relaxed),
            ticks_handed_over: self.counters.handed_over.load(Ordering::Relaxed),
            groups: self.group_count(),
            threads: self.thread_count(),
        }
    }

    /// How many tickers the groups are spread across.
    pub fn ticker_count(&self) -> usize {
        self.tickers.len()
    }

    /// Groups held by each ticker, in shard order.
    ///
    /// Exposed because an uneven spread is the failure this sharding can
    /// have and nothing else would show it: every group on one shard still
    /// works, just at one shard's ceiling.
    pub fn groups_per_ticker(&self) -> Vec<usize> {
        self.tickers.iter().map(Driver::group_count).collect()
    }

    /// Threads the whole runtime uses, whatever the group count.
    pub fn thread_count(&self) -> usize {
        let tickers: usize = self.tickers.iter().map(Driver::thread_count).sum();
        tickers + self.pool.thread_count()
    }

    fn host(
        &self,
        key: DriverGroupKey,
        core: NodeCore,
        tick_interval_ms: u64,
    ) -> Result<Arc<PooledGroup>, RaftError> {
        let group = Arc::new(PooledGroup {
            core: Mutex::new(core),
            ticks_due: AtomicU64::new(0),
        });
        {
            let mut groups = self.groups.lock().expect("hosted groups mutex poisoned");
            if groups.contains_key(&key) {
                return Err(RaftError::InvalidRequest(format!(
                    "shared runtime already hosts group {} node {}",
                    key.group_id, key.node_id
                )));
            }
            groups.insert(key, Arc::clone(&group));
        }
        // The worker first. Registering the ticker first would let a tick
        // arrive for a group the pool does not yet know, and the wake would
        // be dropped.
        self.pool.register_group(
            key,
            Arc::new(PooledGroupWorker {
                group: Arc::clone(&group),
                pool: Arc::clone(&self.pool),
                key,
                max_each_visit: self.max_each_visit,
            }),
        )?;
        self.ticker_for(key).register_group_every(
            key,
            Arc::new(PooledGroupTicker {
                group: Arc::clone(&group),
                pool: Arc::clone(&self.pool),
                counters: Arc::clone(&self.counters),
                key,
            }),
            tick_interval_ms,
        )?;
        Ok(group)
    }

    fn release(&self, key: DriverGroupKey) {
        self.ticker_for(key).cancel_group(key);
        self.pool.cancel_group(key);
        if let Ok(mut groups) = self.groups.lock() {
            groups.remove(&key);
        }
    }

    /// Hands a group to a worker, for a command that has just been queued.
    fn wake(&self, key: DriverGroupKey) {
        let _ = self.pool.send(key, MailPriority::Normal, ());
    }
}

/// A group's command channel, and whatever has to happen after a send.
///
/// On a thread of its own the thread is already waiting on the receiver and
/// the send wakes it. On the shared runtime nothing is waiting, so the send
/// has to hand the group to a worker as well. Doing that here rather than at
/// the sixteen call sites means a command path added later cannot forget to.
#[derive(Clone)]
struct CommandSender {
    tx: mpsc::Sender<NodeRuntimeOp>,
    shared: Option<(Arc<SharedGroupRuntime>, DriverGroupKey)>,
}

impl std::fmt::Debug for CommandSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandSender")
            .field("hosting", &if self.shared.is_some() { "shared" } else { "own thread" })
            .finish()
    }
}

impl CommandSender {
    // Deliberately the same signature as `mpsc::Sender::send`, which is the
    // whole point: it is what lets the sixteen call sites stay untouched.
    // `SendError` hands the undelivered command back, so it is large by
    // design, and std is not linted for the same shape only because it is
    // std. Boxing it here would allocate on the error path and make this no
    // longer a drop-in.
    #[allow(clippy::result_large_err)]
    fn send(&self, command: NodeRuntimeOp) -> Result<(), mpsc::SendError<NodeRuntimeOp>> {
        self.tx.send(command)?;
        if let Some((runtime, key)) = &self.shared {
            runtime.wake(*key);
        }
        Ok(())
    }
}

/// Drives one group's runtime on a thread of its own.
///
/// See [`NodeCore`] for what this costs at scale. The scheduling lives here
/// rather than in the core so that something else can schedule it instead.
fn raft_node_runtime_loop(options: NodeOptions, command_rx: mpsc::Receiver<NodeRuntimeOp>) {
    let mut core = match NodeCore::new(options, command_rx) {
        Ok(core) => core,
        Err((error, command_rx)) => {
            drain_with_error(&command_rx, &error);
            return;
        }
    };
    // When the next tick is due. The loop waits for what is left of the
    // interval rather than starting a fresh one per receive, so arriving
    // commands cannot hold the tick off indefinitely.
    let mut next_tick = Instant::now() + core.heartbeat_interval;
    loop {
        let command = if let Some(command) = core.carried_command.take() {
            command
        } else {
            // An elapsed deadline counts as a tick even when commands are
            // queued. Waiting on the channel here instead would let a steady
            // stream of commands restart the wait forever, and the heartbeat,
            // the peer liveness check, the leader lease and the election clock
            // all hang off this tick -- so a busy node would quietly stop
            // doing all four.
            let now = Instant::now();
            let received = if now >= next_tick {
                Err(mpsc::RecvTimeoutError::Timeout)
            } else {
                core.command_rx.recv_timeout(next_tick - now)
            };
            match received {
                Ok(command) => command,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    // Set from now rather than by adding an interval, so a
                    // loop that fell behind ticks once and resynchronises
                    // instead of firing a burst of catch-up heartbeats.
                    next_tick = Instant::now() + core.heartbeat_interval;
                    core.tick();
                    continue;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        };
        if core.handle(command).is_break() {
            break;
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
        NodeRuntimeOp::TransferLeaderOutcome(_, reply) => {
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

