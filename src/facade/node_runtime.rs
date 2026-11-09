// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// stoppable node runtime worker and command loop.
// Split from src/lib.rs to keep the crate facade small and focused.

// Two waiting operations preserve proposal coalescing without retaining spare
// slots for an idle group.
const RUNTIME_COMMAND_QUEUE_DEPTH: usize = 2;

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

struct RuntimeReplyPool<T> {
    idle: Mutex<Vec<(mpsc::Sender<T>, mpsc::Receiver<T>)>>,
}

impl<T> RuntimeReplyPool<T> {
    fn new() -> Self {
        Self {
            idle: Mutex::new(Vec::new()),
        }
    }

    fn take(&self) -> (mpsc::Sender<T>, mpsc::Receiver<T>) {
        self.idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop()
            .unwrap_or_else(mpsc::channel)
    }

    fn put(&self, sender: mpsc::Sender<T>, receiver: mpsc::Receiver<T>) {
        self.idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((sender, receiver));
    }
}

impl<T> std::fmt::Debug for RuntimeReplyPool<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let idle = self
            .idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        formatter
            .debug_struct("RuntimeReplyPool")
            .field("idle", &idle)
            .finish()
    }
}

#[derive(Debug)]
enum StoredWalRecoveryReport {
    /// The ordinary new-WAL report. Its log range and record count are zero;
    /// only segment metadata varies.
    Clean {
        segments_scanned: u64,
        first_segment_id: u64,
        last_segment_id: u64,
    },
    Detailed(Box<WalRecoveryReport>),
}

impl StoredWalRecoveryReport {
    fn from_report(report: WalRecoveryReport) -> Self {
        let clean_range = report.retained_range.as_ref().filter(|range| {
            range.first_log_index == 0 && range.last_log_index == 0 && range.record_count == 0
        });
        if let Some(range) = clean_range.filter(|_| {
            report.recovered.is_none()
                && !report.truncated_corrupt_tail
                && report.surviving_records == 0
                && report.removed_records == 0
                && report
                    .checksum_format
                    .as_ref()
                    .is_some_and(wal::matrixraft_wal_checksum_format_is_current)
        }) {
            return Self::Clean {
                segments_scanned: report.segments_scanned,
                first_segment_id: range.first_segment_id,
                last_segment_id: range.last_segment_id,
            };
        }
        Self::Detailed(Box::new(report))
    }

    fn to_report(&self) -> WalRecoveryReport {
        match self {
            Self::Clean {
                segments_scanned,
                first_segment_id,
                last_segment_id,
            } => WalRecoveryReport {
                recovered: None,
                truncated_corrupt_tail: false,
                surviving_records: 0,
                removed_records: 0,
                segments_scanned: *segments_scanned,
                checksum_format: Some(matrixraft_wal_checksum_format()),
                retained_range: Some(LogRetainedRange {
                    first_log_index: 0,
                    last_log_index: 0,
                    first_segment_id: *first_segment_id,
                    last_segment_id: *last_segment_id,
                    record_count: 0,
                }),
            },
            Self::Detailed(report) => report.as_ref().clone(),
        }
    }
}

type LifecycleResult = Result<(), RaftError>;
type LifecycleBatchMessage = (usize, LifecycleResult);
type LifecycleBatchSender = mpsc::SyncSender<LifecycleBatchMessage>;

enum LifecycleReply {
    Single(mpsc::SyncSender<LifecycleResult>),
    Batch {
        index: usize,
        sender: LifecycleBatchSender,
    },
}

impl LifecycleReply {
    fn send(self, result: LifecycleResult) {
        match self {
            Self::Single(sender) => {
                let _ = sender.send(result);
            }
            Self::Batch { index, sender } => {
                let _ = sender.send((index, result));
            }
        }
    }
}

enum NodeRuntimeOp {
    Start(LifecycleReply),
    Stop(LifecycleReply),
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
    Shutdown(LifecycleReply),
}

struct RuntimeCommandQueue {
    /// The common case: one command waiting while its group is scheduled.
    first: Option<NodeRuntimeOp>,
    /// The queue is bounded at two. Keep the second slot out of every idle
    /// group and allocate it only when two senders actually overlap.
    overflow: Option<Box<NodeRuntimeOp>>,
    sender_count: usize,
    receiver_alive: bool,
}

impl RuntimeCommandQueue {
    fn new() -> Self {
        debug_assert_eq!(RUNTIME_COMMAND_QUEUE_DEPTH, 2);
        Self {
            first: None,
            overflow: None,
            sender_count: 1,
            receiver_alive: true,
        }
    }

    fn len(&self) -> usize {
        usize::from(self.first.is_some()) + usize::from(self.overflow.is_some())
    }

    fn push(&mut self, command: NodeRuntimeOp) {
        debug_assert!(self.len() < RUNTIME_COMMAND_QUEUE_DEPTH);
        if self.first.is_none() {
            debug_assert!(self.overflow.is_none());
            self.first = Some(command);
        } else {
            debug_assert!(self.overflow.is_none());
            self.overflow = Some(Box::new(command));
        }
    }

    fn pop(&mut self) -> Option<NodeRuntimeOp> {
        let command = self.first.take()?;
        self.first = self.overflow.take().map(|next| *next);
        Some(command)
    }
}

struct RuntimeCommandChannel {
    state: Mutex<RuntimeCommandQueue>,
    changed: std::sync::Condvar,
}

struct RuntimeCommandSender {
    channel: Arc<RuntimeCommandChannel>,
}

impl Clone for RuntimeCommandSender {
    fn clone(&self) -> Self {
        self.channel
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sender_count += 1;
        Self {
            channel: Arc::clone(&self.channel),
        }
    }
}

impl Drop for RuntimeCommandSender {
    fn drop(&mut self) {
        let mut state = self
            .channel
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.sender_count = state.sender_count.saturating_sub(1);
        let disconnected = state.sender_count == 0;
        drop(state);
        if disconnected {
            self.channel.changed.notify_all();
        }
    }
}

impl RuntimeCommandSender {
    #[allow(clippy::result_large_err)]
    fn send(&self, command: NodeRuntimeOp) -> Result<(), mpsc::SendError<NodeRuntimeOp>> {
        let mut command = Some(command);
        let mut state = self
            .channel
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            if !state.receiver_alive {
                return Err(mpsc::SendError(
                    command.take().expect("undelivered runtime command"),
                ));
            }
            if state.len() < RUNTIME_COMMAND_QUEUE_DEPTH {
                state.push(command.take().expect("runtime command admitted once"));
                drop(state);
                self.channel.changed.notify_all();
                return Ok(());
            }
            state = self
                .channel
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

struct RuntimeCommandReceiver {
    channel: Arc<RuntimeCommandChannel>,
}

impl RuntimeCommandReceiver {
    fn try_recv(&self) -> Result<NodeRuntimeOp, mpsc::TryRecvError> {
        let mut state = self
            .channel
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(command) = state.pop() {
            drop(state);
            self.channel.changed.notify_all();
            return Ok(command);
        }
        if state.sender_count == 0 {
            Err(mpsc::TryRecvError::Disconnected)
        } else {
            Err(mpsc::TryRecvError::Empty)
        }
    }

    fn recv(&self) -> Result<NodeRuntimeOp, mpsc::RecvError> {
        let mut state = self
            .channel
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            if let Some(command) = state.pop() {
                drop(state);
                self.channel.changed.notify_all();
                return Ok(command);
            }
            if state.sender_count == 0 {
                return Err(mpsc::RecvError);
            }
            state = self
                .channel
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<NodeRuntimeOp, mpsc::RecvTimeoutError> {
        let started = Instant::now();
        let mut state = self
            .channel
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            if let Some(command) = state.pop() {
                drop(state);
                self.channel.changed.notify_all();
                return Ok(command);
            }
            if state.sender_count == 0 {
                return Err(mpsc::RecvTimeoutError::Disconnected);
            }
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(mpsc::RecvTimeoutError::Timeout);
            }
            let (next, _) = self
                .channel
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state = next;
        }
    }
}

impl Drop for RuntimeCommandReceiver {
    fn drop(&mut self) {
        let mut state = self
            .channel
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.receiver_alive = false;
        state.first = None;
        state.overflow = None;
        drop(state);
        self.channel.changed.notify_all();
    }
}

fn runtime_command_channel() -> (RuntimeCommandSender, RuntimeCommandReceiver) {
    let channel = Arc::new(RuntimeCommandChannel {
        state: Mutex::new(RuntimeCommandQueue::new()),
        changed: std::sync::Condvar::new(),
    });
    (
        RuntimeCommandSender {
            channel: Arc::clone(&channel),
        },
        RuntimeCommandReceiver { channel },
    )
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
    step_replies: RuntimeReplyPool<Result<StepResult, RaftError>>,
    restart_count: u64,
    state: NodeRuntimeState,
}

impl NodeRuntime {
    pub fn create(options: NodeOptions) -> Result<Self, RaftError> {
        let node_id = options.node_id;
        let group_id = options.group_id;
        let (command_tx, command_rx) = runtime_command_channel();
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
            step_replies: RuntimeReplyPool::new(),
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
        let (command_tx, command_rx) = runtime_command_channel();
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
            step_replies: RuntimeReplyPool::new(),
            restart_count: 0,
            state: NodeRuntimeState::Created,
        })
    }

    pub fn start(&mut self) -> Result<(), RaftError> {
        self.send_unit(|reply| NodeRuntimeOp::Start(LifecycleReply::Single(reply)))?;
        self.state = NodeRuntimeState::Running;
        Ok(())
    }

    /// Sends one member of a bulk start without allocating a reply channel for
    /// the group. The indexed result returns through the batch's shared channel.
    fn start_in_batch(
        &mut self,
        index: usize,
        sender: LifecycleBatchSender,
    ) -> Result<(), RaftError> {
        self.sender()?
            .send(NodeRuntimeOp::Start(LifecycleReply::Batch {
                index,
                sender,
            }))
            .map_err(|err| {
                RaftError::Transport(format!(
                    "failed to send lifecycle command to raft node: {err}"
                ))
            })
    }

    fn finish_start_result(&mut self, result: LifecycleResult) -> Result<(), RaftError> {
        result?;
        self.state = NodeRuntimeState::Running;
        Ok(())
    }

    pub fn stop(&mut self) -> Result<(), RaftError> {
        self.send_unit(|reply| NodeRuntimeOp::Stop(LifecycleReply::Single(reply)))?;
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

    /// Shuts the group down: send, wait, join a thread if there is one, and
    /// leave the ticker and the pool.
    ///
    /// Delegates to the two halves rather than holding its own copy of them.
    /// It did hold a copy, and the copy was not covered by anything: when the
    /// drop path moved to `shutdown_in_flight` plus `finish_shutdown`, deleting
    /// the `release` from the one here left every test green, because nothing
    /// reached it any more.
    pub fn shutdown(&mut self) -> Result<(), RaftError> {
        match self.shutdown_in_flight()? {
            Some(reply_rx) => self.finish_shutdown(reply_rx),
            None => Ok(()),
        }
    }

    /// Sends one member of a bulk stop through the batch reply channel.
    fn stop_in_batch(
        &mut self,
        index: usize,
        sender: LifecycleBatchSender,
    ) -> Result<(), RaftError> {
        self.sender()?
            .send(NodeRuntimeOp::Stop(LifecycleReply::Batch {
                index,
                sender,
            }))
            .map_err(|err| {
                RaftError::Transport(format!(
                    "failed to send lifecycle command to raft node: {err}"
                ))
            })
    }

    fn finish_stop_result(&mut self, result: LifecycleResult) -> Result<(), RaftError> {
        result?;
        self.state = NodeRuntimeState::Stopped;
        Ok(())
    }

    /// Sends `Shutdown` and hands back the channel its reply will arrive on,
    /// without waiting for it. `None` when the group is already shut down.
    ///
    /// Dropping a server shuts its groups down one at a time, each waiting for
    /// its reply before the next is sent: measured at about 110us a group, 1.83s
    /// for 16,384 of them, the same shape and the same cost `start_all` had.
    /// Everything after the reply -- joining a thread, leaving the ticker and
    /// the pool -- stays in `finish_shutdown`, because only the waiting is worth
    /// overlapping.
    fn shutdown_in_flight(
        &mut self,
    ) -> Result<Option<mpsc::Receiver<Result<(), RaftError>>>, RaftError> {
        if self.state == NodeRuntimeState::Shutdown {
            return Ok(None);
        }
        let sender = self.command_tx.take().ok_or_else(|| {
            RaftError::InvalidRequest("raft node runtime channel is closed".to_string())
        })?;
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        sender
            .send(NodeRuntimeOp::Shutdown(LifecycleReply::Single(reply_tx)))
            .map_err(|err| RaftError::Transport(format!("failed to shutdown raft node: {err}")))?;
        Ok(Some(reply_rx))
    }

    /// Finishes a shutdown begun by `shutdown_in_flight`, doing everything
    /// `shutdown` does after its own wait.
    fn finish_shutdown(
        &mut self,
        reply_rx: mpsc::Receiver<Result<(), RaftError>>,
    ) -> Result<(), RaftError> {
        self.finish_shutdown_result(recv_runtime_reply(reply_rx)?)
    }

    fn shutdown_in_batch(
        &mut self,
        index: usize,
        sender: LifecycleBatchSender,
    ) -> Result<bool, RaftError> {
        if self.state == NodeRuntimeState::Shutdown {
            return Ok(false);
        }
        let command = self.command_tx.take().ok_or_else(|| {
            RaftError::InvalidRequest("raft node runtime channel is closed".to_string())
        })?;
        command
            .send(NodeRuntimeOp::Shutdown(LifecycleReply::Batch {
                index,
                sender,
            }))
            .map_err(|err| RaftError::Transport(format!("failed to shutdown raft node: {err}")))?;
        Ok(true)
    }

    fn finish_shutdown_result(&mut self, result: LifecycleResult) -> Result<(), RaftError> {
        if let Some(worker) = self.worker.take() {
            worker.join().map_err(|_| {
                RaftError::Transport("raft node worker panicked during shutdown".to_string())
            })?;
        }
        // A hosted group has no thread to join. It has to leave the ticker and
        // the pool instead, or the runtime goes on ticking a group that has
        // shut down.
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
        let sender = self.sender()?;
        let (reply_tx, reply_rx) = self.step_replies.take();
        if let Err(error) = sender.send(NodeRuntimeOp::Step(message, reply_tx.clone())) {
            let transport = RaftError::Transport(format!(
                "failed to send step to raft node: {error}"
            ));
            drop(error);
            self.step_replies.put(reply_tx, reply_rx);
            return Err(transport);
        }
        let result = recv_runtime_reply_ref(&reply_rx);
        if result.is_ok() {
            self.step_replies.put(reply_tx, reply_rx);
        }
        result?
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
        command: fn(mpsc::SyncSender<Result<(), RaftError>>) -> NodeRuntimeOp,
    ) -> Result<(), RaftError> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
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

/// Whether stepping `message` granted a **binding** vote, which is the one
/// thing a voter does that obliges it to hold its own election back.
///
/// Having given its vote away, a voter that then timed out would campaign
/// against the candidate it voted for. The vote is already spent, so the
/// competing election cannot win it back -- it only splits the round. Raft
/// resets the voter's election timer on granting for exactly this reason, and
/// the comparable engine does it on the same path.
///
/// A granted **pre**-vote does not count. It promises nothing and moves no
/// state, so the voter has given nothing away and has no reason to delay its
/// own election -- and a pre-vote that is about to be refused is often the
/// first sign the voter should be campaigning itself.
///
/// The request is what decides, not the result: a pre-vote arrives as
/// `Message::Vote` with `pre_vote` set and comes back as `StepResult::Vote`,
/// exactly like a binding vote, so the result variant alone cannot tell them
/// apart.
fn step_granted_a_binding_vote(
    message_was_a_binding_vote: bool,
    result: &Result<StepResult, RaftError>,
) -> bool {
    message_was_a_binding_vote
        && matches!(result, Ok(StepResult::Vote(response)) if response.vote_granted)
}

/// Whether `message` is a vote request that would bind the voter if granted.
fn is_binding_vote_step_message(message: &Message) -> bool {
    matches!(message, Message::Vote { request, .. } if !request.pre_vote)
}

/// Whether `message` is an append carrying entries a follower would have to
/// keep in order to honour the ack it is about to send.
///
/// Entry-bearing only: a heartbeat carries none, and persisting on every
/// heartbeat would write a record per interval per group for nothing.
fn is_entry_bearing_append_step_message(message: &Message) -> bool {
    matches!(
        message,
        Message::AppendEntries { request, .. } if !request.entries.is_empty()
    )
}

/// Whether the step accepted those entries, so the response is about to claim
/// them to the leader.
fn step_accepted_appended_entries(
    message_carried_entries: bool,
    result: &Result<StepResult, RaftError>,
) -> bool {
    message_carried_entries
        && matches!(result, Ok(StepResult::AppendEntries(response)) if response.success)
}

/// Whether `message` is the final chunk of a snapshot install.
///
/// Only the last chunk: earlier ones are buffered and change nothing durable,
/// and a record per 64 KiB chunk would be an fsync per chunk of every transfer.
fn is_completing_snapshot_step_message(message: &Message) -> bool {
    matches!(
        message,
        Message::InstallSnapshot { request, .. } if request.chunk.done
    )
}

/// Whether the step accepted that final chunk, so the receiver is about to
/// claim the snapshot's state to the leader.
///
/// A throttled response is the receiver asking for less rather than accepting,
/// so it claims nothing and needs no record.
fn step_installed_a_snapshot(
    message_completed_a_snapshot: bool,
    result: &Result<StepResult, RaftError>,
) -> bool {
    message_completed_a_snapshot
        && matches!(
            result,
            Ok(StepResult::InstallSnapshot(response))
                if response.accepted && !response.throttled
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
/// The term this node is in and the vote it has cast.
///
/// Compared across a step so that any change to it is persisted, rather than
/// enumerating the messages that can change it. A campaign is the case that
/// made this necessary: it votes for the candidate itself, which is as binding
/// as a vote granted to a peer, and it reaches the cluster through several
/// routes (an admin command, a `TimeoutNow`, the election tick) none of which
/// looked like a vote.
///
/// The term and the vote only -- **not** the committed index, though
/// `HardState` carries all three. The committed index moves on nearly every
/// proposal and append, so comparing it wrote a second record for work that had
/// already written one; `a_batch_of_proposals_costs_one_fsync` caught that. It
/// also does not need the record: a restart recovers the committed index from
/// the fence, and a leader re-establishes it regardless. The term and the vote
/// are the two a node cannot be told again.
fn local_term_and_vote(
    cluster: &RaftCluster,
    node_id: NodeId,
) -> Option<(Term, Option<NodeId>)> {
    cluster
        .nodes
        .get(&node_id)
        .map(|node| (node.hard_state.current_term, node.hard_state.voted_for))
}

/// Writes the record that backs a promise the node has just made, and fires
/// the fatal event if it does not land.
///
/// Used by the two membership paths, which can propagate the failure with `?`.
/// The append and snapshot paths do the same thing inline because they have to
/// turn the failure into the step's own result instead of returning early --
/// the ack is withheld rather than the call aborted. A node with no WAL
/// configured persists nothing, which is the existing behaviour for proposals.
fn persist_after_promise(
    cluster: &mut RaftCluster,
    wal: &mut Option<PersistentRaftWal>,
    node_id: NodeId,
) -> Result<(), RaftError> {
    let Some(open_wal) = wal.as_mut() else {
        return Ok(());
    };
    let appended = open_wal
        .append_built_without_report(|coverage| cluster.wal_record_for_coverage(node_id, coverage));
    after_durable_write(cluster, node_id, appended)
}

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
            if wal.as_ref().is_some_and(|_| persist_proposal) {
                // Built against what the WAL already holds, so a proposal does
                // not copy and hash the whole log to write one entry.
                let appended = wal
                    .as_mut()
                    .expect("checked just above")
                    .append_built_without_report(|coverage| {
                        cluster.wal_record_for_coverage(node_id, coverage)
                    });
                after_durable_write(cluster, node_id, appended)?;
            }
            Ok(StepResult::Proposed(log_id))
        }
        Message::Membership { operation } => {
            let report = membership_executor.execute(cluster, operation)?;
            // An applied membership change has to reach disk before the report
            // says it was applied. Nothing on this path wrote a record, so a
            // removed peer came back on restart and counted toward quorum
            // again: measured, `remove(3)` reported success with voters [1, 2]
            // and the restarted node was back to [1, 2, 3] with no record at
            // all. Recovery already prefers the record's membership over the
            // configured peers -- which is exactly why the record has to carry
            // the new one rather than a stale one.
            if report.success {
                persist_after_promise(cluster, wal, node_id)?;
            }
            Ok(StepResult::Membership(report))
        }
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

fn apply_unpersisted_proposal(
    cluster: &mut RaftCluster,
    wal: &mut Option<PersistentRaftWal>,
    membership_executor: &mut MembershipExecutor,
    node_id: NodeId,
    proposal: (Message, mpsc::Sender<Result<StepResult, RaftError>>),
) -> (
    Result<StepResult, RaftError>,
    mpsc::Sender<Result<StepResult, RaftError>>,
) {
    let (message, reply) = proposal;
    let result = runtime_step_message(
        cluster,
        wal,
        membership_executor,
        node_id,
        message,
        false,
    );
    (result, reply)
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
    last_wal_recovery_report: StoredWalRecoveryReport,
    state: NodeRuntimeState,
    heartbeat_interval_ms: u64,
    election_timeout_ms: u64,
    leader_lease_timeout_ms: u64,
    heartbeat_ticks: u64,
    election_ticks: u64,
    election_elapsed_ms: u64,
    /// The randomised timeout this node is waiting out, refreshed each time the
    /// election tick runs. Zero until the first tick, which is what a node that
    /// has not reached an election yet should report.
    effective_election_timeout_ms: u64,
    pre_vote_executions: u64,
    campaign_executions: u64,
    leader_transfer_executions: u64,
    /// Why the last tick happened, as one of a handful of literals.
    ///
    /// `&'static str` rather than `String` because it is only ever assigned a
    /// literal, and it is assigned on every tick of every group: as a `String`
    /// that was a heap allocation per tick, 819,200 a second at 8192 groups on a
    /// 10ms interval, to store a word the binary already contains. The one place
    /// that needs an owned copy is `RuntimeTimerStatus`, which is built when
    /// somebody asks for status rather than on a tick.
    last_tick_reason: &'static str,
    blocker_log: RuntimeBlockerLog,
    membership_executor: MembershipExecutor,
    /// A command drained by proposal coalescing that turned out not to be a
    /// proposal; processed first next time round, untouched. It is absent
    /// outside that brief handoff, so keep the large command out of every idle
    /// core.
    carried_command: Option<Box<NodeRuntimeOp>>,
    /// The group's mailbox.
    ///
    /// The core owns it rather than the thread, which is what lets a pool
    /// worker drain the group without one.
    command_rx: RuntimeCommandReceiver,
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
        command_rx: RuntimeCommandReceiver,
    ) -> Result<Self, (RaftError, RuntimeCommandReceiver)> {
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
    // One pass rather than two. `open` followed by `recover` scanned the segment
    // directory, chose the active segment and opened it for appending -- and then
    // did all three again, because `recover` repeated every step `open` had just
    // taken. Traced, creating one group opened its WAL file four times and scanned
    // the directory twice; creating groups is 99% system time, so that is the
    // cost, and at tens of thousands of groups it is most of a bring-up.
    //
    // A recovery failure is fatal here, where it used to be swallowed by an
    // `if let Ok(report)` that left the node running with nothing restored. That
    // is not a new risk being taken: every error `recover` could raise, the `open`
    // on the line above could raise from the same directory read and the same
    // append-open, and those were already fatal to creating a node. Failing is
    // also the safer of the two, since the alternative is a node that quietly did
    // not restore its last record.
    let (wal, last_wal_recovery_report) = match PersistentRaftWal::open_recovered(
        PersistentRaftWalOptions {
            dir: PathBuf::from(&options.wal_dir),
            // See `PersistentRaftWalOptions::new`: this is now just how much the
            // node holds in memory, and smaller is strictly cheaper.
            max_records_per_segment: 1_000,
            max_segment_bytes: options.config.max_segment_bytes,
            min_keep_segments: options.config.min_keep_segment_num as usize,
            fsync_on_append: true,
        },
    ) {
        Ok((wal, report)) => {
            if let Some(record) = report.recovered.clone() {
                let _ = cluster.restore_wal_record(record);
            }
            (Some(wal), StoredWalRecoveryReport::from_report(report))
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
    let heartbeat_ticks = 0;
    let election_ticks = 0;
    let election_elapsed_ms: u64 = 0;
    let effective_election_timeout_ms: u64 = 0;
    let pre_vote_executions = 0;
    let campaign_executions = 0;
    let leader_transfer_executions = 0_u64;
    let last_tick_reason = "runtime_created";
    let blocker_log = RuntimeBlockerLog::default();
    let membership_executor = MembershipExecutor::new();
    // A command drained by proposal coalescing that turned out not to be a
    // proposal; processed first on the next iteration, untouched.
    let carried_command: Option<Box<NodeRuntimeOp>> = None;
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
            heartbeat_ticks,
            election_ticks,
            election_elapsed_ms,
            effective_election_timeout_ms,
            pre_vote_executions,
            campaign_executions,
            leader_transfer_executions,
            last_tick_reason,
            blocker_log,
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
            effective_election_timeout_ms,
            pre_vote_executions,
            campaign_executions,
            leader_transfer_executions,
            last_tick_reason,
            blocker_log,
            wal,
            ..
        } = self;
        let node_id = *node_id;
        let heartbeat_interval_ms = *heartbeat_interval_ms;
        let election_timeout_ms = *election_timeout_ms;
        if let Some(wal) = wal.as_mut() {
            wal.release_idle_segment_descriptor();
        }
                if *state == NodeRuntimeState::Running {
                    *heartbeat_ticks += 1;
                    *election_elapsed_ms = election_elapsed_ms.saturating_add(heartbeat_interval_ms);
                    let _ = cluster.tick_leader_lease(heartbeat_interval_ms);
                    cluster.tick_follower_lease(heartbeat_interval_ms);
                    let _ = cluster.mark_peer_active(node_id);
                    if cluster.leader_id() == Some(node_id) {
                        for peer_id in cluster.tick_peer_liveness(heartbeat_interval_ms) {
                            blocker_log.record(format!("peer_offline_timeout:{peer_id}"), false);
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
                        blocker_log.record(format!("broadcast_heartbeat:{error}"), false);
                    }
                    if received_live_leader_heartbeat {
                        *election_elapsed_ms = 0;
                    }
                    // The reorder queues age by the interval that just
                    // passed. The millisecond-to-microsecond conversion is the
                    // cluster's, where a test can reach it.
                    let _ = cluster
                        .tick_reorder_queues_for_heartbeat(heartbeat_interval_ms);
                    if !cluster.leader_lease_valid && cluster.step_down_leader_if_lost_quorum() {
                        blocker_log.record("lost_quorum_step_down".to_string(), false);
                    }
                    *last_tick_reason = "heartbeat_tick";
                    // The default policy for the decision
                    // `snapshot_threshold_entries` was declared for. A host
                    // wanting a different one reads `snapshot_due()` itself
                    // and ignores this; a host wanting none sets
                    // `can_trigger_snapshot` to false, which is what the
                    // report consults first.
                    if cluster.snapshot_is_due() {
                        match cluster.trigger_snapshot() {
                            Ok(_) => *last_tick_reason = "snapshot_due_tick",
                            Err(error) => blocker_log.record(format!("snapshot_due:{error}"), false),
                        }
                    }
                    if cluster.tick_snapshot_trigger() {
                        let snapshot_id = cluster
                            .snapshot_trigger_status()
                            .snapshot_id
                            .unwrap_or_else(|| "unknown".to_string());
                        blocker_log.record(format!("snapshot_trigger_timeout:{snapshot_id}"), false);
                    }
                    cluster.catch_up_lagging_peers(|peer_id, error| {
                            blocker_log.record(format!("peer_catchup:{peer_id}:{error}"), false);
                    });
                    if let Err(error) = cluster.broadcast_commit_index_to_old_paused_peers() {
                        blocker_log.record(format!("old_paused_commit_broadcast:{error}"), false);
                    }
                    if cluster.leader_transfer_state().is_some() {
                        match cluster.try_complete_leader_transfer() {
                            Ok(true) => {
                                *leader_transfer_executions =
                                    leader_transfer_executions.saturating_add(1);
                            }
                            Ok(false) => {
                                if cluster.tick_leader_transfer() {
                                    blocker_log.record("leader_transfer_timeout".to_string(), false);
                                }
                            }
                            Err(error) => blocker_log.record(format!("leader_transfer:{error}"), false),
                        }
                    }
                    // Randomised per node and per term, so nodes sharing a
                    // config do not campaign on the same tick for ever. Kept in
                    // runtime state rather than computed here, so the value the
                    // status reports is the value this compares against:
                    // computing it twice would let the two drift and leave the
                    // comparison untested, which is exactly what planting found
                    // the first time this was written. See
                    // `matrixraft_randomised_election_timeout_ms`.
                    *effective_election_timeout_ms =
                        matrixraft_randomised_election_timeout_ms(
                            election_timeout_ms,
                            node_id,
                            cluster.current_term,
                        );
                    if *election_elapsed_ms >= *effective_election_timeout_ms {
                        *election_ticks += 1;
                        *election_elapsed_ms = 0;
                        *last_tick_reason = "election_tick";
                        let local_replica_role =
                            cluster.nodes.get(&node_id).map(|node| node.replica_role);
                        let lease_expired = !cluster.is_follower_lease_valid();
                        // A leader holds no FOLLOWER lease, so `lease_expired` is
                        // true for one every cycle and this branch used to run on
                        // the sitting leader: a `pre_vote` that grants itself, a
                        // `campaign` that returns `Ok(())` at its own
                        // already-leader check, and a counter that recorded it.
                        // Measured at 920 campaigns in four seconds on one idle
                        // group, none of which changed anything -- the term does
                        // not move, because `campaign` refuses before touching it.
                        //
                        // Losing quorum is handled above by
                        // `step_down_leader_if_lost_quorum`, so there is no case
                        // where a node that is still leader needs to campaign.
                        let already_leading = cluster.leader_id() == Some(node_id)
                            && cluster
                                .nodes
                                .get(&node_id)
                                .map(|node| node.raft_role == StateRole::Leader)
                                .unwrap_or(false);
                        let local_can_campaign = !already_leading
                            && local_replica_role.is_some_and(ReplicaRole::can_be_leader)
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
                                        blocker_log,
                                        false,
                                    );
                                    // The campaign voted for this node in the
                                    // new term. There is no caller to withhold
                                    // an answer from here, so a failed record
                                    // is recorded as a fatal blocker instead --
                                    // `persist_after_promise` has already
                                    // fired the fatal event.
                                    if result.is_ok() {
                                        if let Err(error) =
                                            persist_after_promise(cluster, wal, node_id)
                                        {
                                            blocker_log.record(
                                                format!("election_tick_campaign_persist:{error}"),
                                                true,
                                            );
                                        }
                                    }
                                }
                                Ok(vote) => blocker_log.record(format!("pre_vote:{}", vote.reason), false),
                                Err(error) => blocker_log.record(format!("pre_vote:{error}"), false),
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
            Some(command) => Some(*command),
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
            effective_election_timeout_ms,
            pre_vote_executions,
            campaign_executions,
            leader_transfer_executions,
            last_tick_reason,
            blocker_log,
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
                let first = (message, reply);
                let second = match command_rx.try_recv() {
                    Ok(NodeRuntimeOp::Step(next @ Message::Propose { .. }, next_reply)) => {
                        Some((next, next_reply))
                    }
                    Ok(other) => {
                        *carried_command = Some(Box::new(other));
                        None
                    }
                    Err(_) => None,
                };
                if let Some(second) = second {
                    let mut outcomes = Vec::with_capacity(4);
                    let mut any_applied = false;
                    let outcome = apply_unpersisted_proposal(
                        cluster,
                        wal,
                        membership_executor,
                        node_id,
                        first,
                    );
                    any_applied |= outcome.0.is_ok();
                    outcomes.push(outcome);
                    let outcome = apply_unpersisted_proposal(
                        cluster,
                        wal,
                        membership_executor,
                        node_id,
                        second,
                    );
                    any_applied |= outcome.0.is_ok();
                    outcomes.push(outcome);
                    // Bounded so a saturated queue cannot starve ticks and other
                    // commands indefinitely.
                    while outcomes.len() < 128 {
                        match command_rx.try_recv() {
                            Ok(NodeRuntimeOp::Step(
                                next @ Message::Propose { .. },
                                next_reply,
                            )) => {
                                let outcome = apply_unpersisted_proposal(
                                    cluster,
                                    wal,
                                    membership_executor,
                                    node_id,
                                    (next, next_reply),
                                );
                                any_applied |= outcome.0.is_ok();
                                outcomes.push(outcome);
                            }
                            Ok(other) => {
                                *carried_command = Some(Box::new(other));
                                break;
                            }
                            Err(_) => break,
                        }
                    }
                    // One record covers every proposal applied above, exactly
                    // as step_batch persists. Nothing applied means nothing to
                    // persist.
                    let persisted: Result<(), RaftError> = if any_applied {
                        match wal.as_mut() {
                            Some(wal) => wal.append_built_without_report(|coverage| {
                                    cluster.wal_record_for_coverage(node_id, coverage)
                                }),
                            None => Ok(()),
                        }
                    } else {
                        Ok(())
                    };
                    for (result, reply) in outcomes {
                        // A proposal that applied but did not persist is not
                        // durable, and must not be acknowledged as if it were.
                        let result = match (&persisted, result) {
                            (Err(error), Ok(_)) => Err(error.clone()),
                            (_, result) => result,
                        };
                        let _ = reply.send(record_runtime_result(
                            "propose",
                            result,
                            blocker_log,
                            true,
                        ));
                    }
                    return ControlFlow::Continue(());
                } else {
                    let (message, reply) = first;
                    NodeRuntimeOp::Step(message, reply)
                }
            }
            other => other,
        };
        match command {
            NodeRuntimeOp::Start(reply) => {
                // Deliberately NOT persisted, though starting does elect and so
                // does vote for this node. The start election is
                // deterministic: it always votes for this node at the recovered
                // term plus one, so a restart re-derives exactly the same vote
                // and will refuse any other candidate in that term. Nothing is
                // forgotten, so nothing has to be written.
                //
                // The cost of writing it anyway would not be small. A durable
                // append is its fsync and very little else -- 224 a second
                // against 85,014 with fsync off -- and this is once per group,
                // against a documented mass start of 1.83s for 16,384 groups.
                //
                // The terms that DO have to be written are the ones a restart
                // cannot re-derive: a campaign from the election tick, or an
                // admin command, can walk the term several steps past what the
                // record holds, and a node that came back at `recovered + 1`
                // would grant terms it had already voted in. Those are
                // persisted where they happen.
                let result = cluster.start();
                if result.is_ok() {
                    *state = NodeRuntimeState::Running;
                    *election_elapsed_ms = 0;
                }
                reply.send(record_runtime_result(
                    "start",
                    result,
                    blocker_log,
                    false,
                ));
            }
            NodeRuntimeOp::Stop(reply) => {
                let result = cluster.stop();
                if result.is_ok() {
                    *state = NodeRuntimeState::Stopped;
                }
                reply.send(record_runtime_result(
                    "stop",
                    result,
                    blocker_log,
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
                    wal_recovery_report: Some(last_wal_recovery_report.to_report()),
                    snapshot_trigger_status: cluster.snapshot_trigger_status(),
                    timer_status: RuntimeTimerStatus {
                        heartbeat_interval_ms,
                        election_timeout_ms,
                        effective_election_timeout_ms: *effective_election_timeout_ms,
                        leader_lease_timeout_ms,
                        leader_lease_elapsed_ms: cluster.leader_lease_elapsed_ms,
                        leader_lease_valid: cluster.leader_lease_valid,
                        heartbeat_ticks: *heartbeat_ticks,
                        election_ticks: *election_ticks,
                        pre_vote_executions: *pre_vote_executions,
                        campaign_executions: *campaign_executions,
                        leader_transfer_executions: *leader_transfer_executions,
                        last_tick_reason: last_tick_reason.to_string(),
                    },
                    peer_runtime: raft_peer_runtime_states(
                        cluster,
                        *election_elapsed_ms,
                        *heartbeat_ticks > 0,
                        *pre_vote_executions > 0,
                    ),
                    fatal_blocker_report: matrixraft_fatal_blocker_report_with_totals(
                        "raft_node_runtime",
                        blocker_log.blockers().to_vec(),
                        blocker_log.fatal_blockers().to_vec(),
                        blocker_log.blockers_seen(),
                        blocker_log.fatal_blockers_seen(),
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
                let _ = reply.send(Ok(Some(last_wal_recovery_report.to_report())));
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
                    blocker_log.record(blocker, true);
                }
                let fatal_on_step_error = !matches!(
                    &message,
                    Message::InstallSnapshot { .. }
                        | Message::ReadIndex { .. }
                        | Message::Membership { .. }
                );
                let binding_vote = is_binding_vote_step_message(&message);
                let append_carries_entries = is_entry_bearing_append_step_message(&message);
                let snapshot_completes_install = is_completing_snapshot_step_message(&message);
                let term_and_vote_before = local_term_and_vote(cluster, node_id);
                let mut result = runtime_step_message(
                    cluster,
                    wal,
                    membership_executor,
                    node_id,
                    message,
                    true,
                );
                if step_granted_a_binding_vote(binding_vote, &result) {
                    *election_elapsed_ms = 0;
                }
                // One record for whatever this step promised, so a step that
                // both moves the hard state and acknowledges entries writes
                // once rather than twice.
                //
                // Three things are promises here. A granted vote: a voter that
                // granted and restarted came back with whatever vote the last
                // proposal happened to carry, and granted the same term again
                // to a different candidate -- two leaders in one term, from a
                // node that did nothing wrong. An accepted append: the leader
                // counts the ack toward the commit quorum, so a quorum of
                // followers that acked out of memory and restarted loses a
                // committed entry. An accepted snapshot: the leader may stop
                // sending, and compact, everything below it.
                //
                // The hard state is compared rather than enumerated because a
                // CAMPAIGN is also a vote -- for this node itself -- and it
                // arrives by routes that do not look like one. Measured: a node
                // that campaigned, led, and restarted recovered no record at
                // all.
                //
                // Whatever the step was, it is withheld when the record does
                // not land: a peer counting on something the node cannot honour
                // after a restart is the whole defect.
                if local_term_and_vote(cluster, node_id) != term_and_vote_before
                    || step_accepted_appended_entries(append_carries_entries, &result)
                    || step_installed_a_snapshot(snapshot_completes_install, &result)
                {
                    if let Some(open_wal) = wal.as_mut() {
                        let appended = open_wal.append_built_without_report(|coverage| {
                            cluster.wal_record_for_coverage(node_id, coverage)
                        });
                        if let Err(error) = after_durable_write(cluster, node_id, appended) {
                            result = Err(error);
                        }
                    }
                }
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
                    blocker_log,
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
                // Raised inside the closure so each result is matched against
                // the message that produced it. Pairing them afterwards would
                // need the messages kept alongside the results, and asking
                // only whether the batch held a binding vote *and* granted
                // some vote would reset the timer for a batch that granted a
                // pre-vote while refusing the binding one.
                let mut granted_a_binding_vote = false;
                let mut accepted_appended_entries = false;
                let mut installed_a_snapshot = false;
                let term_and_vote_before = local_term_and_vote(cluster, node_id);
                let stepped: Result<Vec<StepResult>, RaftError> = messages
                    .into_iter()
                    .map(|message| {
                        let binding_vote = is_binding_vote_step_message(&message);
                        let carries_entries = is_entry_bearing_append_step_message(&message);
                        let completes_snapshot = is_completing_snapshot_step_message(&message);
                        let result = runtime_step_message(
                            cluster,
                            wal,
                            membership_executor,
                            node_id,
                            message,
                            false,
                        );
                        granted_a_binding_vote |= step_granted_a_binding_vote(binding_vote, &result);
                        accepted_appended_entries |=
                            step_accepted_appended_entries(carries_entries, &result);
                        installed_a_snapshot |=
                            step_installed_a_snapshot(completes_snapshot, &result);
                        result
                    })
                    .collect();
                if granted_a_binding_vote {
                    *election_elapsed_ms = 0;
                }
                // Persisted even when a message failed, so what is on disk
                // still describes what the node applied.
                //
                // A granted binding vote needs the record as much as a
                // proposal does, and for a sharper reason: an unpersisted vote
                // lets the voter grant the same term twice across a restart.
                let persisted = if batch_has_proposal
                    || granted_a_binding_vote
                    || accepted_appended_entries
                    || installed_a_snapshot
                    || local_term_and_vote(cluster, node_id) != term_and_vote_before
                {
                    match wal.as_mut() {
                        Some(wal) => wal.append_built_without_report(|coverage| {
                                cluster.wal_record_for_coverage(node_id, coverage)
                            }),
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
                    blocker_log,
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
                    blocker_log,
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
                    blocker_log,
                    false,
                ));
            }
            NodeRuntimeOp::MembershipWorkflowWithRollback(operations, reply) => {
                let mut outcome =
                    membership_executor.execute_all_with_rollback(cluster, operations);
                // One record for the whole workflow: a record describes the
                // membership as it now stands, so there is nothing per-operation
                // to write. Persisted when any operation applied, including a
                // workflow that rolled back -- the rollback is itself a change
                // the record has to carry.
                let applied_any = outcome
                    .as_ref()
                    .map(|reports| reports.iter().any(|report| report.success))
                    .unwrap_or(false);
                if applied_any {
                    if let Err(error) = persist_after_promise(cluster, wal, node_id) {
                        outcome = Err(error);
                    }
                }
                let _ = reply.send(record_runtime_result(
                    "membership_workflow_with_rollback",
                    outcome,
                    blocker_log,
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
                    blocker_log,
                    false,
                ));
            }
            NodeRuntimeOp::PeerPipelineStatus(peer_id, reply) => {
                let _ = reply.send(record_runtime_result(
                    "peer_pipeline_status",
                    cluster.peer_pipeline_status(peer_id),
                    blocker_log,
                    false,
                ));
            }
            NodeRuntimeOp::PeerPipelineStatuses(reply) => {
                let _ = reply.send(record_runtime_result(
                    "peer_pipeline_statuses",
                    Ok(cluster.peer_pipeline_statuses()),
                    blocker_log,
                    false,
                ));
            }
            NodeRuntimeOp::IsBusy(reply) => {
                let _ = reply.send(record_runtime_result(
                    "is_busy",
                    Ok(cluster.is_busy()),
                    blocker_log,
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
                reply.send(record_runtime_result(
                    "shutdown",
                    result,
                    blocker_log,
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
fn drain_with_error(command_rx: &RuntimeCommandReceiver, error: &RaftError) {
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
    /// Ticks the schedule owed a group and did not deliver, over every shard.
    ///
    /// Climbing means the tickers cannot hold the configured interval, and it
    /// is not what `ticks_handed_over` shows. The pool is not involved: an
    /// in-place tick runs the raft work on the ticker thread, and a ticker that
    /// is merely slow never hands anything over. Measured, 1024 groups at a 1ms
    /// interval:
    ///
    /// | shards | ticks delivered | cores | handed over |
    /// |---|---|---|---|
    /// | 4 | 41.5% / 46.1% | 1.97 / 2.02 | 0.0% |
    /// | 8 | 54.3% / 61.0% | 2.87 / 2.78 | 0.0% |
    /// | 16 | 71.5% / 71.9% | 3.67 / 3.62 | 0.0% |
    /// | 32 | 77.8% / 74.7% | 5.07 / 5.08 | 0.0% |
    ///
    /// Two figures per row: the shard counts were swept up and then back down,
    /// so a box drifting under other load shows as the two passes disagreeing
    /// rather than as a trend in the shard count.
    ///
    /// Shards are the tick capacity; the worker pool is not, and sat idle
    /// through all eight runs.
    ///
    /// Handing the late ticks to that idle pool is the obvious next thought,
    /// and it is worse. Measured with a `fire_late_tick` that posted a past-due
    /// group's tick to the pool instead of running it on the ticker -- the
    /// hand-over rate went to 94%, so the change certainly took effect:
    ///
    /// | | ticks delivered | cores | switches/sec |
    /// |---|---|---|---|
    /// | 1024 groups, 1ms, 4 shards | 46.2% -> 44.3% | 2.02 -> 5.26 | 1,892 -> 13,030 |
    /// | 1024 groups, 1ms, 8 shards | 59.7% -> 37.0% | 2.89 -> 7.61 | 4,816 -> 65,231 |
    /// | 4096 groups, 10ms, 4 shards | 100.1% -> 99.8% | 2.02 -> 2.19 | 1,899 -> 1,830 |
    ///
    /// Two passes each way. Two and a half times the CPU and seven times the
    /// context switches to deliver no more ticks, and *fewer* of them at eight
    /// shards. A pool worker is not spare capacity -- it is the same core, and
    /// a mail round trip costs more than the tick it carries. The third row is
    /// the control: at a 10ms interval almost nothing is late, so almost
    /// nothing changed, which is what says the other two rows are the effect
    /// and not a broken arm. Reverted rather than shipped. Past sixteen shards on a sixteen-core box the
    /// return flattens while the cost keeps climbing, which is the shape of a
    /// box running out of cores rather than of a limit in here. Heartbeats, leases and election timeouts are counted
    /// in ticks, so a shortfall here lengthens all three.
    pub ticks_skipped: u64,
    /// Ticks the schedule produced while ticking was switched off, over every
    /// shard.
    ///
    /// Zero unless a host has called [`SharedGroupRuntime::disable_ticks`].
    /// Distinct from `ticks_skipped`, which means owed and late: these were due
    /// on time and withheld on purpose, and counting them together would make
    /// a maintenance window read as a runtime falling behind.
    pub ticks_suppressed: u64,
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
/// There used to be a wall at 1024 live groups per shard, where groups
/// stopped electing and stopped answering. That was not a capacity limit at
/// all: `MailChannel::overflow` compared a selector-wide mail count against
/// one channel's limit, so a worker holding more groups than
/// `max_queue_depth` latched their mailboxes shut. Fixed, and 4096 groups
/// over 4 shards now deliver every tick where 3 of them used to elect.
///
/// # What it costs to bring a store up, and where it stops keeping up
///
/// Measured on a 16-core box at a 10ms interval, one shard per 512 groups, every
/// group a live single voter, **release build**, three repeats:
///
/// | groups | shards | cores | ticks delivered | creating | starting | tearing down |
/// |---|---|---|---|---|---|---|
/// | 1024 | 4 | 0.18-0.23 | ~100% | 0.26s | 0.02s | 0.02s |
/// | 4096 | 8 | 0.77 | 100.0% | 0.56-0.66s | 0.07s | 0.08-0.10s |
/// | 16384 | 32 | 3.4-4.2 | ~100% | 2.4-3.5s | 0.49-0.56s | 0.48-0.56s |
///
/// **The profile matters, and it was missed the first time this table was
/// written.** Every figure here was first measured on a debug build and published
/// without saying so. For the same rows debug reads 0.61, 2.81 and 13.68 cores,
/// and at 16,384 groups it delivers only 79% of the ticks -- which this note then
/// reported as sixteen thousand groups not fitting on sixteen cores. In release
/// it fits with four fifths of the box to spare. What does *not* move with the
/// profile is memory per group, descriptors per group, and the lifecycle timings
/// below: those are bound by io and by round trips rather than by generated code.
///
/// **Creating the groups is the larger half of starting up.** About 0.16ms a
/// group, and it does not parallelise: each group opens its own WAL and writes a
/// first segment, in sequence. It used to fsync that empty segment as well, which
/// cost 2ms a group -- 35s for 16,384 of them -- until that was removed.
/// `start_all` and teardown were each a serial round trip per group until they
/// were pipelined.
///
/// **Sixteen thousand groups at a 10ms interval is served in full on sixteen
/// cores**, at three to four of them. Where a box does run out of cores the
/// shortfall shows up in `ticks_skipped` and in the `kept up` column of
/// `examples/idle_tick_cost.rs`.
///
/// # How much memory a group costs
///
/// A live group's command channel used to retain an empty 32-command block
/// after its first command. `start_all` necessarily sent that first command to
/// every group, so the block cost about 6 KiB per group for the rest of the
/// process lifetime. The runtime first admitted four waiting commands and
/// applied backpressure to additional callers until a worker took one. A
/// 512-group allocation profile fell from 11.66 MiB to 8.96 MiB at steady
/// state, a 5.16 KiB saving per group. The queue still lets `start_all` put one
/// command on every group before it waits for replies, and the shared runtime
/// posts the same worker wake after each admitted command.
///
/// The next largest allocation was the `BTreeMap` leaf holding replication
/// pipelines inline. A leaf reserves several value slots even when the group
/// has one peer, and `ReplicationPipeline` is deliberately rich state. Boxing
/// only the map values keeps its ordering and wire shape while the same
/// 512-group Massif run falls from a 9.74 MiB peak to 6.28 MiB, another 6.9 KiB
/// per group.
///
/// `Node` was the next inline map value with the same shape. Boxing those values
/// moves the 512-group peak again, from 6.28 MiB to 5.18 MiB, another 2.19 KiB
/// per group, while preserving the node map's wire shape.
///
/// A single-peer directory also leaves most of a `BTreeMap` leaf empty. Boxing
/// its values moves the peak from 5.18 MiB to 4.90 MiB, another 569 bytes per
/// group, without changing membership or address behavior.
///
/// Two waiting operations are also enough for automatic proposal coalescing:
/// the concurrent proposal contract still folds 200 callers into fewer than
/// 100 fsyncs. Tightening the bound from four to two moves the current
/// 512-group peak from 4.90 MiB to 4.72 MiB, another 367 bytes per group.
///
/// Batched lifecycle operations used one unbounded reply channel per group.
/// Sending its single result allocated a 32-message block that stayed live
/// until the batch was joined. A one-slot channel has the same one-result
/// contract without that block: the 512-group peak moves from 4.71 MiB to 4.20
/// MiB, another 1.03 KiB per group.
///
/// Two cold fields then dominated `NodeCore`: the 488-byte startup recovery
/// report, including an identical owned checksum description, and the 192-byte
/// command carried only when proposal coalescing drains a non-proposal. Sparse
/// recovery storage and a boxed carry-over command narrow the hosted-group
/// allocation from 1,624 to 1,016 bytes. The 512-group peak moves from 4.20 MiB
/// to 3.56 MiB, a 1.28 KiB saving per group including the checksum strings.
///
/// A fresh WAL's segment vector then reserved four 64-byte slots for its one
/// segment. Sizing it from the directory scan moves the 512-group peak from
/// 3.56 MiB to 3.45 MiB, another 212 bytes per group including allocator
/// overhead.
///
/// The standard bounded channel retained a 640-byte control allocation and a
/// 400-byte slot allocation for each group. A fixed two-slot queue behind one
/// lock and condition variable retains one 448-byte allocation and keeps the
/// same blocking and disconnect behavior. The 512-group peak moves from 3.45
/// MiB to 3.12 MiB, another 678 bytes per group including allocator overhead.
///
/// The multi-group server then held every route key in a second tree just to
/// find its runtime wiring. Keeping the optional wiring beside the node and
/// boxing its large payload removes that duplicate index. The 512-group peak
/// moves from 3.12 MiB to 3.04 MiB, another 171 bytes per group including
/// allocator overhead.
///
/// The shared runtime also retained every pooled group in its own tree, even
/// though the worker pool and ticker already own the group and independently
/// reject duplicate routes. Making those registrations authoritative and
/// retaining only an exact count removes the extra tree. The 512-group peak
/// moves from 3.04 MiB to 3.01 MiB, another 46 bytes per group including
/// allocator overhead.
///
/// Three runtime histories and the membership-index set were still ordinary
/// empty collections in every group. Keeping their public sequence and set
/// shapes while allocating storage only on first use narrows `NodeCore` from
/// 976 to 912 bytes. The 512-group peak moves from 3.01 MiB to 2.98 MiB,
/// another 64 bytes per group including allocator overhead.
///
/// Bulk start, stop, and shutdown still allocated one reply channel per group.
/// Indexed results now share one bounded channel per 1,024-group batch while
/// preserving out-of-order completion and per-group state updates. The same
/// 512-group profile moves from 2.98 MiB to 2.62 MiB, another 383,072 bytes in
/// total or about 748 bytes per group including allocator overhead.
///
/// A solo-voter group then retained a full tree node to index its one boxed
/// replication pipeline. A sorted compact directory keeps the same ordered map
/// wire shape and point access while retaining exactly one entry per peer. The
/// 512-group peak moves from 2,745,064 to 2,653,896 bytes, another 91,168 bytes
/// in total or about 178 bytes per group including allocator overhead.
///
/// The compatibility node also retained a full tree node for its private peer
/// address directory, duplicating neither consensus state nor ownership but
/// still reserving slots around a solo peer. Reusing the sorted compact
/// directory removes that allocation. Two 512-group profiles peak at 2,593,304
/// and 2,602,744 bytes; the conservative move from 2,653,896 is another 51,152
/// bytes in total or about 100 bytes per group including allocator overhead.
///
/// The consensus node directory retained the next one-entry tree. Keeping its
/// boxed values in the same sorted compact directory preserves ordered range
/// walks, membership mutation, indexing, and its serialized map shape while
/// removing the reserved tree slots. Two 512-group profiles peak at 2,519,048
/// and 2,521,192 bytes; compared with the lower preceding peak, the conservative
/// saving is another 72,112 bytes in total or about 141 bytes per group including
/// allocator overhead.
///
/// The multi-group server's live route index was then the largest remaining
/// tree allocation in the profile. Storing the same ordered keys and node
/// handles in the compact directory moves two 512-group peaks to 2,427,584 and
/// 2,427,648 bytes. Compared with the lower preceding peak, the conservative
/// saving is another 91,400 bytes in total or about 179 bytes per group including
/// allocator overhead.
///
/// The worker pool's handler directory is keyed by channel ids allocated in
/// increasing order, so the compact directory appends on registration without
/// making arbitrary group-id order expensive. Two controlled 512-group pairs
/// under the same RAM-backed temporary root move from 2,460,336 to 2,446,984
/// bytes and from 2,460,000 to 2,442,264 bytes. Comparing the least favorable
/// endpoints still saves 13,016 bytes in total, about 25 bytes per group
/// including allocator overhead. At 32,768 groups the dedicated driver probe
/// retains 533-535 bytes per group instead of 560-563, while three descending
/// registration runs finish in 27-40ms and deliver mail to every group.
///
/// The native measurement with all eighteen changes is 41 MiB at
/// whole-MiB resolution for 8,192 live groups, or **4 KiB per group**; the
/// allocation profiles above resolve reductions that RSS rounding does not.
/// The handler-directory reduction is below that whole-MiB resolution. The
/// process is down from 43 MiB before compacting the server route directory,
/// 44 MiB before compacting the consensus node directory, 45 MiB before
/// compacting the compatibility peer directory, 46 MiB before compacting the
/// pipeline directory, 47 MiB before sharing the lifecycle replies, 48 MiB
/// before removing the extra ownership tree, 49 MiB before co-locating runtime
/// wiring, 54 MiB after compacting WAL segment storage, 56 MiB before that,
/// 66 MiB after compacting the lifecycle replies, 68 MiB after tightening the
/// queue, 70 MiB after boxing peer values, 74 MiB and 9 KiB after boxing node
/// values, 94 MiB and 11 KiB after boxing pipeline values, 126 MiB and 15 KiB
/// after first bounding the queues, and 171 MiB and 21 KiB before any of them,
/// under the same options. The original 21 KiB cost was flat as the group count
/// grew: 89 MiB for 4096 groups and 339 MiB for 16,384, both with the measuring
/// binary's own resident size taken off. The same in debug and in release,
/// within a kilobyte a group -- an allocation is the size it is whichever
/// profile asked for it.
///
/// The 16,384-group row was 341 MiB before seven transient maps moved to lazy
/// storage. Their empty representation fell from 24 bytes to 8 bytes each, so
/// the inline cluster state is 112 bytes narrower and the measured process is
/// 2 MiB smaller at this count.
///
/// Measure it with `examples/idle_tick_cost.rs` under `arm=live`, which runs one
/// row in one process. Every other arm setting builds and drops other stores
/// first, and a dropped store's pages return to the allocator rather than to
/// the kernel, so the figure reads as the high-water mark of the run instead of
/// the cost of the store being measured.
///
/// # What a tick costs, and what the group count has to do with it
///
/// The note here used to say the cost of a delivered tick grows as groups
/// multiply -- 6.1us at 1024, 6.9us at 4096, 10.4us at 16,384 -- and that a
/// 342 MiB working set past every cache was consistent with it. **It was a
/// confound, and separating it refutes that reading.** Those three runs changed
/// the group count and the total tick rate together, because the interval was
/// the same in all of them.
///
/// Holding the group count at 16,384, so the working set stays at 341 MiB, and
/// lowering the rate instead -- **release build**, two repeats:
///
/// | interval | delivered ticks/sec | cores |
/// |---|---|---|
/// | 100ms | 164,004 | 0.68 / 0.92 |
/// | 50ms | 327,680 | 1.41 / 1.12 |
/// | 25ms | 655,360 | 2.00 / 2.27 |
/// | 10ms | ~1,638,000 | 5.12 / 4.83 |
///
/// The marginal cost of a tick, taken between two rates at one group count so
/// the fixed cost cancels, is **2.7 to 3.0us**: 2.70 and 2.75us over 100ms to
/// 25ms, 3.02 and 2.65us over 100ms to 10ms. The same arithmetic on a debug build
/// gives 5.3 to 6.6us, roughly double, and that is what this note reported first
/// without saying which profile it had measured.
///
/// The group count drops out of it either way. Debug figures, over a sixteen-fold
/// range of group counts: 5.40us at 1024 groups, 5.34us at 4096, 5.95us at
/// 16,384.
///
/// Debug had one row that did not fit that -- 12.96us between 25ms and 10ms at
/// 16,384 groups -- and it was worth understanding rather than averaging away.
/// The memory is identical in both of those rows and the box went from a third
/// loaded to 13.9 of its 16 cores, so it was saturation and not the working set.
/// In release the row does not appear at all: 10ms at 16,384 groups is served in
/// full.
///
/// There is also a cost that is not per tick, and a correction to make about it.
/// 33 threads with almost nothing to do -- 64 groups on a one-second interval --
/// used to burn 0.196 cores and 14,500 context switches a second doing nothing
/// but waking up. This note called that a fixed cost and put it in a sizing
/// formula as a constant. **It was the cost of an idle driver, not a constant
/// added to every configuration.**
///
/// The tickers now wait until their earliest deadline rather than waking every
/// millisecond, and the measurement shows where that mattered and where it did
/// not:
///
/// | | cores before | after | switches/sec before | after |
/// |---|---|---|---|---|
/// | 64 groups, 1000ms | 0.188 | 0.012 | 14,550 | 402 |
/// | 4096 groups, 100ms | 0.241 | 0.237 | ~7,000 | ~7,000 |
/// | 16384 groups, 10ms | 3.6-4.5 | 3.8-3.9 | noisy | noisy |
///
/// Fifteen times cheaper when the deadlines are sparse, and unchanged when they
/// are dense -- which is the useful half of the result. 4096 groups over 8 shards
/// on a 100ms interval is 512 groups a shard, so something is due about every
/// fifth of a millisecond and the ticker was never idle to begin with. A store
/// with many groups was not paying the 0.196.
///
/// So, for sizing a release build: **about 3us per tick**, groups times
/// 1000/`tick_interval_ms` ticks a second, plus a small amount for the threads
/// themselves. The 0.2 is not added on top for a busy driver; it is what an idle
/// one used to cost and now does not. This is a fit over the rows above, on one
/// 16-core box carrying other work, and it stops holding as a box approaches
/// full.
///
/// # The next limit is file descriptors, not this runtime
///
/// An idle group holds its WAL directory lock and no segment descriptor, so it
/// costs **one file descriptor per group**. The active segment opens on the
/// first write. A lone write closes it again; sustained writes retain it, and a
/// runtime tick releases it after one quiet second. Live-but-idle groups
/// therefore return to one descriptor while a hot group costs two. Before the
/// segment became lazy, even a newly created group cost two and a process
/// with a 10,240 soft limit stopped near 5,119 groups. The same limit now holds
/// roughly twice as many cold groups; a host expecting every group to write
/// still sizes `RLIMIT_NOFILE` above twice the group count.
///
/// Measured after idle release and the eighteen memory changes: 8,192 live groups
/// under that unchanged 10,240 limit all started, retained 41 MiB, and delivered
/// 99.9% of their ticks at a one-second interval. Before idle release, starting
/// those groups opened a segment for each one and crossed the limit before the
/// store was live. Before the memory changes, the same run retained 171 MiB;
/// bounding the queue brought it to 126 MiB, boxing the pipeline values to 94
/// MiB, and boxing the node values to 74 MiB.
///
/// Count it over the whole run and take the maximum. Groups are created one
/// after another, so a single reading taken while that is still going is a lower
/// bound and nothing more -- one sample 30 seconds into a 16,384-group run read
/// 5,536 and looked like a refutation of this paragraph.
///
/// Descriptor exhaustion is reported with the active-segment and directory-lock
/// cost and names `RLIMIT_NOFILE`, so the group that crosses the limit does not
/// look like an isolated storage failure.
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
    hosted_groups: std::sync::atomic::AtomicUsize,
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
            hosted_groups: std::sync::atomic::AtomicUsize::new(0),
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
        self.hosted_groups.load(Ordering::Relaxed)
    }

    /// How the runtime's ticks have been served, and what it holds.
    pub fn stats(&self) -> SharedRuntimeStats {
        SharedRuntimeStats {
            ticks_in_place: self.counters.in_place.load(Ordering::Relaxed),
            ticks_handed_over: self.counters.handed_over.load(Ordering::Relaxed),
            ticks_skipped: self
                .tickers
                .iter()
                .map(|ticker| ticker.stats().ticks_skipped)
                .sum(),
            ticks_suppressed: self
                .tickers
                .iter()
                .map(|ticker| ticker.stats().ticks_suppressed)
                .sum(),
            groups: self.group_count(),
            threads: self.thread_count(),
        }
    }

    /// Stops delivering ticks to every group this runtime hosts.
    ///
    /// The shards each hold their own switch, and a host of this runtime cannot
    /// reach them: `tickers` is private and there is no accessor, so without
    /// this the switch exists on [`crate::Driver`] and not on the thing that
    /// hosts groups.
    ///
    /// Each shard keeps its schedule running, so groups resume on the cadence
    /// they had rather than every group in the runtime coming due at once --
    /// see `Driver::disable_ticks` for what that buys and for the hazard, which
    /// is that a runtime left disabled is a cluster that cannot elect.
    ///
    /// **Not atomic across shards.** Sixteen shards are sixteen stores, so a
    /// group on a shard this has not reached yet can tick once more. What is
    /// guaranteed is that no group ticks after this returns.
    pub fn disable_ticks(&self) {
        for ticker in &self.tickers {
            ticker.disable_ticks();
        }
    }

    /// Delivers ticks again, to every shard. Idempotent, like `disable_ticks`.
    pub fn enable_ticks(&self) {
        for ticker in &self.tickers {
            ticker.enable_ticks();
        }
    }

    /// True while every shard is delivering ticks.
    ///
    /// All of them rather than any, because a partially disabled runtime is not
    /// a runtime that is ticking -- and the only way to reach that state is to
    /// observe it mid-fan-out.
    pub fn ticks_enabled(&self) -> bool {
        self.tickers.iter().all(Driver::ticks_enabled)
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
    ) -> Result<(), RaftError> {
        let group = Arc::new(PooledGroup {
            core: Mutex::new(core),
            ticks_due: AtomicU64::new(0),
        });
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
        if let Err(error) = self.ticker_for(key).register_group_every(
            key,
            Arc::new(PooledGroupTicker {
                group: Arc::clone(&group),
                pool: Arc::clone(&self.pool),
                counters: Arc::clone(&self.counters),
                key,
            }),
            tick_interval_ms,
        ) {
            self.pool.cancel_group(key);
            return Err(error);
        }
        self.hosted_groups.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn release(&self, key: DriverGroupKey) {
        self.ticker_for(key).cancel_group(key);
        if self.pool.cancel_group(key) {
            self.hosted_groups.fetch_sub(1, Ordering::Relaxed);
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
    tx: RuntimeCommandSender,
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
    // Deliberately the same signature as `mpsc::SyncSender::send`, which is the
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
fn raft_node_runtime_loop(options: NodeOptions, command_rx: RuntimeCommandReceiver) {
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
    let heartbeat_interval = Duration::from_millis(core.heartbeat_interval_ms);
    let mut next_tick = Instant::now() + heartbeat_interval;
    loop {
        let command = if let Some(command) = core.carried_command.take() {
            *command
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
                    next_tick = Instant::now() + heartbeat_interval;
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

/// Whether this error means the group cannot be trusted to carry on, as
/// against having refused something it was right to refuse.
///
/// Only a storage error qualifies. The other six variants are all a node
/// declining to do something: a write sent to a follower (`NotLeader`), one
/// sent while an election is running (`NoLeader`), a caller naming a node that
/// is not there (`NodeNotFound`), a request over `max_payload_bytes` or past a
/// queue depth (`InvalidRequest`), a misconfiguration caught at build time
/// (`Config`), and a send that did not arrive (`Transport`, which the next
/// heartbeat retries). A node that refuses those is working.
///
/// A storage error is different in kind: the group asked for something to be
/// made durable and it was not. That is the one condition the comparable
/// engine treats as fatal -- it fires its fatal event from a failed log write
/// and from an apply its state machine cannot perform -- and after it the
/// node's in-memory state is ahead of what survives a restart.
///
/// Everything that reaches this is still recorded as an ordinary blocker, so
/// nothing is hidden; what changes is the **severity**, and with it
/// `FatalBlockerReport::fatal`.
/// A durable write failed, so take this node out of service.
///
/// The group asked for something to be made durable and it was not, which
/// leaves its in-memory state ahead of what survives a restart. Carrying on
/// from there is the unsafe part: an append this node acknowledges still counts
/// toward a commit quorum for entries its log will not have after a restart,
/// and a term or vote it did not persist can be given away twice.
///
/// **The policy here is not new.** `AdminCommand::FireFatalEvent` already steps
/// the node down if it is the leader and marks it unhealthy, which is what
/// takes it out of the quorum; what was missing is that nothing fired it when a
/// durable write actually failed, so a host had to notice and call it. This is
/// the same handling, reached from the condition it was built for -- and the
/// condition the comparable engine fires its own fatal event from, where a
/// failed `log_storage->Write` poisons the node and every later step and
/// proposal is refused.
///
/// Errors from the handling itself are deliberately swallowed. It runs because
/// a write already failed, the caller is already being handed that error, and
/// the alternative is replacing a storage error with a step-down error and
/// losing the cause. `set_node_healthy` failing would mean the node is not in
/// its own group, which `restore_wal_record` and `propose` already report.
/// Hands back what the write returned, taking the node out of service first if
/// the write failed.
///
/// The decision is a function rather than two lines at each write because the
/// write itself cannot be made to fail from a test: the WAL is always open on a
/// live runtime, and `tests/wal_descriptor_exhaustion.rs` can only fail the
/// *open*. Taking the outcome as an argument is what makes both arms reachable
/// -- fire on `Err`, and do not fire on `Ok` -- so each can be planted. A
/// version of this written as `if let Err(..) = appended` inline at both sites
/// would be unplantable, and a guard nothing can fail is not load-bearing.
fn after_durable_write<T>(
    cluster: &mut RaftCluster,
    node_id: NodeId,
    outcome: Result<T, RaftError>,
) -> Result<T, RaftError> {
    if outcome.is_err() {
        // Errors from the handling are deliberately swallowed: it runs because
        // a write already failed, the caller is already being handed that
        // error, and the alternative is replacing a storage error with a
        // step-down error and losing the cause.
        let _ = cluster.apply_fatal_event(node_id);
    }
    outcome
}

fn error_is_unrecoverable(error: &RaftError) -> bool {
    match error {
        RaftError::Storage(_) => true,
        RaftError::Config(_)
        | RaftError::NodeNotFound(_)
        | RaftError::NoLeader
        | RaftError::NotLeader(_)
        | RaftError::InvalidRequest(_)
        | RaftError::Transport(_) => false,
    }
}

/// Blockers a group retains, and how many it has seen.
///
/// The two vectors used to grow for the life of the group: `record_runtime_result`
/// only ever pushed and nothing emptied them. Measured on a release build, a
/// node handed writes it has to refuse because it is not the leader:
///
/// | refused | retained | string bytes | `status()` |
/// |---|---|---|---|
/// | 100 | 100 | 9,900 | 104us |
/// | 1,000 | 1,000 | 99,000 | 427us |
/// | 5,000 | 5,000 | 495,000 | 1,753us |
/// | 20,000 | 20,000 | 1,980,000 | 6,860us |
///
/// About 99 bytes retained per refused request, and `status()` 66x slower at
/// the end of that -- because it clones both vectors whole and then builds a
/// `Blocker` with two `format!`s for each entry. Per group, and a store runs
/// thousands of them.
///
/// So the window is bounded and the totals are counted. What is **not** changed
/// is what the report means: `ready` is still `blockers.is_empty()` and so still
/// false once anything has been refused, because
/// `node_runtime_executes_prevote_and_reports_blockers_from_runtime_tasks` pins
/// that deliberately -- these are a record of what the runtime's tasks did, not
/// a description of the group right now. Keeping the most recent rather than the
/// first follows from the same reading: what is wrong now diagnoses better than
/// what was wrong first.
#[derive(Debug, Default)]
struct RuntimeBlockerLog {
    state: Option<Box<RuntimeBlockerLogState>>,
}

#[derive(Debug, Default)]
struct RuntimeBlockerLogState {
    blockers: SparseVec<String>,
    fatal_blockers: SparseVec<String>,
    blockers_seen: u64,
    fatal_blockers_seen: u64,
}

/// How many blockers of each kind a group keeps.
///
/// At about 99 bytes an entry this is roughly 13KB a group, which leaves
/// `status()` near the 104us it costs with no history at all. Large enough that
/// a burst of related failures is still all there when somebody reads it.
const MATRIXRAFT_RETAINED_BLOCKER_LIMIT: usize = 128;

impl RuntimeBlockerLog {
    fn record(&mut self, blocker: String, fatal: bool) {
        let state = self
            .state
            .get_or_insert_with(|| Box::new(RuntimeBlockerLogState::default()));
        state.blockers_seen = state.blockers_seen.saturating_add(1);
        if fatal {
            state.fatal_blockers_seen = state.fatal_blockers_seen.saturating_add(1);
            state
                .fatal_blockers
                .push_bounded(blocker.clone(), MATRIXRAFT_RETAINED_BLOCKER_LIMIT);
        }
        state
            .blockers
            .push_bounded(blocker, MATRIXRAFT_RETAINED_BLOCKER_LIMIT);
    }

    fn blockers(&self) -> &[String] {
        self.state
            .as_ref()
            .map_or(&[], |state| state.blockers.as_slice())
    }

    fn fatal_blockers(&self) -> &[String] {
        self.state
            .as_ref()
            .map_or(&[], |state| state.fatal_blockers.as_slice())
    }

    fn blockers_seen(&self) -> u64 {
        self.state.as_ref().map_or(0, |state| state.blockers_seen)
    }

    fn fatal_blockers_seen(&self) -> u64 {
        self.state
            .as_ref()
            .map_or(0, |state| state.fatal_blockers_seen)
    }
}

fn record_runtime_result<T>(
    operation: &str,
    result: Result<T, RaftError>,
    blocker_log: &mut RuntimeBlockerLog,
    fatal_on_error: bool,
) -> Result<T, RaftError> {
    if let Err(error) = &result {
        let blocker = format!("{operation}:{error}");
        // Both gates, so this can only ever narrow what was fatal before: the
        // operation has to be one that is allowed to be fatal *and* the error
        // has to be one that is. Without the second gate every errored step
        // was fatal, so 25 writes sent to a follower left a node reporting
        // `fatal=true` with 25 fatal blockers -- and `ready` false for good,
        // because nothing empties these.
        blocker_log.record(blocker, fatal_on_error && error_is_unrecoverable(error));
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
                    .active_leader_transfer()
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
            reply.send(Err(error));
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
            reply.send(Err(error));
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
    recv_runtime_reply_ref(&reply_rx)
}

fn recv_runtime_reply_ref<T>(reply_rx: &mpsc::Receiver<T>) -> Result<T, RaftError> {
    reply_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|err| RaftError::Transport(format!("raft node runtime did not reply: {err}")))
}

#[cfg(test)]
mod runtime_command_queue_tests {
    use super::*;

    #[cfg(target_pointer_width = "64")]
    const _: () = {
        assert!(
            std::mem::size_of::<NodeCore>() <= 840,
            "cold runtime state widened NodeCore"
        );
        assert!(std::mem::size_of::<StoredWalRecoveryReport>() <= 32);
        assert!(std::mem::size_of::<RuntimeBlockerLog>() <= 8);
        assert!(
            std::mem::size_of::<RuntimeCommandQueue>() <= 224,
            "the fixed command queue widened"
        );
        assert!(
            std::mem::size_of::<RuntimeCommandChannel>() <= 240,
            "the command channel widened"
        );
        assert!(std::mem::size_of::<RuntimeCommandSender>() <= 8);
        assert!(std::mem::size_of::<RuntimeCommandReceiver>() <= 8);
        assert!(std::mem::size_of::<SparseVec<u64>>() <= 8);
        assert!(std::mem::size_of::<SparseSet<u64>>() <= 8);
    };

    #[test]
    fn sparse_collections_keep_sequence_and_set_wire_shapes() {
        let empty_values = SparseVec::<u64>::default();
        assert_eq!(
            serde_json::to_value(&empty_values).expect("empty sequence"),
            serde_json::json!([])
        );
        let values: SparseVec<u64> =
            serde_json::from_value(serde_json::json!([3, 1])).expect("sequence");
        assert_eq!(values.as_slice(), &[3, 1]);

        let mut indexes = SparseSet::<u64>::default();
        assert_eq!(
            serde_json::to_value(&indexes).expect("empty set"),
            serde_json::json!([])
        );
        indexes.insert(3);
        indexes.insert(1);
        assert_eq!(
            serde_json::to_value(&indexes).expect("populated set"),
            serde_json::json!([1, 3])
        );
        let round_trip: SparseSet<u64> =
            serde_json::from_value(serde_json::json!([1, 3])).expect("set");
        assert_eq!(round_trip.iter().copied().collect::<Vec<_>>(), vec![1, 3]);
    }

    #[test]
    fn lifecycle_batch_replies_keep_their_indexes_when_out_of_order() {
        let (sender, receiver) = mpsc::sync_channel(2);
        LifecycleReply::Batch {
            index: 9,
            sender: sender.clone(),
        }
        .send(Ok(()));
        LifecycleReply::Batch { index: 2, sender }.send(Err(RaftError::InvalidRequest(
            "second lifecycle result".to_string(),
        )));

        let (first_index, first_result) = receiver.recv().expect("first batch reply");
        let (second_index, second_result) = receiver.recv().expect("second batch reply");
        assert_eq!(first_index, 9);
        assert!(first_result.is_ok());
        assert_eq!(second_index, 2);
        assert!(matches!(
            second_result,
            Err(RaftError::InvalidRequest(message)) if message == "second lifecycle result"
        ));
    }

    #[test]
    fn stored_recovery_report_preserves_the_public_report() {
        let clean_report = WalRecoveryReport {
            recovered: None,
            truncated_corrupt_tail: false,
            surviving_records: 0,
            removed_records: 0,
            segments_scanned: 3,
            checksum_format: Some(matrixraft_wal_checksum_format()),
            retained_range: Some(LogRetainedRange {
                first_log_index: 0,
                last_log_index: 0,
                first_segment_id: 7,
                last_segment_id: 9,
                record_count: 0,
            }),
        };
        let stored = StoredWalRecoveryReport::from_report(clean_report.clone());
        assert!(matches!(stored, StoredWalRecoveryReport::Clean { .. }));
        assert_eq!(stored.to_report(), clean_report);

        let formats = [
            None,
            Some(matrixraft_wal_checksum_format()),
            Some(WalChecksumFormat {
                algorithm: "custom".to_string(),
                encoding: "bytes".to_string(),
                covered_fields: vec!["term".to_string()],
            }),
        ];
        for checksum_format in formats {
            let report = WalRecoveryReport {
                recovered: None,
                truncated_corrupt_tail: true,
                surviving_records: 17,
                removed_records: 3,
                segments_scanned: 4,
                checksum_format,
                retained_range: Some(LogRetainedRange {
                    first_log_index: 11,
                    last_log_index: 29,
                    first_segment_id: 2,
                    last_segment_id: 5,
                    record_count: 19,
                }),
            };
            let stored = StoredWalRecoveryReport::from_report(report.clone());
            assert_eq!(stored.to_report(), report);
        }
    }

    #[test]
    fn a_command_past_the_bound_waits_until_one_is_taken() {
        let (command_tx, command_rx) = runtime_command_channel();
        let mut held_replies = Vec::new();
        for _ in 0..RUNTIME_COMMAND_QUEUE_DEPTH {
            let (reply, reply_rx) = mpsc::sync_channel(1);
            held_replies.push(reply_rx);
            command_tx
                .send(NodeRuntimeOp::Start(LifecycleReply::Single(reply)))
                .expect("fill the command queue to its bound");
        }

        let (second_admitted, second_admitted_rx) = mpsc::channel();
        let second_sender = command_tx.clone();
        let sender = thread::spawn(move || {
            let (second_reply, _second_reply_rx) = mpsc::sync_channel(1);
            let sent = second_sender.send(NodeRuntimeOp::Stop(LifecycleReply::Single(
                second_reply,
            )));
            let _ = second_admitted.send(sent.is_ok());
        });

        assert!(
            matches!(
                second_admitted_rx.recv_timeout(Duration::from_millis(50)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ),
            "the command queue admitted an operation past its bound"
        );
        assert!(
            matches!(command_rx.recv(), Ok(NodeRuntimeOp::Start(_))),
            "a queued operation changed"
        );
        assert!(
            second_admitted_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("the second sender to resume"),
            "the waiting operation was not admitted after capacity became available"
        );
        for _ in 1..RUNTIME_COMMAND_QUEUE_DEPTH {
            assert!(
                matches!(command_rx.recv(), Ok(NodeRuntimeOp::Start(_))),
                "a queued operation changed"
            );
        }
        assert!(matches!(command_rx.recv(), Ok(NodeRuntimeOp::Stop(_))));
        sender.join().expect("second sender");
    }

    #[test]
    fn command_waits_distinguish_timeout_from_disconnect() {
        let (command_tx, command_rx) = runtime_command_channel();
        assert!(matches!(
            command_rx.recv_timeout(Duration::from_millis(1)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        drop(command_tx);
        assert!(matches!(
            command_rx.recv_timeout(Duration::from_secs(1)),
            Err(mpsc::RecvTimeoutError::Disconnected)
        ));
    }

    #[test]
    fn dropping_the_receiver_returns_a_blocked_command() {
        let (command_tx, command_rx) = runtime_command_channel();
        let mut held_replies = Vec::new();
        for _ in 0..RUNTIME_COMMAND_QUEUE_DEPTH {
            let (reply, reply_rx) = mpsc::sync_channel(1);
            held_replies.push(reply_rx);
            command_tx
                .send(NodeRuntimeOp::Start(LifecycleReply::Single(reply)))
                .expect("fill the command queue to its bound");
        }

        let blocked_sender = command_tx.clone();
        let blocked = thread::spawn(move || {
            let (reply, _reply_rx) = mpsc::sync_channel(1);
            matches!(
                blocked_sender.send(NodeRuntimeOp::Stop(LifecycleReply::Single(reply))),
                Err(mpsc::SendError(NodeRuntimeOp::Stop(_)))
            )
        });
        thread::sleep(Duration::from_millis(20));
        drop(command_rx);

        assert!(blocked.join().expect("blocked sender"));
    }
}

