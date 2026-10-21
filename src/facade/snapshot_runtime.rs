// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// snapshot metadata, lifecycle, stores, and install/read-index messages.
// Split from src/lib.rs to keep the crate facade small and focused.

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotMetadata {
    pub snapshot_id: SnapshotId,
    pub last_log_id: LogId,
    pub membership: Vec<NodeId>,
    #[serde(default)]
    pub members: Vec<Peer>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotChunk {
    pub meta: SnapshotMetadata,
    pub offset: u64,
    /// Chunk bytes travel as base64, not as a JSON number array.
    ///
    /// A number array costs about 3.9 bytes per data byte and a chunk is
    /// 64 KiB by default, so this was the largest single wire cost in the
    /// protocol. Decode still accepts the number-array form, so chunks and
    /// snapshot files written before this codec still read; an old receiver
    /// rejects the new form, so a mixed-version cluster upgrades receivers
    /// first.
    #[serde(with = "crate::wal::bytes_as_base64")]
    pub data: SnapshotPayload,
    pub done: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GenericSnapshot<G = GroupId, P = SnapshotPayload> {
    pub group_id: G,
    pub meta: SnapshotMetadata,
    pub payload: P,
}

pub type RaftSnapshot = GenericSnapshot<GroupId, SnapshotPayload>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(bound(
    serialize = "P: serde::Serialize + AsRef<[u8]>",
    deserialize = "P: serde::Deserialize<'de> + From<Vec<u8>>"
))]
pub struct GenericSnapshotChunk<P = SnapshotPayload> {
    pub meta: SnapshotMetadata,
    pub offset: u64,
    /// Chunk bytes travel as base64, not as a JSON number array.
    ///
    /// A number array costs about 3.9 bytes per data byte and a chunk is
    /// 64 KiB by default, so this is the largest single wire cost in the
    /// protocol. Decode still accepts the number-array form, so chunks and
    /// snapshot files written before this codec still read; an old receiver
    /// rejects the new form, so a mixed-version cluster upgrades receivers
    /// first. The serde bounds above are what the codec itself needs -- a
    /// payload type that is not byte-like was never base64-serializable.
    #[serde(with = "crate::wal::bytes_as_base64")]
    pub data: P,
    pub done: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotInstallState {
    pub meta: SnapshotMetadata,
    pub bytes: SnapshotPayload,
    pub next_offset: u64,
    pub complete: bool,
}

impl SnapshotInstallState {
    pub fn new(meta: SnapshotMetadata) -> Self {
        Self {
            meta,
            bytes: Vec::new(),
            next_offset: 0,
            complete: false,
        }
    }

    pub fn install_chunk(&mut self, chunk: SnapshotChunk) -> Result<(), RaftError> {
        if chunk.meta != self.meta {
            return Err(RaftError::InvalidRequest(
                "snapshot chunk metadata changed during install".to_string(),
            ));
        }
        if chunk.offset != self.next_offset {
            return Err(RaftError::InvalidRequest(format!(
                "snapshot chunk offset {} does not match next offset {}",
                chunk.offset, self.next_offset
            )));
        }
        // Only reachable once the offset above matched, so this grows by
        // bytes actually received and would need 2^64 of them to wrap.
        // Saturating anyway, so the invariant does not rest on that.
        self.next_offset = self.next_offset.saturating_add(chunk.data.len() as u64);
        self.bytes.extend_from_slice(&chunk.data);
        self.complete = chunk.done;
        Ok(())
    }

    pub fn finish(self, group_id: GroupId) -> Result<RaftSnapshot, RaftError> {
        if !self.complete {
            return Err(RaftError::InvalidRequest(
                "snapshot install is incomplete".to_string(),
            ));
        }
        Ok(RaftSnapshot {
            group_id,
            meta: self.meta,
            payload: self.bytes,
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotLifecycleConfig {
    pub chunk_size: u64,
    pub max_chunks_per_tick: u64,
    pub max_bytes_per_tick: u64,
    pub max_retry_attempts: u64,
}

impl Default for SnapshotLifecycleConfig {
    fn default() -> Self {
        Self {
            chunk_size: 64 * 1024,
            max_chunks_per_tick: 16,
            max_bytes_per_tick: 4 * 1024 * 1024,
            max_retry_attempts: 3,
        }
    }
}

impl SnapshotLifecycleConfig {
    pub fn validate(&self) -> Result<(), RaftError> {
        if self.chunk_size == 0 {
            return Err(RaftError::InvalidRequest(
                "snapshot chunk_size must be greater than zero".to_string(),
            ));
        }
        if self.max_chunks_per_tick == 0 {
            return Err(RaftError::InvalidRequest(
                "snapshot max_chunks_per_tick must be greater than zero".to_string(),
            ));
        }
        if self.max_bytes_per_tick == 0 {
            return Err(RaftError::InvalidRequest(
                "snapshot max_bytes_per_tick must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotLifecycleStatus {
    pub snapshot_id: Option<SnapshotId>,
    pub sending: bool,
    pub installing: bool,
    pub total_chunks: u64,
    pub sent_chunks: u64,
    pub received_chunks: u64,
    pub retry_count: u64,
    pub throttled_ticks: u64,
    pub rate_limited_ticks: u64,
    /// Responses that declined a chunk without failing the send.
    ///
    /// Kept apart from `retry_count` because they mean opposite things: a retry
    /// is progress lost and is budgeted, and a throttle is the receiver asking
    /// for less. Adding them together would make a paced transfer look like a
    /// failing one.
    ///
    /// **Not budgeted**, which is deliberate and is the one thing to watch: a
    /// receiver that throttles for ever makes no progress and never fails
    /// either. Nothing here has a clock, so a host that wants a ceiling watches
    /// this and calls `cancel_send`.
    #[serde(default)]
    pub throttled_responses: u64,
    /// Chunks refused because the install is cancelled.
    ///
    /// Counted apart from everything else because it is the only rejection
    /// that says "stop sending" rather than "try again": a sender that keeps
    /// pushing against this is wasting a whole snapshot's worth of bandwidth.
    #[serde(default)]
    pub refused_chunks: u64,
    /// Installs stopped by `cancel_install` while a transfer was in progress.
    #[serde(default)]
    pub cancelled_installs: u64,
    /// Whether this group is currently refusing chunks.
    #[serde(default)]
    pub install_cancelled: bool,
    pub rolled_back: u64,
    pub completed: bool,
    pub installed_index: LogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotSendState {
    pub group_id: GroupId,
    pub term: Term,
    pub leader_id: NodeId,
    pub chunks: Vec<SnapshotChunk>,
    pub next_chunk: usize,
    pub accepted_chunks: u64,
    pub retry_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotLifecycle {
    config: SnapshotLifecycleConfig,
    sender: Option<SnapshotSendState>,
    installer: Option<SnapshotInstallState>,
    status: SnapshotLifecycleStatus,
    /// Set by `cancel_install`, cleared by `admit_install`.
    ///
    /// Without it a cancel does not stick: `install_request` begins a fresh
    /// install whenever it holds no installer, so the next chunk to arrive
    /// would quietly resurrect the transfer that was just stopped.
    install_closed: bool,
}

impl SnapshotLifecycle {
    pub fn new(config: SnapshotLifecycleConfig) -> Result<Self, RaftError> {
        config.validate()?;
        Ok(Self {
            config,
            install_closed: false,
            sender: None,
            installer: None,
            status: SnapshotLifecycleStatus {
                snapshot_id: None,
                sending: false,
                installing: false,
                total_chunks: 0,
                sent_chunks: 0,
                received_chunks: 0,
                retry_count: 0,
                throttled_responses: 0,
                refused_chunks: 0,
                cancelled_installs: 0,
                install_cancelled: false,
                throttled_ticks: 0,
                rate_limited_ticks: 0,
                rolled_back: 0,
                completed: false,
                installed_index: 0,
            },
        })
    }

    /// Splits a snapshot into the chunks a send will transmit.
    ///
    /// Every chunk is built up front, so the result is a second copy of the
    /// payload. Measured with `examples/snapshot_checkpoint_cost`, that is
    /// 1.01x the payload -- 258 MiB held for a 256 MiB snapshot, of which about
    /// 1% is the per-chunk metadata clone and the rest is the payload itself.
    ///
    /// Chunks cannot simply be dropped as they are sent: a rejected response
    /// rewinds `next_chunk`, so an earlier chunk can be asked for again.
    /// Removing the second copy means the sender holding the snapshot and
    /// slicing a chunk when it is asked for, which needs `begin_send` to share
    /// the snapshot rather than take it by reference.
    pub fn checkpoint(
        snapshot: &RaftSnapshot,
        chunk_size: u64,
    ) -> Result<Vec<SnapshotChunk>, RaftError> {
        if chunk_size == 0 {
            return Err(RaftError::InvalidRequest(
                "snapshot chunk_size must be greater than zero".to_string(),
            ));
        }
        let mut chunks = Vec::new();
        let mut offset = 0;
        while offset < snapshot.payload.len() {
            let end = (offset + chunk_size as usize).min(snapshot.payload.len());
            chunks.push(SnapshotChunk {
                meta: snapshot.meta.clone(),
                offset: offset as u64,
                data: snapshot.payload[offset..end].to_vec(),
                done: end == snapshot.payload.len(),
            });
            offset = end;
        }
        if chunks.is_empty() {
            chunks.push(SnapshotChunk {
                meta: snapshot.meta.clone(),
                offset: 0,
                data: Vec::new(),
                done: true,
            });
        }
        Ok(chunks)
    }

    /// Starts sending a snapshot.
    ///
    /// This holds every chunk for the whole transfer, so a send costs roughly
    /// twice the snapshot in memory until it completes -- and it begins exactly
    /// when a follower has fallen behind, which is not when a leader has memory
    /// to spare. See [`Self::checkpoint`] for what it would take to avoid.
    pub fn begin_send(
        &mut self,
        snapshot: &RaftSnapshot,
        term: Term,
        leader_id: NodeId,
    ) -> Result<(), RaftError> {
        if self.sender.is_some() || self.installer.is_some() {
            return Err(RaftError::InvalidRequest(
                "snapshot lifecycle is already active".to_string(),
            ));
        }
        let chunks = Self::checkpoint(snapshot, self.config.chunk_size)?;
        self.status = SnapshotLifecycleStatus {
            snapshot_id: Some(snapshot.meta.snapshot_id.clone()),
            sending: true,
            installing: false,
            total_chunks: chunks.len() as u64,
            sent_chunks: 0,
            received_chunks: 0,
            retry_count: 0,
            throttled_responses: 0,
            // Carried over, not reset. Beginning a send says nothing about
            // whether this group is refusing snapshots, and zeroing the flag
            // here would re-admit an install a host had cancelled -- the same
            // resurrection `cancel_install` exists to stop, by another route.
            refused_chunks: self.status.refused_chunks,
            cancelled_installs: self.status.cancelled_installs,
            install_cancelled: self.install_closed,
            throttled_ticks: 0,
            rate_limited_ticks: self.status.rate_limited_ticks,
            rolled_back: self.status.rolled_back,
            completed: false,
            installed_index: 0,
        };
        self.sender = Some(SnapshotSendState {
            group_id: snapshot.group_id,
            term,
            leader_id,
            chunks,
            next_chunk: 0,
            accepted_chunks: 0,
            retry_count: 0,
        });
        Ok(())
    }

    pub fn poll_send_requests(&mut self) -> Result<Vec<InstallSnapshotRequest>, RaftError> {
        let sender = self
            .sender
            .as_mut()
            .ok_or_else(|| RaftError::InvalidRequest("snapshot send is not active".to_string()))?;
        let mut requests = Vec::new();
        let mut bytes = 0u64;
        while sender.next_chunk < sender.chunks.len()
            && requests.len() < self.config.max_chunks_per_tick as usize
        {
            let chunk = sender.chunks[sender.next_chunk].clone();
            let next_bytes = bytes + chunk.data.len() as u64;
            if !requests.is_empty() && next_bytes > self.config.max_bytes_per_tick {
                self.status.throttled_ticks += 1;
                break;
            }
            bytes = next_bytes;
            sender.next_chunk += 1;
            self.status.sent_chunks += 1;
            requests.push(InstallSnapshotRequest {
                group_id: sender.group_id,
                term: sender.term,
                leader_id: sender.leader_id,
                chunk,
            });
        }
        if sender.next_chunk < sender.chunks.len() {
            self.status.throttled_ticks += 1;
        }
        Ok(requests)
    }

    pub fn poll_send_requests_with_limiter(
        &mut self,
        limiter: &mut impl RateLimiter,
    ) -> Result<Vec<InstallSnapshotRequest>, RaftError> {
        let sender = self
            .sender
            .as_mut()
            .ok_or_else(|| RaftError::InvalidRequest("snapshot send is not active".to_string()))?;
        let mut requests = Vec::new();
        let mut bytes = 0u64;
        while sender.next_chunk < sender.chunks.len()
            && requests.len() < self.config.max_chunks_per_tick as usize
        {
            let mut chunk = sender.chunks[sender.next_chunk].clone();
            let chunk_bytes = chunk.data.len() as u64;
            let next_bytes = bytes + chunk_bytes;
            if !requests.is_empty() && next_bytes > self.config.max_bytes_per_tick {
                self.status.throttled_ticks += 1;
                break;
            }
            let decision = limiter.reserve_limited_bytes(chunk_bytes);
            if !decision.allowed {
                self.status.throttled_ticks += 1;
                self.status.rate_limited_ticks += 1;
                break;
            }
            let granted_bytes = decision.granted_bytes as usize;
            if granted_bytes < chunk.data.len() {
                let remaining = chunk.data.split_off(granted_bytes);
                let remaining_chunk = SnapshotChunk {
                    meta: chunk.meta.clone(),
                    offset: chunk.offset + decision.granted_bytes,
                    data: remaining,
                    done: chunk.done,
                };
                chunk.done = false;
                sender.chunks[sender.next_chunk] = remaining_chunk;
                self.status.total_chunks = self.status.total_chunks.saturating_add(1);
            } else {
                sender.next_chunk += 1;
            }
            bytes += decision.granted_bytes;
            self.status.sent_chunks += 1;
            requests.push(InstallSnapshotRequest {
                group_id: sender.group_id,
                term: sender.term,
                leader_id: sender.leader_id,
                chunk,
            });
        }
        if sender.next_chunk < sender.chunks.len() {
            self.status.throttled_ticks += 1;
        }
        Ok(requests)
    }

    pub fn record_send_response(
        &mut self,
        response: &InstallSnapshotResponse,
    ) -> Result<(), RaftError> {
        if self.sender.is_none() && response.accepted {
            return Ok(());
        }
        let sender = self
            .sender
            .as_mut()
            .ok_or_else(|| RaftError::InvalidRequest("snapshot send is not active".to_string()))?;
        if response.accepted {
            sender.accepted_chunks += 1;
            if sender.accepted_chunks >= sender.chunks.len() as u64 {
                self.status.sending = false;
                self.status.completed = true;
                self.sender = None;
            }
            return Ok(());
        }
        // A throttle is not a failure, so it does not spend the retry budget.
        // Before this, a receiver pacing a transfer with three declines killed
        // it -- the thing a receiver-side limiter is supposed to prevent.
        //
        // The rewind below is shared: a throttled response and a rejection both
        // resume from the offset the receiver asked for, which for a throttle is
        // the chunk it declined.
        if !response.throttled {
            sender.retry_count += 1;
            self.status.retry_count += 1;
            if sender.retry_count > self.config.max_retry_attempts {
                self.sender = None;
                self.status.sending = false;
                return Err(RaftError::Transport(
                    "snapshot send retry budget exhausted".to_string(),
                ));
            }
        } else {
            self.status.throttled_responses += 1;
        }
        sender.next_chunk = sender
            .chunks
            .iter()
            .position(|chunk| chunk.offset >= response.next_offset)
            .unwrap_or(sender.chunks.len());
        Ok(())
    }

    pub fn record_send_timeout(&mut self) -> Result<(), RaftError> {
        let sender = self
            .sender
            .as_mut()
            .ok_or_else(|| RaftError::InvalidRequest("snapshot send is not active".to_string()))?;
        sender.retry_count += 1;
        self.status.retry_count += 1;
        if sender.retry_count > self.config.max_retry_attempts {
            self.sender = None;
            self.status.sending = false;
            return Err(RaftError::Transport(
                "snapshot send timeout retry budget exhausted".to_string(),
            ));
        }
        Ok(())
    }

    pub fn cancel_send(&mut self) -> bool {
        let was_sending = self.sender.take().is_some();
        if was_sending {
            self.status.sending = false;
            self.status.completed = false;
        }
        was_sending
    }

    pub fn install_request(
        &mut self,
        request: InstallSnapshotRequest,
    ) -> Result<Option<RaftSnapshot>, RaftError> {
        // A closed install refuses every chunk, including the first of a new
        // snapshot. This is the whole point of closing it: dropping the
        // partial install alone leaves the next chunk free to start another.
        if self.install_closed {
            self.status.refused_chunks += 1;
            return Err(RaftError::InvalidRequest(
                "snapshot install is cancelled for this group".to_string(),
            ));
        }
        if self.installer.is_none() {
            self.status.snapshot_id = Some(request.chunk.meta.snapshot_id.clone());
            self.status.installing = true;
            self.status.sending = false;
            self.status.completed = false;
            self.status.received_chunks = 0;
            self.status.total_chunks = 0;
            self.installer = Some(SnapshotInstallState::new(request.chunk.meta.clone()));
        }
        let installer = self.installer.as_mut().expect("installer exists");
        if request.chunk.offset == 0 && installer.meta != request.chunk.meta {
            return Ok(None);
        }
        installer.install_chunk(request.chunk)?;
        self.status.received_chunks += 1;
        self.status.total_chunks = self.status.total_chunks.max(self.status.received_chunks);
        if installer.complete {
            let installer = self.installer.take().expect("installer complete");
            let snapshot = installer.finish(request.group_id)?;
            self.status.installing = false;
            self.status.completed = true;
            self.status.installed_index = snapshot.meta.last_log_id.index;
            return Ok(Some(snapshot));
        }
        Ok(None)
    }

    /// Throws away a partial install so the next attempt starts clean.
    ///
    /// **This does not stop the transfer**, and that is the difference from
    /// [`SnapshotLifecycle::cancel_install`]: a rollback says "that attempt
    /// was bad, send it again", so the next chunk legitimately begins a fresh
    /// install. A host that wants the transfer to stop wants `cancel_install`.
    pub fn rollback_install(&mut self) {
        self.installer = None;
        self.status.installing = false;
        self.status.rolled_back += 1;
    }

    /// Stops installing into this group, and keeps it stopped.
    ///
    /// Returns whether an install was in progress. The flag matters more than
    /// the partial state: `install_request` starts a new install whenever it
    /// holds none, so **discarding the partial without closing the install
    /// lets the very next chunk start over** -- which is what a host removing
    /// a replica, or refusing a snapshot it has decided it does not want,
    /// would have got.
    ///
    /// The counterpart to [`SnapshotLifecycle::cancel_send`] on the other end
    /// of the transfer. A cancelled install refuses chunks with an error
    /// rather than ignoring them, so a sender finds out instead of pushing a
    /// whole snapshot at a receiver that is discarding it.
    ///
    /// Reopen with [`SnapshotLifecycle::admit_install`]. Nothing reopens it
    /// implicitly, because every implicit route is the resurrection this
    /// exists to prevent.
    pub fn cancel_install(&mut self) -> bool {
        let was_installing = self.installer.take().is_some();
        self.install_closed = true;
        self.status.installing = false;
        self.status.install_cancelled = true;
        if was_installing {
            self.status.cancelled_installs += 1;
        }
        was_installing
    }

    /// Lets this group receive snapshots again after a cancel.
    ///
    /// Returns whether it had been closed. A host calls this when the replica
    /// should receive again -- it is deliberately explicit, so that "stopped"
    /// never ends quietly.
    pub fn admit_install(&mut self) -> bool {
        let was_closed = self.install_closed;
        self.install_closed = false;
        self.status.install_cancelled = false;
        was_closed
    }

    /// Whether this group is refusing snapshot chunks.
    pub fn install_cancelled(&self) -> bool {
        self.install_closed
    }

    pub fn status(&self) -> SnapshotLifecycleStatus {
        self.status.clone()
    }
}

/// What a stored checkpoint is, without reading the snapshot itself.
///
/// Written beside each checkpoint so that deciding whether to keep one costs a
/// few hundred bytes instead of the whole snapshot. The comparable engine
/// keeps its snapshot *desc* in a separate file from the data for the same
/// reason.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredSnapshot {
    pub snapshot_id: SnapshotId,
    pub group_id: GroupId,
    pub last_log_id: LogId,
    /// Bytes the checkpoint file occupies on disk.
    pub bytes: u64,
}

/// What a release swept, and what it kept.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct SnapshotReleaseReport {
    pub released: Vec<StoredSnapshot>,
    pub retained: Vec<StoredSnapshot>,
    pub released_bytes: u64,
    pub retained_bytes: u64,
}

impl SnapshotReleaseReport {
    pub fn released_ids(&self) -> Vec<&str> {
        self.released
            .iter()
            .map(|stored| stored.snapshot_id.as_str())
            .collect()
    }

    pub fn retained_ids(&self) -> Vec<&str> {
        self.retained
            .iter()
            .map(|stored| stored.snapshot_id.as_str())
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistentRaftSnapshotStoreOptions {
    pub dir: PathBuf,
    pub chunk_size: u64,
}

impl PersistentRaftSnapshotStoreOptions {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            chunk_size: 64 * 1024,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PersistentRaftSnapshotStore {
    options: PersistentRaftSnapshotStoreOptions,
}

impl PersistentRaftSnapshotStore {
    pub fn open(options: PersistentRaftSnapshotStoreOptions) -> Result<Self, RaftError> {
        if options.chunk_size == 0 {
            return Err(RaftError::InvalidRequest(
                "snapshot store chunk_size must be greater than zero".to_string(),
            ));
        }
        fs::create_dir_all(&options.dir).map_err(|err| {
            RaftError::Storage(format!(
                "failed to create snapshot directory {}: {err}",
                options.dir.display()
            ))
        })?;
        Ok(Self { options })
    }

    pub fn save_checkpoint(&self, snapshot: &RaftSnapshot) -> Result<PathBuf, RaftError> {
        let path = self.snapshot_path(&snapshot.meta.snapshot_id);
        let encoded = serde_json::to_vec(snapshot)
            .map_err(|err| RaftError::Storage(format!("failed to encode snapshot: {err}")))?;
        // Written under a staging name and renamed into place, because
        // `File::create` on the final path TRUNCATES it. A crash between that
        // create and the sync left a torn checkpoint where a good one had
        // been, and `list_checkpoints` went on advertising it -- measured: a
        // file cut to a third of its length is still listed, and then refuses
        // to load with "EOF while parsing a list". That is the state the
        // sidecar ordering below exists to prevent, reached by the other
        // route.
        //
        // A rename within a directory is atomic, so a reader sees the whole
        // old checkpoint or the whole new one and never a prefix of either. A
        // crash before the rename leaves a `.snapshot.tmp`, which listing
        // ignores because it filters on the `.snapshot` extension, and which
        // the next save for that id overwrites rather than accumulating.
        let staging = self.staging_path(&snapshot.meta.snapshot_id);
        let mut file = File::create(&staging).map_err(|err| {
            RaftError::Storage(format!(
                "failed to create snapshot staging file {}: {err}",
                staging.display()
            ))
        })?;
        file.write_all(&encoded)
            .and_then(|_| file.sync_data())
            .map_err(|err| {
                RaftError::Storage(format!(
                    "failed to persist snapshot staging file {}: {err}",
                    staging.display()
                ))
            })?;
        fs::rename(&staging, &path).map_err(|err| {
            RaftError::Storage(format!(
                "failed to move snapshot {} into place: {err}",
                path.display()
            ))
        })?;
        // The sidecar last, and only after the checkpoint itself is synced: a
        // sidecar without its snapshot would advertise a checkpoint that
        // cannot be loaded, which is worse than a snapshot with no sidecar --
        // that one is merely slower to list.
        self.write_descriptor(snapshot, encoded.len() as u64)?;
        Ok(path)
    }

    /// Every checkpoint this store holds, oldest first.
    ///
    /// Reads the sidecar where there is one and falls back to the checkpoint
    /// itself where there is not, so a store written before sidecars existed
    /// still lists correctly -- slowly, which is the right way round.
    pub fn list_checkpoints(&self) -> Result<Vec<StoredSnapshot>, RaftError> {
        let entries = fs::read_dir(&self.options.dir).map_err(|err| {
            RaftError::Storage(format!(
                "failed to read snapshot directory {}: {err}",
                self.options.dir.display()
            ))
        })?;
        let mut stored = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|err| {
                RaftError::Storage(format!("failed to read a snapshot directory entry: {err}"))
            })?;
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("snapshot") {
                continue;
            }
            let Some(snapshot_id) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            stored.push(self.describe(snapshot_id)?);
        }
        // By index, then by id, so the order is total: two checkpoints at the
        // same index would otherwise sort by whatever the directory returned,
        // and "the newest" has to be one thing rather than either of two.
        stored.sort_by(|left, right| {
            left.last_log_id
                .index
                .cmp(&right.last_log_id.index)
                .then_with(|| left.snapshot_id.cmp(&right.snapshot_id))
        });
        Ok(stored)
    }

    /// Bytes every checkpoint in this store occupies.
    pub fn owned_bytes(&self) -> Result<u64, RaftError> {
        Ok(self
            .list_checkpoints()?
            .iter()
            .map(|stored| stored.bytes)
            .sum())
    }

    /// Deletes checkpoints nothing needs any more.
    ///
    /// **Nothing deleted a checkpoint before this.** Every snapshot a group
    /// ever wrote stayed on disk, so a group that checkpoints on a timer grew
    /// its snapshot directory without bound.
    ///
    /// A checkpoint is released only when **both** watermarks have passed it:
    ///
    /// - `applied_index` -- this node has applied past it, so it would never
    ///   restore from it.
    /// - `min_replicated_index` -- the **least** index replicated to any peer.
    ///   A follower behind that point may still have to be caught up, and an
    ///   in-flight send is reading a checkpoint file; deleting it mid-transfer
    ///   breaks the transfer rather than slowing it.
    ///
    /// Taking the *minimum* of the two is what makes this safe to call on a
    /// timer: a single lagging follower holds everything, which is the
    /// conservative direction. A host that wants to stop waiting for it should
    /// remove it from the group, not pass a higher number.
    ///
    /// **The newest checkpoint is never released**, whatever the watermarks
    /// say, because a store with nothing in it cannot restore anything. That
    /// means this can be called with any arguments without leaving the group
    /// unable to recover.
    pub fn release_checkpoints(
        &self,
        applied_index: LogIndex,
        min_replicated_index: LogIndex,
    ) -> Result<SnapshotReleaseReport, RaftError> {
        let stored = self.list_checkpoints()?;
        let watermark = applied_index.min(min_replicated_index);
        let mut report = SnapshotReleaseReport::default();
        // `stored` is oldest first, so the last is the newest and is kept
        // whatever else happens.
        let newest = stored.len().saturating_sub(1);
        for (nth, candidate) in stored.into_iter().enumerate() {
            let releasable = nth != newest && candidate.last_log_id.index <= watermark;
            if releasable {
                self.remove_checkpoint(&candidate.snapshot_id)?;
                report.released_bytes += candidate.bytes;
                report.released.push(candidate);
            } else {
                report.retained_bytes += candidate.bytes;
                report.retained.push(candidate);
            }
        }
        Ok(report)
    }

    /// Reads a checkpoint's descriptor, from its sidecar if there is one.
    fn describe(&self, snapshot_id: &str) -> Result<StoredSnapshot, RaftError> {
        let path = self.snapshot_path(snapshot_id);
        let bytes = fs::metadata(&path).map(|meta| meta.len()).map_err(|err| {
            RaftError::Storage(format!(
                "failed to stat snapshot {}: {err}",
                path.display()
            ))
        })?;
        let descriptor_path = self.descriptor_path(snapshot_id);
        if let Ok(raw) = fs::read(&descriptor_path) {
            if let Ok(mut stored) = serde_json::from_slice::<StoredSnapshot>(&raw) {
                // The file's own length wins over the recorded one: the
                // sidecar is a hint, and a hint that disagrees with the disk
                // would make the byte totals fiction.
                stored.bytes = bytes;
                return Ok(stored);
            }
        }
        // No sidecar, or an unreadable one: fall back to the snapshot.
        let snapshot = self.load_checkpoint(snapshot_id)?;
        Ok(StoredSnapshot {
            snapshot_id: snapshot.meta.snapshot_id,
            group_id: snapshot.group_id,
            last_log_id: snapshot.meta.last_log_id,
            bytes,
        })
    }

    fn write_descriptor(&self, snapshot: &RaftSnapshot, bytes: u64) -> Result<(), RaftError> {
        let stored = StoredSnapshot {
            snapshot_id: snapshot.meta.snapshot_id.clone(),
            group_id: snapshot.group_id,
            last_log_id: snapshot.meta.last_log_id.clone(),
            bytes,
        };
        let path = self.descriptor_path(&stored.snapshot_id);
        let encoded = serde_json::to_vec(&stored).map_err(|err| {
            RaftError::Storage(format!("failed to encode a snapshot descriptor: {err}"))
        })?;
        let mut file = File::create(&path).map_err(|err| {
            RaftError::Storage(format!(
                "failed to create snapshot descriptor {}: {err}",
                path.display()
            ))
        })?;
        file.write_all(&encoded)
            .and_then(|_| file.sync_data())
            .map_err(|err| {
                RaftError::Storage(format!(
                    "failed to persist snapshot descriptor {}: {err}",
                    path.display()
                ))
            })
    }

    /// Deletes one checkpoint and its sidecar.
    ///
    /// The checkpoint goes first: a sidecar left behind describes nothing and
    /// is ignored by `list_checkpoints`, while a checkpoint left behind after
    /// its sidecar went would be listed by the slow path and look live.
    fn remove_checkpoint(&self, snapshot_id: &str) -> Result<(), RaftError> {
        let path = self.snapshot_path(snapshot_id);
        fs::remove_file(&path).map_err(|err| {
            RaftError::Storage(format!(
                "failed to remove snapshot {}: {err}",
                path.display()
            ))
        })?;
        // Best effort: a sidecar whose snapshot is gone is already inert.
        let _ = fs::remove_file(self.descriptor_path(snapshot_id));
        Ok(())
    }

    pub fn load_checkpoint(&self, snapshot_id: &str) -> Result<RaftSnapshot, RaftError> {
        let path = self.snapshot_path(snapshot_id);
        let bytes = fs::read(&path).map_err(|err| {
            RaftError::Storage(format!("failed to read snapshot {}: {err}", path.display()))
        })?;
        serde_json::from_slice(&bytes)
            .map_err(|err| RaftError::Storage(format!("failed to decode snapshot: {err}")))
    }

    pub fn checkpoint_chunks(
        &self,
        snapshot_id: &str,
    ) -> Result<Vec<SnapshotChunk>, RaftError> {
        let snapshot = self.load_checkpoint(snapshot_id)?;
        SnapshotLifecycle::checkpoint(&snapshot, self.options.chunk_size)
    }

    fn snapshot_path(&self, snapshot_id: &str) -> PathBuf {
        self.options.dir.join(format!("{snapshot_id}.snapshot"))
    }

    fn descriptor_path(&self, snapshot_id: &str) -> PathBuf {
        self.options.dir.join(format!("{snapshot_id}.descriptor"))
    }

    /// Where a checkpoint is written before it is renamed into place.
    ///
    /// The extension matters: `list_checkpoints` keeps only entries whose
    /// extension is `snapshot`, and `Path::extension` on this name answers
    /// `tmp`, so a staging file left by a crash is never mistaken for a
    /// checkpoint.
    fn staging_path(&self, snapshot_id: &str) -> PathBuf {
        self.options.dir.join(format!("{snapshot_id}.snapshot.tmp"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstallSnapshotRequest {
    pub group_id: GroupId,
    pub term: Term,
    pub leader_id: NodeId,
    pub chunk: SnapshotChunk,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstallSnapshotResponse {
    pub term: Term,
    pub accepted: bool,
    pub next_offset: u64,
    #[serde(default)]
    pub committed_index: LogIndex,
    pub reason: String,
    /// The receiver declined this chunk **for now**, rather than failing.
    ///
    /// A plain `accepted: false` is a failure: `record_send_response` counts it
    /// as a retry and abandons the send once `max_retry_attempts` is exceeded.
    /// That is right for a rejection and wrong for a receiver asking for less,
    /// which is why a throttle has to be something the sender can tell apart --
    /// without this, a receiver that throttled three times killed the transfer
    /// it was trying to pace.
    ///
    /// A throttled response does not consume the retry budget. It rewinds the
    /// sender to the chunk it declined, and
    /// [`SnapshotLifecycleStatus::throttled_responses`] counts it.
    ///
    /// `#[serde(default)]` so a peer that does not set it reads as a plain
    /// rejection, which is what it was before this field existed.
    #[serde(default)]
    pub throttled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadIndexRequest {
    pub group_id: GroupId,
    pub requester_id: NodeId,
    pub min_commit_index: LogIndex,
    pub allow_lease_read: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadIndexResponse {
    pub safe: bool,
    pub read_index: LogIndex,
    pub lease_read: bool,
    pub reason: String,
}

