// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// snapshot metadata, lifecycle, stores, and install/read-index messages.
// Split from src/lib.rs to keep the crate facade small and focused.

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftSnapshotMeta {
    pub snapshot_id: RustRaftSnapshotId,
    pub last_log_id: RustRaftLogId,
    pub membership: Vec<RustRaftNodeId>,
    #[serde(default)]
    pub members: Vec<RustRaftPeer>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftSnapshotChunk {
    pub meta: RustRaftSnapshotMeta,
    pub offset: u64,
    pub data: RustRaftSnapshotPayload,
    pub done: bool,
}

pub type SnapshotChunk = RustRaftSnapshotChunk;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftGenericSnapshot<G = RustRaftGroupId, P = RustRaftSnapshotPayload> {
    pub group_id: G,
    pub meta: RustRaftSnapshotMeta,
    pub payload: P,
}

pub type RaftSnapshot = RustRaftGenericSnapshot<RustRaftGroupId, RustRaftSnapshotPayload>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftGenericSnapshotChunk<P = RustRaftSnapshotPayload> {
    pub meta: RustRaftSnapshotMeta,
    pub offset: u64,
    pub data: P,
    pub done: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSnapshotInstallState {
    pub meta: RustRaftSnapshotMeta,
    pub bytes: RustRaftSnapshotPayload,
    pub next_offset: u64,
    pub complete: bool,
}

impl RaftSnapshotInstallState {
    pub fn new(meta: RustRaftSnapshotMeta) -> Self {
        Self {
            meta,
            bytes: Vec::new(),
            next_offset: 0,
            complete: false,
        }
    }

    pub fn install_chunk(&mut self, chunk: RustRaftSnapshotChunk) -> Result<(), RaftError> {
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
        self.next_offset += chunk.data.len() as u64;
        self.bytes.extend_from_slice(&chunk.data);
        self.complete = chunk.done;
        Ok(())
    }

    pub fn finish(self, group_id: RustRaftGroupId) -> Result<RaftSnapshot, RaftError> {
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
pub struct RaftSnapshotLifecycleConfig {
    pub chunk_size: u64,
    pub max_chunks_per_tick: u64,
    pub max_bytes_per_tick: u64,
    pub max_retry_attempts: u64,
}

impl Default for RaftSnapshotLifecycleConfig {
    fn default() -> Self {
        Self {
            chunk_size: 64 * 1024,
            max_chunks_per_tick: 16,
            max_bytes_per_tick: 4 * 1024 * 1024,
            max_retry_attempts: 3,
        }
    }
}

impl RaftSnapshotLifecycleConfig {
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
pub struct RaftSnapshotLifecycleStatus {
    pub snapshot_id: Option<RustRaftSnapshotId>,
    pub sending: bool,
    pub installing: bool,
    pub total_chunks: u64,
    pub sent_chunks: u64,
    pub received_chunks: u64,
    pub retry_count: u64,
    pub throttled_ticks: u64,
    pub rate_limited_ticks: u64,
    pub rolled_back: u64,
    pub completed: bool,
    pub installed_index: RustRaftLogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSnapshotSendState {
    pub group_id: RustRaftGroupId,
    pub term: RustRaftTerm,
    pub leader_id: RustRaftNodeId,
    pub chunks: Vec<RustRaftSnapshotChunk>,
    pub next_chunk: usize,
    pub accepted_chunks: u64,
    pub retry_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftSnapshotLifecycle {
    config: RaftSnapshotLifecycleConfig,
    sender: Option<RaftSnapshotSendState>,
    installer: Option<RaftSnapshotInstallState>,
    status: RaftSnapshotLifecycleStatus,
}

impl RaftSnapshotLifecycle {
    pub fn new(config: RaftSnapshotLifecycleConfig) -> Result<Self, RaftError> {
        config.validate()?;
        Ok(Self {
            config,
            sender: None,
            installer: None,
            status: RaftSnapshotLifecycleStatus {
                snapshot_id: None,
                sending: false,
                installing: false,
                total_chunks: 0,
                sent_chunks: 0,
                received_chunks: 0,
                retry_count: 0,
                throttled_ticks: 0,
                rate_limited_ticks: 0,
                rolled_back: 0,
                completed: false,
                installed_index: 0,
            },
        })
    }

    pub fn checkpoint(
        snapshot: &RaftSnapshot,
        chunk_size: u64,
    ) -> Result<Vec<RustRaftSnapshotChunk>, RaftError> {
        if chunk_size == 0 {
            return Err(RaftError::InvalidRequest(
                "snapshot chunk_size must be greater than zero".to_string(),
            ));
        }
        let mut chunks = Vec::new();
        let mut offset = 0;
        while offset < snapshot.payload.len() {
            let end = (offset + chunk_size as usize).min(snapshot.payload.len());
            chunks.push(RustRaftSnapshotChunk {
                meta: snapshot.meta.clone(),
                offset: offset as u64,
                data: snapshot.payload[offset..end].to_vec(),
                done: end == snapshot.payload.len(),
            });
            offset = end;
        }
        if chunks.is_empty() {
            chunks.push(RustRaftSnapshotChunk {
                meta: snapshot.meta.clone(),
                offset: 0,
                data: Vec::new(),
                done: true,
            });
        }
        Ok(chunks)
    }

    pub fn begin_send(
        &mut self,
        snapshot: &RaftSnapshot,
        term: RustRaftTerm,
        leader_id: RustRaftNodeId,
    ) -> Result<(), RaftError> {
        if self.sender.is_some() || self.installer.is_some() {
            return Err(RaftError::InvalidRequest(
                "snapshot lifecycle is already active".to_string(),
            ));
        }
        let chunks = Self::checkpoint(snapshot, self.config.chunk_size)?;
        self.status = RaftSnapshotLifecycleStatus {
            snapshot_id: Some(snapshot.meta.snapshot_id.clone()),
            sending: true,
            installing: false,
            total_chunks: chunks.len() as u64,
            sent_chunks: 0,
            received_chunks: 0,
            retry_count: 0,
            throttled_ticks: 0,
            rate_limited_ticks: self.status.rate_limited_ticks,
            rolled_back: self.status.rolled_back,
            completed: false,
            installed_index: 0,
        };
        self.sender = Some(RaftSnapshotSendState {
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
        limiter: &mut impl RustRaftRateLimiter,
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
                let remaining_chunk = RustRaftSnapshotChunk {
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
        sender.retry_count += 1;
        self.status.retry_count += 1;
        if sender.retry_count > self.config.max_retry_attempts {
            self.sender = None;
            self.status.sending = false;
            return Err(RaftError::Transport(
                "snapshot send retry budget exhausted".to_string(),
            ));
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
        if self.installer.is_none() {
            self.status.snapshot_id = Some(request.chunk.meta.snapshot_id.clone());
            self.status.installing = true;
            self.status.sending = false;
            self.status.completed = false;
            self.status.received_chunks = 0;
            self.status.total_chunks = 0;
            self.installer = Some(RaftSnapshotInstallState::new(request.chunk.meta.clone()));
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

    pub fn rollback_install(&mut self) {
        self.installer = None;
        self.status.installing = false;
        self.status.rolled_back += 1;
    }

    pub fn status(&self) -> RaftSnapshotLifecycleStatus {
        self.status.clone()
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
        let mut file = File::create(&path).map_err(|err| {
            RaftError::Storage(format!(
                "failed to create snapshot {}: {err}",
                path.display()
            ))
        })?;
        file.write_all(&encoded)
            .and_then(|_| file.sync_data())
            .map_err(|err| {
                RaftError::Storage(format!(
                    "failed to persist snapshot {}: {err}",
                    path.display()
                ))
            })?;
        Ok(path)
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
    ) -> Result<Vec<RustRaftSnapshotChunk>, RaftError> {
        let snapshot = self.load_checkpoint(snapshot_id)?;
        RaftSnapshotLifecycle::checkpoint(&snapshot, self.options.chunk_size)
    }

    fn snapshot_path(&self, snapshot_id: &str) -> PathBuf {
        self.options.dir.join(format!("{snapshot_id}.snapshot"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftInstallSnapshotRequest {
    pub group_id: RustRaftGroupId,
    pub term: RustRaftTerm,
    pub leader_id: RustRaftNodeId,
    pub chunk: RustRaftSnapshotChunk,
}

pub type InstallSnapshotRequest = RustRaftInstallSnapshotRequest;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftInstallSnapshotResponse {
    pub term: RustRaftTerm,
    pub accepted: bool,
    pub next_offset: u64,
    #[serde(default)]
    pub committed_index: RustRaftLogIndex,
    pub reason: String,
}

pub type InstallSnapshotResponse = RustRaftInstallSnapshotResponse;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadIndexRequest {
    pub group_id: RustRaftGroupId,
    pub requester_id: RustRaftNodeId,
    pub min_commit_index: RustRaftLogIndex,
    pub allow_lease_read: bool,
}

pub type ReadIndexRequest = RustRaftReadIndexRequest;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReadIndexResponse {
    pub safe: bool,
    pub read_index: RustRaftLogIndex,
    pub lease_read: bool,
    pub reason: String,
}

pub type ReadIndexResponse = RustRaftReadIndexResponse;
