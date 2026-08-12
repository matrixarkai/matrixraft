// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// authenticated, in-memory, TCP, and cluster transport runtime.
// Split from src/lib.rs to keep the crate facade small and focused.

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthenticatedRaftRpc<M, A = String> {
    pub auth: A,
    pub message: M,
}

pub trait RaftAuthPolicy<A = String> {
    fn token_for(&self, target: RustRaftNodeId) -> A;
    fn validate(&self, target: RustRaftNodeId, auth: &A) -> Result<(), RaftError>;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StaticRaftAuthToken {
    pub token: String,
}

impl StaticRaftAuthToken {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }
}

impl RaftAuthPolicy<String> for StaticRaftAuthToken {
    fn token_for(&self, _target: RustRaftNodeId) -> String {
        self.token.clone()
    }

    fn validate(&self, _target: RustRaftNodeId, auth: &String) -> Result<(), RaftError> {
        if auth == &self.token {
            Ok(())
        } else {
            Err(RaftError::Transport(
                "raft transport authentication failed".to_string(),
            ))
        }
    }
}

#[derive(Debug, Clone)]
pub struct AuthenticatedRaftTransport<T, A = String, P = StaticRaftAuthToken> {
    inner: T,
    policy: P,
    _auth: PhantomData<A>,
}

impl<T, A, P> AuthenticatedRaftTransport<T, A, P>
where
    P: RaftAuthPolicy<A>,
{
    pub fn new(inner: T, policy: P) -> Self {
        Self {
            inner,
            policy,
            _auth: PhantomData,
        }
    }

    pub fn inner(&self) -> &T {
        &self.inner
    }

    pub fn into_inner(self) -> T {
        self.inner
    }

    pub fn wrap_request<M>(
        &self,
        target: RustRaftNodeId,
        message: M,
    ) -> AuthenticatedRaftRpc<M, A> {
        AuthenticatedRaftRpc {
            auth: self.policy.token_for(target),
            message,
        }
    }
}

impl<T, A, P> AuthenticatedRaftTransport<T, A, P>
where
    T: RustRaftTransport,
    P: RaftAuthPolicy<A>,
{
    pub fn append_entries_authenticated(
        &self,
        target: RustRaftNodeId,
        request: AuthenticatedRaftRpc<AppendEntriesRequest, A>,
    ) -> Result<AppendEntriesResponse, RaftError> {
        self.policy.validate(target, &request.auth)?;
        self.inner.append_entries(target, request.message)
    }

    pub fn vote_authenticated(
        &self,
        target: RustRaftNodeId,
        request: AuthenticatedRaftRpc<VoteRequest, A>,
    ) -> Result<VoteResponse, RaftError> {
        self.policy.validate(target, &request.auth)?;
        self.inner.vote(target, request.message)
    }

    pub fn install_snapshot_authenticated(
        &self,
        target: RustRaftNodeId,
        request: AuthenticatedRaftRpc<InstallSnapshotRequest, A>,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        self.policy.validate(target, &request.auth)?;
        self.inner.install_snapshot(target, request.message)
    }

    pub fn read_index_authenticated(
        &self,
        target: RustRaftNodeId,
        request: AuthenticatedRaftRpc<ReadIndexRequest, A>,
    ) -> Result<ReadIndexResponse, RaftError> {
        self.policy.validate(target, &request.auth)?;
        self.inner.read_index(target, request.message)
    }
}

impl<T, A, P> RustRaftTransport for AuthenticatedRaftTransport<T, A, P>
where
    T: RustRaftTransport,
    P: RaftAuthPolicy<A>,
{
    fn append_entries(
        &self,
        target: u64,
        request: RustRaftAppendEntriesRequest,
    ) -> Result<RustRaftAppendEntriesResponse, RustRaftError> {
        self.inner.append_entries(target, request)
    }

    fn vote(
        &self,
        target: u64,
        request: RustRaftVoteRequest,
    ) -> Result<RustRaftVoteResponse, RustRaftError> {
        self.inner.vote(target, request)
    }

    fn install_snapshot(
        &self,
        target: u64,
        request: RustRaftInstallSnapshotRequest,
    ) -> Result<RustRaftInstallSnapshotResponse, RustRaftError> {
        self.inner.install_snapshot(target, request)
    }

    fn read_index(
        &self,
        target: u64,
        request: RustRaftReadIndexRequest,
    ) -> Result<RustRaftReadIndexResponse, RustRaftError> {
        self.inner.read_index(target, request)
    }
}

#[derive(Clone, Default)]
pub struct InMemoryRaftTransport {
    peers: Arc<Mutex<BTreeMap<RustRaftNodeId, Arc<dyn RustRaftTransport + Send + Sync>>>>,
    validate_messages: bool,
}

impl std::fmt::Debug for InMemoryRaftTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InMemoryRaftTransport")
            .field("validate_messages", &self.validate_messages)
            .finish_non_exhaustive()
    }
}

impl InMemoryRaftTransport {
    pub fn new() -> Self {
        Self::with_validation(true)
    }

    pub fn with_validation(validate_messages: bool) -> Self {
        Self {
            peers: Arc::new(Mutex::new(BTreeMap::new())),
            validate_messages,
        }
    }

    pub fn register<T>(&self, node_id: RustRaftNodeId, handler: T) -> Result<(), RaftError>
    where
        T: RustRaftTransport + Send + Sync + 'static,
    {
        self.register_handler(node_id, Arc::new(handler))
    }

    pub fn register_handler(
        &self,
        node_id: RustRaftNodeId,
        handler: Arc<dyn RustRaftTransport + Send + Sync>,
    ) -> Result<(), RaftError> {
        if node_id == 0 {
            return Err(RaftError::InvalidRequest(
                "in-memory raft transport node_id must be greater than zero".to_string(),
            ));
        }
        self.peers
            .lock()
            .map_err(|_| {
                RaftError::Transport("in-memory raft transport lock poisoned".to_string())
            })?
            .insert(node_id, handler);
        Ok(())
    }

    pub fn unregister(&self, node_id: RustRaftNodeId) -> Result<(), RaftError> {
        self.peers
            .lock()
            .map_err(|_| {
                RaftError::Transport("in-memory raft transport lock poisoned".to_string())
            })?
            .remove(&node_id);
        Ok(())
    }

    fn handler(
        &self,
        target: RustRaftNodeId,
    ) -> Result<Arc<dyn RustRaftTransport + Send + Sync>, RaftError> {
        if target == 0 {
            return Err(RaftError::InvalidRequest(
                "in-memory raft transport target must be greater than zero".to_string(),
            ));
        }
        self.peers
            .lock()
            .map_err(|_| {
                RaftError::Transport("in-memory raft transport lock poisoned".to_string())
            })?
            .get(&target)
            .cloned()
            .ok_or_else(|| {
                RaftError::Transport(format!(
                    "in-memory raft transport target {target} is not registered"
                ))
            })
    }
}

impl RustRaftTransport for InMemoryRaftTransport {
    fn append_entries(
        &self,
        target: u64,
        request: RustRaftAppendEntriesRequest,
    ) -> Result<RustRaftAppendEntriesResponse, RustRaftError> {
        if self.validate_messages {
            require_transport_validation(rustraft_validate_append_entries_request(&request))?;
        }
        let response = self.handler(target)?.append_entries(target, request)?;
        if self.validate_messages {
            require_transport_validation(rustraft_validate_append_entries_response(&response))?;
        }
        Ok(response)
    }

    fn vote(
        &self,
        target: u64,
        request: RustRaftVoteRequest,
    ) -> Result<RustRaftVoteResponse, RustRaftError> {
        if self.validate_messages {
            require_transport_validation(rustraft_validate_vote_request(&request))?;
        }
        let response = self.handler(target)?.vote(target, request)?;
        if self.validate_messages {
            require_transport_validation(rustraft_validate_vote_response(&response))?;
        }
        Ok(response)
    }

    fn install_snapshot(
        &self,
        target: u64,
        request: RustRaftInstallSnapshotRequest,
    ) -> Result<RustRaftInstallSnapshotResponse, RustRaftError> {
        if self.validate_messages {
            require_transport_validation(rustraft_validate_install_snapshot_request(&request))?;
        }
        let response = self.handler(target)?.install_snapshot(target, request)?;
        if self.validate_messages {
            require_transport_validation(rustraft_validate_install_snapshot_response(&response))?;
        }
        Ok(response)
    }

    fn read_index(
        &self,
        target: u64,
        request: RustRaftReadIndexRequest,
    ) -> Result<RustRaftReadIndexResponse, RustRaftError> {
        if self.validate_messages {
            require_transport_validation(rustraft_validate_read_index_request(&request))?;
        }
        let response = self.handler(target)?.read_index(target, request)?;
        if self.validate_messages {
            require_transport_validation(rustraft_validate_read_index_response(&response))?;
        }
        Ok(response)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "rpc", content = "payload", rename_all = "snake_case")]
pub enum TcpRaftTransportRequest {
    AppendEntries {
        target: RustRaftNodeId,
        request: AppendEntriesRequest,
    },
    Vote {
        target: RustRaftNodeId,
        request: VoteRequest,
    },
    InstallSnapshot {
        target: RustRaftNodeId,
        request: InstallSnapshotRequest,
    },
    ReadIndex {
        target: RustRaftNodeId,
        request: ReadIndexRequest,
    },
    Batch {
        requests: Vec<TcpRaftTransportRequest>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TcpRaftRpcResult<T> {
    pub ok: Option<T>,
    pub error: Option<String>,
}

impl<T> TcpRaftRpcResult<T> {
    fn from_result(result: Result<T, RaftError>) -> Self {
        match result {
            Ok(ok) => Self {
                ok: Some(ok),
                error: None,
            },
            Err(error) => Self {
                ok: None,
                error: Some(error.to_string()),
            },
        }
    }

    pub fn into_result(self) -> Result<T, RaftError> {
        self.ok.ok_or_else(|| {
            RaftError::Transport(self.error.unwrap_or_else(|| "raft RPC failed".to_string()))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "rpc", content = "payload", rename_all = "snake_case")]
pub enum TcpRaftTransportResponse {
    AppendEntries(TcpRaftRpcResult<AppendEntriesResponse>),
    Vote(TcpRaftRpcResult<VoteResponse>),
    InstallSnapshot(TcpRaftRpcResult<InstallSnapshotResponse>),
    ReadIndex(TcpRaftRpcResult<ReadIndexResponse>),
    Batch(Vec<TcpRaftTransportResponse>),
}

#[derive(Debug, Clone, Default)]
pub struct TcpRaftTransport {
    peers: BTreeMap<RustRaftNodeId, String>,
}

impl TcpRaftTransport {
    pub fn new(peers: BTreeMap<RustRaftNodeId, String>) -> Self {
        Self { peers }
    }

    pub fn set_peer_addr(&mut self, node_id: RustRaftNodeId, addr: impl Into<String>) {
        self.peers.insert(node_id, addr.into());
    }

    fn send_rpc(
        &self,
        target: RustRaftNodeId,
        request: TcpRaftTransportRequest,
    ) -> Result<TcpRaftTransportResponse, RaftError> {
        let addr = self.peers.get(&target).ok_or_else(|| {
            RaftError::Transport(format!("raft transport target {target} has no address"))
        })?;
        let mut stream = TcpStream::connect(addr)
            .map_err(|err| RaftError::Transport(format!("failed to connect to {addr}: {err}")))?;
        let encoded = serde_json::to_vec(&request)
            .map_err(|err| RaftError::Transport(format!("failed to encode raft RPC: {err}")))?;
        stream
            .write_all(&encoded)
            .and_then(|_| stream.write_all(b"\n"))
            .map_err(|err| RaftError::Transport(format!("failed to write raft RPC: {err}")))?;
        let mut response = String::new();
        BufReader::new(stream)
            .read_line(&mut response)
            .map_err(|err| RaftError::Transport(format!("failed to read raft RPC: {err}")))?;
        serde_json::from_str(&response)
            .map_err(|err| RaftError::Transport(format!("failed to decode raft RPC: {err}")))
    }

    pub fn send_batch_rpc(
        &self,
        target: RustRaftNodeId,
        requests: Vec<TcpRaftTransportRequest>,
    ) -> Result<Vec<TcpRaftTransportResponse>, RaftError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        for request in &requests {
            require_transport_validation(rustraft_validate_tcp_transport_request(request))?;
        }
        match self.send_rpc(target, TcpRaftTransportRequest::Batch { requests })? {
            TcpRaftTransportResponse::Batch(responses) => Ok(responses),
            _ => Err(RaftError::Transport(
                "unexpected batch RPC response".to_string(),
            )),
        }
    }
}

impl RustRaftTransport for TcpRaftTransport {
    fn append_entries(
        &self,
        target: u64,
        request: RustRaftAppendEntriesRequest,
    ) -> Result<RustRaftAppendEntriesResponse, RustRaftError> {
        require_transport_validation(rustraft_validate_append_entries_request(&request))?;
        match self.send_rpc(
            target,
            TcpRaftTransportRequest::AppendEntries { target, request },
        )? {
            TcpRaftTransportResponse::AppendEntries(response) => {
                let response = response.into_result()?;
                require_transport_validation(rustraft_validate_append_entries_response(&response))?;
                Ok(response)
            }
            _ => Err(RaftError::Transport(
                "unexpected append-entries RPC response".to_string(),
            )),
        }
    }

    fn vote(
        &self,
        target: u64,
        request: RustRaftVoteRequest,
    ) -> Result<RustRaftVoteResponse, RustRaftError> {
        require_transport_validation(rustraft_validate_vote_request(&request))?;
        match self.send_rpc(target, TcpRaftTransportRequest::Vote { target, request })? {
            TcpRaftTransportResponse::Vote(response) => {
                let response = response.into_result()?;
                require_transport_validation(rustraft_validate_vote_response(&response))?;
                Ok(response)
            }
            _ => Err(RaftError::Transport(
                "unexpected vote RPC response".to_string(),
            )),
        }
    }

    fn install_snapshot(
        &self,
        target: u64,
        request: RustRaftInstallSnapshotRequest,
    ) -> Result<RustRaftInstallSnapshotResponse, RustRaftError> {
        require_transport_validation(rustraft_validate_install_snapshot_request(&request))?;
        match self.send_rpc(
            target,
            TcpRaftTransportRequest::InstallSnapshot { target, request },
        )? {
            TcpRaftTransportResponse::InstallSnapshot(response) => {
                let response = response.into_result()?;
                require_transport_validation(rustraft_validate_install_snapshot_response(
                    &response,
                ))?;
                Ok(response)
            }
            _ => Err(RaftError::Transport(
                "unexpected install-snapshot RPC response".to_string(),
            )),
        }
    }

    fn read_index(
        &self,
        target: u64,
        request: RustRaftReadIndexRequest,
    ) -> Result<RustRaftReadIndexResponse, RustRaftError> {
        require_transport_validation(rustraft_validate_read_index_request(&request))?;
        match self.send_rpc(
            target,
            TcpRaftTransportRequest::ReadIndex { target, request },
        )? {
            TcpRaftTransportResponse::ReadIndex(response) => {
                let response = response.into_result()?;
                require_transport_validation(rustraft_validate_read_index_response(&response))?;
                Ok(response)
            }
            _ => Err(RaftError::Transport(
                "unexpected read-index RPC response".to_string(),
            )),
        }
    }
}

pub struct TcpRaftTransportServer {
    addr: String,
    shutdown: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl TcpRaftTransportServer {
    pub fn start<T>(addr: impl Into<String>, handler: Arc<T>) -> Result<Self, RaftError>
    where
        T: RustRaftTransport + Send + Sync + 'static,
    {
        let listener = TcpListener::bind(addr.into()).map_err(|err| {
            RaftError::Transport(format!("failed to bind raft TCP server: {err}"))
        })?;
        listener.set_nonblocking(true).map_err(|err| {
            RaftError::Transport(format!("failed to configure raft TCP server: {err}"))
        })?;
        let addr = listener
            .local_addr()
            .map_err(|err| RaftError::Transport(format!("failed to read raft TCP addr: {err}")))?
            .to_string();
        let shutdown = Arc::new(AtomicBool::new(false));
        let worker_shutdown = Arc::clone(&shutdown);
        let worker = thread::Builder::new()
            .name("rustraft-tcp-transport".to_string())
            .spawn(move || {
                while !worker_shutdown.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let _ = handle_tcp_raft_stream(stream, handler.as_ref());
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(_) => break,
                    }
                }
            })
            .map_err(|err| {
                RaftError::Transport(format!("failed to spawn raft TCP server: {err}"))
            })?;
        Ok(Self {
            addr,
            shutdown,
            worker: Some(worker),
        })
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub fn shutdown(&mut self) -> Result<(), RaftError> {
        self.shutdown.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(&self.addr);
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| RaftError::Transport("raft TCP server panicked".to_string()))?;
        }
        Ok(())
    }
}

impl Drop for TcpRaftTransportServer {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn handle_tcp_raft_stream<T>(stream: TcpStream, handler: &T) -> Result<(), RaftError>
where
    T: RustRaftTransport + ?Sized,
{
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .map_err(|err| RaftError::Transport(format!("failed to read raft TCP request: {err}")))?;
    if line.trim().is_empty() {
        return Ok(());
    }
    let request: TcpRaftTransportRequest = serde_json::from_str(&line)
        .map_err(|err| RaftError::Transport(format!("failed to decode raft TCP request: {err}")))?;
    require_transport_validation(rustraft_validate_tcp_transport_request(&request))?;
    let response = handle_tcp_raft_request(request, handler);
    let encoded = serde_json::to_vec(&response).map_err(|err| {
        RaftError::Transport(format!("failed to encode raft TCP response: {err}"))
    })?;
    let stream = reader.get_mut();
    stream
        .write_all(&encoded)
        .and_then(|_| stream.write_all(b"\n"))
        .map_err(|err| RaftError::Transport(format!("failed to write raft TCP response: {err}")))?;
    Ok(())
}

fn handle_tcp_raft_request<T>(
    request: TcpRaftTransportRequest,
    handler: &T,
) -> TcpRaftTransportResponse
where
    T: RustRaftTransport + ?Sized,
{
    match request {
        TcpRaftTransportRequest::AppendEntries { target, request } => {
            TcpRaftTransportResponse::AppendEntries(TcpRaftRpcResult::from_result(
                handler.append_entries(target, request),
            ))
        }
        TcpRaftTransportRequest::Vote { target, request } => TcpRaftTransportResponse::Vote(
            TcpRaftRpcResult::from_result(handler.vote(target, request)),
        ),
        TcpRaftTransportRequest::InstallSnapshot { target, request } => {
            TcpRaftTransportResponse::InstallSnapshot(TcpRaftRpcResult::from_result(
                handler.install_snapshot(target, request),
            ))
        }
        TcpRaftTransportRequest::ReadIndex { target, request } => {
            TcpRaftTransportResponse::ReadIndex(TcpRaftRpcResult::from_result(
                handler.read_index(target, request),
            ))
        }
        TcpRaftTransportRequest::Batch { requests } => TcpRaftTransportResponse::Batch(
            requests
                .into_iter()
                .map(|request| handle_tcp_raft_request(request, handler))
                .collect(),
        ),
    }
}

#[derive(Debug, Clone)]
pub struct ClusterRaftTransport {
    cluster: Arc<Mutex<RaftCluster>>,
}

impl ClusterRaftTransport {
    pub fn new(cluster: Arc<Mutex<RaftCluster>>) -> Self {
        Self { cluster }
    }
}

impl RustRaftTransport for ClusterRaftTransport {
    fn append_entries(
        &self,
        target: u64,
        request: RustRaftAppendEntriesRequest,
    ) -> Result<RustRaftAppendEntriesResponse, RustRaftError> {
        self.cluster
            .lock()
            .map_err(|_| RaftError::Transport("raft cluster transport lock poisoned".to_string()))?
            .append_entries_to(target, request)
    }

    fn vote(
        &self,
        target: u64,
        request: RustRaftVoteRequest,
    ) -> Result<RustRaftVoteResponse, RustRaftError> {
        self.cluster
            .lock()
            .map_err(|_| RaftError::Transport("raft cluster transport lock poisoned".to_string()))?
            .vote_to(target, request)
    }

    fn install_snapshot(
        &self,
        target: u64,
        request: RustRaftInstallSnapshotRequest,
    ) -> Result<RustRaftInstallSnapshotResponse, RustRaftError> {
        self.cluster
            .lock()
            .map_err(|_| RaftError::Transport("raft cluster transport lock poisoned".to_string()))?
            .install_snapshot_chunk_to(target, request)
    }

    fn read_index(
        &self,
        _target: u64,
        request: RustRaftReadIndexRequest,
    ) -> Result<RustRaftReadIndexResponse, RustRaftError> {
        self.cluster
            .lock()
            .map_err(|_| RaftError::Transport("raft cluster transport lock poisoned".to_string()))?
            .read_index(request)
    }
}
