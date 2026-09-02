// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// authenticated, in-memory, TCP, and cluster transport runtime.
// Split from src/lib.rs to keep the crate facade small and focused.

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthenticatedRaftRpc<M, A = String> {
    pub auth: A,
    pub message: M,
}

pub trait AuthPolicy<A = String> {
    fn token_for(&self, target: NodeId) -> A;
    fn validate(&self, target: NodeId, auth: &A) -> Result<(), RaftError>;
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

impl AuthPolicy<String> for StaticRaftAuthToken {
    fn token_for(&self, _target: NodeId) -> String {
        self.token.clone()
    }

    fn validate(&self, _target: NodeId, auth: &String) -> Result<(), RaftError> {
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
    P: AuthPolicy<A>,
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
        target: NodeId,
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
    T: Transport,
    P: AuthPolicy<A>,
{
    pub fn append_entries_authenticated(
        &self,
        target: NodeId,
        request: AuthenticatedRaftRpc<AppendEntriesRequest, A>,
    ) -> Result<AppendEntriesResponse, RaftError> {
        self.policy.validate(target, &request.auth)?;
        self.inner.append_entries(target, request.message)
    }

    pub fn vote_authenticated(
        &self,
        target: NodeId,
        request: AuthenticatedRaftRpc<VoteRequest, A>,
    ) -> Result<VoteResponse, RaftError> {
        self.policy.validate(target, &request.auth)?;
        self.inner.vote(target, request.message)
    }

    pub fn install_snapshot_authenticated(
        &self,
        target: NodeId,
        request: AuthenticatedRaftRpc<InstallSnapshotRequest, A>,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        self.policy.validate(target, &request.auth)?;
        self.inner.install_snapshot(target, request.message)
    }

    pub fn read_index_authenticated(
        &self,
        target: NodeId,
        request: AuthenticatedRaftRpc<ReadIndexRequest, A>,
    ) -> Result<ReadIndexResponse, RaftError> {
        self.policy.validate(target, &request.auth)?;
        self.inner.read_index(target, request.message)
    }
}

impl<T, A, P> Transport for AuthenticatedRaftTransport<T, A, P>
where
    T: Transport,
    P: AuthPolicy<A>,
{
    fn append_entries(
        &self,
        target: u64,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        self.inner.append_entries(target, request)
    }

    fn vote(
        &self,
        target: u64,
        request: VoteRequest,
    ) -> Result<VoteResponse, RaftError> {
        self.inner.vote(target, request)
    }

    fn install_snapshot(
        &self,
        target: u64,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        self.inner.install_snapshot(target, request)
    }

    fn read_index(
        &self,
        target: u64,
        request: ReadIndexRequest,
    ) -> Result<ReadIndexResponse, RaftError> {
        self.inner.read_index(target, request)
    }
}

#[derive(Clone, Default)]
pub struct InMemoryRaftTransport {
    peers: Arc<Mutex<BTreeMap<NodeId, Arc<dyn Transport + Send + Sync>>>>,
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

    pub fn register<T>(&self, node_id: NodeId, handler: T) -> Result<(), RaftError>
    where
        T: Transport + Send + Sync + 'static,
    {
        self.register_handler(node_id, Arc::new(handler))
    }

    pub fn register_handler(
        &self,
        node_id: NodeId,
        handler: Arc<dyn Transport + Send + Sync>,
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

    pub fn unregister(&self, node_id: NodeId) -> Result<(), RaftError> {
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
        target: NodeId,
    ) -> Result<Arc<dyn Transport + Send + Sync>, RaftError> {
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

impl Transport for InMemoryRaftTransport {
    fn append_entries(
        &self,
        target: u64,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        if self.validate_messages {
            require_transport_validation(matrixraft_validate_append_entries_request(&request))?;
        }
        let response = self.handler(target)?.append_entries(target, request)?;
        if self.validate_messages {
            require_transport_validation(matrixraft_validate_append_entries_response(&response))?;
        }
        Ok(response)
    }

    fn vote(
        &self,
        target: u64,
        request: VoteRequest,
    ) -> Result<VoteResponse, RaftError> {
        if self.validate_messages {
            require_transport_validation(matrixraft_validate_vote_request(&request))?;
        }
        let response = self.handler(target)?.vote(target, request)?;
        if self.validate_messages {
            require_transport_validation(matrixraft_validate_vote_response(&response))?;
        }
        Ok(response)
    }

    fn install_snapshot(
        &self,
        target: u64,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        if self.validate_messages {
            require_transport_validation(matrixraft_validate_install_snapshot_request(&request))?;
        }
        let response = self.handler(target)?.install_snapshot(target, request)?;
        if self.validate_messages {
            require_transport_validation(matrixraft_validate_install_snapshot_response(&response))?;
        }
        Ok(response)
    }

    fn read_index(
        &self,
        target: u64,
        request: ReadIndexRequest,
    ) -> Result<ReadIndexResponse, RaftError> {
        if self.validate_messages {
            require_transport_validation(matrixraft_validate_read_index_request(&request))?;
        }
        let response = self.handler(target)?.read_index(target, request)?;
        if self.validate_messages {
            require_transport_validation(matrixraft_validate_read_index_response(&response))?;
        }
        Ok(response)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "rpc", content = "payload", rename_all = "snake_case")]
pub enum TcpRaftTransportRequest {
    AppendEntries {
        target: NodeId,
        request: AppendEntriesRequest,
    },
    Vote {
        target: NodeId,
        request: VoteRequest,
    },
    InstallSnapshot {
        target: NodeId,
        request: InstallSnapshotRequest,
    },
    ReadIndex {
        target: NodeId,
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
    peers: BTreeMap<NodeId, String>,
    /// Connections kept open per peer, so an RPC does not pay a TCP handshake.
    ///
    /// Shared across clones on purpose: a cloned transport talks to the same
    /// peers, and a per-clone pool would only multiply idle sockets. The reader
    /// is pooled rather than the raw stream so that anything a read buffered
    /// stays with its connection instead of being dropped.
    idle: Arc<Mutex<BTreeMap<NodeId, Vec<BufReader<TcpStream>>>>>,
}

/// Enough to keep a leader's concurrent RPCs to one follower off the handshake
/// path, without holding sockets open for a peer that has gone quiet.
const MAX_IDLE_CONNECTIONS_PER_PEER: usize = 4;

impl TcpRaftTransport {
    fn take_idle(&self, target: NodeId) -> Option<BufReader<TcpStream>> {
        self.idle.lock().ok()?.get_mut(&target)?.pop()
    }

    fn store_idle(&self, target: NodeId, connection: BufReader<TcpStream>) {
        if let Ok(mut idle) = self.idle.lock() {
            let pooled = idle.entry(target).or_default();
            if pooled.len() < MAX_IDLE_CONNECTIONS_PER_PEER {
                pooled.push(connection);
            }
        }
    }

    /// Sends one framed request and reads one framed response.
    ///
    /// Hands the connection back so a successful exchange can return it to the
    /// pool. A failed one drops it, which is what closes a broken socket.
    fn exchange(
        &self,
        mut connection: BufReader<TcpStream>,
        encoded: &[u8],
    ) -> Result<(String, BufReader<TcpStream>), RaftError> {
        connection
            .get_mut()
            .write_all(encoded)
            .map_err(|err| RaftError::Transport(format!("failed to write raft RPC: {err}")))?;
        let mut response = String::new();
        connection
            .read_line(&mut response)
            .map_err(|err| RaftError::Transport(format!("failed to read raft RPC: {err}")))?;
        if response.is_empty() {
            return Err(RaftError::Transport(
                "raft RPC connection closed before a response".to_string(),
            ));
        }
        Ok((response, connection))
    }

    pub fn new(peers: BTreeMap<NodeId, String>) -> Self {
        Self {
            peers,
            idle: Arc::default(),
        }
    }

    pub fn set_peer_addr(&mut self, node_id: NodeId, addr: impl Into<String>) {
        self.peers.insert(node_id, addr.into());
    }

    fn send_rpc(
        &self,
        target: NodeId,
        request: TcpRaftTransportRequest,
    ) -> Result<TcpRaftTransportResponse, RaftError> {
        let addr = self.peers.get(&target).ok_or_else(|| {
            RaftError::Transport(format!("raft transport target {target} has no address"))
        })?;
        let mut encoded = serde_json::to_vec(&request)
            .map_err(|err| RaftError::Transport(format!("failed to encode raft RPC: {err}")))?;
        // One write, not a body followed by a one-byte newline: the peer reads
        // a line, so splitting them puts a round trip between the request and
        // the peer being able to see it.
        encoded.push(b'\n');

        // A pooled connection can have been closed by the peer while it sat
        // idle, and there is no way to learn that but to use it. So a failure
        // on a *pooled* connection is retried once on a fresh one, while a
        // failure on a fresh connection is returned. The retry is sound because
        // it only happens where the peer had closed the socket, and because
        // these are the RPCs Raft already re-sends: an AppendEntries or a Vote
        // at the same term from the same sender lands the same way twice.
        if let Some(connection) = self.take_idle(target) {
            if let Ok((response, connection)) = self.exchange(connection, &encoded) {
                self.store_idle(target, connection);
                return serde_json::from_str(&response).map_err(|err| {
                    RaftError::Transport(format!("failed to decode raft RPC: {err}"))
                });
            }
        }

        let stream = TcpStream::connect(addr)
            .map_err(|err| RaftError::Transport(format!("failed to connect to {addr}: {err}")))?;
        // Raft RPCs are small request/response exchanges, which is the shape
        // Nagle delays. Without this the framing newline below can sit in the
        // sender's buffer waiting for an ACK the peer will not send until it
        // has the newline -- the classic write/write/read stall.
        let _ = stream.set_nodelay(true);
        let (response, connection) = self.exchange(BufReader::new(stream), &encoded)?;
        self.store_idle(target, connection);
        serde_json::from_str(&response)
            .map_err(|err| RaftError::Transport(format!("failed to decode raft RPC: {err}")))
    }

    pub fn send_batch_rpc(
        &self,
        target: NodeId,
        requests: Vec<TcpRaftTransportRequest>,
    ) -> Result<Vec<TcpRaftTransportResponse>, RaftError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        for request in &requests {
            require_transport_validation(matrixraft_validate_tcp_transport_request(request))?;
        }
        match self.send_rpc(target, TcpRaftTransportRequest::Batch { requests })? {
            TcpRaftTransportResponse::Batch(responses) => Ok(responses),
            _ => Err(RaftError::Transport(
                "unexpected batch RPC response".to_string(),
            )),
        }
    }
}

impl Transport for TcpRaftTransport {
    fn append_entries(
        &self,
        target: u64,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        require_transport_validation(matrixraft_validate_append_entries_request(&request))?;
        match self.send_rpc(
            target,
            TcpRaftTransportRequest::AppendEntries { target, request },
        )? {
            TcpRaftTransportResponse::AppendEntries(response) => {
                let response = response.into_result()?;
                require_transport_validation(matrixraft_validate_append_entries_response(&response))?;
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
        request: VoteRequest,
    ) -> Result<VoteResponse, RaftError> {
        require_transport_validation(matrixraft_validate_vote_request(&request))?;
        match self.send_rpc(target, TcpRaftTransportRequest::Vote { target, request })? {
            TcpRaftTransportResponse::Vote(response) => {
                let response = response.into_result()?;
                require_transport_validation(matrixraft_validate_vote_response(&response))?;
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
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        require_transport_validation(matrixraft_validate_install_snapshot_request(&request))?;
        match self.send_rpc(
            target,
            TcpRaftTransportRequest::InstallSnapshot { target, request },
        )? {
            TcpRaftTransportResponse::InstallSnapshot(response) => {
                let response = response.into_result()?;
                require_transport_validation(matrixraft_validate_install_snapshot_response(
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
        request: ReadIndexRequest,
    ) -> Result<ReadIndexResponse, RaftError> {
        require_transport_validation(matrixraft_validate_read_index_request(&request))?;
        match self.send_rpc(
            target,
            TcpRaftTransportRequest::ReadIndex { target, request },
        )? {
            TcpRaftTransportResponse::ReadIndex(response) => {
                let response = response.into_result()?;
                require_transport_validation(matrixraft_validate_read_index_response(&response))?;
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
        T: Transport + Send + Sync + 'static,
    {
        let listener = TcpListener::bind(addr.into()).map_err(|err| {
            RaftError::Transport(format!("failed to bind raft TCP server: {err}"))
        })?;
        // Deliberately blocking. This used to poll with a 5ms sleep between
        // `accept` attempts, which put 0-5ms in front of every inbound RPC --
        // measured at a 5.35ms median on loopback, where the round trip itself
        // costs about 275us. `shutdown` already wakes a blocked `accept` by
        // connecting to its own address, so nothing needed the poll.
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
                            // `shutdown` wakes this thread by connecting to us,
                            // so re-check before serving what may be that
                            // wake-up rather than a peer.
                            if worker_shutdown.load(Ordering::Relaxed) {
                                break;
                            }
                            let _ = stream.set_nodelay(true);
                            // A connection is served on its own thread now. It
                            // used to be served inline here, so one peer's RPC
                            // blocked every other peer's for its whole round
                            // trip -- and with connections kept alive that
                            // would have been a stall for as long as the
                            // connection lived rather than for one exchange.
                            let conn_handler = Arc::clone(&handler);
                            let conn_shutdown = Arc::clone(&worker_shutdown);
                            let spawned = thread::Builder::new()
                                .name("rustraft-tcp-conn".to_string())
                                .spawn(move || {
                                    let _ = serve_tcp_raft_connection(
                                        stream,
                                        conn_handler.as_ref(),
                                        &conn_shutdown,
                                    );
                                });
                            if spawned.is_err() {
                                // Out of threads. Drop the connection rather
                                // than serve it here and stall the listener;
                                // the peer sees a closed socket and retries.
                                continue;
                            }
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

/// Serves one connection until the peer closes it.
///
/// This used to serve exactly one request and hang up, which made every RPC pay
/// a TCP handshake. The loop is what lets a caller keep the connection.
fn serve_tcp_raft_connection<T>(
    stream: TcpStream,
    handler: &T,
    shutdown: &AtomicBool,
) -> Result<(), RaftError>
where
    T: Transport + ?Sized,
{
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }
        line.clear();
        let read = reader.read_line(&mut line).map_err(|err| {
            RaftError::Transport(format!("failed to read raft TCP request: {err}"))
        })?;
        // A zero-length read is the peer closing, which is the ordinary way a
        // kept-alive connection ends rather than a failure.
        if read == 0 {
            return Ok(());
        }
        if line.trim().is_empty() {
            continue;
        }
        // Re-checked after the read, not only before it. A thread parked in
        // `read_line` when shutdown was signalled would otherwise wake on the
        // next request and serve it, so a kept-alive connection could be served
        // by a server that had already been shut down.
        if shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }
        let request: TcpRaftTransportRequest = serde_json::from_str(&line).map_err(|err| {
            RaftError::Transport(format!("failed to decode raft TCP request: {err}"))
        })?;
        require_transport_validation(matrixraft_validate_tcp_transport_request(&request))?;
        let response = handle_tcp_raft_request(request, handler);
        let mut encoded = serde_json::to_vec(&response).map_err(|err| {
            RaftError::Transport(format!("failed to encode raft TCP response: {err}"))
        })?;
        // One write, for the same reason the request side does it: the caller
        // is reading a line, so a separate one-byte newline puts a round trip
        // between the response and the caller being able to see it.
        encoded.push(b'\n');
        reader.get_mut().write_all(&encoded).map_err(|err| {
            RaftError::Transport(format!("failed to write raft TCP response: {err}"))
        })?;
    }
}

fn handle_tcp_raft_request<T>(
    request: TcpRaftTransportRequest,
    handler: &T,
) -> TcpRaftTransportResponse
where
    T: Transport + ?Sized,
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

impl Transport for ClusterRaftTransport {
    fn append_entries(
        &self,
        target: u64,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        self.cluster
            .lock()
            .map_err(|_| RaftError::Transport("raft cluster transport lock poisoned".to_string()))?
            .append_entries_to(target, request)
    }

    fn vote(
        &self,
        target: u64,
        request: VoteRequest,
    ) -> Result<VoteResponse, RaftError> {
        self.cluster
            .lock()
            .map_err(|_| RaftError::Transport("raft cluster transport lock poisoned".to_string()))?
            .vote_to(target, request)
    }

    fn install_snapshot(
        &self,
        target: u64,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        self.cluster
            .lock()
            .map_err(|_| RaftError::Transport("raft cluster transport lock poisoned".to_string()))?
            .install_snapshot_chunk_to(target, request)
    }

    fn read_index(
        &self,
        _target: u64,
        request: ReadIndexRequest,
    ) -> Result<ReadIndexResponse, RaftError> {
        self.cluster
            .lock()
            .map_err(|_| RaftError::Transport("raft cluster transport lock poisoned".to_string()))?
            .read_index(request)
    }
}
