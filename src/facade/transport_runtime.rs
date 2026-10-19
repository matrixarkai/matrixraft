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

/// The cluster id that talks to every cluster, and that every cluster answers.
///
/// This is the default, so a transport and a server that were built without
/// one behave exactly as they did before the id existed. It is also what a
/// peer that predates the field decodes as, because the field is
/// `serde(default)`: an unstamped request is an any-cluster request rather
/// than a request from cluster 0.
pub const ANY_CLUSTER_ID: u64 = 0;

/// Whether a request stamped `sender` may be served by a node in `local`.
///
/// Equal ids talk. Either side being [`ANY_CLUSTER_ID`] talks, which is what
/// keeps a configured cluster reachable from an unconfigured tool and makes
/// the whole check opt-in. Two different non-zero ids do not talk, and that
/// is the entire point: node ids are small integers, and two clusters that
/// both number their nodes from 1 will happily append each other's entries
/// if nothing stops them.
pub fn raft_clusters_may_talk(local: u64, sender: u64) -> bool {
    local == ANY_CLUSTER_ID || sender == ANY_CLUSTER_ID || local == sender
}

/// A request with the sending transport's cluster id on it.
///
/// A separate struct rather than a field on each variant because the cluster
/// id belongs to the transport, not to the RPC -- and because `Batch` would
/// otherwise have to carry one per nested request and could disagree with
/// itself.
///
/// `flatten` keeps the wire shape of the request exactly as it was, so this
/// is compatible in both directions: a peer that predates the field sends no
/// `cluster_id` and reads as [`ANY_CLUSTER_ID`], and a peer that predates it
/// ignores the extra field on arrival.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TcpRaftTransportEnvelope {
    /// The cluster the sender believes it is in. [`ANY_CLUSTER_ID`] when it
    /// was not told, or when the sender predates this field.
    #[serde(default)]
    pub cluster_id: u64,
    #[serde(flatten)]
    pub request: TcpRaftTransportRequest,
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

#[derive(Clone, Default)]
pub struct TcpRaftTransport {
    /// Behind a lock because an address can change while a send is in flight:
    /// that is the whole point of a resolver. A send reads the address and
    /// releases the lock before it connects, so a slow connect never blocks a
    /// peer being moved.
    peers: Arc<Mutex<BTreeMap<NodeId, String>>>,
    /// Connections kept open per peer, so an RPC does not pay a TCP handshake.
    ///
    /// Shared across clones on purpose: a cloned transport talks to the same
    /// peers, and a per-clone pool would only multiply idle sockets. The reader
    /// is pooled rather than the raw stream so that anything a read buffered
    /// stays with its connection instead of being dropped.
    idle: Arc<Mutex<BTreeMap<NodeId, Vec<BufReader<TcpStream>>>>>,
    /// Stamped on every request this transport sends. [`ANY_CLUSTER_ID`]
    /// unless the host asked for one, which is why `Default` is still right.
    cluster_id: u64,
    /// Consulted when a connect fails, if the host said addresses move.
    /// `None` is the static map this transport has always been.
    resolver: Option<Arc<dyn HeartbeatAddressResolver + Send + Sync>>,
    /// The `from` a resolver is asked with. A resolver is allowed to answer
    /// differently depending on who wants to know -- the baseline's resolver
    /// takes both ends too, and a host behind more than one network needs it.
    local_node_id: NodeId,
    re_resolved: Arc<AtomicU64>,
    /// Applied to reads and writes on every connection this transport opens.
    /// `None` blocks indefinitely, which is what this transport did before the
    /// field existed and what [`TcpRaftTransport::new`] still does.
    timeout: Option<Duration>,
}

// Hand-written because a resolver is not `Debug`, and requiring that of a
// host's service discovery would be a strange thing to demand.
impl std::fmt::Debug for TcpRaftTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpRaftTransport")
            .field("peers", &self.peers.lock().ok().map(|peers| peers.len()))
            .field("cluster_id", &self.cluster_id)
            .field("resolver", &self.resolver.is_some())
            .field("local_node_id", &self.local_node_id)
            .field("re_resolved", &self.re_resolved.load(Ordering::Relaxed))
            .field("timeout", &self.timeout)
            .finish()
    }
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
    /// Puts this transport's timeout on a connection.
    ///
    /// Called for a fresh connection and again for a pooled one, because a
    /// pooled connection may have been opened by a clone of this transport
    /// that carried a different timeout -- the pool is shared, the timeout is
    /// per transport.
    fn apply_timeout(&self, stream: &TcpStream) {
        if let Some(timeout) = self.timeout {
            let _ = stream.set_read_timeout(Some(timeout));
            let _ = stream.set_write_timeout(Some(timeout));
        }
    }

    fn exchange(
        &self,
        mut connection: BufReader<TcpStream>,
        encoded: &[u8],
    ) -> Result<(String, BufReader<TcpStream>), RaftError> {
        self.apply_timeout(connection.get_ref());
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

    /// A transport with no timeout: a send to a peer that accepts and never
    /// answers waits for ever. See [`TcpRaftTransport::with_timeout`], and
    /// note that [`MatrixRaftTransportOptions::into_tcp_transport`] applies
    /// the one its options carry.
    pub fn new(peers: BTreeMap<NodeId, String>) -> Self {
        Self::new_in_cluster(peers, ANY_CLUSTER_ID)
    }

    /// A transport that stamps `cluster_id` on everything it sends.
    ///
    /// A server built with a different non-zero id refuses the request
    /// instead of serving it. See [`raft_clusters_may_talk`] for which pairs
    /// of ids talk, and [`TcpRaftTransportServer::start_in_cluster`] for the
    /// half that does the refusing -- stamping alone guards nothing, because
    /// it is the receiver that decides.
    pub fn new_in_cluster(peers: BTreeMap<NodeId, String>, cluster_id: u64) -> Self {
        Self {
            peers: Arc::new(Mutex::new(peers)),
            idle: Arc::default(),
            cluster_id,
            resolver: None,
            local_node_id: 0,
            re_resolved: Arc::default(),
            timeout: None,
        }
    }

    /// Bounds how long a send will wait for a peer that has stopped answering.
    ///
    /// **Without this a send can wait for ever.** A peer that accepts the
    /// connection and then never writes -- a process wedged, a host paging, a
    /// container frozen -- leaves the read with nothing to return and nothing
    /// to fail on. The caller is a Raft send path, so for ever means the
    /// leader stops replicating to anybody on that thread, not just to the
    /// peer that went quiet.
    ///
    /// Applied to **reads and writes**, not to the connect. A connect to a
    /// port nothing is listening on fails at once; a connect that hangs is a
    /// dropped-packet route, and bounding it needs a `SocketAddr`, which means
    /// refusing the hostnames `TcpStream::connect` accepts. That trade is the
    /// host's to make, so the OS timeout stands there and this says so rather
    /// than implying a bound it does not give.
    ///
    /// A timed-out connection is **dropped, not pooled**: the peer may still
    /// answer the old request later, and a connection with a stale response
    /// waiting on it would hand that response to the next caller.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// What a send will wait for a peer, or `None` if it will wait for ever.
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    /// Re-resolves a peer's address when a connect to it fails.
    ///
    /// This is what makes an address *map* rather than an address list. A peer
    /// that moved from one host to another leaves every send to it failing at
    /// the connect, for ever, because nothing in Raft re-asks where it is --
    /// and [`TcpRaftTransport::set_peer_addr`] only helps a host that already
    /// knows. With a resolver attached, the first failure re-asks, applies a
    /// changed answer (dropping the connections pooled to the old host), and
    /// retries the send once.
    ///
    /// Deliberately only on failure. Re-resolving before every send would put
    /// the host's service discovery on the Raft hot path, which is the kind of
    /// cost that does not show up until the discovery service is slow.
    ///
    /// A resolver that returns the *same* address is not an error and is not
    /// counted: it is the ordinary answer when the peer is simply down.
    pub fn watching_addresses(
        mut self,
        local_node_id: NodeId,
        resolver: Arc<dyn HeartbeatAddressResolver + Send + Sync>,
    ) -> Self {
        self.local_node_id = local_node_id;
        self.resolver = Some(resolver);
        self
    }

    /// Points a peer at a new address, and drops the connections pooled to its
    /// old one.
    ///
    /// Returns the address it replaced, or `None` for a peer this transport did
    /// not know.
    ///
    /// The pool is the part that is easy to miss. Up to
    /// `MAX_IDLE_CONNECTIONS_PER_PEER` connections are kept open per peer, and
    /// they are connected to the host this transport knew when they were
    /// opened. Replacing the address without dropping them left the next
    /// `MAX_IDLE_CONNECTIONS_PER_PEER` sends going to the **old** host -- a
    /// peer that had moved kept receiving raft RPCs at an address it no longer
    /// answers on, until those sockets happened to break.
    ///
    /// Only an address that actually changes drops anything. Re-setting the
    /// same address is how a host that polls its service discovery would call
    /// this, and throwing away a warm pool on every poll would undo the pooling
    /// it exists for.
    pub fn set_peer_addr(&self, node_id: NodeId, addr: impl Into<String>) -> Option<String> {
        let addr = addr.into();
        let previous = match self.peers.lock() {
            Ok(mut peers) => peers.insert(node_id, addr.clone()),
            Err(_) => return None,
        };
        if previous.as_deref() != Some(addr.as_str()) {
            if let Ok(mut idle) = self.idle.lock() {
                idle.remove(&node_id);
            }
        }
        previous
    }

    /// Asks the resolver where a peer is now, and applies the answer.
    ///
    /// The half a host drives itself: service discovery says a peer moved, and
    /// this is how that reaches the transport without waiting for a send to
    /// fail first. Returns the address that was replaced, as
    /// [`TcpRaftTransport::set_peer_addr`] does, or `Ok(None)` when the answer
    /// was the address already held.
    ///
    /// `Err` when there is no resolver, or when the resolver refuses: a host
    /// calling this wants to know that nothing happened, rather than read a
    /// quiet `None` as "already up to date".
    pub fn refresh_peer_addr(&self, node_id: NodeId) -> Result<Option<String>, RaftError> {
        let resolver = self.resolver.as_ref().ok_or_else(|| {
            RaftError::Transport(
                "this raft transport has no address resolver to refresh from".to_string(),
            )
        })?;
        let resolved = resolver.resolve_raft_addr(self.local_node_id, node_id)?;
        if self.peer_addr(node_id).as_deref() == Some(resolved.as_str()) {
            return Ok(None);
        }
        self.re_resolved.fetch_add(1, Ordering::Relaxed);
        Ok(self.set_peer_addr(node_id, resolved))
    }

    /// Times an address actually changed under a resolver.
    ///
    /// Counts changes, not lookups: a resolver asked about a peer that is
    /// merely down answers with the address this transport already has, and
    /// that is not a move.
    pub fn re_resolved_addresses(&self) -> u64 {
        self.re_resolved.load(Ordering::Relaxed)
    }

    /// Connections this transport is holding open for a peer.
    ///
    /// Exposed because the pool is otherwise invisible, and "the stale
    /// connections were dropped" is not a claim that can be made from the
    /// outside without it. A host can also read it to see whether a peer has
    /// gone quiet.
    pub fn idle_connections(&self, node_id: NodeId) -> usize {
        self.idle
            .lock()
            .map(|idle| idle.get(&node_id).map_or(0, Vec::len))
            .unwrap_or(0)
    }

    /// The address this transport will use for a peer, if it knows one.
    ///
    /// Owned rather than borrowed because the map is shared and can change
    /// under a resolver -- handing out a borrow into it would be lending out
    /// the answer to a question whose answer moves.
    pub fn peer_addr(&self, node_id: NodeId) -> Option<String> {
        self.peers.lock().ok()?.get(&node_id).cloned()
    }

    /// The cluster id this transport stamps on what it sends.
    pub fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    fn send_rpc(
        &self,
        target: NodeId,
        request: TcpRaftTransportRequest,
    ) -> Result<TcpRaftTransportResponse, RaftError> {
        // Read and release. Holding the peers lock across a connect would
        // make a slow or blackholed address block the very call that fixes it.
        let addr = self.peer_addr(target).ok_or_else(|| {
            RaftError::Transport(format!("raft transport target {target} has no address"))
        })?;
        let envelope = TcpRaftTransportEnvelope {
            cluster_id: self.cluster_id,
            request,
        };
        let mut encoded = serde_json::to_vec(&envelope)
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

        let stream = match TcpStream::connect(&addr) {
            Ok(stream) => stream,
            Err(err) => {
                // The peer may have moved rather than died. Re-ask, and retry
                // only if the answer actually changed -- a resolver that
                // repeats the address this transport already has has told us
                // the peer is down, and connecting to it again would just pay
                // the timeout twice.
                let moved = match self.refresh_peer_addr(target) {
                    Ok(Some(_)) => self.peer_addr(target),
                    _ => None,
                };
                match moved {
                    Some(moved) => TcpStream::connect(&moved).map_err(|retry| {
                        RaftError::Transport(format!(
                            "failed to connect to {addr}, and to {moved} after re-resolving: \
                             {retry}"
                        ))
                    })?,
                    None => {
                        return Err(RaftError::Transport(format!(
                            "failed to connect to {addr}: {err}"
                        )))
                    }
                }
            }
        };
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
    cluster_id: u64,
    refused: Arc<AtomicU64>,
}

impl TcpRaftTransportServer {
    pub fn start<T>(addr: impl Into<String>, handler: Arc<T>) -> Result<Self, RaftError>
    where
        T: Transport + Send + Sync + 'static,
    {
        Self::start_in_cluster(addr, handler, ANY_CLUSTER_ID)
    }

    /// A server that serves only its own cluster.
    ///
    /// A request stamped with a different non-zero cluster id is answered
    /// with an error and **never reaches the handler**, which is the only
    /// part that matters: a raft node that is asked to append entries has
    /// already changed its log by the time it can object, so the refusal has
    /// to happen before the dispatch rather than inside it.
    ///
    /// [`ANY_CLUSTER_ID`] serves everything, which is what
    /// [`TcpRaftTransportServer::start`] does and what every existing host
    /// gets.
    ///
    /// Count the refusals with
    /// [`TcpRaftTransportServer::refused_foreign_cluster`]: a misconfigured
    /// peer that cannot talk to anyone looks exactly like a peer that is
    /// down, and the counter is the difference.
    pub fn start_in_cluster<T>(
        addr: impl Into<String>,
        handler: Arc<T>,
        cluster_id: u64,
    ) -> Result<Self, RaftError>
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
        let refused = Arc::new(AtomicU64::new(0));
        let worker_refused = Arc::clone(&refused);
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
                            let conn_refused = Arc::clone(&worker_refused);
                            let spawned = thread::Builder::new()
                                .name("rustraft-tcp-conn".to_string())
                                .spawn(move || {
                                    let _ = serve_tcp_raft_connection(
                                        stream,
                                        conn_handler.as_ref(),
                                        &conn_shutdown,
                                        cluster_id,
                                        &conn_refused,
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
            cluster_id,
            refused,
        })
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// The cluster this server serves. [`ANY_CLUSTER_ID`] serves everything.
    pub fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    /// Requests refused because they came stamped with another cluster's id.
    ///
    /// Worth watching rather than merely available. A node whose transport
    /// was given the wrong cluster id is indistinguishable from a node that
    /// is unreachable -- every RPC fails -- and this is what tells the two
    /// apart.
    pub fn refused_foreign_cluster(&self) -> u64 {
        self.refused.load(Ordering::Relaxed)
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
    cluster_id: u64,
    refused: &AtomicU64,
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
        let envelope: TcpRaftTransportEnvelope =
            serde_json::from_str(&line).map_err(|err| {
                RaftError::Transport(format!("failed to decode raft TCP request: {err}"))
            })?;
        require_transport_validation(matrixraft_validate_tcp_transport_request(
            &envelope.request,
        ))?;
        // Before the dispatch, deliberately. A refusal that ran inside the
        // handler would already have appended the entries it was refusing.
        let response = if raft_clusters_may_talk(cluster_id, envelope.cluster_id) {
            handle_tcp_raft_request(envelope.request, handler)
        } else {
            refused.fetch_add(1, Ordering::Relaxed);
            refuse_foreign_cluster(&envelope.request, cluster_id, envelope.cluster_id)
        };
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

/// The refusal, in the shape of the request that earned it.
///
/// A caller decodes the response by the RPC it sent, so a refusal has to come
/// back as that same kind with the error set -- not as a single generic error,
/// which the caller could not decode, and not as a dropped connection, which
/// it would read as the peer being down.
fn refuse_foreign_cluster(
    request: &TcpRaftTransportRequest,
    local: u64,
    sender: u64,
) -> TcpRaftTransportResponse {
    // A generic function rather than a closure: a closure's return type is
    // fixed by its first use, so one shared `refusal()` would make every arm
    // an `AppendEntriesResponse`.
    fn refusal<T>(local: u64, sender: u64) -> TcpRaftRpcResult<T> {
        TcpRaftRpcResult::from_result(Err(RaftError::Transport(format!(
            "raft request from cluster {sender} refused by cluster {local}"
        ))))
    }
    match request {
        TcpRaftTransportRequest::AppendEntries { .. } => {
            TcpRaftTransportResponse::AppendEntries(refusal(local, sender))
        }
        TcpRaftTransportRequest::Vote { .. } => {
            TcpRaftTransportResponse::Vote(refusal(local, sender))
        }
        TcpRaftTransportRequest::InstallSnapshot { .. } => {
            TcpRaftTransportResponse::InstallSnapshot(refusal(local, sender))
        }
        TcpRaftTransportRequest::ReadIndex { .. } => {
            TcpRaftTransportResponse::ReadIndex(refusal(local, sender))
        }
        // Every nested request is refused, so a batch cannot smuggle one
        // through by wrapping it.
        TcpRaftTransportRequest::Batch { requests } => TcpRaftTransportResponse::Batch(
            requests
                .iter()
                .map(|request| refuse_foreign_cluster(request, local, sender))
                .collect(),
        ),
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
