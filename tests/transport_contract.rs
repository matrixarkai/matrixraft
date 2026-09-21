// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    matrixraft_validate_read_index_response, matrixraft_validate_tcp_transport_request,
    matrixraft_validate_vote_request, AppendEntriesRequest, AppendEntriesResponse,
    AuthenticatedRaftTransport, ClusterRaftTransport, Driver, DriverGroupKey, DriverOptions,
    DriverTickReceiver, HeartbeatFlusher, HeartbeatMerger, InMemoryRaftTransport,
    InstallSnapshotRequest, InstallSnapshotResponse, LogEntry, LogId, MergedHeartbeatBatch,
    MergedHeartbeatSender, Message, Peer, RaftCluster, RaftError, ReadIndexRequest,
    ReadIndexResponse, ReplicaRole, SnapshotChunk, SnapshotMetadata, SnapshotState,
    StaticRaftAuthToken, TcpRaftTransport, TcpRaftTransportRequest, TcpRaftTransportServer,
    Transport, VoteRequest, VoteResponse,
};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Polls to a deadline, so an assertion is about behaviour and not about how
/// busy the machine was.
fn wait_until(what: &str, timeout: Duration, mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if done() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    let settled = done();
    if !settled {
        eprintln!("wait_until({what}) gave up after {timeout:?}");
    }
    settled
}

fn peer(node_id: u64, role: ReplicaRole) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 7_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 8_000 + node_id),
        role,
        auto_promote: false,
    }
}

#[derive(Debug, Clone)]
struct EchoTransport;

impl Transport for EchoTransport {
    fn append_entries(
        &self,
        _target: u64,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        Ok(AppendEntriesResponse {
            term: request.term,
            success: true,
            match_index: request.leader_commit,
            rejection_hint: None,
            rejected_index: None,
            require_snapshot: None,
            snapshot_state: SnapshotState::None,
            lease_confirmation_epoch: 0,
            lease_duration_ms: 0,
        })
    }

    fn vote(&self, _target: u64, request: VoteRequest) -> Result<VoteResponse, RaftError> {
        Ok(VoteResponse {
            term: request.term,
            vote_granted: true,
            reason: "granted".to_string(),
        })
    }

    fn install_snapshot(
        &self,
        _target: u64,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        Ok(InstallSnapshotResponse {
            term: request.term,
            accepted: true,
            next_offset: request.chunk.offset + request.chunk.data.len() as u64,
            committed_index: 0,
            reason: "accepted".to_string(),
        })
    }

    fn read_index(
        &self,
        _target: u64,
        request: ReadIndexRequest,
    ) -> Result<ReadIndexResponse, RaftError> {
        Ok(ReadIndexResponse {
            safe: true,
            read_index: request.min_commit_index,
            lease_read: request.allow_lease_read,
            reason: "read_index".to_string(),
        })
    }
}

fn assert_raft_transport<T: Transport>(_transport: &T) {}

#[test]
fn transport_aliases_cover_all_rpc_messages() {
    let append: AppendEntriesRequest = AppendEntriesRequest {
        group_id: 3,
        term: 2,
        leader_id: 1,
        prev_log_id: Some(LogId { term: 2, index: 4 }),
        entries: Vec::new(),
        leader_commit: 4,
        lease_epoch: 0,
    };
    let vote: VoteRequest = VoteRequest {
        group_id: 3,
        term: 2,
        candidate_id: 1,
        last_log_id: append.prev_log_id.clone(),
        pre_vote: true,
        force: false,
    };
    let snapshot: InstallSnapshotRequest = InstallSnapshotRequest {
        group_id: 3,
        term: 2,
        leader_id: 1,
        chunk: SnapshotChunk {
            meta: SnapshotMetadata {
                snapshot_id: "snap".to_string(),
                last_log_id: LogId { term: 2, index: 4 },
                membership: vec![1, 2, 3],
                members: Vec::new(),
            },
            offset: 0,
            data: b"snapshot".to_vec(),
            done: true,
        },
    };
    let read: ReadIndexRequest = ReadIndexRequest {
        group_id: 3,
        requester_id: 1,
        min_commit_index: 4,
        allow_lease_read: true,
    };

    assert_eq!(vote.candidate_id, append.leader_id);
    assert_eq!(snapshot.chunk.data, b"snapshot");
    assert!(read.allow_lease_read);
}

#[test]
fn authenticated_transport_wrapper_accepts_and_rejects_tokens() {
    let transport =
        AuthenticatedRaftTransport::new(EchoTransport, StaticRaftAuthToken::new("secret"));
    assert_raft_transport(&transport);

    let request = transport.wrap_request(
        2,
        ReadIndexRequest {
            group_id: 3,
            requester_id: 1,
            min_commit_index: 7,
            allow_lease_read: true,
        },
    );
    let response = transport
        .read_index_authenticated(2, request)
        .expect("authenticated read");
    assert_eq!(response.read_index, 7);
    assert!(response.lease_read);

    let rejected = transport.read_index_authenticated(
        2,
        matrixraft::AuthenticatedRaftRpc {
            auth: "wrong".to_string(),
            message: ReadIndexRequest {
                group_id: 3,
                requester_id: 1,
                min_commit_index: 7,
                allow_lease_read: false,
            },
        },
    );
    assert!(matches!(rejected, Err(RaftError::Transport(_))));
}

#[test]
fn transport_validation_reports_bad_requests_and_responses() {
    let bad_vote = VoteRequest {
        group_id: 0,
        term: 1,
        candidate_id: 0,
        last_log_id: Some(LogId { term: 1, index: 0 }),
        pre_vote: true,
        force: false,
    };
    let vote_report = matrixraft_validate_vote_request(&bad_vote);
    assert!(!vote_report.valid);
    assert!(vote_report
        .blockers
        .iter()
        .any(|blocker| blocker.contains("group_id")));
    assert!(vote_report
        .blockers
        .iter()
        .any(|blocker| blocker.contains("candidate_id")));

    let bad_read_response = ReadIndexResponse {
        safe: false,
        read_index: 5,
        lease_read: true,
        reason: "".to_string(),
    };
    let read_report = matrixraft_validate_read_index_response(&bad_read_response);
    assert!(!read_report.valid);
    assert!(read_report
        .blockers
        .iter()
        .any(|blocker| blocker.contains("lease_read")));

    let bad_tcp = TcpRaftTransportRequest::Vote {
        target: 0,
        request: bad_vote,
    };
    let tcp_report = matrixraft_validate_tcp_transport_request(&bad_tcp);
    assert!(!tcp_report.valid);
    assert!(tcp_report
        .blockers
        .iter()
        .any(|blocker| blocker.contains("target")));

    let empty_batch = TcpRaftTransportRequest::Batch {
        requests: Vec::new(),
    };
    let empty_batch_report = matrixraft_validate_tcp_transport_request(&empty_batch);
    assert!(!empty_batch_report.valid);
    assert!(empty_batch_report
        .blockers
        .iter()
        .any(|blocker| blocker.contains("must not be empty")));

    let nested_batch = TcpRaftTransportRequest::Batch {
        requests: vec![TcpRaftTransportRequest::Batch {
            requests: Vec::new(),
        }],
    };
    let nested_batch_report = matrixraft_validate_tcp_transport_request(&nested_batch);
    assert!(!nested_batch_report.valid);
    assert!(nested_batch_report
        .blockers
        .iter()
        .any(|blocker| blocker.contains("must not be nested")));
}

#[test]
fn in_memory_transport_forwards_and_validates_all_rpc_messages() {
    let transport = InMemoryRaftTransport::new();
    transport.register(2, EchoTransport).expect("register peer");
    assert_raft_transport(&transport);

    let append = transport
        .append_entries(
            2,
            AppendEntriesRequest {
                group_id: 3,
                term: 1,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![LogEntry {
                    log_id: LogId { term: 1, index: 1 },
                    payload: b"x".to_vec(),
                    is_command: true,
                }],
                leader_commit: 1,
                lease_epoch: 0,
            },
        )
        .expect("append through memory transport");
    assert!(append.success);

    let vote = transport
        .vote(
            2,
            VoteRequest {
                group_id: 3,
                term: 2,
                candidate_id: 2,
                last_log_id: Some(LogId { term: 1, index: 1 }),
                pre_vote: true,
                force: true,
            },
        )
        .expect("vote through memory transport");
    assert!(vote.vote_granted);

    let snapshot = transport
        .install_snapshot(
            2,
            InstallSnapshotRequest {
                group_id: 3,
                term: 2,
                leader_id: 1,
                chunk: SnapshotChunk {
                    meta: SnapshotMetadata {
                        snapshot_id: "memory-snap".to_string(),
                        last_log_id: LogId { term: 2, index: 4 },
                        membership: vec![1, 2, 3],
                        members: Vec::new(),
                    },
                    offset: 0,
                    data: b"snapshot".to_vec(),
                    done: true,
                },
            },
        )
        .expect("snapshot through memory transport");
    assert!(snapshot.accepted);

    let read = transport
        .read_index(
            2,
            ReadIndexRequest {
                group_id: 3,
                requester_id: 1,
                min_commit_index: 4,
                allow_lease_read: true,
            },
        )
        .expect("read-index through memory transport");
    assert!(read.safe);
    assert!(read.lease_read);

    let rejected = transport.read_index(
        2,
        ReadIndexRequest {
            group_id: 0,
            requester_id: 1,
            min_commit_index: 4,
            allow_lease_read: false,
        },
    );
    assert!(matches!(rejected, Err(RaftError::InvalidRequest(_))));
}

#[test]
fn heartbeat_merger_queues_empty_append_heartbeats() {
    let mut resolver = HashMap::new();
    resolver.insert((1, 2), "127.0.0.1:7002".to_string());
    resolver.insert((3, 2), "127.0.0.1:7002".to_string());
    resolver.insert((2, 1), "127.0.0.1:7001".to_string());
    let mut merger = HeartbeatMerger::enabled();

    let first = Message::AppendEntries {
        target: 2,
        request: AppendEntriesRequest {
            group_id: 11,
            term: 4,
            leader_id: 1,
            prev_log_id: None,
            entries: Vec::new(),
            leader_commit: 9,
            lease_epoch: 15,
        },
    };
    let second = Message::AppendEntries {
        target: 2,
        request: AppendEntriesRequest {
            group_id: 12,
            term: 3,
            leader_id: 3,
            prev_log_id: None,
            entries: Vec::new(),
            leader_commit: 7,
            lease_epoch: 8,
        },
    };
    let response = AppendEntriesResponse {
        term: 4,
        success: true,
        match_index: 9,
        rejection_hint: None,
        rejected_index: None,
        require_snapshot: None,
        snapshot_state: SnapshotState::None,
        lease_confirmation_epoch: 15,
        lease_duration_ms: 10,
    };

    assert!(merger
        .maybe_merge(first, &resolver)
        .expect("merge first heartbeat")
        .is_none());
    assert!(merger
        .maybe_merge(second, &resolver)
        .expect("merge second heartbeat")
        .is_none());
    merger
        .merge_heartbeat_response(2, 1, response, &resolver)
        .expect("merge known heartbeat response");
    assert_eq!(merger.pending_len(), 3);

    let mut batches = merger.flush();
    batches.sort_by(|left, right| left.raft_addr.cmp(&right.raft_addr));
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].raft_addr, "127.0.0.1:7001");
    assert_eq!(batches[0].messages.len(), 1);
    assert_eq!(batches[1].raft_addr, "127.0.0.1:7002");
    assert_eq!(batches[1].messages.len(), 2);
    assert_eq!(merger.pending_len(), 0);

    let stats = merger.stats();
    assert_eq!(stats.queued_requests, 2);
    assert_eq!(stats.queued_responses, 1);
    assert_eq!(stats.flushed_requests, 2);
    assert_eq!(stats.flushed_responses, 1);
}

#[test]
fn heartbeat_merger_bypasses_disabled_and_non_heartbeat_appends() {
    let resolver = |from, to| Ok(format!("{from}->{to}"));
    let request_with_entry = Message::AppendEntries {
        target: 2,
        request: AppendEntriesRequest {
            group_id: 11,
            term: 4,
            leader_id: 1,
            prev_log_id: None,
            entries: vec![LogEntry {
                log_id: LogId { term: 4, index: 1 },
                payload: b"not-heartbeat".to_vec(),
                is_command: true,
            }],
            leader_commit: 0,
            lease_epoch: 0,
        },
    };
    let mut enabled = HeartbeatMerger::enabled();
    let bypassed = enabled
        .maybe_merge(request_with_entry.clone(), &resolver)
        .expect("non-heartbeat append bypasses");
    assert_eq!(bypassed, Some(request_with_entry.clone()));
    let append_response = Message::AppendEntriesResponse {
        local_node_id: 2,
        peer_id: 1,
        response: AppendEntriesResponse {
            term: 4,
            success: true,
            match_index: 1,
            rejection_hint: None,
            rejected_index: None,
            require_snapshot: None,
            snapshot_state: SnapshotState::None,
            lease_confirmation_epoch: 0,
            lease_duration_ms: 0,
        },
    };
    let bypassed = enabled
        .maybe_merge(append_response.clone(), &resolver)
        .expect("generic append response bypasses without explicit heartbeat marker");
    assert_eq!(bypassed, Some(append_response));
    assert_eq!(enabled.pending_len(), 0);
    assert_eq!(enabled.stats().bypassed_messages, 2);

    let heartbeat = Message::AppendEntries {
        target: 2,
        request: AppendEntriesRequest {
            group_id: 11,
            term: 4,
            leader_id: 1,
            prev_log_id: None,
            entries: Vec::new(),
            leader_commit: 0,
            lease_epoch: 0,
        },
    };
    let mut disabled = HeartbeatMerger::disabled();
    let bypassed = disabled
        .maybe_merge(heartbeat.clone(), &resolver)
        .expect("disabled merger bypasses");
    assert_eq!(bypassed, Some(heartbeat));
    assert_eq!(disabled.pending_len(), 0);
    assert_eq!(disabled.stats().bypassed_messages, 1);
}

#[test]
fn cluster_installs_snapshot_from_chunked_snapshot_rpc() {
    let mut cluster = RaftCluster::new(
        3,
        Default::default(),
        vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
        ],
    )
    .expect("cluster");
    cluster.start().expect("start");

    let response = cluster
        .install_snapshot_chunk_to(
            2,
            InstallSnapshotRequest {
                group_id: 3,
                term: 1,
                leader_id: 1,
                chunk: SnapshotChunk {
                    meta: SnapshotMetadata {
                        snapshot_id: "snap-9".to_string(),
                        last_log_id: LogId { term: 1, index: 9 },
                        membership: vec![1, 2, 3],
                        members: Vec::new(),
                    },
                    offset: 0,
                    data: b"state".to_vec(),
                    done: true,
                },
            },
        )
        .expect("install snapshot rpc");
    assert!(response.accepted);
    assert_eq!(response.reason, "snapshot_installed");
    assert_eq!(cluster.status(2).expect("status").last_snapshot_index, 9);

    let stale = cluster
        .install_snapshot_chunk_to(
            2,
            InstallSnapshotRequest {
                group_id: 3,
                term: 1,
                leader_id: 1,
                chunk: SnapshotChunk {
                    meta: SnapshotMetadata {
                        snapshot_id: "snap-8-stale".to_string(),
                        last_log_id: LogId { term: 1, index: 8 },
                        membership: vec![1, 2, 3],
                        members: Vec::new(),
                    },
                    offset: 0,
                    data: b"stale".to_vec(),
                    done: true,
                },
            },
        )
        .expect("stale snapshot rpc");
    assert!(stale.accepted);
    assert_eq!(stale.reason, "stale_snapshot_ignored");
    assert_eq!(cluster.status(2).expect("status").last_snapshot_index, 9);
}

#[test]
fn cluster_reassembles_multi_chunk_snapshot_rpc() {
    let mut cluster = RaftCluster::new(
        3,
        Default::default(),
        vec![
            peer(1, ReplicaRole::Voter),
            peer(2, ReplicaRole::Voter),
            peer(3, ReplicaRole::Voter),
        ],
    )
    .expect("cluster");
    cluster.start().expect("start");
    let meta = SnapshotMetadata {
        snapshot_id: "snap-10".to_string(),
        last_log_id: LogId { term: 1, index: 10 },
        membership: vec![1, 2, 3],
        members: Vec::new(),
    };

    let first = cluster
        .install_snapshot_chunk_to(
            2,
            InstallSnapshotRequest {
                group_id: 3,
                term: 1,
                leader_id: 1,
                chunk: SnapshotChunk {
                    meta: meta.clone(),
                    offset: 0,
                    data: b"state-".to_vec(),
                    done: false,
                },
            },
        )
        .expect("first snapshot chunk");
    assert!(first.accepted);
    assert_eq!(first.reason, "snapshot_chunk_accepted");
    assert_eq!(first.next_offset, 6);
    assert_eq!(cluster.status(2).expect("status").last_snapshot_index, 0);

    let finish = cluster
        .install_snapshot_chunk_to(
            2,
            InstallSnapshotRequest {
                group_id: 3,
                term: 1,
                leader_id: 1,
                chunk: SnapshotChunk {
                    meta,
                    offset: 6,
                    data: b"done".to_vec(),
                    done: true,
                },
            },
        )
        .expect("final snapshot chunk");
    assert!(finish.accepted);
    assert_eq!(finish.reason, "snapshot_installed");
    assert_eq!(finish.next_offset, 10);
    assert_eq!(cluster.status(2).expect("status").last_snapshot_index, 10);
}

#[test]
fn tcp_transport_round_trips_append_snapshot_vote_and_read_index() {
    let cluster = Arc::new(Mutex::new(
        RaftCluster::new(
            3,
            Default::default(),
            vec![
                peer(1, ReplicaRole::Voter),
                peer(2, ReplicaRole::Voter),
                peer(3, ReplicaRole::Voter),
            ],
        )
        .expect("cluster"),
    ));
    cluster.lock().expect("lock").start().expect("start");
    let handler = Arc::new(ClusterRaftTransport::new(Arc::clone(&cluster)));
    let mut server =
        TcpRaftTransportServer::start("127.0.0.1:0", handler).expect("start tcp server");

    let mut peers = BTreeMap::new();
    peers.insert(2, server.addr().to_string());
    let transport = TcpRaftTransport::new(peers);
    let append = transport
        .append_entries(
            2,
            AppendEntriesRequest {
                group_id: 3,
                term: 1,
                leader_id: 1,
                prev_log_id: None,
                entries: vec![LogEntry {
                    log_id: LogId { term: 1, index: 1 },
                    payload: b"x".to_vec(),
                    is_command: true,
                }],
                leader_commit: 1,
                lease_epoch: 0,
            },
        )
        .expect("append over tcp");
    assert!(append.success);
    assert_eq!(append.match_index, 1);

    let vote = transport
        .vote(
            2,
            VoteRequest {
                group_id: 3,
                term: 2,
                candidate_id: 2,
                last_log_id: Some(LogId { term: 1, index: 1 }),
                pre_vote: true,
                force: true,
            },
        )
        .expect("vote over tcp");
    assert!(vote.vote_granted);

    let read = transport
        .read_index(
            2,
            ReadIndexRequest {
                group_id: 3,
                requester_id: 2,
                min_commit_index: 1,
                allow_lease_read: true,
            },
        )
        .expect("read over tcp");
    assert!(!read.safe);
    assert_eq!(read.reason, "not_leader");

    let snapshot = transport
        .install_snapshot(
            2,
            InstallSnapshotRequest {
                group_id: 3,
                term: 1,
                leader_id: 1,
                chunk: SnapshotChunk {
                    meta: SnapshotMetadata {
                        snapshot_id: "tcp-snap".to_string(),
                        last_log_id: LogId { term: 1, index: 4 },
                        membership: vec![1, 2, 3],
                        members: Vec::new(),
                    },
                    offset: 0,
                    data: b"state".to_vec(),
                    done: true,
                },
            },
        )
        .expect("snapshot over tcp");
    assert!(snapshot.accepted);
    assert_eq!(
        cluster
            .lock()
            .expect("lock")
            .status(2)
            .expect("status")
            .last_snapshot_index,
        4
    );

    server.shutdown().expect("shutdown server");
}

#[test]
fn tcp_transport_batches_mixed_rpc_requests() {
    let cluster = Arc::new(Mutex::new(
        RaftCluster::new(
            3,
            Default::default(),
            vec![
                peer(1, ReplicaRole::Voter),
                peer(2, ReplicaRole::Voter),
                peer(3, ReplicaRole::Voter),
            ],
        )
        .expect("cluster"),
    ));
    cluster.lock().expect("lock").start().expect("start");
    let handler = Arc::new(ClusterRaftTransport::new(Arc::clone(&cluster)));
    let mut server =
        TcpRaftTransportServer::start("127.0.0.1:0", handler).expect("start tcp server");

    let mut peers = BTreeMap::new();
    peers.insert(2, server.addr().to_string());
    let transport = TcpRaftTransport::new(peers);
    let responses = transport
        .send_batch_rpc(
            2,
            vec![
                TcpRaftTransportRequest::AppendEntries {
                    target: 2,
                    request: AppendEntriesRequest {
                        group_id: 3,
                        term: 1,
                        leader_id: 1,
                        prev_log_id: None,
                        entries: vec![LogEntry {
                            log_id: LogId { term: 1, index: 1 },
                            payload: b"batched".to_vec(),
                            is_command: true,
                        }],
                        leader_commit: 1,
                        lease_epoch: 0,
                    },
                },
                TcpRaftTransportRequest::Vote {
                    target: 2,
                    request: VoteRequest {
                        group_id: 3,
                        term: 2,
                        candidate_id: 2,
                        last_log_id: Some(LogId { term: 1, index: 1 }),
                        pre_vote: true,
                        force: true,
                    },
                },
                TcpRaftTransportRequest::ReadIndex {
                    target: 2,
                    request: ReadIndexRequest {
                        group_id: 3,
                        requester_id: 2,
                        min_commit_index: 1,
                        allow_lease_read: true,
                    },
                },
            ],
        )
        .expect("batch rpc");

    assert_eq!(responses.len(), 3);
    match &responses[0] {
        matrixraft::TcpRaftTransportResponse::AppendEntries(response) => {
            let response = response.clone().into_result().expect("append response");
            assert!(response.success);
            assert_eq!(response.match_index, 1);
        }
        other => panic!("unexpected first batch response: {other:?}"),
    }
    match &responses[1] {
        matrixraft::TcpRaftTransportResponse::Vote(response) => {
            let response = response.clone().into_result().expect("vote response");
            assert!(response.vote_granted);
        }
        other => panic!("unexpected second batch response: {other:?}"),
    }
    match &responses[2] {
        matrixraft::TcpRaftTransportResponse::ReadIndex(response) => {
            let response = response.clone().into_result().expect("read response");
            assert!(!response.safe);
            assert_eq!(response.reason, "not_leader");
        }
        other => panic!("unexpected third batch response: {other:?}"),
    }

    server.shutdown().expect("shutdown server");
}

/// The wire format changed from number arrays to base64 for entry payloads.
/// An old sender still emits number arrays, and a new receiver has to accept
/// them -- that is the direction a rolling upgrade needs.
#[test]
fn a_number_array_request_from_an_old_sender_still_decodes() {
    let legacy = r#"{
        "rpc": "append_entries",
        "payload": {
            "target": 2,
            "request": {
                "group_id": 3,
                "term": 1,
                "leader_id": 1,
                "prev_log_id": null,
                "entries": [{
                    "log_id": {"term": 1, "index": 1},
                    "payload": [104, 101, 108, 108, 111],
                    "is_command": true
                }],
                "leader_commit": 1
            }
        }
    }"#;
    let decoded: TcpRaftTransportRequest =
        serde_json::from_str(legacy).expect("legacy number-array request decodes");
    match decoded {
        TcpRaftTransportRequest::AppendEntries { request, .. } => {
            assert_eq!(request.entries.len(), 1);
            assert_eq!(request.entries[0].payload, b"hello");
        }
        other => panic!("decoded to the wrong variant: {other:?}"),
    }
}

/// And the new encoding is what a new sender emits: base64 text, not an array.
#[test]
fn a_new_request_carries_base64_payloads_on_the_wire() {
    let request = TcpRaftTransportRequest::AppendEntries {
        target: 2,
        request: AppendEntriesRequest {
            group_id: 3,
            term: 1,
            leader_id: 1,
            prev_log_id: None,
            entries: vec![LogEntry {
                log_id: LogId { term: 1, index: 1 },
                payload: b"hello".to_vec(),
                is_command: true,
            }],
            leader_commit: 1,
            lease_epoch: 0,
        },
    };
    let encoded = serde_json::to_string(&request).expect("encode");
    assert!(
        encoded.contains("\"aGVsbG8=\""),
        "payload should be base64 text: {encoded}"
    );
    assert!(
        !encoded.contains("[104"),
        "payload must not be a number array: {encoded}"
    );
    let round: TcpRaftTransportRequest = serde_json::from_str(&encoded).expect("round trip");
    match round {
        TcpRaftTransportRequest::AppendEntries { request, .. } => {
            assert_eq!(request.entries[0].payload, b"hello");
        }
        other => panic!("decoded to the wrong variant: {other:?}"),
    }
}

/// Snapshot chunks changed encoding too. An old sender's number-array chunk
/// still decodes -- the direction a rolling upgrade needs -- and the new form
/// is base64 text.
#[test]
fn a_number_array_snapshot_chunk_from_an_old_sender_still_decodes() {
    let legacy = r#"{
        "meta": {
            "snapshot_id": "snap-1",
            "last_log_id": {"term": 1, "index": 100},
            "membership": [1, 2, 3]
        },
        "offset": 0,
        "data": [104, 101, 108, 108, 111],
        "done": true
    }"#;
    let decoded: SnapshotChunk =
        serde_json::from_str(legacy).expect("legacy number-array chunk decodes");
    assert_eq!(decoded.data, b"hello");

    let reencoded = serde_json::to_string(&decoded).expect("encode");
    assert!(
        reencoded.contains("\"aGVsbG8=\""),
        "chunk data should be base64 text: {reencoded}"
    );
    assert!(
        !reencoded.contains("[104"),
        "chunk data must not be a number array: {reencoded}"
    );
    let round: SnapshotChunk = serde_json::from_str(&reencoded).expect("round trip");
    assert_eq!(round.data, b"hello");
    assert_eq!(round, decoded);
}

/// Builds a cluster and a TCP server for it, on an ephemeral loopback port.
fn tcp_server_for_a_three_node_cluster() -> TcpRaftTransportServer {
    let cluster = Arc::new(Mutex::new(
        RaftCluster::new(
            3,
            Default::default(),
            vec![
                peer(1, ReplicaRole::Voter),
                peer(2, ReplicaRole::Voter),
                peer(3, ReplicaRole::Voter),
            ],
        )
        .expect("cluster"),
    ));
    cluster.lock().expect("lock").start().expect("start");
    let handler = Arc::new(ClusterRaftTransport::new(Arc::clone(&cluster)));
    TcpRaftTransportServer::start("127.0.0.1:0", handler).expect("start tcp server")
}

/// Always index 1: with `prev_log_id: None` the request has to start the log,
/// and anything else fails contiguity validation before the transport is even
/// exercised. Re-sending the same append is fine -- it is idempotent, and what
/// is under test here is the connection rather than the log.
fn append_one(transport: &TcpRaftTransport) -> Result<AppendEntriesResponse, RaftError> {
    transport.append_entries(
        2,
        AppendEntriesRequest {
            group_id: 3,
            term: 1,
            leader_id: 1,
            prev_log_id: None,
            entries: vec![LogEntry {
                log_id: LogId { term: 1, index: 1 },
                payload: b"x".to_vec(),
                is_command: true,
            }],
            leader_commit: 1,
            lease_epoch: 0,
        },
    )
}

/// Connections are kept open, so a caller can hand back one the peer has since
/// closed. There is no way to learn that but to use it, so the failure has to
/// be retried on a fresh connection rather than surfaced.
#[test]
fn a_pooled_connection_the_peer_closed_is_retried_on_a_fresh_one() {
    let mut first = tcp_server_for_a_three_node_cluster();
    let addr = first.addr().to_string();

    let mut peers = BTreeMap::new();
    peers.insert(2, addr.clone());
    let transport = TcpRaftTransport::new(peers);

    append_one(&transport).expect("the first append opens and pools a connection");

    // Take the peer away, then bring it back at the same address. The pooled
    // connection is now dead, and the caller has no way to know.
    first.shutdown().expect("shut the first server down");
    let cluster = Arc::new(Mutex::new(
        RaftCluster::new(
            3,
            Default::default(),
            vec![
                peer(1, ReplicaRole::Voter),
                peer(2, ReplicaRole::Voter),
                peer(3, ReplicaRole::Voter),
            ],
        )
        .expect("cluster"),
    ));
    cluster.lock().expect("lock").start().expect("start");
    let handler = Arc::new(ClusterRaftTransport::new(Arc::clone(&cluster)));
    let mut second =
        TcpRaftTransportServer::start(addr.as_str(), handler).expect("rebind the same address");

    append_one(&transport).expect("a dead pooled connection must not fail the RPC");
    second.shutdown().expect("shut the second server down");
}

/// The listener used to serve each connection inline, so one peer's round trip
/// blocked every other peer's. With connections kept open that would have been
/// a stall for as long as the connection lived, so this covers the concurrency
/// the reuse depends on.
#[test]
fn concurrent_callers_are_all_served() {
    let mut server = tcp_server_for_a_three_node_cluster();
    let mut peers = BTreeMap::new();
    peers.insert(2, server.addr().to_string());
    let transport = Arc::new(TcpRaftTransport::new(peers));

    let workers: Vec<_> = (0..6)
        .map(|_worker| {
            let transport = Arc::clone(&transport);
            std::thread::spawn(move || {
                for _round in 0..10 {
                    append_one(&transport).expect("every concurrent RPC has to be served");
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().expect("no worker panics");
    }
    server.shutdown().expect("shutdown");
}

// ---------------------------------------------------------------------------
// The heartbeat flusher: the driver's tick is what spends
// merge_heartbeat_interval_milli.
// ---------------------------------------------------------------------------

/// Collects what a flush sends.
#[derive(Debug, Default)]
struct SentBatches {
    batches: Mutex<Vec<MergedHeartbeatBatch>>,
}

impl SentBatches {
    fn batch_count(&self) -> usize {
        self.batches.lock().expect("sent mutex").len()
    }
    fn message_count(&self) -> usize {
        self.batches
            .lock()
            .expect("sent mutex")
            .iter()
            .map(|b| b.messages.len())
            .sum()
    }
    fn addresses(&self) -> Vec<String> {
        let mut addrs: Vec<String> = self
            .batches
            .lock()
            .expect("sent mutex")
            .iter()
            .map(|b| b.raft_addr.clone())
            .collect();
        addrs.sort();
        addrs.dedup();
        addrs
    }
}

impl MergedHeartbeatSender for SentBatches {
    fn send_merged(&self, batches: Vec<MergedHeartbeatBatch>) {
        self.batches.lock().expect("sent mutex").extend(batches);
    }
}

/// Every pair resolves to one address per target, so heartbeats from many
/// groups to the same peer share a destination -- which is the case merging
/// exists for.
fn resolver() -> HashMap<(u64, u64), String> {
    let mut map = HashMap::new();
    for from in 1..=64_u64 {
        for to in 1..=4_u64 {
            map.insert((from, to), format!("10.0.0.{to}:9000"));
        }
    }
    map
}

fn heartbeat(from: u64, to: u64) -> Message {
    Message::AppendEntries {
        target: to,
        request: AppendEntriesRequest {
            group_id: from,
            term: 1,
            leader_id: from,
            prev_log_id: None,
            entries: Vec::new(), // empty: that is what makes it a heartbeat
            leader_commit: 0,
            lease_epoch: 0,
        },
    }
}

#[test]
fn many_groups_heartbeating_one_peer_send_one_batch() {
    // The whole point of merging: 64 groups, 4 peers, and what goes out is 4
    // batches rather than 256 messages.
    let sent = Arc::new(SentBatches::default());
    let flusher = HeartbeatFlusher::new(
        HeartbeatMerger::enabled(),
        Arc::clone(&sent) as Arc<dyn MergedHeartbeatSender>,
    );
    let resolver = resolver();

    let mut absorbed = 0;
    for from in 1..=64_u64 {
        for to in 1..=4_u64 {
            let outcome = flusher
                .maybe_merge(heartbeat(from, to), &resolver)
                .expect("merge");
            if outcome.is_none() {
                absorbed += 1;
            }
        }
    }
    assert_eq!(absorbed, 256, "every heartbeat should have been absorbed");
    assert_eq!(
        sent.batch_count(),
        0,
        "nothing goes out until a flush: that is what the interval is for"
    );

    let batches = flusher.flush_now();
    assert_eq!(batches, 4, "one batch per destination address");
    assert_eq!(sent.batch_count(), 4);
    assert_eq!(
        sent.message_count(),
        256,
        "every heartbeat must still arrive, just together"
    );
    assert_eq!(sent.addresses().len(), 4);

    let stats = flusher.stats();
    assert_eq!(stats.batches_sent, 4);
    assert_eq!(stats.messages_sent, 256);
    assert_eq!(stats.pending, 0, "a flush should leave nothing behind");
}

#[test]
fn a_disabled_merger_absorbs_nothing() {
    // The setting turns the behaviour off; the caller keeps the same call.
    let sent = Arc::new(SentBatches::default());
    let flusher = HeartbeatFlusher::new(
        HeartbeatMerger::disabled(),
        Arc::clone(&sent) as Arc<dyn MergedHeartbeatSender>,
    );
    let resolver = resolver();

    for from in 1..=8_u64 {
        let outcome = flusher
            .maybe_merge(heartbeat(from, 1), &resolver)
            .expect("merge");
        assert!(
            outcome.is_some(),
            "a disabled merger must hand every message back"
        );
    }
    assert_eq!(flusher.flush_now(), 0);
    assert_eq!(sent.batch_count(), 0);
    assert!(!flusher.is_enabled());
}

#[test]
fn a_message_that_is_not_a_heartbeat_is_handed_back() {
    // An AppendEntries carrying entries is replication, not a heartbeat, and
    // must not be held back.
    let sent = Arc::new(SentBatches::default());
    let flusher = HeartbeatFlusher::new(
        HeartbeatMerger::enabled(),
        Arc::clone(&sent) as Arc<dyn MergedHeartbeatSender>,
    );
    let resolver = resolver();

    let mut with_entries = heartbeat(1, 1);
    if let Message::AppendEntries { request, .. } = &mut with_entries {
        request.entries.push(LogEntry {
            log_id: LogId { term: 1, index: 1 },
            payload: vec![1, 2, 3],
            is_command: false,
        });
    }
    let outcome = flusher.maybe_merge(with_entries, &resolver).expect("merge");
    assert!(
        outcome.is_some(),
        "an append carrying entries must not be held back as a heartbeat"
    );
    assert_eq!(flusher.stats().pending, 0);
}

#[test]
fn the_drivers_tick_is_what_flushes() {
    // merge_heartbeat_interval_milli is spent by registering the flusher with
    // the driver at that interval. Nothing else in the crate has a timer.
    let sent = Arc::new(SentBatches::default());
    let flusher = HeartbeatFlusher::new(
        HeartbeatMerger::enabled(),
        Arc::clone(&sent) as Arc<dyn MergedHeartbeatSender>,
    );
    let resolver = resolver();
    for from in 1..=16_u64 {
        flusher
            .maybe_merge(heartbeat(from, 2), &resolver)
            .expect("merge");
    }
    assert_eq!(sent.batch_count(), 0, "held until the interval passes");

    let driver = Driver::start(DriverOptions {
        worker_num: 1,
        tick_interval_ms: 2,
        ..DriverOptions::default()
    })
    .expect("driver");
    driver
        .register_group_every(
            DriverGroupKey::new(1, 1),
            Arc::clone(&flusher) as Arc<dyn DriverTickReceiver>,
            2,
        )
        .expect("register");

    assert!(
        wait_until("the tick flushes", Duration::from_secs(20), || {
            sent.batch_count() >= 1
        }),
        "the driver's tick never flushed the merger"
    );
    assert_eq!(
        sent.message_count(),
        16,
        "all sixteen heartbeats should arrive in the flush"
    );
    assert_eq!(sent.addresses(), vec!["10.0.0.2:9000".to_string()]);
}
