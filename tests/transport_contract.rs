// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    matrixraft_validate_read_index_response, matrixraft_validate_tcp_transport_request,
    matrixraft_validate_vote_request, AppendEntriesRequest, AppendEntriesResponse,
    AuthenticatedRaftTransport, ClusterRaftTransport, HeartbeatMerger, InMemoryRaftTransport,
    InstallSnapshotRequest, InstallSnapshotResponse, LogEntry, LogId, Message, Peer, RaftCluster,
    RaftError, ReadIndexRequest, ReadIndexResponse, ReplicaRole, SnapshotChunk, SnapshotMetadata,
    SnapshotState, StaticRaftAuthToken, TcpRaftTransport, TcpRaftTransportRequest,
    TcpRaftTransportServer, Transport, VoteRequest, VoteResponse,
};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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
