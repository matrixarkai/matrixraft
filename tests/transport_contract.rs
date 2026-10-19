// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    matrixraft_is_health_check_request, matrixraft_validate_read_index_response,
    matrixraft_validate_tcp_transport_request, matrixraft_validate_vote_request,
    AppendEntriesRequest, AppendEntriesResponse, AuthenticatedRaftTransport, ClusterRaftTransport,
    Driver, DriverGroupKey, DriverOptions, DriverTickReceiver, HeartbeatAddressResolver,
    HeartbeatFlusher, HeartbeatMergeMessage, HeartbeatMerger, InMemoryRaftTransport,
    InstallSnapshotRequest, InstallSnapshotResponse, LogEntry, LogId, MatrixRaftRuntimeWiring,
    MatrixRaftTransportBuilder, MergedHeartbeatBatch, MergedHeartbeatSender, Message, Peer,
    RaftCluster, RaftError, ReadIndexRequest, ReadIndexResponse, ReplicaRole, SnapshotChunk,
    SnapshotMetadata, SnapshotState, StaticRaftAuthToken, TcpRaftRpcResult, TcpRaftTransport,
    TcpRaftTransportEnvelope, TcpRaftTransportRequest, TcpRaftTransportResponse,
    TcpRaftTransportServer, Transport, VoteRequest, VoteResponse, ANY_CLUSTER_ID,
    DEFAULT_CONNECTIONS_PER_PEER,
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
            throttled: false,
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

/// Offers heartbeats for one peer from one caller, so the order the flush
/// hands them back is the order they were offered.
///
/// The address decides the bucket, so one peer's heartbeats are always in one
/// bucket and always under one lock. That is what makes the order a property
/// rather than a coincidence, and it is the reason the merger buckets by
/// address and not by group.
/// A wiring with the heartbeat merge settings and nothing else of interest.
///
/// Written out because `MatrixRaftRuntimeWiring` has no `Default` and should
/// not: a `worker_num` of zero is not a runtime anyone configured.
fn wiring_with_merge(merge: bool, interval_ms: u64) -> MatrixRaftRuntimeWiring {
    MatrixRaftRuntimeWiring {
        group_id: 5,
        node_id: 1,
        creator_index: Some(0),
        store_id: 1,
        worker_num: 1,
        reader_num: 1,
        executor_num: 1,
        applier_num: 1,
        apply_max_batch_count: 8,
        snapshot_loader_num: 1,
        snapshot_downloader_num: 1,
        snapshot_creator_num: 1,
        snapshot_sender_num: 1,
        flexible_apply: false,
        heartbeat_merge: merge,
        merge_heartbeat_interval_milli: interval_ms,
        watched_address_resolver: false,
        transport: Default::default(),
        snapshot_send_rate_limiter: None,
        snapshot_download_rate_limiter: None,
        has_store_fsm: false,
        has_group_storage: false,
    }
}

#[test]
fn the_configured_merge_interval_reaches_a_registered_flusher() {
    // `enable_heartbeat_merge()` and `merge_heartbeat_interval_milli(25)` were
    // both recorded in the wiring and read by nothing, so a host that set them
    // got no merging at all: the flusher that does the work has to be
    // registered with a driver, and nothing connected the setting to the
    // registration.
    assert_eq!(
        wiring_with_merge(true, 25).heartbeat_merge_interval_ms(),
        Some(25)
    );
    assert_eq!(
        wiring_with_merge(false, 25).heartbeat_merge_interval_ms(),
        None,
        "merging off means no flusher, whatever interval was left behind"
    );
    assert_eq!(
        wiring_with_merge(true, 0).heartbeat_merge_interval_ms(),
        Some(0),
        "zero is the driver's own cadence, not an unset interval"
    );

    // And the interval registers a flusher that actually flushes.
    let driver = Driver::start(DriverOptions {
        worker_num: 1,
        tick_interval_ms: 2,
        ..DriverOptions::default()
    })
    .expect("driver");
    let sender = Arc::new(SentBatches::default());
    let flusher = HeartbeatFlusher::new(
        HeartbeatMerger::enabled(),
        Arc::clone(&sender) as Arc<dyn MergedHeartbeatSender>,
    );
    let wiring = wiring_with_merge(true, 2);
    let interval = wiring
        .heartbeat_merge_interval_ms()
        .expect("merging is on for this group");
    let key = DriverGroupKey::new(5, 1);
    flusher
        .register_every(&driver, key, interval)
        .expect("register the flusher");
    assert_eq!(driver.group_count(), 1);

    let mut resolver = HashMap::new();
    resolver.insert((1, 2), "127.0.0.1:7100".to_string());
    let message = Message::AppendEntries {
        target: 2,
        request: AppendEntriesRequest {
            group_id: 5,
            term: 1,
            leader_id: 1,
            prev_log_id: None,
            entries: Vec::new(),
            leader_commit: 1,
            lease_epoch: 1,
        },
    };
    assert!(flusher
        .maybe_merge(message, &resolver)
        .expect("merge")
        .is_none());

    assert!(
        wait_until(
            "the driver flushes the merger",
            Duration::from_secs(20),
            || { sender.batch_count() > 0 }
        ),
        "the registered flusher never ran, so the interval reached nothing"
    );
}

#[test]
fn one_peers_heartbeats_keep_the_order_they_were_offered() {
    let mut resolver = HashMap::new();
    for group in 1..=16u64 {
        resolver.insert((group, 99), "10.0.0.1:7000".to_string());
    }
    let merger = HeartbeatMerger::enabled();

    for group in 1..=16u64 {
        let message = Message::AppendEntries {
            target: 99,
            request: AppendEntriesRequest {
                group_id: group,
                term: 1,
                leader_id: group,
                prev_log_id: None,
                entries: Vec::new(),
                leader_commit: group,
                lease_epoch: 1,
            },
        };
        assert!(merger
            .maybe_merge(message, &resolver)
            .expect("merge")
            .is_none());
    }

    let batches = merger.flush();
    assert_eq!(batches.len(), 1, "one peer, one batch");
    let groups: Vec<u64> = batches[0]
        .messages
        .iter()
        .map(|message| match message {
            HeartbeatMergeMessage::AppendEntriesRequest { request, .. } => request.group_id,
            other => panic!("unexpected merged message: {other:?}"),
        })
        .collect();
    assert_eq!(groups, (1..=16u64).collect::<Vec<_>>());
}

/// Many callers offer at once, which the merger could not even be asked to do
/// while its methods took `&mut self`.
///
/// What is asserted is that nothing is lost or duplicated: every heartbeat
/// offered comes back from the flush exactly once, grouped under the address it
/// was offered for. It does not assert anything about how long the offers took
/// -- `examples/heartbeat_merge_contention.rs` measures that, because a
/// contention figure is not something to assert on a shared runner.
#[test]
fn every_concurrent_offer_reaches_the_flush() {
    const CALLERS: u64 = 8;
    const PER_CALLER: u64 = 500;

    let mut resolver = HashMap::new();
    for caller in 0..CALLERS {
        for round in 0..PER_CALLER {
            let group = caller * PER_CALLER + round + 1;
            resolver.insert((group, 99), format!("10.0.{caller}.1:7000"));
        }
    }
    let resolver = Arc::new(resolver);
    let merger = Arc::new(HeartbeatMerger::enabled());

    let mut callers = Vec::new();
    for caller in 0..CALLERS {
        let merger = Arc::clone(&merger);
        let resolver = Arc::clone(&resolver);
        callers.push(std::thread::spawn(move || {
            for round in 0..PER_CALLER {
                let group = caller * PER_CALLER + round + 1;
                let message = Message::AppendEntries {
                    target: 99,
                    request: AppendEntriesRequest {
                        group_id: group,
                        term: 1,
                        leader_id: group,
                        prev_log_id: None,
                        entries: Vec::new(),
                        leader_commit: round,
                        lease_epoch: 1,
                    },
                };
                assert!(merger
                    .maybe_merge(message, resolver.as_ref())
                    .expect("the resolver knows every pair offered")
                    .is_none());
            }
        }));
    }
    for caller in callers {
        caller.join().expect("caller");
    }

    let offered = CALLERS * PER_CALLER;
    assert_eq!(merger.pending_len() as u64, offered);
    let batches = merger.flush();
    assert_eq!(
        batches.len() as u64,
        CALLERS,
        "one batch per address, and there is one address per caller"
    );

    let mut seen: Vec<u64> = batches
        .iter()
        .flat_map(|batch| batch.messages.iter())
        .map(|message| match message {
            HeartbeatMergeMessage::AppendEntriesRequest { request, .. } => request.group_id,
            other => panic!("unexpected merged message: {other:?}"),
        })
        .collect();
    seen.sort_unstable();
    assert_eq!(
        seen,
        (1..=offered).collect::<Vec<_>>(),
        "every heartbeat offered must come back exactly once"
    );
    assert_eq!(merger.pending_len(), 0);
    assert_eq!(merger.stats().queued_requests, offered);
    assert_eq!(merger.stats().flushed_requests, offered);
}

/// Many peers must not all land in one bucket.
///
/// A merger that put every peer in bucket zero would behave exactly as this one
/// does -- same batches, same order, same counters -- and only a contention
/// measurement would see the difference. That plant passes every other test in
/// this file, which is why `pending_in_bucket` exists and why this asserts on
/// it.
#[test]
fn many_peers_are_spread_over_the_buckets() {
    const PEERS: u64 = 64;
    const BUCKETS: usize = 16;

    let mut resolver = HashMap::new();
    for peer in 0..PEERS {
        resolver.insert(
            (1, peer + 2),
            format!("10.0.{}.{}:7000", peer / 8, peer % 8),
        );
    }
    let merger = HeartbeatMerger::with_bucket_count(true, BUCKETS);

    for peer in 0..PEERS {
        let message = Message::AppendEntries {
            target: peer + 2,
            request: AppendEntriesRequest {
                group_id: 1,
                term: 1,
                leader_id: 1,
                prev_log_id: None,
                entries: Vec::new(),
                leader_commit: peer,
                lease_epoch: 1,
            },
        };
        assert!(merger
            .maybe_merge(message, &resolver)
            .expect("merge")
            .is_none());
    }

    let held: Vec<usize> = (0..BUCKETS)
        .map(|index| merger.pending_in_bucket(index).expect("a real bucket"))
        .collect();
    let occupied = held.iter().filter(|held| **held > 0).count();
    assert_eq!(
        held.iter().sum::<usize>() as u64,
        PEERS,
        "every heartbeat is in some bucket: {held:?}"
    );
    // The threshold is deliberately far below even: what is being refused is
    // one bucket carrying everything, not an uneven hash.
    assert!(
        occupied > BUCKETS / 2,
        "64 peers landed in only {occupied} of {BUCKETS} buckets: {held:?}"
    );
    assert_eq!(merger.pending_in_bucket(BUCKETS), None);
}

/// The buckets are what the locking is spread over, so the count is worth
/// being able to read.
#[test]
fn a_merger_reports_how_many_buckets_it_spreads_over() {
    assert_eq!(
        HeartbeatMerger::enabled().bucket_count(),
        matrixraft::MATRIXRAFT_HEARTBEAT_MERGE_BUCKETS
    );
    assert_eq!(
        HeartbeatMerger::with_bucket_count(true, 4).bucket_count(),
        4
    );
    assert_eq!(
        HeartbeatMerger::with_bucket_count(true, 0).bucket_count(),
        1,
        "zero buckets would be nowhere to put a heartbeat"
    );
}

#[test]
fn heartbeat_merger_queues_empty_append_heartbeats() {
    let mut resolver = HashMap::new();
    resolver.insert((1, 2), "127.0.0.1:7002".to_string());
    resolver.insert((3, 2), "127.0.0.1:7002".to_string());
    resolver.insert((2, 1), "127.0.0.1:7001".to_string());
    let merger = HeartbeatMerger::enabled();

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
    let enabled = HeartbeatMerger::enabled();
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
    let disabled = HeartbeatMerger::disabled();
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

#[test]
fn a_moved_peer_stops_getting_rpcs_at_its_old_address() {
    // A peer that moves -- restarted on another port, rescheduled onto another
    // host -- is the same member at a new address. `set_peer_addr` pointed the
    // transport at the new one and left up to four connections pooled to the
    // old one, so the next four sends went to the host that had moved.
    let mut old_home = tcp_server_for_a_three_node_cluster();
    let old_addr = old_home.addr().to_string();

    let mut peers = BTreeMap::new();
    peers.insert(2, old_addr.clone());
    let transport = TcpRaftTransport::new(peers);

    append_one(&transport).expect("the first append opens and pools a connection");
    assert_eq!(
        transport.idle_connections(2),
        1,
        "the append should have pooled its connection"
    );
    assert_eq!(transport.peer_addr(2).as_deref(), Some(old_addr.as_str()));

    // The peer moves. The old address is shut down, so a send that still went
    // there would fail rather than quietly succeed -- which is what makes this
    // test able to tell the difference.
    let new_home = tcp_server_for_a_three_node_cluster();
    let new_addr = new_home.addr().to_string();
    assert_ne!(new_addr, old_addr, "the two servers must differ");

    let replaced = transport.set_peer_addr(2, new_addr.clone());
    assert_eq!(
        replaced.as_deref(),
        Some(old_addr.as_str()),
        "set_peer_addr reports the address it replaced"
    );
    assert_eq!(transport.peer_addr(2).as_deref(), Some(new_addr.as_str()));
    assert_eq!(
        transport.idle_connections(2),
        0,
        "the connections pooled to the old address must be dropped"
    );

    old_home.shutdown().expect("shut the old home down");
    append_one(&transport).expect("the append must reach the peer at its new address");

    // Re-setting the same address keeps the warm pool: a host polling service
    // discovery calls this on every poll, and dropping the pool each time would
    // undo the pooling it exists for.
    assert_eq!(
        transport.idle_connections(2),
        1,
        "the append to the new address pools a connection"
    );
    assert_eq!(
        transport.set_peer_addr(2, new_addr.clone()).as_deref(),
        Some(new_addr.as_str())
    );
    assert_eq!(
        transport.idle_connections(2),
        1,
        "an unchanged address must not throw the pool away"
    );

    // And a peer this transport did not know reports no previous address.
    assert_eq!(transport.set_peer_addr(9, "127.0.0.1:1".to_string()), None);
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

/// `EchoTransport`, but it says how many times it was asked.
///
/// The whole claim about a refused cluster is that the handler never runs, and
/// that cannot be asserted from the response alone: an error could equally
/// well have come back from a handler that had already appended the entries.
#[derive(Debug, Clone, Default)]
struct CountingEcho {
    calls: Arc<std::sync::atomic::AtomicU64>,
}

impl CountingEcho {
    fn calls(&self) -> u64 {
        self.calls.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn count(&self) {
        self.calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

impl Transport for CountingEcho {
    fn append_entries(
        &self,
        target: u64,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        self.count();
        EchoTransport.append_entries(target, request)
    }

    fn vote(&self, target: u64, request: VoteRequest) -> Result<VoteResponse, RaftError> {
        self.count();
        EchoTransport.vote(target, request)
    }

    fn install_snapshot(
        &self,
        target: u64,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        self.count();
        EchoTransport.install_snapshot(target, request)
    }

    fn read_index(
        &self,
        target: u64,
        request: ReadIndexRequest,
    ) -> Result<ReadIndexResponse, RaftError> {
        self.count();
        EchoTransport.read_index(target, request)
    }
}

fn cluster_append() -> AppendEntriesRequest {
    AppendEntriesRequest {
        group_id: 1,
        term: 2,
        leader_id: 1,
        prev_log_id: None,
        entries: Vec::new(),
        leader_commit: 0,
        lease_epoch: 0,
    }
}

/// Starts a counting server in `cluster_id` and returns it with its handler.
fn counting_server(cluster_id: u64) -> (TcpRaftTransportServer, Arc<CountingEcho>) {
    let handler = Arc::new(CountingEcho::default());
    let server =
        TcpRaftTransportServer::start_in_cluster("127.0.0.1:0", Arc::clone(&handler), cluster_id)
            .expect("start tcp server");
    (server, handler)
}

fn transport_to(server: &TcpRaftTransportServer, cluster_id: u64) -> TcpRaftTransport {
    let mut peers = BTreeMap::new();
    peers.insert(2, server.addr().to_string());
    TcpRaftTransport::new_in_cluster(peers, cluster_id)
}

#[test]
fn a_server_refuses_a_request_stamped_with_another_cluster() {
    let (mut server, handler) = counting_server(7);
    let transport = transport_to(&server, 9);

    let refused = transport.append_entries(2, cluster_append());
    let message = match refused {
        Err(err) => err.to_string(),
        Ok(response) => panic!("cluster 9 must not be served by cluster 7, got {response:?}"),
    };
    assert!(
        message.contains("cluster 9") && message.contains("cluster 7"),
        "the refusal should name both clusters, got {message}"
    );
    assert_eq!(
        handler.calls(),
        0,
        "the refusal has to happen before the dispatch: a raft node that has \
         already appended the entries cannot un-append them"
    );
    assert_eq!(server.refused_foreign_cluster(), 1);
    assert_eq!(server.cluster_id(), 7);
    server.shutdown().expect("shutdown");
}

#[test]
fn a_server_serves_its_own_cluster_and_the_any_cluster_either_way() {
    // Three pairings have to work, or the check is not opt-in: matching ids,
    // an unconfigured sender, and an unconfigured server.
    let (mut same, same_handler) = counting_server(7);
    transport_to(&same, 7)
        .append_entries(2, cluster_append())
        .expect("cluster 7 serves cluster 7");
    assert_eq!(same_handler.calls(), 1);
    assert_eq!(same.refused_foreign_cluster(), 0);
    same.shutdown().expect("shutdown");

    let (mut configured, configured_handler) = counting_server(7);
    transport_to(&configured, ANY_CLUSTER_ID)
        .append_entries(2, cluster_append())
        .expect("an unstamped sender reaches a configured server");
    assert_eq!(configured_handler.calls(), 1);
    assert_eq!(configured.refused_foreign_cluster(), 0);
    configured.shutdown().expect("shutdown");

    let (mut anywhere, anywhere_handler) = counting_server(ANY_CLUSTER_ID);
    assert_eq!(anywhere.cluster_id(), 0, "any-cluster is zero");
    transport_to(&anywhere, 9)
        .append_entries(2, cluster_append())
        .expect("an unconfigured server serves every cluster");
    assert_eq!(anywhere_handler.calls(), 1);
    assert_eq!(anywhere.refused_foreign_cluster(), 0);
    anywhere.shutdown().expect("shutdown");
}

#[test]
fn a_batch_from_another_cluster_is_refused_in_every_slot() {
    // A batch is the way a refusal could be smuggled past: one check on the
    // outer frame that then dispatched the nested requests would serve them.
    let (mut server, handler) = counting_server(7);
    let transport = transport_to(&server, 9);

    let responses = transport
        .send_batch_rpc(
            2,
            vec![
                TcpRaftTransportRequest::AppendEntries {
                    target: 2,
                    request: cluster_append(),
                },
                TcpRaftTransportRequest::AppendEntries {
                    target: 2,
                    request: cluster_append(),
                },
            ],
        )
        .expect("the batch itself completes; its entries are refused");
    assert_eq!(responses.len(), 2);
    for response in responses {
        let result = match response {
            TcpRaftTransportResponse::AppendEntries(result) => result,
            other => panic!("expected an append response, got {other:?}"),
        };
        assert!(
            result.ok.is_none(),
            "a nested request must not be served either"
        );
    }
    assert_eq!(handler.calls(), 0, "no nested request reached the handler");
    assert_eq!(
        server.refused_foreign_cluster(),
        1,
        "one frame refused, however many requests it carried"
    );
    server.shutdown().expect("shutdown");
}

#[test]
fn a_peer_that_sends_no_cluster_id_is_served_as_any_cluster() {
    // Wire compatibility, asserted on the wire rather than argued. A sender
    // that predates the field writes the bare request with no `cluster_id`,
    // and `serde(default)` has to read that as the any-cluster id -- not as
    // cluster 0 being a real cluster that a configured server would refuse.
    use std::io::{BufRead, BufReader, Write};

    let (mut server, handler) = counting_server(7);
    let bare = TcpRaftTransportRequest::AppendEntries {
        target: 2,
        request: cluster_append(),
    };
    let mut encoded = serde_json::to_vec(&bare).expect("encode the pre-field shape");
    assert!(
        !String::from_utf8_lossy(&encoded).contains("cluster_id"),
        "this test is only meaningful if the bytes really carry no cluster id"
    );
    encoded.push(b'\n');

    let mut stream = std::net::TcpStream::connect(server.addr()).expect("connect");
    stream.write_all(&encoded).expect("write");
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).expect("read a response");
    assert!(
        line.contains("\"success\":true"),
        "an unstamped request should be served, got {line}"
    );
    assert_eq!(handler.calls(), 1);
    assert_eq!(server.refused_foreign_cluster(), 0);

    // And the other direction: a stamped request is still the same shape plus
    // one field, so a peer that predates it can decode one by ignoring that
    // field.
    let stamped = TcpRaftTransportEnvelope {
        cluster_id: 7,
        user_payload: None,
        request: bare.clone(),
    };
    let stamped_json = serde_json::to_string(&stamped).expect("encode the envelope");
    let decoded_by_an_old_peer: TcpRaftTransportRequest =
        serde_json::from_str(&stamped_json).expect("an old peer ignores the extra field");
    assert_eq!(decoded_by_an_old_peer, bare);

    server.shutdown().expect("shutdown");
}

#[test]
fn the_recorded_cluster_id_reaches_the_transport_and_the_server_it_builds() {
    // The point of the change. `set_cluster_id` used to be recorded on the
    // builder, reported back, and read by nothing that ran, so two clusters
    // that both numbered their nodes from 1 could append each other's
    // entries whatever it was set to.
    let options = MatrixRaftTransportBuilder::new()
        .set_cluster_id(42)
        .bind_address_resolver()
        .build()
        .expect("build the transport options");
    assert_eq!(options.cluster_id, 42);

    let handler = Arc::new(CountingEcho::default());
    let mut server = options
        .start_tcp_server("127.0.0.1:0", Arc::clone(&handler))
        .expect("start a server in the recorded cluster");
    assert_eq!(server.cluster_id(), 42);

    let mut peers = BTreeMap::new();
    peers.insert(2, server.addr().to_string());
    let transport = options.into_tcp_transport(peers.clone());
    assert_eq!(transport.cluster_id(), 42);
    transport
        .append_entries(2, cluster_append())
        .expect("the cluster talks to itself");
    assert_eq!(handler.calls(), 1);

    // A transport built from different options is refused by the same server.
    let foreign = MatrixRaftTransportBuilder::new()
        .set_cluster_id(43)
        .bind_address_resolver()
        .build()
        .expect("build")
        .into_tcp_transport(peers);
    assert!(
        foreign.append_entries(2, cluster_append()).is_err(),
        "cluster 43 must not be served by cluster 42"
    );
    assert_eq!(handler.calls(), 1, "and it did not reach the handler");
    assert_eq!(server.refused_foreign_cluster(), 1);
    server.shutdown().expect("shutdown");
}

/// An address nothing is listening on.
///
/// Bound and then dropped, so it is a port the OS just confirmed was free
/// rather than a number picked and hoped for. On loopback a connect to it
/// fails at once with a refusal, which is the failure a moved peer produces.
fn dead_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a port to free it");
    let addr = listener.local_addr().expect("addr").to_string();
    drop(listener);
    addr
}

/// A resolver that answers whatever it was last told to, and counts.
///
/// The count is the part that matters: "the resolver was not consulted" is
/// the whole claim about a static address map, and it cannot be read off the
/// transport.
#[derive(Clone)]
struct ScriptedResolver {
    answer: Arc<Mutex<String>>,
    asked: Arc<std::sync::atomic::AtomicU64>,
}

impl ScriptedResolver {
    fn new(answer: impl Into<String>) -> Self {
        Self {
            answer: Arc::new(Mutex::new(answer.into())),
            asked: Arc::default(),
        }
    }

    fn asked(&self) -> u64 {
        self.asked.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl HeartbeatAddressResolver for ScriptedResolver {
    fn resolve_raft_addr(&self, _from: u64, _to: u64) -> Result<String, RaftError> {
        self.asked
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(self.answer.lock().expect("answer").clone())
    }
}

#[test]
fn a_peer_that_moved_is_found_again_by_the_resolver() {
    let (mut server, handler) = counting_server(ANY_CLUSTER_ID);
    let moved_to = server.addr().to_string();
    let was_at = dead_addr();

    let resolver = Arc::new(ScriptedResolver::new(moved_to.clone()));
    let mut peers = BTreeMap::new();
    peers.insert(2, was_at.clone());
    let transport =
        TcpRaftTransport::new(peers).watching_addresses(1, Arc::clone(&resolver) as Arc<_>);

    // Nothing is listening where this transport thinks peer 2 is.
    assert_eq!(transport.peer_addr(2).as_deref(), Some(was_at.as_str()));
    transport
        .append_entries(2, cluster_append())
        .expect("a moved peer should be found again rather than failing for ever");

    assert_eq!(
        handler.calls(),
        1,
        "the RPC reached the peer at its new home"
    );
    assert_eq!(
        transport.peer_addr(2).as_deref(),
        Some(moved_to.as_str()),
        "the new address should be kept, so the next send does not pay the failure again"
    );
    assert_eq!(resolver.asked(), 1, "asked once, on the failure");
    assert_eq!(transport.re_resolved_addresses(), 1);
    server.shutdown().expect("shutdown");
}

#[test]
fn a_static_address_map_does_not_consult_the_resolver() {
    // The A/B that makes `dynamic_address_map` a live setting rather than a
    // recorded one: same resolver, same dead address, the flag the only
    // difference.
    let (mut server, handler) = counting_server(ANY_CLUSTER_ID);
    let moved_to = server.addr().to_string();
    let was_at = dead_addr();
    let mut peers = BTreeMap::new();
    peers.insert(2, was_at.clone());

    let static_map = MatrixRaftTransportBuilder::new()
        .bind_address_resolver()
        .build()
        .expect("build");
    assert!(!static_map.dynamic_address_map);
    let static_resolver = Arc::new(ScriptedResolver::new(moved_to.clone()));
    let static_transport = static_map.into_tcp_transport_watching(
        peers.clone(),
        1,
        Arc::clone(&static_resolver) as Arc<_>,
    );
    assert!(
        static_transport
            .append_entries(2, cluster_append())
            .is_err(),
        "a static address map must not chase a moved peer"
    );
    assert_eq!(
        static_resolver.asked(),
        0,
        "and it must not consult the resolver at all"
    );
    assert_eq!(static_transport.re_resolved_addresses(), 0);
    assert_eq!(handler.calls(), 0);

    let dynamic_map = MatrixRaftTransportBuilder::new()
        .set_dynamic_address_map()
        .bind_address_resolver()
        .build()
        .expect("build");
    assert!(dynamic_map.dynamic_address_map);
    let dynamic_resolver = Arc::new(ScriptedResolver::new(moved_to));
    let dynamic_transport =
        dynamic_map.into_tcp_transport_watching(peers, 1, Arc::clone(&dynamic_resolver) as Arc<_>);
    dynamic_transport
        .append_entries(2, cluster_append())
        .expect("the same resolver, with the flag set, finds the peer");
    assert_eq!(dynamic_resolver.asked(), 1);
    assert_eq!(dynamic_transport.re_resolved_addresses(), 1);
    assert_eq!(handler.calls(), 1);
    server.shutdown().expect("shutdown");
}

#[test]
fn a_resolver_that_repeats_the_address_is_not_a_move() {
    // A peer that is merely down resolves to where it already was. Retrying
    // that address would pay the connect failure twice for nothing, and
    // counting it would make a down peer look like a moving one.
    let was_at = dead_addr();
    let resolver = Arc::new(ScriptedResolver::new(was_at.clone()));
    let mut peers = BTreeMap::new();
    peers.insert(2, was_at.clone());
    let transport =
        TcpRaftTransport::new(peers).watching_addresses(1, Arc::clone(&resolver) as Arc<_>);

    let failed = transport.append_entries(2, cluster_append());
    assert!(failed.is_err(), "a down peer still fails");
    let message = failed.expect_err("err").to_string();
    assert!(
        !message.contains("after re-resolving"),
        "the unchanged address must not be retried, got {message}"
    );
    assert_eq!(resolver.asked(), 1, "asked exactly once, not in a loop");
    assert_eq!(
        transport.re_resolved_addresses(),
        0,
        "an unchanged answer is not a move"
    );
    assert_eq!(transport.peer_addr(2).as_deref(), Some(was_at.as_str()));
}

#[test]
fn refresh_peer_addr_is_the_half_a_host_drives_itself() {
    let (mut server, handler) = counting_server(ANY_CLUSTER_ID);
    let live = server.addr().to_string();

    // No resolver: an error rather than a quiet nothing, so a host cannot
    // read "already up to date" off a transport that could never refresh.
    let mut peers = BTreeMap::new();
    peers.insert(2, live.clone());
    let unresolved = TcpRaftTransport::new(peers.clone());
    assert!(unresolved.refresh_peer_addr(2).is_err());

    // Warm the pool, so the claim about dropping stale connections is about
    // connections that exist.
    let resolver = Arc::new(ScriptedResolver::new(live.clone()));
    let transport =
        TcpRaftTransport::new(peers).watching_addresses(1, Arc::clone(&resolver) as Arc<_>);
    transport
        .append_entries(2, cluster_append())
        .expect("warm the pool");
    assert_eq!(handler.calls(), 1);
    assert_eq!(transport.idle_connections(2), 1);

    // The answer is the address already held: nothing replaced, pool intact.
    assert_eq!(
        transport.refresh_peer_addr(2).expect("refresh"),
        None,
        "an unchanged answer replaces nothing"
    );
    assert_eq!(transport.idle_connections(2), 1, "and keeps the warm pool");
    assert_eq!(transport.re_resolved_addresses(), 0);

    // Now it moves. The old address comes back, and the connections pooled to
    // it are gone -- they point at a host that no longer answers.
    let elsewhere = dead_addr();
    *resolver.answer.lock().expect("answer") = elsewhere.clone();
    assert_eq!(
        transport.refresh_peer_addr(2).expect("refresh"),
        Some(live.clone()),
        "the replaced address is returned"
    );
    assert_eq!(transport.peer_addr(2).as_deref(), Some(elsewhere.as_str()));
    assert_eq!(
        transport.idle_connections(2),
        0,
        "connections to the old host must not survive the move"
    );
    assert_eq!(transport.re_resolved_addresses(), 1);
    server.shutdown().expect("shutdown");
}

/// A peer that accepts a connection and then says nothing, for ever.
///
/// The failure a timeout exists for, and the one no amount of retrying fixes:
/// the socket is open, the write succeeded, and the read has nothing to
/// return and nothing to fail on. Holding the accepted streams is the point --
/// dropping them would close the connection and give the caller an error for
/// the wrong reason.
struct Blackhole {
    addr: String,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl Blackhole {
    fn start() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr").to_string();
        let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker_shutdown = Arc::clone(&shutdown);
        let worker = std::thread::spawn(move || {
            let mut held = Vec::new();
            while !worker_shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => held.push(stream),
                    Err(_) => break,
                }
            }
            drop(held);
        });
        Self {
            addr,
            shutdown,
            worker: Some(worker),
        }
    }
}

impl Drop for Blackhole {
    fn drop(&mut self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::Relaxed);
        // Wake the blocked `accept`, the same way the real server does.
        let _ = std::net::TcpStream::connect(&self.addr);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

/// Runs `call` on another thread and gives up after `limit`.
///
/// `None` means it had not returned, which for these tests is the failure
/// being asserted against -- and the reason this helper exists rather than
/// calling directly, because a test that hangs reports nothing at all.
fn within<T: Send + 'static>(
    limit: Duration,
    call: impl FnOnce() -> T + Send + 'static,
) -> Option<T> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(call());
    });
    rx.recv_timeout(limit).ok()
}

#[test]
fn a_send_to_a_peer_that_never_answers_gives_up() {
    let blackhole = Blackhole::start();
    let mut peers = BTreeMap::new();
    peers.insert(2, blackhole.addr.clone());
    let transport = TcpRaftTransport::new(peers).with_timeout(Duration::from_millis(250));
    assert_eq!(transport.timeout(), Some(Duration::from_millis(250)));

    // Ten seconds against a 250ms timeout: this asserts that the send is
    // BOUNDED, not that it is fast. Asserting the 250ms itself would be a
    // claim about how busy the machine is.
    let result = within(Duration::from_secs(10), move || {
        transport.append_entries(2, cluster_append())
    });
    let result = result.expect("the send must return rather than wait for ever");
    let message = match result {
        Err(err) => err.to_string(),
        Ok(response) => panic!("a peer that never answers cannot succeed, got {response:?}"),
    };
    assert!(
        message.contains("failed to read raft RPC"),
        "the failure should be the read, got {message}"
    );
}

#[test]
fn a_timed_out_connection_is_not_pooled() {
    // A peer that went quiet may answer the old request later. Pooling that
    // connection would hand the stale response to the next caller, as the
    // answer to a different request.
    let blackhole = Blackhole::start();
    let mut peers = BTreeMap::new();
    peers.insert(2, blackhole.addr.clone());
    let transport = TcpRaftTransport::new(peers).with_timeout(Duration::from_millis(100));

    let probe = transport.clone();
    let failed = within(Duration::from_secs(10), move || {
        probe.append_entries(2, cluster_append())
    });
    assert!(failed.expect("returned").is_err());
    assert_eq!(
        transport.idle_connections(2),
        0,
        "a connection that timed out must not be handed to the next caller"
    );
}

#[test]
fn a_timeout_does_not_disturb_a_peer_that_answers() {
    // The other half: a bound that also fails a working peer would be worse
    // than no bound at all.
    let (mut server, handler) = counting_server(ANY_CLUSTER_ID);
    let mut peers = BTreeMap::new();
    peers.insert(2, server.addr().to_string());
    let transport = TcpRaftTransport::new(peers).with_timeout(Duration::from_millis(250));

    for _ in 0..3 {
        transport
            .append_entries(2, cluster_append())
            .expect("a peer that answers is unaffected by the timeout");
    }
    assert_eq!(handler.calls(), 3);
    assert_eq!(
        transport.idle_connections(2),
        1,
        "and the connection is still pooled across sends"
    );
    server.shutdown().expect("shutdown");
}

#[test]
fn the_recorded_timeout_reaches_the_transport_and_new_still_waits_for_ever() {
    // `timeout_ms` was recorded and read by nothing. Its default is 250ms,
    // which is what a host gets by building options -- while `new` keeps the
    // unbounded behaviour it has always had, because the crate's own tests
    // and examples call it and a deadline there would fail on a busy machine
    // rather than on a broken peer.
    let defaults = MatrixRaftTransportBuilder::new()
        .bind_address_resolver()
        .build()
        .expect("build");
    assert_eq!(defaults.timeout_ms, 250);
    assert_eq!(
        defaults.into_tcp_transport(BTreeMap::new()).timeout(),
        Some(Duration::from_millis(250))
    );

    let configured = MatrixRaftTransportBuilder::new()
        .set_timeout_ms(1_500)
        .bind_address_resolver()
        .build()
        .expect("build");
    assert_eq!(
        configured.into_tcp_transport(BTreeMap::new()).timeout(),
        Some(Duration::from_millis(1_500))
    );

    // And the watching constructor carries it too, rather than dropping it on
    // the way through.
    let watching = MatrixRaftTransportBuilder::new()
        .set_timeout_ms(1_500)
        .set_dynamic_address_map()
        .bind_address_resolver()
        .build()
        .expect("build");
    let resolver = Arc::new(ScriptedResolver::new("127.0.0.1:1"));
    assert_eq!(
        watching
            .into_tcp_transport_watching(BTreeMap::new(), 1, resolver)
            .timeout(),
        Some(Duration::from_millis(1_500))
    );

    assert_eq!(
        TcpRaftTransport::new(BTreeMap::new()).timeout(),
        None,
        "new waits for ever, as it always has"
    );
}

#[test]
fn a_peer_that_answers_once_and_then_goes_quiet_is_still_bounded() {
    // The pooled path needs the timeout too, and nothing else reaches it: a
    // peer that was never reachable has no pooled connection to reuse, so
    // only a peer that answered *first* exercises this. It is also the
    // realistic failure -- a follower that was healthy and then wedged.
    use std::io::{BufRead, BufReader, Write};

    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr").to_string();
    let worker = std::thread::spawn(move || {
        // One connection, one answer, then silence -- while holding the
        // stream, so the connection stays open and pooled.
        let Ok((stream, _)) = listener.accept() else {
            return;
        };
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        if reader.read_line(&mut line).is_err() {
            return;
        }
        let response = TcpRaftTransportResponse::AppendEntries(TcpRaftRpcResult {
            ok: Some(AppendEntriesResponse {
                term: 2,
                success: true,
                match_index: 0,
                rejection_hint: None,
                rejected_index: None,
                require_snapshot: None,
                snapshot_state: SnapshotState::None,
                lease_confirmation_epoch: 0,
                lease_duration_ms: 0,
            }),
            error: None,
        });
        let mut encoded = serde_json::to_vec(&response).expect("encode");
        encoded.push(b'\n');
        if reader.get_mut().write_all(&encoded).is_err() {
            return;
        }
        // Hold it, answering nothing further, until the test drops the peer.
        std::thread::sleep(Duration::from_secs(20));
        drop(reader);
    });

    let mut peers = BTreeMap::new();
    peers.insert(2, addr);
    let transport = TcpRaftTransport::new(peers).with_timeout(Duration::from_millis(150));

    let first = transport
        .append_entries(2, cluster_append())
        .expect("the first send is answered");
    // No separate "the peer served it" counter. The peer set one AFTER
    // writing the response, so this thread could read it before the peer got
    // there -- which is what it did, under coverage instrumentation, while
    // passing everywhere else. A successful response is already the proof
    // that the peer answered, and it cannot be observed early.
    assert!(first.success);
    assert_eq!(
        transport.idle_connections(2),
        1,
        "the answered connection is pooled, which is what the next send reuses"
    );

    // Now the peer is quiet. The pooled connection must time out rather than
    // park the caller -- and the retry on a fresh connection must too.
    let probe = transport.clone();
    let second = within(Duration::from_secs(10), move || {
        probe.append_entries(2, cluster_append())
    });
    assert!(
        second.expect("the send must return").is_err(),
        "a peer that has gone quiet cannot succeed"
    );
    assert_eq!(transport.idle_connections(2), 0);

    drop(transport);
    let _ = worker;
}

/// Echoes, but only once every caller has arrived.
///
/// The pool bound is about connections that exist **at the same time**, and
/// sequential sends reuse one connection however large the bound is. A barrier
/// is what makes "three at once" a fact rather than a hope about scheduling.
#[derive(Debug)]
struct BarrierEcho {
    barrier: std::sync::Barrier,
}

impl Transport for BarrierEcho {
    fn append_entries(
        &self,
        target: u64,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        self.barrier.wait();
        EchoTransport.append_entries(target, request)
    }

    fn vote(&self, target: u64, request: VoteRequest) -> Result<VoteResponse, RaftError> {
        EchoTransport.vote(target, request)
    }

    fn install_snapshot(
        &self,
        target: u64,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        EchoTransport.install_snapshot(target, request)
    }

    fn read_index(
        &self,
        target: u64,
        request: ReadIndexRequest,
    ) -> Result<ReadIndexResponse, RaftError> {
        EchoTransport.read_index(target, request)
    }
}

/// Sends `count` appends at once through `transport` and waits for them all,
/// giving up after ten seconds.
///
/// The deadline is the point. These tests need the sends to be in flight
/// together, so an implementation that serialised them would **deadlock on the
/// server's barrier** rather than fail -- and a hanging test reports nothing
/// and holds a CI job until its own timeout. `JoinHandle::join` cannot be
/// bounded, so the results come back over a channel instead.
fn send_together(transport: &TcpRaftTransport, count: usize) {
    let (tx, rx) = std::sync::mpsc::channel();
    for _ in 0..count {
        let sender = transport.clone();
        let tx = tx.clone();
        std::thread::spawn(move || {
            let _ = tx.send(sender.append_entries(2, cluster_append()));
        });
    }
    drop(tx);
    for nth in 0..count {
        let sent = rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap_or_else(|_| {
                panic!(
                "only {nth} of {count} concurrent sends finished within 10s: the sends are not \
                 running together, which is what a pool that bounded concurrency instead of \
                 retention would do"
            )
            });
        sent.expect("every concurrent send should be answered");
    }
}

#[test]
fn the_pool_keeps_what_it_was_told_to_and_no_more() {
    let handler = Arc::new(BarrierEcho {
        barrier: std::sync::Barrier::new(3),
    });
    let mut server =
        TcpRaftTransportServer::start("127.0.0.1:0", Arc::clone(&handler)).expect("server");
    let mut peers = BTreeMap::new();
    peers.insert(2, server.addr().to_string());

    // Three connections exist at once; two may be kept.
    let bounded = TcpRaftTransport::new(peers.clone()).with_connections_per_peer(2);
    assert_eq!(bounded.connections_per_peer(), 2);
    send_together(&bounded, 3);
    assert_eq!(
        bounded.idle_connections(2),
        2,
        "the third connection must be dropped rather than pooled"
    );

    server.shutdown().expect("shutdown");

    // The same three sends, with room for all of them.
    let roomy_handler = Arc::new(BarrierEcho {
        barrier: std::sync::Barrier::new(3),
    });
    let mut roomy_server =
        TcpRaftTransportServer::start("127.0.0.1:0", Arc::clone(&roomy_handler)).expect("server");
    let mut roomy_peers = BTreeMap::new();
    roomy_peers.insert(2, roomy_server.addr().to_string());
    let roomy = TcpRaftTransport::new(roomy_peers).with_connections_per_peer(4);
    send_together(&roomy, 3);
    assert_eq!(
        roomy.idle_connections(2),
        3,
        "with room for four, all three are kept"
    );
    roomy_server.shutdown().expect("shutdown");
}

#[test]
fn a_pool_of_one_still_serves_concurrent_sends() {
    // The reading this setting invites and that is wrong: that it caps
    // parallel RPCs to a peer. Three sends have to be in flight together for
    // the barrier to release at all, so if a pool of one serialised them this
    // test would hang rather than fail.
    let handler = Arc::new(BarrierEcho {
        barrier: std::sync::Barrier::new(3),
    });
    let mut server =
        TcpRaftTransportServer::start("127.0.0.1:0", Arc::clone(&handler)).expect("server");
    let mut peers = BTreeMap::new();
    peers.insert(2, server.addr().to_string());

    let narrow = TcpRaftTransport::new(peers).with_connections_per_peer(1);
    send_together(&narrow, 3);
    assert_eq!(narrow.idle_connections(2), 1, "and keeps exactly one warm");
    server.shutdown().expect("shutdown");
}

#[test]
fn the_recorded_connection_count_reaches_the_transport() {
    // `num_connection_group` was recorded and read by nothing.
    let defaults = MatrixRaftTransportBuilder::new()
        .bind_address_resolver()
        .build()
        .expect("build");
    assert_eq!(defaults.num_connection_group, 1);
    assert_eq!(
        defaults
            .into_tcp_transport(BTreeMap::new())
            .connections_per_peer(),
        1,
        "a transport built from default options keeps one, not the four `new` keeps"
    );

    let configured = MatrixRaftTransportBuilder::new()
        .set_num_connection_group(3)
        .bind_address_resolver()
        .build()
        .expect("build");
    assert_eq!(
        configured
            .into_tcp_transport(BTreeMap::new())
            .connections_per_peer(),
        3
    );

    // And through the watching constructor, which builds on the same call.
    let watching = MatrixRaftTransportBuilder::new()
        .set_num_connection_group(3)
        .set_dynamic_address_map()
        .bind_address_resolver()
        .build()
        .expect("build");
    let resolver = Arc::new(ScriptedResolver::new("127.0.0.1:1"));
    assert_eq!(
        watching
            .into_tcp_transport_watching(BTreeMap::new(), 1, resolver)
            .connections_per_peer(),
        3
    );

    // Zero would make every send pay a handshake, which is the opposite of
    // what a pool is for, so it is raised to one.
    assert_eq!(
        TcpRaftTransport::new(BTreeMap::new())
            .with_connections_per_peer(0)
            .connections_per_peer(),
        1
    );
    assert_eq!(
        TcpRaftTransport::new(BTreeMap::new()).connections_per_peer(),
        DEFAULT_CONNECTIONS_PER_PEER
    );
}

fn append_with_entries() -> AppendEntriesRequest {
    AppendEntriesRequest {
        group_id: 1,
        term: 2,
        leader_id: 1,
        prev_log_id: None,
        entries: vec![LogEntry {
            log_id: LogId { term: 2, index: 1 },
            payload: b"not-a-heartbeat".to_vec(),
            is_command: true,
        }],
        leader_commit: 0,
        lease_epoch: 0,
    }
}

fn a_vote(pre_vote: bool) -> VoteRequest {
    VoteRequest {
        group_id: 1,
        term: 2,
        candidate_id: 1,
        last_log_id: None,
        pre_vote,
        force: false,
    }
}

#[test]
fn a_payload_rides_only_on_a_health_check() {
    // A (pre-)vote or an empty append. These are the frames a peer sends to a
    // peer it may not be in contact with yet, so they are the ones worth
    // carrying host data; an append carrying entries is already the frame
    // carrying bulk.
    let heartbeat = TcpRaftTransportRequest::AppendEntries {
        target: 2,
        request: cluster_append(),
    };
    let carrying = TcpRaftTransportRequest::AppendEntries {
        target: 2,
        request: append_with_entries(),
    };
    let vote = TcpRaftTransportRequest::Vote {
        target: 2,
        request: a_vote(false),
    };
    let pre_vote = TcpRaftTransportRequest::Vote {
        target: 2,
        request: a_vote(true),
    };
    let read = TcpRaftTransportRequest::ReadIndex {
        target: 2,
        request: ReadIndexRequest {
            group_id: 1,
            requester_id: 1,
            min_commit_index: 0,
            allow_lease_read: false,
        },
    };

    assert!(matrixraft_is_health_check_request(&heartbeat));
    assert!(matrixraft_is_health_check_request(&vote));
    assert!(matrixraft_is_health_check_request(&pre_vote));
    assert!(!matrixraft_is_health_check_request(&carrying));
    assert!(!matrixraft_is_health_check_request(&read));

    // A batch counts if ANY of its requests does, which is what the comparable
    // engine's parcel flag does too.
    assert!(matrixraft_is_health_check_request(
        &TcpRaftTransportRequest::Batch {
            requests: vec![carrying.clone(), heartbeat.clone()],
        }
    ));
    assert!(!matrixraft_is_health_check_request(
        &TcpRaftTransportRequest::Batch {
            requests: vec![carrying, read],
        }
    ));
    assert!(
        !matrixraft_is_health_check_request(&TcpRaftTransportRequest::Batch {
            requests: Vec::new()
        }),
        "an empty batch carries no health check"
    );
}

/// Starts a server that records the payloads it is shown, with its handler.
fn observing_server(
    answer: impl Fn(&TcpRaftTransportRequest, &str) -> Result<(), RaftError> + Send + Sync + 'static,
) -> (
    TcpRaftTransportServer,
    Arc<CountingEcho>,
    Arc<Mutex<Vec<String>>>,
) {
    let handler = Arc::new(CountingEcho::default());
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&seen);
    let observer = Arc::new(move |request: &TcpRaftTransportRequest, payload: &str| {
        recorded.lock().expect("lock").push(payload.to_string());
        answer(request, payload)
    });
    let server = TcpRaftTransportServer::start_with_payload_observer(
        "127.0.0.1:0",
        Arc::clone(&handler),
        ANY_CLUSTER_ID,
        Some(observer),
    )
    .expect("server");
    (server, handler, seen)
}

fn payload_transport(server: &TcpRaftTransportServer, text: &'static str) -> TcpRaftTransport {
    let mut peers = BTreeMap::new();
    peers.insert(2, server.addr().to_string());
    TcpRaftTransport::new(peers)
        .with_user_payload(Arc::new(move |target| Ok(Some(format!("{text}-{target}")))))
}

#[test]
fn what_a_peer_attaches_reaches_the_observer_and_nothing_else_does() {
    let (mut server, handler, seen) = observing_server(|_, _| Ok(()));
    let transport = payload_transport(&server, "hello");
    assert!(transport.attaches_user_payload());

    transport
        .append_entries(2, cluster_append())
        .expect("the heartbeat is served");
    assert_eq!(handler.calls(), 1, "and the request is still served");
    assert_eq!(server.observed_payloads(), 1);
    assert_eq!(
        seen.lock().expect("lock").as_slice(),
        ["hello-2".to_string()]
    );

    // An append carrying entries attaches nothing, so the observer is not
    // called -- the payload is not on every frame.
    transport
        .append_entries(2, append_with_entries())
        .expect("served");
    assert_eq!(handler.calls(), 2);
    assert_eq!(
        server.observed_payloads(),
        1,
        "an append with entries carries no payload"
    );
    assert_eq!(seen.lock().expect("lock").len(), 1);

    // A transport with no callback sends the same frames with nothing on them.
    let mut peers = BTreeMap::new();
    peers.insert(2, server.addr().to_string());
    let silent = TcpRaftTransport::new(peers);
    assert!(!silent.attaches_user_payload());
    silent.append_entries(2, cluster_append()).expect("served");
    assert_eq!(handler.calls(), 3);
    assert_eq!(server.observed_payloads(), 1);

    server.shutdown().expect("shutdown");
}

#[test]
fn an_observer_that_refuses_refuses_the_frame() {
    // A host that could not accept what it was told must not then serve the
    // request as though it had not been told.
    let (mut server, handler, seen) = observing_server(|_, _| {
        Err(RaftError::Transport(
            "this host will not hold that group".to_string(),
        ))
    });
    let transport = payload_transport(&server, "take-group-9");

    let refused = transport.append_entries(2, cluster_append());
    let message = match refused {
        Err(err) => err.to_string(),
        Ok(response) => panic!("a refused frame must not succeed, got {response:?}"),
    };
    assert!(
        message.contains("will not hold that group"),
        "the observer's own reason should reach the peer, got {message}"
    );
    assert_eq!(
        handler.calls(),
        0,
        "the request must not be served after the payload was refused"
    );
    assert_eq!(seen.lock().expect("lock").len(), 1, "it was still shown");
    assert_eq!(server.observed_payloads(), 0, "but not counted as observed");
    assert_eq!(
        server.refused_foreign_cluster(),
        1,
        "it is counted as a refusal"
    );
    server.shutdown().expect("shutdown");
}

#[test]
fn a_payload_callback_that_fails_fails_the_send() {
    // The sending half of the same principle: a host that cannot say what it
    // needs to say would rather retry than be understood as having nothing to
    // add, so the send fails instead of going out bare.
    let (mut server, handler, _seen) = observing_server(|_, _| Ok(()));
    let mut peers = BTreeMap::new();
    peers.insert(2, server.addr().to_string());
    let transport = TcpRaftTransport::new(peers).with_user_payload(Arc::new(|_| {
        Err(RaftError::Transport(
            "cannot reach the placement service".to_string(),
        ))
    }));

    let failed = transport.append_entries(2, cluster_append());
    assert!(
        failed
            .as_ref()
            .err()
            .map(|err| err.to_string().contains("placement service"))
            .unwrap_or(false),
        "the callback's own error should surface, got {failed:?}"
    );
    assert_eq!(handler.calls(), 0, "and nothing was sent");
    assert_eq!(server.observed_payloads(), 0);

    // But a frame that carries no payload is unaffected: the callback is only
    // asked on a health check.
    transport
        .append_entries(2, append_with_entries())
        .expect("an append with entries never asks the callback");
    assert_eq!(handler.calls(), 1);
    server.shutdown().expect("shutdown");
}

#[test]
fn the_recorded_payload_flag_decides_whether_anything_is_attached() {
    // `user_payload_callback_bound` was recorded and read by nothing. Passing
    // a callback is deliberately not enough on its own.
    let callback: matrixraft::MatrixRaftUserPayload = Arc::new(|_| Ok(Some("x".to_string())));

    let unbound = MatrixRaftTransportBuilder::new()
        .bind_address_resolver()
        .build()
        .expect("build");
    assert!(!unbound.user_payload_callback_bound);
    assert!(
        !unbound
            .into_tcp_transport_with_payload(BTreeMap::new(), Arc::clone(&callback))
            .attaches_user_payload(),
        "without the flag the callback must be ignored"
    );

    let bound = MatrixRaftTransportBuilder::new()
        .set_get_user_payload_callback()
        .bind_address_resolver()
        .build()
        .expect("build");
    assert!(bound.user_payload_callback_bound);
    assert!(bound
        .into_tcp_transport_with_payload(BTreeMap::new(), callback)
        .attaches_user_payload());

    // And the plain constructor attaches nothing whatever the flag says,
    // because it has no callback to attach.
    assert!(!bound
        .into_tcp_transport(BTreeMap::new())
        .attaches_user_payload());
}

#[test]
fn a_peer_that_sends_no_payload_is_read_as_having_none() {
    // Wire compatibility, on bytes: a sender that predates the field writes an
    // envelope with no `user_payload`, and that must decode as "nothing
    // attached" rather than failing or inventing one.
    let bare = TcpRaftTransportRequest::AppendEntries {
        target: 2,
        request: cluster_append(),
    };
    let old_shape = TcpRaftTransportEnvelope {
        cluster_id: 7,
        user_payload: None,
        request: bare.clone(),
    };
    let encoded = serde_json::to_string(&old_shape).expect("encode");
    assert!(
        !encoded.contains("user_payload"),
        "an absent payload should not be written at all, got {encoded}"
    );
    let decoded: TcpRaftTransportEnvelope = serde_json::from_str(&encoded).expect("decode");
    assert_eq!(decoded.user_payload, None);
    assert_eq!(decoded.request, bare);

    // And a stamped envelope still decodes as a bare request for a peer that
    // predates both fields.
    let stamped = TcpRaftTransportEnvelope {
        cluster_id: 7,
        user_payload: Some("hold group 9".to_string()),
        request: bare.clone(),
    };
    let stamped_json = serde_json::to_string(&stamped).expect("encode");
    assert!(stamped_json.contains("hold group 9"));
    let as_old: TcpRaftTransportRequest =
        serde_json::from_str(&stamped_json).expect("an old peer ignores both extra fields");
    assert_eq!(as_old, bare);
}
