// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    ApplySnapshotFence, Config, HardState, LogRetainedRange, MatrixRaftWalBackend,
    MatrixRaftWalBatchCoordinator, MatrixRaftWalBatchOptions, MatrixRaftWalBatchRecord,
    MatrixRaftWalBatchSink, MatrixRaftWalHandle, MatrixRaftWalOpenOptions, MatrixRaftWalStorage,
    Membership, NodeOptions, NodeRuntime, Peer, RaftError, ReplicaRole, StorageApplyFence, Term,
    WalCompactionReport, WalLifecycleStatus, WalRecord, WalRecoveryReport,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Duration;

fn record(group_id: u64, node_id: u64) -> WalRecord {
    WalRecord {
        group_id,
        node_id,
        hard_state: HardState {
            current_term: 1,
            voted_for: Some(node_id),
            committed: None,
        },
        membership: Membership {
            group_id,
            voters: vec![node_id],
            learners: Vec::new(),
            witnesses: Vec::new(),
            epoch: 0,
        },
        entries: Vec::new(),
        entries_are_delta: false,
        installed_snapshot: None,
        apply_snapshot_fence: ApplySnapshotFence {
            applied_index: 0,
            commit_index: 0,
            installed_snapshot_index: 0,
            first_retained_log_index: 0,
        },
        checksum: String::new(),
    }
}

#[derive(Default)]
struct MemoryBatchState {
    batches: Vec<Vec<(u64, u64)>>,
    records: BTreeMap<(u64, u64), Vec<WalRecord>>,
    fail_next: bool,
}

#[derive(Default)]
struct MemoryBatchSink {
    state: Mutex<MemoryBatchState>,
}

impl MemoryBatchSink {
    fn batch_sizes(&self) -> Vec<usize> {
        self.state
            .lock()
            .expect("batch state")
            .batches
            .iter()
            .map(Vec::len)
            .collect()
    }

    fn clear_batches(&self) {
        self.state.lock().expect("batch state").batches.clear();
    }

    fn fail_next(&self) {
        self.state.lock().expect("batch state").fail_next = true;
    }

    fn latest(&self, key: (u64, u64)) -> Option<WalRecord> {
        self.state
            .lock()
            .expect("batch state")
            .records
            .get(&key)
            .and_then(|records| records.last())
            .cloned()
    }
}

impl MatrixRaftWalBatchSink for MemoryBatchSink {
    fn commit(&self, records: &[MatrixRaftWalBatchRecord]) -> Result<(), RaftError> {
        let mut state = self.state.lock().expect("batch state");
        if std::mem::take(&mut state.fail_next) {
            return Err(RaftError::Storage("injected batch refusal".to_string()));
        }
        state.batches.push(
            records
                .iter()
                .map(|record| (record.group_id(), record.node_id()))
                .collect(),
        );
        for record in records {
            state
                .records
                .entry((record.group_id(), record.node_id()))
                .or_default()
                .push(record.record.clone());
        }
        Ok(())
    }
}

fn coordinator(sink: Arc<MemoryBatchSink>) -> MatrixRaftWalBatchCoordinator {
    MatrixRaftWalBatchCoordinator::start(
        MatrixRaftWalBatchOptions {
            max_records: 32,
            max_bytes: 1024 * 1024,
            queue_depth: 64,
            max_wait: Duration::from_millis(50),
        },
        sink,
    )
    .expect("batch coordinator")
}

#[test]
fn concurrent_groups_share_one_atomic_commit() {
    let sink = Arc::new(MemoryBatchSink::default());
    let coordinator = coordinator(Arc::clone(&sink));
    let start = Arc::new(Barrier::new(9));
    let mut workers = Vec::new();
    for group_id in 1..=8 {
        let coordinator = coordinator.clone();
        let start = Arc::clone(&start);
        workers.push(thread::spawn(move || {
            start.wait();
            coordinator.submit(record(group_id, 1))
        }));
    }
    start.wait();
    for worker in workers {
        worker.join().expect("submit thread").expect("batch commit");
    }
    assert_eq!(sink.batch_sizes(), vec![8]);
}

#[test]
fn record_and_byte_limits_close_a_batch_without_stalling_large_records() {
    let run = |max_records: usize, max_bytes: usize, count: usize| {
        let sink = Arc::new(MemoryBatchSink::default());
        let coordinator = MatrixRaftWalBatchCoordinator::start(
            MatrixRaftWalBatchOptions {
                max_records,
                max_bytes,
                queue_depth: 16,
                max_wait: Duration::from_millis(50),
            },
            Arc::clone(&sink) as Arc<dyn MatrixRaftWalBatchSink>,
        )
        .expect("batch coordinator");
        let start = Arc::new(Barrier::new(count + 1));
        let workers: Vec<_> = (1..=count)
            .map(|group_id| {
                let coordinator = coordinator.clone();
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    coordinator.submit(record(group_id as u64, 1))
                })
            })
            .collect();
        start.wait();
        for worker in workers {
            worker.join().expect("submit thread").expect("batch commit");
        }
        sink.batch_sizes()
    };

    let mut record_limited = run(2, 1024 * 1024, 5);
    record_limited.sort_unstable();
    assert_eq!(record_limited, vec![1, 2, 2]);
    assert_eq!(run(16, 1, 2), vec![1, 1]);
}

#[test]
fn a_failed_atomic_commit_refuses_every_waiter() {
    let sink = Arc::new(MemoryBatchSink::default());
    sink.fail_next();
    let coordinator = coordinator(Arc::clone(&sink));
    let start = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for group_id in 1..=2 {
        let coordinator = coordinator.clone();
        let start = Arc::clone(&start);
        workers.push(thread::spawn(move || {
            start.wait();
            coordinator.submit(record(group_id, 1))
        }));
    }
    start.wait();
    for worker in workers {
        let error = worker
            .join()
            .expect("submit thread")
            .expect_err("commit must fail");
        assert!(error.to_string().contains("injected batch refusal"));
    }
    assert!(sink.batch_sizes().is_empty());
}

struct CoordinatedMemoryStorage {
    sink: Arc<MemoryBatchSink>,
    coordinator: MatrixRaftWalBatchCoordinator,
}

impl MatrixRaftWalStorage for CoordinatedMemoryStorage {
    fn open(
        &self,
        options: MatrixRaftWalOpenOptions,
    ) -> Result<(MatrixRaftWalHandle, WalRecoveryReport), RaftError> {
        let key = (options.group_id, options.node_id);
        let recovered = self.sink.latest(key);
        let coverage = recovered.as_ref().and_then(record_coverage);
        Ok((
            MatrixRaftWalHandle::custom(Box::new(CoordinatedMemoryBackend {
                key,
                coverage,
                sink: Arc::clone(&self.sink),
                coordinator: self.coordinator.clone(),
            })),
            WalRecoveryReport {
                surviving_records: usize::from(recovered.is_some()),
                recovered,
                truncated_corrupt_tail: false,
                removed_records: 0,
                segments_scanned: 0,
                checksum_format: None,
                retained_range: None,
            },
        ))
    }
}

struct CoordinatedMemoryBackend {
    key: (u64, u64),
    coverage: Option<(u64, u64, Term)>,
    sink: Arc<MemoryBatchSink>,
    coordinator: MatrixRaftWalBatchCoordinator,
}

impl MatrixRaftWalBackend for CoordinatedMemoryBackend {
    fn active_coverage(&self) -> Option<(u64, u64, Term)> {
        self.coverage
    }

    fn append_record(&mut self, record: WalRecord) -> Result<(), RaftError> {
        self.coordinator.submit(record.clone())?;
        self.coverage = record_coverage(&record);
        Ok(())
    }

    fn compact_through_with_fence(
        &mut self,
        log_index: u64,
        _fence: &StorageApplyFence,
    ) -> Result<WalCompactionReport, RaftError> {
        Ok(WalCompactionReport {
            requested_log_index: log_index,
            released_segments: 0,
            retained_range: retained_range(self.coverage),
            fence_valid: true,
            blocker: None,
        })
    }

    fn status(&self) -> WalLifecycleStatus {
        let records = self
            .sink
            .state
            .lock()
            .expect("batch state")
            .records
            .get(&self.key)
            .map_or(0, Vec::len) as u64;
        let range = retained_range(self.coverage);
        WalLifecycleStatus {
            segment_count: u64::from(records > 0),
            active_segment_id: 0,
            first_retained_segment_id: 0,
            last_retained_segment_id: 0,
            total_bytes: 0,
            active_segment_bytes: 0,
            total_records: records,
            first_sequence: 0,
            last_sequence: records.saturating_sub(1),
            first_log_index: range.first_log_index,
            last_log_index: range.last_log_index,
            released_segment_count: 0,
            slow_fsync_backpressure_observed: false,
            fsync_count: records,
            slow_fsync_threshold_ms: 0,
            slow_fsync_count: 0,
            consecutive_slow_fsync_count: 0,
            max_fsync_elapsed_ms: 0,
            compacted_after_slow_fsync_count: 0,
        }
    }

    fn release_idle_segment_descriptor(&mut self) -> bool {
        false
    }
}

fn record_coverage(record: &WalRecord) -> Option<(u64, u64, Term)> {
    let first = record.entries.first()?;
    let last = record.entries.last()?;
    Some((first.log_id.index, last.log_id.index, last.log_id.term))
}

fn retained_range(coverage: Option<(u64, u64, Term)>) -> LogRetainedRange {
    LogRetainedRange {
        first_log_index: coverage.map_or(0, |coverage| coverage.0),
        last_log_index: coverage.map_or(0, |coverage| coverage.1),
        first_segment_id: 0,
        last_segment_id: 0,
        record_count: u64::from(coverage.is_some()),
    }
}

fn runtime_options(group_id: u64) -> NodeOptions {
    let peer = Peer {
        node_id: 1,
        raft_addr: format!("127.0.0.1:{}", 42_000 + group_id),
        snapshot_addr: format!("127.0.0.1:{}", 43_000 + group_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    };
    NodeOptions {
        group_id,
        node_id: 1,
        raft_addr: peer.raft_addr.clone(),
        snapshot_addr: peer.snapshot_addr.clone(),
        wal_dir: format!("unused-{group_id}"),
        snapshot_dir: format!("unused-{group_id}"),
        role: ReplicaRole::Voter,
        config: Config::default(),
        peers: vec![peer],
    }
}

fn propose_together(
    runtimes: &[Arc<NodeRuntime>],
    sequence: usize,
) -> Vec<Result<matrixraft::LogId, RaftError>> {
    let start = Arc::new(Barrier::new(runtimes.len() + 1));
    let workers: Vec<_> = runtimes
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, runtime)| {
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                runtime.propose(format!("write-{sequence}-{index}").into_bytes())
            })
        })
        .collect();
    start.wait();
    workers
        .into_iter()
        .map(|worker| worker.join().expect("proposal thread"))
        .collect()
}

#[test]
fn runtime_replies_wait_for_the_shared_commit() {
    let sink = Arc::new(MemoryBatchSink::default());
    let coordinator = coordinator(Arc::clone(&sink));
    let storage = Arc::new(CoordinatedMemoryStorage {
        sink: Arc::clone(&sink),
        coordinator,
    });
    let mut runtimes = Vec::new();
    for group_id in [101, 102] {
        let mut runtime =
            NodeRuntime::create_with_wal_storage(runtime_options(group_id), storage.clone())
                .expect("runtime");
        runtime.start().expect("start");
        runtime.campaign(true).expect("single-node leadership");
        runtimes.push(Arc::new(runtime));
    }
    sink.clear_batches();

    let committed = propose_together(&runtimes, 1);
    assert!(committed.iter().all(Result::is_ok));

    assert_eq!(sink.batch_sizes(), vec![2]);
    {
        let state = sink.state.lock().expect("batch state");
        assert_eq!(state.records.get(&(101, 1)).map(Vec::len), Some(1));
        assert_eq!(state.records.get(&(102, 1)).map(Vec::len), Some(1));
    }

    sink.fail_next();
    let refused = propose_together(&runtimes, 2);
    assert!(refused.iter().all(|result| {
        result
            .as_ref()
            .expect_err("failed commit must refuse proposal")
            .to_string()
            .contains("injected batch refusal")
    }));
    assert_eq!(sink.batch_sizes(), vec![2]);
}
