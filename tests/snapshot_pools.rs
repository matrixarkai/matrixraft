// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Four snapshot phases, four pools, many groups.
//!
//! The assertion that justifies four pools rather than one is that a stalled
//! phase does not stop the others. Everything else here is bookkeeping around
//! that.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use matrixraft::{
    DriverGroupKey, SnapshotPhase, SnapshotPhaseHandler, SnapshotPoolOptions, SnapshotPools,
};

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

/// Records which phase each piece of work arrived on.
#[derive(Debug, Default)]
struct PhaseRecorder {
    seen: Mutex<Vec<(SnapshotPhase, u64)>>,
}

impl PhaseRecorder {
    fn count(&self, phase: SnapshotPhase) -> usize {
        self.seen
            .lock()
            .expect("recorder mutex")
            .iter()
            .filter(|(p, _)| *p == phase)
            .count()
    }
    fn total(&self) -> usize {
        self.seen.lock().expect("recorder mutex").len()
    }
    fn work_for(&self, phase: SnapshotPhase) -> Vec<u64> {
        let mut items: Vec<u64> = self
            .seen
            .lock()
            .expect("recorder mutex")
            .iter()
            .filter(|(p, _)| *p == phase)
            .map(|(_, w)| *w)
            .collect();
        items.sort_unstable();
        items
    }
}

impl SnapshotPhaseHandler<u64> for PhaseRecorder {
    fn run(&self, phase: SnapshotPhase, work: u64) {
        self.seen
            .lock()
            .expect("recorder mutex")
            .push((phase, work));
    }
}

#[test]
fn a_stalled_phase_does_not_stop_the_others() {
    // This is why there are four pools and four counts. Creating a checkpoint
    // is disk and CPU; sending is a peer that may stop reading. One pool for
    // both means a peer that goes quiet stops this store from checkpointing.
    let pools: SnapshotPools<u64> = SnapshotPools::start(SnapshotPoolOptions {
        snapshot_creator_num: 1,
        snapshot_sender_num: 1,
        snapshot_downloader_num: 1,
        snapshot_loader_num: 1,
        max_queue_depth: 64,
    })
    .expect("pools");

    struct StallOneSend {
        gate: Arc<(Mutex<bool>, Condvar)>,
        create_done: Arc<AtomicU64>,
        send_started: Arc<AtomicU64>,
    }
    impl SnapshotPhaseHandler<u64> for StallOneSend {
        fn run(&self, phase: SnapshotPhase, _work: u64) {
            match phase {
                SnapshotPhase::Send => {
                    self.send_started.fetch_add(1, Ordering::Relaxed);
                    // Bounded, so a failing assertion below cannot wedge the
                    // suite on a parked worker.
                    let (lock, cv) = &*self.gate;
                    let deadline = Instant::now() + Duration::from_secs(10);
                    let mut released = lock.lock().expect("gate");
                    while !*released {
                        let now = Instant::now();
                        if now >= deadline {
                            break;
                        }
                        let (next, _) = cv.wait_timeout(released, deadline - now).expect("gate");
                        released = next;
                    }
                }
                SnapshotPhase::Create => {
                    self.create_done.fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            }
        }
    }

    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let create_done = Arc::new(AtomicU64::new(0));
    let send_started = Arc::new(AtomicU64::new(0));
    let key = DriverGroupKey::new(1, 1);
    pools
        .register_group(
            key,
            Arc::new(StallOneSend {
                gate: Arc::clone(&gate),
                create_done: Arc::clone(&create_done),
                send_started: Arc::clone(&send_started),
            }),
        )
        .expect("register");

    // Stall the single send thread.
    pools.submit(key, SnapshotPhase::Send, 1).expect("submit");
    assert!(
        wait_until("the send stalls", Duration::from_secs(20), || {
            send_started.load(Ordering::Relaxed) >= 1
        }),
        "the send never started, so nothing is stalled"
    );

    // Creating must carry on regardless.
    for n in 0..20 {
        pools.submit(key, SnapshotPhase::Create, n).expect("submit");
    }
    assert!(
        wait_until("creates finish", Duration::from_secs(20), || {
            create_done.load(Ordering::Relaxed) >= 20
        }),
        "a stalled send blocked creating: only {} of 20 finished",
        create_done.load(Ordering::Relaxed)
    );

    {
        let (lock, cv) = &*gate;
        *lock.lock().expect("gate") = true;
        cv.notify_all();
    }
}

#[test]
fn work_runs_on_the_pool_for_its_phase() {
    let pools: SnapshotPools<u64> =
        SnapshotPools::start(SnapshotPoolOptions::default()).expect("pools");
    let key = DriverGroupKey::new(3, 1);
    let recorder = Arc::new(PhaseRecorder::default());
    pools
        .register_group(
            key,
            Arc::clone(&recorder) as Arc<dyn SnapshotPhaseHandler<u64>>,
        )
        .expect("register");

    // Each phase gets its own range, so a phase that received another's work
    // would show it.
    for (index, phase) in SnapshotPhase::ALL.iter().enumerate() {
        let base = (index as u64 + 1) * 100;
        for n in 0..5 {
            pools.submit(key, *phase, base + n).expect("submit");
        }
    }

    assert!(
        wait_until("all phases run", Duration::from_secs(30), || {
            recorder.total() >= 20
        }),
        "only {} of 20 arrived",
        recorder.total()
    );

    for (index, phase) in SnapshotPhase::ALL.iter().enumerate() {
        let base = (index as u64 + 1) * 100;
        assert_eq!(
            recorder.work_for(*phase),
            (0..5).map(|n| base + n).collect::<Vec<_>>(),
            "{} received work addressed to another phase",
            phase.name()
        );
    }
}

#[test]
fn the_thread_count_is_the_four_counts_and_not_the_group_count() {
    let pools: SnapshotPools<u64> = SnapshotPools::start(SnapshotPoolOptions {
        snapshot_creator_num: 1,
        snapshot_sender_num: 2,
        snapshot_downloader_num: 3,
        snapshot_loader_num: 4,
        max_queue_depth: 16,
    })
    .expect("pools");
    assert_eq!(pools.thread_count(), 10);

    let recorder = Arc::new(PhaseRecorder::default());
    for group_id in 1..=64 {
        pools
            .register_group(
                DriverGroupKey::new(group_id, 1),
                Arc::clone(&recorder) as Arc<dyn SnapshotPhaseHandler<u64>>,
            )
            .expect("register");
    }
    assert_eq!(pools.group_count(), 64);
    assert_eq!(
        pools.thread_count(),
        10,
        "sixty-four groups must not add threads"
    );

    for phase in SnapshotPhase::ALL {
        let stats = pools.stats(phase);
        assert_eq!(stats.threads, pools.options().threads_for(phase));
    }
}

#[test]
fn cancelling_a_group_stops_all_four_phases() {
    let pools: SnapshotPools<u64> =
        SnapshotPools::start(SnapshotPoolOptions::default()).expect("pools");
    let key = DriverGroupKey::new(7, 1);
    let recorder = Arc::new(PhaseRecorder::default());
    pools
        .register_group(
            key,
            Arc::clone(&recorder) as Arc<dyn SnapshotPhaseHandler<u64>>,
        )
        .expect("register");
    pools.submit(key, SnapshotPhase::Load, 1).expect("submit");
    assert!(
        wait_until("the load runs", Duration::from_secs(20), || {
            recorder.count(SnapshotPhase::Load) >= 1
        }),
        "nothing ran, so cancelling proves nothing"
    );

    assert!(pools.cancel_group(key));
    assert!(!pools.cancel_group(key), "cancelling twice finds nothing");
    assert_eq!(pools.group_count(), 0);

    for phase in SnapshotPhase::ALL {
        assert!(
            pools.submit(key, phase, 99).is_err(),
            "{} still accepted work for a cancelled group",
            phase.name()
        );
    }
}

#[test]
fn a_group_cannot_register_twice() {
    let pools: SnapshotPools<u64> =
        SnapshotPools::start(SnapshotPoolOptions::default()).expect("pools");
    let key = DriverGroupKey::new(2, 1);
    let recorder = Arc::new(PhaseRecorder::default());
    pools
        .register_group(
            key,
            Arc::clone(&recorder) as Arc<dyn SnapshotPhaseHandler<u64>>,
        )
        .expect("first");
    assert!(pools
        .register_group(key, recorder as Arc<dyn SnapshotPhaseHandler<u64>>)
        .is_err());
    assert_eq!(pools.group_count(), 1);
}

#[test]
fn every_phase_needs_a_thread() {
    // A zero anywhere would leave one phase with nothing to run it, and work
    // for that phase would queue for ever.
    for phase in SnapshotPhase::ALL {
        let mut options = SnapshotPoolOptions::default();
        match phase {
            SnapshotPhase::Create => options.snapshot_creator_num = 0,
            SnapshotPhase::Send => options.snapshot_sender_num = 0,
            SnapshotPhase::Download => options.snapshot_downloader_num = 0,
            SnapshotPhase::Load => options.snapshot_loader_num = 0,
        }
        let error = SnapshotPools::<u64>::start(options).expect_err("must refuse");
        assert!(
            format!("{error:?}").contains(phase.name()),
            "the error should name the phase with no threads: {error:?}"
        );
    }
}

#[test]
fn stats_count_submissions_and_work_done_per_phase() {
    let pools: SnapshotPools<u64> =
        SnapshotPools::start(SnapshotPoolOptions::default()).expect("pools");
    let key = DriverGroupKey::new(4, 1);
    let recorder = Arc::new(PhaseRecorder::default());
    pools
        .register_group(
            key,
            Arc::clone(&recorder) as Arc<dyn SnapshotPhaseHandler<u64>>,
        )
        .expect("register");

    for n in 0..7 {
        pools
            .submit(key, SnapshotPhase::Download, n)
            .expect("submit");
    }
    assert!(
        wait_until("downloads run", Duration::from_secs(20), || {
            pools.stats(SnapshotPhase::Download).handled >= 7
        }),
        "only {} handled",
        pools.stats(SnapshotPhase::Download).handled
    );

    let download = pools.stats(SnapshotPhase::Download);
    assert_eq!(download.submitted, 7);
    assert_eq!(download.handled, 7);
    assert_eq!(download.refused, 0);

    // The other three did nothing, and say so.
    for phase in [
        SnapshotPhase::Create,
        SnapshotPhase::Send,
        SnapshotPhase::Load,
    ] {
        let stats = pools.stats(phase);
        assert_eq!(stats.submitted, 0, "{} should be idle", phase.name());
        assert_eq!(stats.handled, 0);
    }
    assert_eq!(pools.all_stats().len(), 4);
}
