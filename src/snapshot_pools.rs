// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Snapshot work for many groups, on a pool per phase.
//!
//! Moving a snapshot is four jobs, not one. **Creating** it reads the state
//! machine and writes a checkpoint. **Sending** it pushes chunks at a peer that
//! asked. **Downloading** pulls them from a peer that has them. **Loading**
//! installs what arrived. They cost different things -- one is disk and CPU,
//! two are network, one is disk again -- and they stall for different reasons.
//!
//! That is why there are four counts rather than one:
//! `snapshot_creator_num`, `snapshot_sender_num`, `snapshot_downloader_num`
//! and `snapshot_loader_num`. Each sizes its own pool, so a peer that stops
//! reading cannot stop this store from creating checkpoints, and a slow disk
//! cannot stop it from serving the snapshots it already has.
//!
//! The pools carry the host's own work type and hand it back; nothing here
//! knows what a snapshot is. What it owns is which pool the work runs on, how
//! many threads that pool has, and what happens to in-flight work when a group
//! goes away.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::{
    DriverGroupKey, DriverMailHandler, DriverOptions, DriverWorkerPool, MailPriority, RaftError,
};

/// Which of the four jobs a piece of snapshot work is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SnapshotPhase {
    /// Read the state machine and write a checkpoint.
    Create,
    /// Push chunks to a peer that asked for them.
    Send,
    /// Pull chunks from a peer that has them.
    Download,
    /// Install what arrived.
    Load,
}

impl SnapshotPhase {
    pub const ALL: [SnapshotPhase; 4] = [
        SnapshotPhase::Create,
        SnapshotPhase::Send,
        SnapshotPhase::Download,
        SnapshotPhase::Load,
    ];

    pub fn name(&self) -> &'static str {
        match self {
            SnapshotPhase::Create => "create",
            SnapshotPhase::Send => "send",
            SnapshotPhase::Download => "download",
            SnapshotPhase::Load => "load",
        }
    }
}

/// How the four pools are sized.
///
/// The names match `MatrixRaftGroupContext` and `MatrixRaftNodeCreator`, which
/// have carried them as a record of intended wiring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotPoolOptions {
    pub snapshot_creator_num: usize,
    pub snapshot_sender_num: usize,
    pub snapshot_downloader_num: usize,
    pub snapshot_loader_num: usize,
    /// Per group, per phase. A group that stalls one phase is refused more of
    /// that work rather than queueing without bound.
    pub max_queue_depth: usize,
}

impl Default for SnapshotPoolOptions {
    fn default() -> Self {
        Self {
            snapshot_creator_num: 1,
            snapshot_sender_num: 2,
            snapshot_downloader_num: 2,
            snapshot_loader_num: 1,
            max_queue_depth: 256,
        }
    }
}

impl SnapshotPoolOptions {
    pub fn threads_for(&self, phase: SnapshotPhase) -> usize {
        match phase {
            SnapshotPhase::Create => self.snapshot_creator_num,
            SnapshotPhase::Send => self.snapshot_sender_num,
            SnapshotPhase::Download => self.snapshot_downloader_num,
            SnapshotPhase::Load => self.snapshot_loader_num,
        }
    }

    fn validate(&self) -> Result<(), RaftError> {
        for phase in SnapshotPhase::ALL {
            if self.threads_for(phase) == 0 {
                return Err(RaftError::InvalidRequest(format!(
                    "snapshot {} pool requires at least one thread",
                    phase.name()
                )));
            }
        }
        Ok(())
    }

    fn driver_options(&self, phase: SnapshotPhase) -> DriverOptions {
        DriverOptions {
            worker_num: self.threads_for(phase),
            max_queue_depth: self.max_queue_depth,
            ..DriverOptions::default()
        }
    }
}

/// What a host does with a piece of snapshot work.
///
/// Called from the pool for the phase, so two phases can be running for the
/// same group at once -- which is the point of separating them.
pub trait SnapshotPhaseHandler<Work>: Send + Sync {
    fn run(&self, phase: SnapshotPhase, work: Work);
}

/// Routes one phase's mail to the host's handler.
struct PhaseAdapter<Work> {
    phase: SnapshotPhase,
    handler: Arc<dyn SnapshotPhaseHandler<Work>>,
    handled: Arc<AtomicU64>,
}

impl<Work: Send + 'static> DriverMailHandler<Work> for PhaseAdapter<Work> {
    fn handle_mail(&self, work: Vec<Work>) {
        for item in work {
            self.handler.run(self.phase, item);
            self.handled.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// What each pool has done.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotPhaseStats {
    pub phase: SnapshotPhase,
    pub threads: usize,
    pub submitted: u64,
    pub handled: u64,
    pub refused: u64,
}

/// Runs the four snapshot phases for many groups, each on its own pool.
pub struct SnapshotPools<Work> {
    options: SnapshotPoolOptions,
    create: DriverWorkerPool<Work>,
    send: DriverWorkerPool<Work>,
    download: DriverWorkerPool<Work>,
    load: DriverWorkerPool<Work>,
    groups: Mutex<BTreeSet<DriverGroupKey>>,
    submitted: [Arc<AtomicU64>; 4],
    handled: [Arc<AtomicU64>; 4],
    refused: [Arc<AtomicU64>; 4],
}

impl<Work> std::fmt::Debug for SnapshotPools<Work> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotPools")
            .field("options", &self.options)
            .field(
                "registered_groups",
                &self.groups.lock().map(|g| g.len()).unwrap_or(0),
            )
            .finish()
    }
}

fn phase_index(phase: SnapshotPhase) -> usize {
    match phase {
        SnapshotPhase::Create => 0,
        SnapshotPhase::Send => 1,
        SnapshotPhase::Download => 2,
        SnapshotPhase::Load => 3,
    }
}

impl<Work: Send + 'static> SnapshotPools<Work> {
    pub fn start(options: SnapshotPoolOptions) -> Result<Self, RaftError> {
        options.validate()?;
        Ok(Self {
            options,
            create: DriverWorkerPool::start(options.driver_options(SnapshotPhase::Create))?,
            send: DriverWorkerPool::start(options.driver_options(SnapshotPhase::Send))?,
            download: DriverWorkerPool::start(options.driver_options(SnapshotPhase::Download))?,
            load: DriverWorkerPool::start(options.driver_options(SnapshotPhase::Load))?,
            groups: Mutex::new(BTreeSet::new()),
            submitted: std::array::from_fn(|_| Arc::new(AtomicU64::new(0))),
            handled: std::array::from_fn(|_| Arc::new(AtomicU64::new(0))),
            refused: std::array::from_fn(|_| Arc::new(AtomicU64::new(0))),
        })
    }

    pub fn options(&self) -> &SnapshotPoolOptions {
        &self.options
    }

    fn pool(&self, phase: SnapshotPhase) -> &DriverWorkerPool<Work> {
        match phase {
            SnapshotPhase::Create => &self.create,
            SnapshotPhase::Send => &self.send,
            SnapshotPhase::Download => &self.download,
            SnapshotPhase::Load => &self.load,
        }
    }

    /// Registers a group with all four pools.
    ///
    /// All or nothing: a registration that fails part way removes what it
    /// managed, so a group is never half present.
    pub fn register_group(
        &self,
        key: DriverGroupKey,
        handler: Arc<dyn SnapshotPhaseHandler<Work>>,
    ) -> Result<(), RaftError> {
        {
            let mut groups = self.groups.lock().expect("snapshot groups mutex poisoned");
            if groups.contains(&key) {
                return Err(RaftError::InvalidRequest(format!(
                    "snapshot pools already hold group {} node {}",
                    key.group_id, key.node_id
                )));
            }
            groups.insert(key);
        }

        let mut registered: Vec<SnapshotPhase> = Vec::with_capacity(4);
        for phase in SnapshotPhase::ALL {
            let adapter = PhaseAdapter {
                phase,
                handler: Arc::clone(&handler),
                handled: Arc::clone(&self.handled[phase_index(phase)]),
            };
            match self
                .pool(phase)
                .register_group(key, Arc::new(adapter) as Arc<dyn DriverMailHandler<Work>>)
            {
                Ok(()) => registered.push(phase),
                Err(error) => {
                    for done in registered {
                        self.pool(done).cancel_group(key);
                    }
                    self.groups
                        .lock()
                        .expect("snapshot groups mutex poisoned")
                        .remove(&key);
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    /// Stops all four phases for a group.
    pub fn cancel_group(&self, key: DriverGroupKey) -> bool {
        let present = self
            .groups
            .lock()
            .expect("snapshot groups mutex poisoned")
            .remove(&key);
        if !present {
            return false;
        }
        for phase in SnapshotPhase::ALL {
            self.pool(phase).cancel_group(key);
        }
        true
    }

    /// Queues work on the pool for its phase.
    pub fn submit(
        &self,
        key: DriverGroupKey,
        phase: SnapshotPhase,
        work: Work,
    ) -> Result<(), RaftError> {
        let index = phase_index(phase);
        match self.pool(phase).send(key, MailPriority::Normal, work) {
            Ok(()) => {
                self.submitted[index].fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(error) => {
                self.refused[index].fetch_add(1, Ordering::Relaxed);
                Err(error)
            }
        }
    }

    pub fn group_count(&self) -> usize {
        self.groups
            .lock()
            .expect("snapshot groups mutex poisoned")
            .len()
    }

    /// Threads across all four pools. A property of the options, not of how
    /// many groups are registered.
    pub fn thread_count(&self) -> usize {
        SnapshotPhase::ALL
            .iter()
            .map(|phase| self.options.threads_for(*phase))
            .sum()
    }

    pub fn stats(&self, phase: SnapshotPhase) -> SnapshotPhaseStats {
        let index = phase_index(phase);
        SnapshotPhaseStats {
            phase,
            threads: self.options.threads_for(phase),
            submitted: self.submitted[index].load(Ordering::Relaxed),
            handled: self.handled[index].load(Ordering::Relaxed),
            refused: self.refused[index].load(Ordering::Relaxed),
        }
    }

    pub fn all_stats(&self) -> Vec<SnapshotPhaseStats> {
        SnapshotPhase::ALL
            .iter()
            .map(|phase| self.stats(*phase))
            .collect()
    }

    pub fn stop(&mut self) {
        self.create.stop();
        self.send.stop();
        self.download.stop();
        self.load.stop();
    }
}
