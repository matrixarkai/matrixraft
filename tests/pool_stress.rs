// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Does every mail the pool accepts reach its handler?
//!
//! The hosted group runtime depends on it, and there is reason to doubt it.
//! Removing a redundant-looking pool send from the tick path wedged a store
//! of four thousand groups: a command sent, every worker parked, nothing
//! left to rouse them. That send was re-arming each group once an interval
//! and, by accident, covering a wake-up that goes missing somewhere.
//!
//! Nothing here involves raft. The pool is driven on its own so that a
//! failure is the pool's, and `send` returning `Ok` is treated as a promise:
//! the mail is the pool's to deliver, and a count that comes up short is a
//! mail accepted and dropped, or a worker asleep over work it was handed.
//!
//! # What these do not cover, and why no test here could
//!
//! Breaking `fire` so it never wakes a worker, and breaking it so it marks a
//! channel active without queueing it, are both caught. Breaking
//! `MailChannel::consume` so it assigns where it must append is **not** --
//! and that is the one real bug this family has had.
//!
//! It is unreachable from here. That assignment only loses mail when two
//! threads are inside `fetch` for the same channel at once, and the pool
//! pins each group to one worker and gives each worker its own selector, so
//! exactly one thread ever fetches a given channel. The fix in
//! `consume` is defending a shape the pool no longer builds.
//!
//! Worth knowing before someone reads that fix as load-bearing here, or
//! writes a test they think covers it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use matrixraft::{
    DriverGroupKey, DriverMailHandler, DriverOptions, DriverWorkerPool, MailPriority,
};

#[derive(Debug, Default)]
struct Counted {
    handled: AtomicU64,
}

impl DriverMailHandler<u64> for Counted {
    fn handle_mail(&self, mails: Vec<u64>) {
        self.handled
            .fetch_add(mails.len() as u64, Ordering::Relaxed);
    }
}

/// Waits for `done`, or gives up, so a failure is an assertion and not a
/// suite that hangs.
fn settle(what: &str, timeout: Duration, mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if done() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let answer = done();
    if !answer {
        eprintln!("gave up waiting for {what}");
    }
    answer
}

#[test]
fn every_mail_the_pool_accepts_reaches_its_handler() {
    let groups = 256_u64;
    let per_group = 40_u64;
    let pool: DriverWorkerPool<u64> = DriverWorkerPool::start(DriverOptions {
        worker_num: 4,
        max_messages_each_poll: 16,
        ..DriverOptions::default()
    })
    .expect("pool");

    let counted = Arc::new(Counted::default());
    for group_id in 1..=groups {
        pool.register_group(
            DriverGroupKey::new(group_id, 1),
            Arc::clone(&counted) as Arc<dyn DriverMailHandler<u64>>,
        )
        .expect("register");
    }

    // Several senders at once, because a single sender never races the
    // worker that is draining the channel it is writing to -- and that race
    // is the whole point.
    let senders = 4;
    let ready = Arc::new(Barrier::new(senders));
    let accepted = Arc::new(AtomicU64::new(0));
    std::thread::scope(|scope| {
        for sender in 0..senders {
            let pool = &pool;
            let ready = Arc::clone(&ready);
            let accepted = Arc::clone(&accepted);
            scope.spawn(move || {
                ready.wait();
                for round in 0..per_group {
                    for group_id in 1..=groups {
                        if (group_id as usize + sender) % senders != 0 {
                            continue;
                        }
                        let key = DriverGroupKey::new(group_id, 1);
                        if pool.send(key, MailPriority::Normal, round).is_ok() {
                            accepted.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            });
        }
    });

    let sent = accepted.load(Ordering::Relaxed);
    assert!(
        sent > 0,
        "the pool refused everything, so this proves nothing"
    );
    let arrived = settle(
        "every accepted mail to be handled",
        Duration::from_secs(30),
        || counted.handled.load(Ordering::Relaxed) >= sent,
    );
    let handled = counted.handled.load(Ordering::Relaxed);
    assert!(
        arrived,
        "the pool accepted {sent} mails and handled {handled}; {} never reached a \
         handler, so either a mail was dropped after being accepted or a worker is \
         asleep over work it was given",
        sent - handled
    );
}

#[test]
fn a_group_written_to_while_it_is_being_drained_loses_nothing() {
    // The narrow case: one group, two senders, a worker draining it
    // throughout. `fire` declines to notify when a channel is already queued,
    // so this is where that decision has to be right.
    let pool: DriverWorkerPool<u64> = DriverWorkerPool::start(DriverOptions {
        worker_num: 1,
        max_messages_each_poll: 4,
        ..DriverOptions::default()
    })
    .expect("pool");
    let key = DriverGroupKey::new(7, 1);
    let counted = Arc::new(Counted::default());
    pool.register_group(key, Arc::clone(&counted) as Arc<dyn DriverMailHandler<u64>>)
        .expect("register");

    let senders = 3;
    let each = 500_u64;
    let ready = Arc::new(Barrier::new(senders));
    let accepted = Arc::new(AtomicU64::new(0));
    std::thread::scope(|scope| {
        for _ in 0..senders {
            let pool = &pool;
            let ready = Arc::clone(&ready);
            let accepted = Arc::clone(&accepted);
            scope.spawn(move || {
                ready.wait();
                for n in 0..each {
                    if pool.send(key, MailPriority::Normal, n).is_ok() {
                        accepted.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    });

    let sent = accepted.load(Ordering::Relaxed);
    assert!(
        sent > 0,
        "the pool refused everything, so this proves nothing"
    );
    let arrived = settle(
        "the drained group to catch up",
        Duration::from_secs(30),
        || counted.handled.load(Ordering::Relaxed) >= sent,
    );
    let handled = counted.handled.load(Ordering::Relaxed);
    assert!(
        arrived,
        "one group accepted {sent} mails and handled {handled}, short by {}",
        sent - handled
    );
}

/// Takes a lock and costs time, the way the real handler does.
///
/// `PooledGroupWorker` locks the group and runs raft work inside it, so the
/// worker is away from its selector for a while and senders queue behind it.
/// A handler that returns instantly never leaves that window open.
#[derive(Debug, Default)]
struct SlowCounted {
    handled: AtomicU64,
    /// One lock for every group, as a group's own lock is: held across the
    /// whole of its handler and contended only by work for that group.
    busy: Mutex<()>,
}

impl DriverMailHandler<u64> for SlowCounted {
    fn handle_mail(&self, mails: Vec<u64>) {
        let _held = self
            .busy
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Long enough that senders arrive while the worker is inside here,
        // short enough that the test is not a soak.
        std::thread::sleep(Duration::from_micros(200));
        self.handled
            .fetch_add(mails.len() as u64, Ordering::Relaxed);
    }
}

#[test]
fn mail_arriving_while_a_slow_handler_runs_is_not_lost() {
    // The case the other two miss. Their handler returns at once, so the
    // worker is never away from its selector while a sender is writing --
    // and that window is where a wake-up would go missing.
    //
    // It passes, and it did not reproduce the wedge it was written to chase:
    // a four-thousand-group store where a command was sent, every worker
    // parked, and nothing left to rouse them. Worth saying, so the next
    // person does not read this file as proof the pool cannot lose a
    // wake-up. It proves only that these patterns do not make it.
    //
    // Note also that mail reaches a handler in batches, so the sleep below
    // is paid per batch and not per mail -- the window it opens is smaller
    // than the arithmetic suggests.
    let groups = 32_u64;
    let per_group = 20_u64;
    let pool: DriverWorkerPool<u64> = DriverWorkerPool::start(DriverOptions {
        worker_num: 2,
        max_messages_each_poll: 4,
        ..DriverOptions::default()
    })
    .expect("pool");

    let handlers: Vec<Arc<SlowCounted>> = (0..groups)
        .map(|_| Arc::new(SlowCounted::default()))
        .collect();
    for (index, handler) in handlers.iter().enumerate() {
        pool.register_group(
            DriverGroupKey::new(index as u64 + 1, 1),
            Arc::clone(handler) as Arc<dyn DriverMailHandler<u64>>,
        )
        .expect("register");
    }

    let senders = 3;
    let ready = Arc::new(Barrier::new(senders));
    let accepted = Arc::new(AtomicU64::new(0));
    std::thread::scope(|scope| {
        for _ in 0..senders {
            let pool = &pool;
            let ready = Arc::clone(&ready);
            let accepted = Arc::clone(&accepted);
            scope.spawn(move || {
                ready.wait();
                for round in 0..per_group {
                    for group_id in 1..=groups {
                        let key = DriverGroupKey::new(group_id, 1);
                        if pool.send(key, MailPriority::Normal, round).is_ok() {
                            accepted.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            });
        }
    });

    let sent = accepted.load(Ordering::Relaxed);
    assert!(
        sent > 0,
        "the pool refused everything, so this proves nothing"
    );
    let total = || -> u64 {
        handlers
            .iter()
            .map(|handler| handler.handled.load(Ordering::Relaxed))
            .sum()
    };
    let arrived = settle(
        "a slow handler to catch up with its senders",
        Duration::from_secs(60),
        || total() >= sent,
    );
    assert!(
        arrived,
        "the pool accepted {sent} mails and slow handlers took {}; {} never arrived, \
         so a mail was dropped or a worker is parked over work it was given",
        total(),
        sent - total()
    );
}
