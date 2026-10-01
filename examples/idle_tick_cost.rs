// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What does hosting an idle group cost in CPU, and which hosting model?
//!
//! This crate has two ways to host many raft groups in one process, and they
//! have never been compared:
//!
//! - **a thread each.** `MatrixRaftMultiRaftServer` gives every group a
//!   `NodeRuntime`, and a `NodeRuntime` is an OS thread sitting in
//!   `command_rx.recv_timeout(next_tick - now)`. The thread wakes once per
//!   heartbeat interval whether or not anything happened -- and whether or not
//!   the node was ever started, because only the tick *work* is gated on the
//!   running state, not the wake-up itself.
//! - **a shared ticker.** [`Driver`] holds every group on a due-time heap and
//!   visits them all from one thread. The server reaches this through
//!   `shared_runtime(true)` on its context, which is the arm to read: same
//!   public API, same groups, same commands, only the hosting differs. The
//!   bare `Driver` row underneath it is the floor that hosting cannot beat.
//!
//! `group_scaling` already reports what a group costs at rest: threads and
//! resident bytes. It cannot report this one, because it deliberately sets
//! `tick_interval_ms` to an hour so that no tick fires while it measures. That
//! is right for the question it asks, and it leaves this one untouched: at a
//! realistic interval, what does the wake-up cost, multiplied by the number of
//! groups?
//!
//! Three arms. Two are idle and unstarted, so neither pays for raft work and
//! what is left is the price of the hosting model alone. The third starts its
//! groups as single voters, so each wins its own election at once and then
//! heartbeats alone -- a live idle group, which is what a real store holds.
//! Three peers and no transport would campaign forever instead, and the probe
//! would be measuring a broken cluster rather than a steady one, so the
//! leaders column is reported and a shortfall is called out.
//!
//! Reported per arm: CPU cores burned, context switches per second, threads,
//! how many sampled groups hold leadership, the share of ticks delivered
//! against the interval's rate, the share handed to a worker, and the two
//! halves of bringing the store up -- creating the groups and starting them.
//! Cores burned is the number to trust; it comes from `/proc/self/stat` and
//! counts the whole process.
//!
//! The two start-up halves are separate because one of them is almost all of
//! it. At 16384 groups `start_all` takes a few seconds and creating the groups
//! takes about half a minute: each one opens its own WAL, writes a first
//! segment and fsyncs it, in sequence. Only the second number was reported
//! before, which made the smaller half look like the cost.
//!
//! # What it found
//!
//! On a 16-core box at a 10ms interval, cores burned:
//!
//! ```text
//!   groups   thread each   thread each, live   shared ticker   ratio
//!       64         0.087               0.093           0.013      7x
//!      256         0.270               0.307           0.013     24x
//!     1024         1.180               1.410           0.023     61x
//!     2048         2.720                   -           0.030     91x
//! ```
//!
//! Every live row reached leadership on every group that was sampled, so those
//! are steady states and not groups still campaigning. Sampled, not counted:
//! the leaders column asks at most 64 groups, because a status is a round trip
//! through the runtime being measured. It cannot say "1024 of 1024", and this
//! note used to.
//!
//! **Being live costs 14-19% on top of merely existing.** The switch counts of
//! the two thread-each arms are indistinguishable -- 25,357 against 25,360 at
//! 256 groups, 102,981 against 103,382 at 1024 -- so the raft work is the
//! small part and the wake-up is about 85% of the bill. That is the part a
//! shared ticker removes outright.
//!
//! The context-switch column shows the mechanism rather than just the cost:
//! the thread-each arm switches almost exactly `groups / interval` times a
//! second -- 102,700/sec at 1024 groups and 207,915/sec at 2048, against the
//! 102,400 and 204,800 the arithmetic calls for. That is one switch per group
//! per interval, which is what a thread parked in `recv_timeout` must pay. The
//! shared ticker sits at about 910/sec whatever the group count, because that
//! is its own loop and nothing else.
//!
//! So roughly **one core per thousand idle groups**, for no raft work at all,
//! and it stays linear. The shared ticker is flat and was separately measured
//! holding five million ticks a second on one thread (`tick_scaling`).
//!
//! The catch is that nothing in `src/` constructs a [`Driver`]: the shared
//! model is exported and exercised by tests and examples, while the server a
//! user actually reaches for is the thread-each one. This probe is the
//! measurement that quantifies the difference; closing it is a separate piece
//! of work, and not a small one, because every `NodeRuntime` operation is a
//! synchronous request and reply bound to that group's own thread.
//!
//! Linux only, for `/proc/self/stat` and `/proc/self/status`.
//!
//! ```bash
//! cargo run --release --example idle_tick_cost            # sweep
//! cargo run --release --example idle_tick_cost -- 256 10 3
//! cargo run --release --example idle_tick_cost -- 8192 10 3 shared 8
//! ```
//!
//! The fifth argument is `worker_num`, which is the shard count and so the
//! tick capacity: the runtime documents roughly one shard per 1,250 groups
//! at a 10ms interval, and this is how to check that.
//!
//! The fourth argument selects arms: `both` (the default), `driver` for the
//! bare `Driver` alone, `facade` to leave that out, or `shared` for only the
//! hosted arms -- which is what to use past a thousand or so groups, where
//! building two thread-each arms at the same size costs more than the whole
//! rest of the run.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use matrixraft::{
    Driver, DriverGroupKey, DriverOptions, DriverTickReceiver, MatrixRaftGroupContextBuilder,
    MatrixRaftMultiRaftServer, MatrixRaftOptions, MatrixRaftTransportBuilder, Peer, ReplicaRole,
};

/// CPU seconds this process has used, user plus system.
fn cpu_seconds() -> f64 {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    // The comm field can hold spaces and brackets, so the fields are counted
    // from after the last close bracket rather than by splitting the line.
    let Some((_, rest)) = stat.rsplit_once(')') else {
        return 0.0;
    };
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // The first field after the bracket is `state`, which is field 3. So
    // utime (field 14) and stime (field 15) sit at offsets 11 and 12.
    let utime = fields.get(11).and_then(|v| v.parse::<u64>().ok());
    let stime = fields.get(12).and_then(|v| v.parse::<u64>().ok());
    // _SC_CLK_TCK is 100 on every Linux this runs on.
    (utime.unwrap_or(0) + stime.unwrap_or(0)) as f64 / 100.0
}

fn status_field(name: &str) -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix(name) {
            let cleaned = rest.trim().trim_end_matches(" kB");
            return cleaned.trim().parse().unwrap_or(0);
        }
    }
    0
}

/// Context switches for every thread in the process.
///
/// `/proc/self/status` answers for the *calling thread*, not the process, and
/// the thread that calls this one spends the measurement window asleep. Read
/// that way the count is zero in both arms, which would say the thread-each
/// model switches no more than the shared one -- the opposite of the truth.
/// The per-thread files have to be summed.
fn context_switches() -> u64 {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return 0;
    };
    let mut total = 0;
    for task in tasks.flatten() {
        let Ok(status) = std::fs::read_to_string(task.path().join("status")) else {
            // A thread can exit between the listing and the read.
            continue;
        };
        for line in status.lines() {
            let voluntary = line.strip_prefix("voluntary_ctxt_switches:");
            let involuntary = line.strip_prefix("nonvoluntary_ctxt_switches:");
            if let Some(rest) = voluntary.or(involuntary) {
                total += rest.trim().parse::<u64>().unwrap_or(0);
            }
        }
    }
    total
}

struct Sample {
    cores: f64,
    switches_per_sec: f64,
    threads: u64,
    /// Resident memory at the close of the window, in KiB, from
    /// `/proc/self/status`.
    ///
    /// This is the whole process, so it only divides cleanly by the group count
    /// on a run with one arm -- `arm=live`. The other arm settings build and
    /// drop several stores in one process, and a dropped store's pages are
    /// returned to the allocator rather than to the kernel, so the second arm
    /// reads the first one's high-water mark.
    resident_kib: u64,
}

/// Opens a measurement window now; the returned closure closes it.
///
/// Built this way so the groups under test stay alive across the whole
/// window: the caller holds them until after the closure returns.
fn open_window(seconds: u64) -> impl FnOnce() -> Sample {
    // Let construction settle before the window opens, so the sample is of a
    // steady state rather than of the last group being built.
    std::thread::sleep(Duration::from_millis(500));
    let cpu_at_start = cpu_seconds();
    let switches_at_start = context_switches();
    let started = Instant::now();
    move || {
        std::thread::sleep(Duration::from_secs(seconds));
        let elapsed = started.elapsed().as_secs_f64();
        Sample {
            cores: (cpu_seconds() - cpu_at_start) / elapsed,
            switches_per_sec: (context_switches() - switches_at_start) as f64 / elapsed,
            resident_kib: status_field("VmRSS:"),
            threads: status_field("Threads:"),
        }
    }
}

// ---- the thread-each arm -------------------------------------------------

fn peer(group_id: u64, node_id: u64) -> Peer {
    Peer {
        node_id,
        // Nothing dials these: the probe never starts the transport. They only
        // have to be distinct and well formed.
        raft_addr: format!("127.0.0.1:{}", 30_000 + (group_id % 1_000) * 8 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 40_000 + (group_id % 1_000) * 8 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn options(
    group_id: u64,
    interval_ms: u64,
    solo: bool,
    wal: &Path,
    snapshot: &Path,
) -> MatrixRaftOptions {
    // A solo group is its own quorum, so it wins an election immediately and
    // then heartbeats with nobody to send to. That is a live idle group. With
    // three peers and no transport it would campaign forever instead, and the
    // probe would be measuring a broken cluster.
    let peers = if solo {
        vec![peer(group_id, 1)]
    } else {
        vec![peer(group_id, 1), peer(group_id, 2), peer(group_id, 3)]
    };
    MatrixRaftOptions {
        group_id,
        peer_id: 1,
        raft_addr: peer(group_id, 1).raft_addr,
        snapshot_addr: peer(group_id, 1).snapshot_addr,
        wal_dir: wal.display().to_string(),
        snapshot_dir: snapshot.display().to_string(),
        peers,
        role: ReplicaRole::Voter,
        // Idle and undurable on purpose: the question is what the hosting
        // model costs, not what raft costs.
        wal_sync: false,
        election_cycle_tick: 4,
        transfer_timeout_tick: 3,
        offline_timeout_tick: 10,
        // Unlike `group_scaling`, this is a realistic interval, and that is
        // the whole point: the wake-up is the thing being measured.
        tick_interval_ms: interval_ms,
        lease_duration_ms: 20,
        last_lease_duration_ms: 10,
        assume_lease_when_start: false,
        max_memory_replicate_log_bytes: 64 * 1024,
        max_disk_replicate_log_num: 64,
        max_cache_memory_bytes: 16 * 1024 * 1024,
        max_apply_batch_bytes: 64 * 1024,
        enable_reorder_queue: true,
        reorder_timeout_us: 3_000,
        reorder_window_size: 128,
        max_inflights_apply_task: 5,
        max_inflights_replicate: 128,
        enable_pre_vote: true,
        max_segment_bytes: 4 * 1024 * 1024,
        min_keep_segment_num: 2,
        can_trigger_snapshot: true,
        max_applied_log_bytes: u64::MAX,
        send_snapshot_timeout_ms: 60_000,
    }
}

struct ThreadEach {
    sample: Sample,
    /// Ticks delivered as a share of the interval's rate, over a sample of
    /// groups, or `None` when the groups were never started and so were
    /// never meant to tick.
    kept_up: Option<(f64, usize, usize)>,
    /// Share of ticks the tickers could not run in place and handed to a
    /// worker, for the hosted arms.
    ///
    /// It was described here as the early warning, rising before `kept up`
    /// falls. **It is not, and that reading is the wrong thing to watch.** A
    /// hand-off happens only when the group's lock is *held*; a ticker that is
    /// simply too slow holds nothing and hands over nothing. Measured: this
    /// column read 0.0% while `kept up` fell to 37% at 1024 groups on a 1ms
    /// interval, and 0.0% again at 16384 groups on a 10ms one. The column that
    /// answers "am I keeping up" is `kept up`, and inside a process it is
    /// `SharedRuntimeStats::ticks_skipped`.
    handed_over: Option<f64>,
    /// How long `start_all` took, for the arms that start.
    ///
    /// Measured because it is what actually gave way first: 4096 hosted
    /// groups on four shards never finished starting, while the same groups
    /// on eight started and then ticked at 100% for a fraction of a core.
    started_in: Option<Duration>,
    /// How long dropping the server took.
    ///
    /// The third part of a store's lifecycle and the last one to be measured.
    /// `stop_all` and `shutdown_all` have the same shape `start_all` had before
    /// it was pipelined -- a command sent to each group and its reply waited for
    /// before the next is sent -- and dropping a server runs that path for every
    /// group it holds.
    torn_down_in: Duration,
    /// How long creating the groups took, before any of them started.
    ///
    /// This is the larger half of bringing a store up and it was invisible.
    /// `start_all` reports 4.84s for 16384 groups; creating them took about
    /// 32s, measured by watching the descriptor count climb from outside the
    /// process. Each group opens its own WAL, writes a first segment and
    /// fsyncs it, one after another, so the cost is roughly 2ms a group and
    /// nothing about it is parallel.
    created_in: Duration,
    /// How many groups actually hold leadership. Zero on the unstarted arm by
    /// definition; on the live arm anything short of every group means the
    /// sample is of groups still campaigning, not of a steady state.
    with_a_leader: u64,
    leaders_asked: u64,
}

fn hosted(
    groups: u64,
    interval_ms: u64,
    seconds: u64,
    live: bool,
    shared: bool,
    workers: usize,
    root: &Path,
) -> ThreadEach {
    // Every directory first, so the sweep does not measure directory creation
    // as though it were the cost of a raft group.
    let dirs: Vec<(PathBuf, PathBuf)> = (1..=groups)
        .map(|group_id| {
            let wal = root.join(format!("g{group_id}/wal"));
            let snapshot = root.join(format!("g{group_id}/snapshot"));
            std::fs::create_dir_all(&wal).expect("wal dir");
            std::fs::create_dir_all(&snapshot).expect("snapshot dir");
            (wal, snapshot)
        })
        .collect();

    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(1)
        .set_num_connection_group(1)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .tick_interval(interval_ms)
        // Only meaningful to the shared arms, where it is the shard count
        // and so the tick capacity. See the sizing rule on
        // `MatrixRaftGroupContext::shared_runtime`.
        .worker_num(workers)
        .shared_runtime(shared)
        .build()
        .expect("group context");
    let mut server = MatrixRaftMultiRaftServer::new(context);

    let creating = Instant::now();
    for (index, (wal, snapshot)) in dirs.iter().enumerate() {
        server
            .create_node(
                options(index as u64 + 1, interval_ms, live, wal, snapshot),
                0,
            )
            .expect("create node");
    }
    // Before `start_all`, so the two halves of bringing a store up are
    // separate numbers rather than one.
    let created_in = creating.elapsed();
    let handover_before = server.shared_stats();
    let started_in = live.then(|| {
        let began = Instant::now();
        server.start_all(0).expect("start every group");
        began.elapsed()
    });

    // A spread of groups rather than all of them: a status is a round trip
    // through the runtime being measured, and asking thousands of times
    // would perturb the very number being read.
    let watched: Vec<u64> = (0..64.min(groups))
        .map(|n| 1 + n * (groups / 64).max(1))
        .filter(|group_id| *group_id <= groups)
        .collect();
    // Answers a sum and how many groups actually answered.
    //
    // Time-boxed, because `NodeRuntime::status` waits up to five seconds and
    // sixty-four unanswered ones is five minutes of apparent hang. A run
    // that did exactly that was read as a runtime unable to host the groups,
    // and that reading reached the sizing note before a backtrace showed the
    // groups were all up and it was this sampling that had stalled.
    let ticks_of = |server: &MatrixRaftMultiRaftServer| -> (u64, usize) {
        let give_up_at = Instant::now() + Duration::from_secs(10);
        let mut total = 0;
        let mut answered = 0;
        for group_id in &watched {
            if Instant::now() >= give_up_at {
                break;
            }
            if let Ok(status) = server
                .node(*group_id, 1)
                .and_then(|node| node.runtime_status())
            {
                total += status.timer_status.heartbeat_ticks;
                answered += 1;
            }
        }
        (total, answered)
    };

    // After `open_window`, not before: it sleeps to let construction settle
    // and only then starts its own clock. Reading the baseline first counted
    // that settle in the ticks and not in the span, which is how this came
    // out at 117% of a rate nothing can exceed.
    let close_window = open_window(seconds);
    let (ticks_before, answered_before) = if live { ticks_of(&server) } else { (0, 0) };
    let tick_window = Instant::now();
    let sample = close_window();
    let (ticks_after, answered_after) = if live { ticks_of(&server) } else { (0, 0) };
    let delivered = ticks_after.saturating_sub(ticks_before);
    // Only the groups that answered both times are comparable.
    let answered = answered_before.min(answered_after);
    // Taken after the closing read, so the round trips it costs sit inside
    // the span they are counted against rather than inflating the rate.
    let watched_for = tick_window.elapsed().as_secs_f64();
    let kept_up = live.then(|| {
        // Divided by what answered, not by what was asked: a group that did
        // not answer contributed no ticks either, and counting it would
        // report a shortfall that is the sampling's and not the runtime's.
        let wanted = answered as f64 * watched_for * 1000.0 / interval_ms as f64;
        (delivered as f64 / wanted.max(1.0), answered, watched.len())
    });
    let handed_over = match (handover_before, server.shared_stats()) {
        (Some(before), Some(after)) => {
            let handed = after
                .ticks_handed_over
                .saturating_sub(before.ticks_handed_over);
            let in_place = after.ticks_in_place.saturating_sub(before.ticks_in_place);
            let total = handed + in_place;
            (total > 0).then(|| handed as f64 / total as f64)
        }
        _ => None,
    };
    // Asked after the window closes: each call is a round trip to that
    // group's own thread, which would otherwise be measured as its cost.
    //
    // Sampled, and time-boxed, for the same reason as the tick count above:
    // sweeping every group means one round trip each, `NodeRuntime::leader`
    // waits up to five seconds, and a few thousand of those is an hour of
    // apparent hang. This is a control -- it only has to show the groups
    // reached leadership -- and a sample shows that as well as a sweep.
    let leader_deadline = Instant::now() + Duration::from_secs(10);
    let mut with_a_leader = 0_u64;
    let mut leaders_asked = 0_u64;
    for group_id in &watched {
        if Instant::now() >= leader_deadline {
            break;
        }
        leaders_asked += 1;
        if matches!(
            server.node(*group_id, 1).and_then(|node| node.leader()),
            Ok(Some(_))
        ) {
            with_a_leader += 1;
        }
    }
    // Held until here on purpose: the groups must be alive for the whole
    // window or the arm measures their teardown.
    let tearing_down = Instant::now();
    drop(server);
    let torn_down_in = tearing_down.elapsed();
    ThreadEach {
        sample,
        kept_up,
        handed_over,
        started_in,
        created_in,
        torn_down_in,
        with_a_leader,
        leaders_asked,
    }
}

// ---- the shared-ticker arm -----------------------------------------------

/// Counts ticks and does nothing else, so the arm measures the hosting.
#[derive(Debug, Default)]
struct Idle(AtomicU64);

impl DriverTickReceiver for Idle {
    fn fire_tick(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn shared_ticker(groups: u64, interval_ms: u64, seconds: u64) -> Sample {
    let driver = Driver::start(DriverOptions {
        worker_num: 1,
        tick_interval_ms: interval_ms,
        ..DriverOptions::default()
    })
    .expect("driver");
    let idle = Arc::new(Idle::default());
    for group_id in 1..=groups {
        driver
            .register_group(
                DriverGroupKey::new(group_id, 1),
                Arc::clone(&idle) as Arc<dyn DriverTickReceiver>,
            )
            .expect("register");
    }

    let close_window = open_window(seconds);
    let sample = close_window();
    drop(driver);
    sample
}

fn probe_root() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "matrixraft-idle-tick-{}-{nonce}",
        std::process::id()
    ))
}

fn main() {
    let only: Option<u64> = std::env::args().nth(1).and_then(|a| a.parse().ok());
    let interval_ms: u64 = std::env::args()
        .nth(2)
        .and_then(|a| a.parse().ok())
        .unwrap_or(10);
    let seconds: u64 = std::env::args()
        .nth(3)
        .and_then(|a| a.parse().ok())
        .unwrap_or(3);
    let arm = std::env::args()
        .nth(4)
        .unwrap_or_else(|| "both".to_string());
    let workers: usize = std::env::args()
        .nth(5)
        .and_then(|a| a.parse().ok())
        .unwrap_or(4);

    if status_field("Threads:") == 0 {
        println!("/proc/self/status unavailable: this probe needs Linux");
        return;
    }

    // Before any store exists. Without it the per-group figure is mostly this
    // binary: 128 groups read 114 KiB each on a process that was already 13 MiB
    // resident before a single group had been created.
    let baseline_kib = status_field("VmRSS:");

    println!(
        "interval={interval_ms}ms window={seconds}s worker_num={workers}; live \
         groups are solo voters holding their own leadership\n"
    );
    println!(
        "  {:>21}  {:>8}  {:>12}  {:>14}  {:>8}  {:>7}  {:>8}  {:>8}  {:>8}  {:>8}  {:>9}  {:>9}  {:>9}",
        "hosting",
        "groups",
        "cores",
        "switches/sec",
        "threads",
        "leaders",
        "kept up",
        "handed",
        "create",
        "start",
        "teardown",
        "resident",
        "per group"
    );

    let counts: Vec<u64> = match only {
        Some(groups) => vec![groups],
        None => vec![64, 256, 1024],
    };

    for groups in counts {
        let facade = [
            ("thread each", false, false),
            ("thread each, live", true, false),
            ("shared runtime", false, true),
            ("shared runtime, live", true, true),
        ];
        for (label, live, shared) in facade {
            if arm == "driver" {
                continue;
            }
            // At a few thousand groups the thread-each arms are the whole
            // cost of the run: two of them, each spawning a thread per
            // group. `shared` skips them so the hosted arms can be measured
            // at a size where a thread each is no longer the point.
            if arm == "shared" && !shared {
                continue;
            }
            // One row, one process: the only way the resident figure divides by
            // the group count and means anything.
            if arm == "live" && !(shared && live) {
                continue;
            }
            let root = probe_root();
            let seen = hosted(groups, interval_ms, seconds, live, shared, workers, &root);
            println!(
                "  {:>21}  {:>8}  {:>12.3}  {:>14.0}  {:>8}  {:>7}  {:>8}  {:>8}  {:>8}  {:>8}  {:>9}  {:>9}  {:>9}",
                label,
                groups,
                seen.sample.cores,
                seen.sample.switches_per_sec,
                seen.sample.threads,
                seen.with_a_leader,
                match seen.kept_up {
                    // `(n/m)` when some groups did not answer in time, so a
                    // thin sample is never mistaken for a confident number.
                    Some((share, answered, asked)) if answered < asked => {
                        format!("{:.0}%({answered}/{asked})", share * 100.0)
                    }
                    Some((share, _, _)) => format!("{:.1}%", share * 100.0),
                    None => "-".to_string(),
                },
                match seen.handed_over {
                    Some(share) => format!("{:.1}%", share * 100.0),
                    None => "-".to_string(),
                },
                format!("{:.2}s", seen.created_in.as_secs_f64()),
                match seen.started_in {
                    Some(took) => format!("{:.2}s", took.as_secs_f64()),
                    None => "-".to_string(),
                },
                format!("{:.2}s", seen.torn_down_in.as_secs_f64()),
                format!("{} MiB", seen.sample.resident_kib / 1024),
                format!(
                    "{} KiB",
                    seen.sample.resident_kib.saturating_sub(baseline_kib) / groups.max(1)
                )
            );
            if let Some((share, answered, asked)) = seen.kept_up {
                if answered < asked {
                    println!(
                        "  {:>21}  only {answered} of {asked} sampled groups answered \
                         within the time allowed, so `kept up` is from a thin sample \
                         -- the slow part is answering, not ticking",
                        "NOTE"
                    );
                }
                if share < 0.95 {
                    println!(
                        "  {:>21}  delivered {:.1}% of the ticks the interval asks \
                         for, so leases on these groups outlive their configuration",
                        "WARNING",
                        share * 100.0
                    );
                }
            }
            if live && seen.with_a_leader < seen.leaders_asked {
                println!(
                    "  {:>21}  only {} of {} sampled groups reached leadership, so this row is \
                     groups still campaigning rather than a steady state",
                    "WARNING", seen.with_a_leader, seen.leaders_asked
                );
            }
            let _ = std::fs::remove_dir_all(&root);
        }
        if arm != "facade" && arm != "live" {
            let seen = shared_ticker(groups, interval_ms, seconds);
            println!(
                "  {:>21}  {:>8}  {:>12.3}  {:>14.0}  {:>8}  {:>7}  {:>8}  {:>8}  {:>8}  {:>8}  {:>9}  {:>9}  {:>9}",
                "shared ticker",
                groups,
                seen.cores,
                seen.switches_per_sec,
                seen.threads,
                "-",
                "-",
                "-",
                "-",
                "-",
                "-",
                format!("{} MiB", seen.resident_kib / 1024),
                format!(
                    "{} KiB",
                    seen.resident_kib.saturating_sub(baseline_kib) / groups.max(1)
                )
            );
        }
    }

    println!(
        "\n  The unstarted rows do no raft work at all, so what separates them from\n  \
         the shared ticker is purely what the hosting model costs to hold a\n  \
         group. The live rows add the raft work a real store does, and add\n  \
         little: the wake-up is the bill."
    );
}
