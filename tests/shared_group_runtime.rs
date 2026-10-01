// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Many groups, a fixed number of threads.
//!
//! A group on its own thread costs that thread and one context switch per
//! heartbeat interval, whether or not anything happened -- about a core per
//! thousand groups, which `examples/idle_tick_cost.rs` measures. The shared
//! runtime answers exactly the same commands from one ticker and a fixed
//! pool.
//!
//! The control matters more than the rest here. A flag that quietly did
//! nothing would still let every "the hosted group works" test pass, because
//! a group on its own thread works too. So each test that asserts the shared
//! runtime holds threads flat is paired with one asserting the default does
//! not.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use matrixraft::{
    DriverOptions, MatrixRaftGroupContextBuilder, MatrixRaftMultiRaftServer, MatrixRaftOptions,
    MatrixRaftTransportBuilder, NodeRuntimeState, Peer, ReplicaRole, SharedGroupRuntime,
};

/// Polls to a deadline rather than sleeping a fixed time, so a loaded machine
/// does not turn an assertion about behaviour into one about scheduling.
fn wait_until(what: &str, timeout: Duration, mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if done() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let answer = done();
    if !answer {
        eprintln!("timed out waiting for {what}");
    }
    answer
}

fn probe_root(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "matrixraft-shared-{label}-{}-{nonce}",
        std::process::id()
    ))
}

fn peer(group_id: u64) -> Peer {
    Peer {
        node_id: 1,
        // Nothing dials these: no transport is started.
        raft_addr: format!("127.0.0.1:{}", 31_000 + (group_id % 1_000)),
        snapshot_addr: format!("127.0.0.1:{}", 41_000 + (group_id % 1_000)),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn options(group_id: u64, interval_ms: u64, wal: &Path, snapshot: &Path) -> MatrixRaftOptions {
    MatrixRaftOptions {
        group_id,
        peer_id: 1,
        raft_addr: peer(group_id).raft_addr,
        snapshot_addr: peer(group_id).snapshot_addr,
        wal_dir: wal.display().to_string(),
        snapshot_dir: snapshot.display().to_string(),
        // A single voter is its own quorum, so it reaches leadership without
        // a transport and the test is about hosting, not about elections.
        peers: vec![peer(group_id)],
        role: ReplicaRole::Voter,
        wal_sync: false,
        election_cycle_tick: 4,
        transfer_timeout_tick: 3,
        offline_timeout_tick: 10,
        tick_interval_ms: interval_ms,
        lease_duration_ms: 20,
        last_lease_duration_ms: 10,
        assume_lease_when_start: false,
        max_memory_replicate_log_bytes: 64 * 1024,
        max_disk_replicate_log_num: 64,
        max_cache_memory_bytes: 1024 * 1024,
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

/// A server, its groups' directories, and whether it shares a runtime.
fn server_with(groups: u64, shared: bool, root: &Path) -> MatrixRaftMultiRaftServer {
    server_with_interval(groups, shared, 5, root)
}

fn server_with_interval(
    groups: u64,
    shared: bool,
    interval_ms: u64,
    root: &Path,
) -> MatrixRaftMultiRaftServer {
    server_with_ids(&(1..=groups).collect::<Vec<_>>(), shared, interval_ms, root)
}

fn server_with_ids(
    ids: &[u64],
    shared: bool,
    interval_ms: u64,
    root: &Path,
) -> MatrixRaftMultiRaftServer {
    let transport = MatrixRaftTransportBuilder::new()
        .set_cluster_id(1)
        .set_num_connection_group(1)
        .bind_address_resolver()
        .build()
        .expect("transport");
    let context = MatrixRaftGroupContextBuilder::new()
        .transport(transport)
        .tick_interval(interval_ms)
        .worker_num(2)
        .shared_runtime(shared)
        .build()
        .expect("group context");
    let mut server = MatrixRaftMultiRaftServer::new(context);
    for group_id in ids.iter().copied() {
        let wal = root.join(format!("g{group_id}/wal"));
        let snapshot = root.join(format!("g{group_id}/snapshot"));
        std::fs::create_dir_all(&wal).expect("wal dir");
        std::fs::create_dir_all(&snapshot).expect("snapshot dir");
        server
            .create_node(options(group_id, interval_ms, &wal, &snapshot), 0)
            .expect("create node");
    }
    server
}

#[cfg(target_os = "linux")]
fn process_threads() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("Threads:") {
            return rest.trim().parse().unwrap_or(0);
        }
    }
    0
}

#[test]
fn a_hosted_group_reaches_leadership_without_a_thread_of_its_own() {
    let root = probe_root("leader");
    let mut server = server_with(1, true, &root);
    server.start_all(0).expect("start");

    assert!(
        wait_until(
            "the hosted group to take leadership",
            Duration::from_secs(5),
            || {
                matches!(
                    server.node(1, 1).and_then(|node| node.leader()),
                    Ok(Some(_))
                )
            }
        ),
        "a group hosted on the shared runtime never became leader, so the pool is \
         not driving it"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_hosted_group_answers_commands() {
    let root = probe_root("commands");
    let mut server = server_with(1, true, &root);
    server.start_all(0).expect("start");

    let status = server
        .node(1, 1)
        .expect("node")
        .runtime_status()
        .expect("status");
    assert_eq!(status.group_id, 1, "the status came from the wrong group");
    assert!(
        status.worker_running,
        "a hosted group reports no worker, so a command reached nothing"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_hosted_group_ticks() {
    let root = probe_root("ticks");
    let mut server = server_with(1, true, &root);
    server.start_all(0).expect("start");

    // The tick is what the shared ticker exists to deliver, and it is the
    // part that does not go through a command, so nothing else proves it.
    assert!(
        wait_until(
            "the shared ticker to tick the group",
            Duration::from_secs(5),
            || {
                server
                    .node(1, 1)
                    .and_then(|node| node.runtime_status())
                    .map(|status| status.timer_status.heartbeat_ticks > 0)
                    .unwrap_or(false)
            }
        ),
        "a hosted group never counted a heartbeat tick, so the shared ticker is not \
         reaching it"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn hosting_is_opt_in_and_the_default_gives_each_group_its_own_thread() {
    // The control. Without this a flag that did nothing would pass every
    // other test in this file, because a group on its own thread works too.
    let root = probe_root("default");
    let server = server_with(2, false, &root);
    assert_eq!(
        server.shared_thread_count(),
        None,
        "the default started a shared runtime, so hosting is not opt-in"
    );
    let _ = std::fs::remove_dir_all(&root);

    let root = probe_root("optin");
    let server = server_with(2, true, &root);
    assert!(
        server.shared_thread_count().is_some(),
        "asking for a shared runtime did not start one"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_fixed_pool_carries_any_number_of_groups() {
    // Asserted against the runtime's own count rather than the process's.
    //
    // An earlier version read `/proc/self/status` before and after building
    // the server and subtracted. That overflowed in the real suite: the other
    // tests in this binary run at the same time and spawn threads of their
    // own, so "after" can be below "before". Saturating the subtraction would
    // have hidden it rather than fixed it -- a process-global count simply
    // cannot answer a question about one server, because a sibling test's
    // threads land in the number.
    //
    // This is also the sharper claim. Not "fewer threads than groups", but
    // that the count does not move with the group count at all.
    let few = probe_root("few");
    let server_few = server_with(4, true, &few);
    let many = probe_root("many");
    let server_many = server_with(32, true, &many);

    let with_four = server_few.shared_thread_count().expect("a shared runtime");
    let with_thirty_two = server_many.shared_thread_count().expect("a shared runtime");
    assert_eq!(
        with_four, with_thirty_two,
        "4 groups took {with_four} threads and 32 took {with_thirty_two}, so the \
         pool is not fixed"
    );
    assert!(
        with_thirty_two < 32,
        "32 groups took {with_thirty_two} threads, which is not a pool"
    );

    drop(server_few);
    drop(server_many);
    let _ = std::fs::remove_dir_all(&few);
    let _ = std::fs::remove_dir_all(&many);
}

#[cfg(target_os = "linux")]
#[test]
fn a_thread_each_is_what_the_default_actually_costs() {
    // The measured control for the test above, and the reason it is measured
    // from /proc rather than from an accessor: the claim is about what the
    // default really spends, and an accessor would just be the code agreeing
    // with itself.
    //
    // A `>=` bound is sound here even with sibling tests running, because
    // their threads can only add to the count. Noise cannot make this pass.
    let root = probe_root("cost");
    let before = process_threads();
    let server = server_with(16, false, &root);
    let spent = process_threads().saturating_sub(before);
    assert!(
        spent >= 16,
        "16 groups on their own threads took only {spent} threads, so the control \
         is not measuring what it claims"
    );
    drop(server);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_shared_runtime_holds_its_groups_and_lets_them_go() {
    let runtime = Arc::new(
        SharedGroupRuntime::start(DriverOptions {
            worker_num: 2,
            tick_interval_ms: 5,
            ..DriverOptions::default()
        })
        .expect("shared runtime"),
    );
    assert_eq!(runtime.group_count(), 0, "a fresh runtime holds nothing");
    let threads = runtime.thread_count();
    assert!(threads > 0, "a started runtime has threads");

    let root = probe_root("hold");
    let mut server = server_with(3, true, &root);
    server.start_all(0).expect("start");
    // The server's own runtime, not the one above: what is asserted is that
    // hosting three groups did not cost three threads.
    let hosted = server.shared_thread_count().expect("a shared runtime");
    assert!(
        hosted <= 8,
        "three hosted groups took {hosted} threads, which is not a fixed pool"
    );

    server.stop_all().expect("stop");

    // The second half of this test's name, which it did not check. Stopping a
    // group does not release it; shutting it down does, and a group that stays
    // in the runtime is one the tickers go on ticking after it has shut down.
    //
    // Found by mutation: deleting the `runtime.release(key)` from the shutdown
    // path left every test in this file green, including this one.
    let held = server.shared_stats().expect("a shared runtime").groups;
    assert_eq!(held, 3, "the runtime is holding {held} of three groups");
    server.shutdown_all().expect("shutdown");
    let left = server.shared_stats().expect("a shared runtime").groups;
    assert_eq!(
        left, 0,
        "{left} groups are still in the runtime after shutting them down"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_command_is_answered_without_waiting_for_the_next_tick() {
    // This exists because a mutant that dropped the wake-up from the send
    // path survived every other test in this file. Nothing broke: the ticker
    // hands the group to a worker each interval anyway, and the worker drains
    // whatever is queued when it arrives. Correctness is fine. Latency is
    // not -- a command waits up to a whole interval, which at the default
    // 100ms makes a propose unusable while the suite stays green.
    //
    // The interval is long on purpose, and the margin wide on purpose. A
    // command served promptly takes a millisecond or two; one that waited
    // for the tick waits up to the whole interval and averages half of it.
    // Two seconds against a quarter-second bound separates those by enough
    // that a loaded machine cannot turn one into the other -- this box has
    // been seen at a load average of 50 with other work on it.
    let interval_ms = 2_000;
    let root = probe_root("latency");
    let mut server = server_with_interval(1, true, interval_ms, &root);
    server.start_all(0).expect("start");
    // Settle, so this measures a steady-state command and not registration.
    std::thread::sleep(Duration::from_millis(50));

    let started = Instant::now();
    let status = server
        .node(1, 1)
        .expect("node")
        .runtime_status()
        .expect("status");
    let waited = started.elapsed();

    assert_eq!(status.group_id, 1, "the answer came from the wrong group");
    assert!(
        waited < Duration::from_millis(interval_ms / 8),
        "a command took {waited:?} against a {interval_ms}ms tick interval, so it \
         waited for the tick rather than waking a worker"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_tick_runs_on_the_ticker_when_the_group_is_free() {
    // Nothing about behaviour can see this. A tick posted to the pool and a
    // tick run on the ticker do identical work, so removing the in-place
    // path leaves every other test in this file green -- it only shows up as
    // a context switch per group per interval, which is a measurement and
    // not a guard. Hence the counter, and hence this.
    let root = probe_root("inplace");
    let runtime = Arc::new(
        SharedGroupRuntime::start(DriverOptions {
            worker_num: 2,
            tick_interval_ms: 5,
            ..DriverOptions::default()
        })
        .expect("shared runtime"),
    );
    let before = runtime.stats();
    assert_eq!(before.ticks_in_place, 0, "nothing has ticked yet");

    let mut server = server_with(4, true, &root);
    server.start_all(0).expect("start");
    assert!(
        wait_until(
            "ticks to be served in place",
            Duration::from_secs(5),
            || {
                server
                    .node(1, 1)
                    .and_then(|node| node.runtime_status())
                    .map(|status| status.timer_status.heartbeat_ticks > 2)
                    .unwrap_or(false)
            }
        ),
        "the groups never ticked"
    );

    // The server has its own runtime; the one above is only the control that
    // an untouched runtime counts nothing.
    let stats = server.shared_stats().expect("a shared runtime");
    assert!(
        stats.ticks_in_place > 0,
        "every tick was handed to a worker ({} of them), so the in-place path \
         is not being taken",
        stats.ticks_handed_over
    );
    assert!(
        stats.ticks_in_place > stats.ticks_handed_over,
        "{} ticks went in place against {} handed over; idle groups are not \
         contended and should almost all take the cheap path",
        stats.ticks_in_place,
        stats.ticks_handed_over
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn groups_are_spread_across_the_tickers() {
    // A ticker runs its own groups' work now, so one shard holding all of
    // them still behaves correctly and simply ticks at one thread's ceiling.
    // Only the spread shows it, which is why it is reported.
    let root = probe_root("spread");
    let server = server_with(64, true, &root);
    let per_ticker = server.shared_groups_per_ticker().expect("a shared runtime");

    assert!(
        per_ticker.len() > 1,
        "there is only one ticker, so nothing is sharded"
    );
    assert_eq!(
        per_ticker.iter().sum::<usize>(),
        64,
        "the shards hold {:?}, which is not the 64 groups created",
        per_ticker
    );
    assert!(
        per_ticker.iter().all(|held| *held > 0),
        "shard counts {per_ticker:?}: a shard holds nothing, so the hash is not \
         spreading consecutive group ids"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn group_ids_with_a_stride_still_spread_across_the_tickers() {
    // The case hashing exists for, and the one the other spread test cannot
    // see. Consecutive ids spread perfectly well under a plain
    // `group_id % shards`; a stride does not. A store numbering its groups
    // 4, 8, 12, 16 against four shards would land every one of them on
    // shard zero -- and it would still report a fixed thread count while
    // ticking on a single thread, which is the worst kind of wrong: cheap
    // looking and not working.
    let root = probe_root("stride");
    let ids: Vec<u64> = (1..=32).map(|n| n * 4).collect();
    let server = server_with_ids(&ids, true, 5, &root);
    let per_ticker = server.shared_groups_per_ticker().expect("a shared runtime");

    assert_eq!(
        per_ticker.iter().sum::<usize>(),
        ids.len(),
        "the shards hold {per_ticker:?}, which is not the {} groups created",
        ids.len()
    );
    let busiest = per_ticker.iter().max().copied().unwrap_or(0);
    assert!(
        busiest < ids.len(),
        "every strided group landed on one shard ({per_ticker:?}), so the ids are \
         being taken modulo rather than hashed"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_busy_group_is_handed_to_a_worker_rather_than_blocking_its_ticker() {
    // `try_lock`, not `lock`. Swapping them leaves every other test green --
    // in-place ticks would simply become all of them -- while a ticker would
    // once again stall its entire shard on one busy group, which is the
    // thing sharding and the hand-off were both for.
    //
    // Contention is made rather than waited for: a group answering a stream
    // of commands holds its lock often enough that some tick finds it taken.
    let root = probe_root("busy");
    let mut server = server_with_interval(1, true, 1, &root);
    server.start_all(0).expect("start");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut handed_over = 0;
    while Instant::now() < deadline {
        for _ in 0..200 {
            let _ = server.node(1, 1).and_then(|node| node.runtime_status());
        }
        handed_over = server
            .shared_stats()
            .expect("a shared runtime")
            .ticks_handed_over;
        if handed_over > 0 {
            break;
        }
    }

    assert!(
        handed_over > 0,
        "a group answering commands continuously never had a tick handed to a \
         worker, so the ticker is taking the lock instead of trying it"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn start_all_starts_every_group_past_a_batch_boundary() {
    // `start_all` sends every group's Start before waiting for any reply, in
    // batches of 1024 rather than all at once -- one reply channel per group is
    // about 1.25 KiB while held, which is 20 MiB at sixteen thousand groups.
    //
    // A batch boundary is somewhere to drop the tail, so this crosses two of
    // them: 2049 groups is three batches and the last one holds a single group.
    // It was cheap enough to write only because creating a group stopped
    // fsyncing an empty WAL segment.
    let root = probe_root("batch-boundary");
    let mut server = server_with(2049, true, &root);
    server.start_all(0).expect("start every group");

    // Every group, not a sample. The failure this guards against is precisely
    // the last batch, or the one group in it, never being sent or never being
    // collected -- and a sample of the first sixty-four would miss both.
    let mut not_running = Vec::new();
    for group_id in 1..=2049u64 {
        let running = server
            .node(group_id, 1)
            .and_then(|node| node.runtime_status())
            .map(|status| status.state == NodeRuntimeState::Running)
            .unwrap_or(false);
        if !running {
            not_running.push(group_id);
        }
    }
    assert!(
        not_running.is_empty(),
        "{} of 2049 groups are not running after start_all, starting at {:?}",
        not_running.len(),
        &not_running[..not_running.len().min(8)]
    );

    let _ = std::fs::remove_dir_all(&root);
}
