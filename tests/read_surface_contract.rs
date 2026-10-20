// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A refused read carries the index to wait for, and the queue that waits on
//! it is the host's.
//!
//! Two halves, and the first is the one that matters: the README describes a
//! three-call recipe for turning a refused `read_index` into a read that is
//! answered when the apply catches up. This **runs** it, so the recipe is
//! executed rather than described.
//!
//! The second half is the same shape as `tests/storage_surface_contract.rs`
//! and exists for the same reason: a host could find `PendingReadIndexQueue`
//! in the public API and assume the engine drives one. Saying it does not is
//! only useful while it is accurate, and wiring one in is exactly the change
//! that would quietly make the prose wrong.
//!
//! The list is read out of the README rather than copied here, so there is one
//! place for it to be right.

use matrixraft::{Config, Peer, PendingReadIndexQueue, RaftCluster, ReadIndexRequest, ReplicaRole};

const GROUP_ID: u64 = 61;

fn peer(node_id: u64) -> Peer {
    Peer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 61_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 62_000 + node_id),
        role: ReplicaRole::Voter,
        auto_promote: false,
    }
}

fn read_request(requester_id: u64) -> ReadIndexRequest {
    read_request_from(requester_id, 0)
}

fn read_request_from(requester_id: u64, min_commit_index: u64) -> ReadIndexRequest {
    ReadIndexRequest {
        group_id: GROUP_ID,
        requester_id,
        min_commit_index,
        allow_lease_read: false,
    }
}

#[test]
fn a_read_refused_for_an_unapplied_index_is_answered_by_the_queue() {
    let members = (1..=3u64).map(peer).collect::<Vec<_>>();
    let mut cluster = RaftCluster::new(GROUP_ID, Config::default(), members).expect("cluster");
    cluster.start().expect("start");
    let leader = cluster.leader_id().expect("leader");

    let log_id = cluster
        .propose(b"before-the-read".to_vec())
        .expect("propose");

    // Hold the apply in flight, which is the state the refusal is about: the
    // entry is committed and the node has not applied it.
    cluster
        .mark_apply_task_inflight(leader, log_id.index)
        .expect("hold the apply in flight");
    assert!(
        cluster.safety_applied_index(leader).expect("applied") < log_id.index,
        "the apply has to be behind for this test to be about anything"
    );

    let request = read_request(leader);
    let refused = cluster.read_index(request.clone()).expect("read index");
    assert!(!refused.safe, "{refused:?}");
    assert_eq!(
        refused.reason, "applied_index_behind_read_index",
        "this is the refusal the recipe is for: {refused:?}"
    );
    assert!(
        refused.read_index >= log_id.index,
        "and it has to carry the index to wait for: {refused:?}"
    );

    // The host's queue. The engine holds none.
    let mut queue = PendingReadIndexQueue::new();
    assert_eq!(queue.push(request, refused.read_index), 1);

    let still_waiting =
        queue.notify_applied(cluster.safety_applied_index(leader).expect("applied"));
    assert!(
        still_waiting.is_empty(),
        "nothing is ready while the apply is still in flight: {still_waiting:?}"
    );
    assert_eq!(queue.pending_len(), 1);

    cluster
        .submit_apply_result(leader, log_id.index, false)
        .expect("finish the apply");
    let released = queue.notify_applied(cluster.safety_applied_index(leader).expect("applied"));
    assert_eq!(released.len(), 1, "{released:?}");
    assert!(released[0].ready, "{:?}", released[0]);
    assert_eq!(released[0].read_index, refused.read_index);
    assert_eq!(released[0].reason, "read_index_applied");
    assert!(queue.is_empty(), "and the queue is drained");

    // And the engine agrees the read is safe now, which is the point of having
    // waited rather than answered early.
    let allowed = cluster
        .read_index(read_request(leader))
        .expect("read index");
    assert!(allowed.safe, "{allowed:?}");
}

#[test]
fn a_leadership_change_hands_every_waiting_read_back_unready() {
    // `release_all` is the other half: a read waiting on a term that is over
    // must be handed back rather than answered.
    let mut queue = PendingReadIndexQueue::new();
    queue.push(read_request(1), 7);
    queue.push(read_request(1), 9);

    let released = queue.release_all(3, "leader_changed");
    assert_eq!(released.len(), 2);
    for result in &released {
        assert!(!result.ready, "{result:?}");
        assert_eq!(result.reason, "leader_changed");
        assert_eq!(result.applied_index, 3);
    }
    assert!(queue.is_empty());
}

#[test]
fn the_fence_is_the_higher_of_the_read_index_and_the_request_s_own_bound() {
    // The README's claim that pushing the request alongside the returned index
    // is right *even for a read refused because its own min_commit_index was
    // the thing out ahead*. Every other test here uses `min_commit_index: 0`,
    // where `max(read_index, 0)` is the read index and the second term of the
    // fence is never exercised -- a plant that dropped it SURVIVED until this
    // test existed.
    let mut queue = PendingReadIndexQueue::new();
    queue.push(read_request_from(1, 9), 4);

    let at_the_read_index = queue.notify_applied(4);
    assert!(
        at_the_read_index.is_empty(),
        "the read index alone does not satisfy a request that asked for 9: {at_the_read_index:?}"
    );
    let below_the_bound = queue.notify_applied(8);
    assert!(below_the_bound.is_empty(), "{below_the_bound:?}");

    let released = queue.notify_applied(9);
    assert_eq!(released.len(), 1, "{released:?}");
    assert!(released[0].ready);
    assert_eq!(
        released[0].read_index, 4,
        "and it is answered at the read index, not at the bound it waited for"
    );
}

#[test]
fn a_read_refused_for_its_own_bound_is_answered_by_the_same_recipe() {
    let members = (1..=3u64).map(peer).collect::<Vec<_>>();
    let mut cluster = RaftCluster::new(GROUP_ID, Config::default(), members).expect("cluster");
    cluster.start().expect("start");
    let leader = cluster.leader_id().expect("leader");
    let log_id = cluster.propose(b"committed".to_vec()).expect("propose");

    // A host asking to read at an index the group has not committed yet. This
    // is the other refusal arm, and it reports the committed index rather than
    // the bound -- which is exactly why the fence has to consult the request.
    let ahead = log_id.index + 5;
    let request = read_request_from(leader, ahead);
    let refused = cluster.read_index(request.clone()).expect("read index");
    assert!(!refused.safe, "{refused:?}");
    assert_eq!(
        refused.reason, "applied_index_behind_min_commit",
        "{refused:?}"
    );
    assert!(
        refused.read_index < ahead,
        "the response carries the committed index here, not the bound: {refused:?}"
    );

    let mut queue = PendingReadIndexQueue::new();
    queue.push(request, refused.read_index);
    assert!(
        queue.notify_applied(refused.read_index).is_empty(),
        "so a queue that ignored the request's bound would answer this read early"
    );
    let released = queue.notify_applied(ahead);
    assert_eq!(released.len(), 1, "{released:?}");
    assert!(released[0].ready);
}

/// The types the README says the engine holds none of.
fn types_the_readme_calls_unwired() -> Vec<String> {
    let readme = include_str!("../README.md");
    let marker = "<!-- unwired-types: ";
    let line = readme
        .lines()
        .find(|line| line.trim_start().starts_with(marker))
        .expect(
            "README has no `<!-- unwired-types: ... -->` marker; the read section must \
             name them in a form this test can read",
        );
    line.trim()
        .trim_start_matches(marker)
        .trim_end_matches("-->")
        .trim()
        .split(',')
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .collect()
}

/// The engine's sources, minus the module that declares these types.
fn engine_sources() -> Vec<(&'static str, &'static str)> {
    vec![
        ("src/lib.rs", include_str!("../src/lib.rs")),
        ("src/driver.rs", include_str!("../src/driver.rs")),
        ("src/applier.rs", include_str!("../src/applier.rs")),
        ("src/node.rs", include_str!("../src/node.rs")),
        ("src/cluster.rs", include_str!("../src/cluster.rs")),
        ("src/pipeline.rs", include_str!("../src/pipeline.rs")),
        ("src/status.rs", include_str!("../src/status.rs")),
        ("src/readiness.rs", include_str!("../src/readiness.rs")),
        (
            "src/facade/read_safety_runtime.rs",
            include_str!("../src/facade/read_safety_runtime.rs"),
        ),
        (
            "src/facade/node_runtime.rs",
            include_str!("../src/facade/node_runtime.rs"),
        ),
        (
            "src/facade/cluster_runtime.rs",
            include_str!("../src/facade/cluster_runtime.rs"),
        ),
        (
            "src/facade/matrixraft_compat.rs",
            include_str!("../src/facade/matrixraft_compat.rs"),
        ),
    ]
}

/// Code with the things that *name* a type without building one removed:
/// comments, `use` statements, declarations and `impl` headers.
///
/// **This is what the first version of the scan got wrong.** `pub struct X {`
/// and `impl X {` both look exactly like the struct literal `X {`, so every
/// declaration read as a construction and the surface came back wired. A
/// survey that cannot tell naming a type from building one confirms whatever
/// it was pointed at.
fn constructions_only(source: &str) -> String {
    let mut out = Vec::new();
    let mut swallowing_use = false;
    for line in source.lines() {
        let trimmed = line.trim_start();
        if swallowing_use {
            if trimmed.contains(';') {
                swallowing_use = false;
            }
            out.push("");
            continue;
        }
        if trimmed.starts_with("use ") || trimmed.starts_with("pub use ") {
            if !trimmed.contains(';') {
                swallowing_use = true;
            }
            out.push("");
            continue;
        }
        let is_declaration = ["pub struct ", "pub enum ", "struct ", "enum ", "trait "]
            .iter()
            .any(|head| trimmed.starts_with(head));
        if is_declaration || trimmed.starts_with("impl ") || trimmed.starts_with("impl<") {
            out.push("");
            continue;
        }
        out.push(match line.find("//") {
            Some(at) => &line[..at],
            None => line,
        });
    }
    out.join("\n")
}

/// Whether `code` builds a `name`, rather than mentioning it.
///
/// The forms that count: a struct literal, an associated call, and holding one
/// in a field or a binding -- a type the engine stores is a type the engine
/// drives even if it never says `new`.
fn constructs(code: &str, name: &str) -> bool {
    let bytes = code.as_bytes();
    let is_ident = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_';
    let mut from = 0;
    while let Some(at) = code[from..].find(name) {
        let start = from + at;
        let end = start + name.len();
        from = end;
        // A longer identifier that merely begins with the name, e.g.
        // `PendingReadIndexResult` when looking for `PendingReadIndex`.
        if bytes.get(end).is_some_and(|byte| is_ident(*byte)) {
            continue;
        }
        // One that merely ends with it, e.g. `MyPendingReadIndex`.
        if start > 0 && is_ident(bytes[start - 1]) {
            continue;
        }
        let after = code[end..].trim_start();
        let before = code[..start].trim_end();
        let is_literal = after.starts_with('{');
        let is_associated = after.starts_with("::");
        let is_held = before.ends_with(':') || before.ends_with('<');
        if is_literal || is_associated || is_held {
            return true;
        }
    }
    false
}

#[test]
fn the_read_queue_types_the_readme_calls_unwired_really_are() {
    let claimed = types_the_readme_calls_unwired();
    assert!(
        !claimed.is_empty(),
        "the README's unwired-types marker is empty, so this test would pass whatever \
         the code did"
    );

    let mut wired = Vec::new();
    for name in &claimed {
        for (path, source) in engine_sources() {
            if constructs(&constructions_only(source), name) {
                wired.push(format!("{name} is built or held in {path}"));
            }
        }
    }
    assert!(
        wired.is_empty(),
        "the README says the engine holds none of these, but it does:\n  {}\nWiring one \
         in is good news; the README section needs updating with it.",
        wired.join("\n  ")
    );
}

#[test]
fn every_type_the_readme_calls_unwired_is_a_real_one() {
    // The other direction: a name that no longer exists, or never did, would
    // make the test above pass by describing nothing.
    let read_safety = include_str!("../src/read_safety.rs");
    for name in &types_the_readme_calls_unwired() {
        assert!(
            read_safety.contains(&format!("pub struct {name} ")),
            "the README calls `{name}` an unwired read-queue type, but no such struct is \
             declared in src/read_safety.rs"
        );
    }
}

#[test]
fn the_scan_can_tell_naming_a_type_from_building_one() {
    // The test above is only worth anything if its matcher works, and its
    // first version did not: every `pub struct X {` read as a literal.
    let declaration =
        constructions_only("pub struct PendingReadIndexQueue {\n    pending: u64,\n}");
    assert!(
        !constructs(&declaration, "PendingReadIndexQueue"),
        "a declaration must not read as a construction"
    );
    let impl_header = constructions_only("impl PendingReadIndexQueue {\n    fn f() {}\n}");
    assert!(
        !constructs(&impl_header, "PendingReadIndexQueue"),
        "nor an impl header"
    );
    let import = constructions_only("use crate::read_safety::PendingReadIndexQueue;");
    assert!(
        !constructs(&import, "PendingReadIndexQueue"),
        "nor an import"
    );
    let multiline_import = constructions_only(
        "pub use read_safety::{\n    PendingReadIndex, PendingReadIndexQueue,\n};",
    );
    assert!(
        !constructs(&multiline_import, "PendingReadIndexQueue"),
        "nor a re-export spread over lines, which is how lib.rs names them"
    );
    let comment = constructions_only("// PendingReadIndexQueue { } is what a host would hold");
    assert!(
        !constructs(&comment, "PendingReadIndexQueue"),
        "nor a comment"
    );

    assert!(
        constructs(
            &constructions_only("let q = PendingReadIndexQueue::new();"),
            "PendingReadIndexQueue"
        ),
        "an associated call is a construction"
    );
    assert!(
        constructs(
            &constructions_only("let q = PendingReadIndexQueue { pending: 0 };"),
            "PendingReadIndexQueue"
        ),
        "so is a literal"
    );
    assert!(
        constructs(
            &constructions_only("    reads: PendingReadIndexQueue,"),
            "PendingReadIndexQueue"
        ),
        "and so is holding one in a field"
    );
    assert!(
        constructs(
            &constructions_only("    reads: Option<PendingReadIndexQueue>,"),
            "PendingReadIndexQueue"
        ),
        "including behind an Option"
    );
    assert!(
        !constructs(
            &constructions_only("let r = PendingReadIndexResultSomething::new();"),
            "PendingReadIndexResult"
        ),
        "a longer identifier is not the type"
    );
}
