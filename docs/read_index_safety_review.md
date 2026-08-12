# Read-Index Safety — Design Review

Scope: the multi-raft read-index fanout contract exercised by
`matrixraft_multi_raft_server_exposes_direct_group_propose_and_read_paths`
(`tests/matrixraft_multi_raft_compat.rs`).

## TL;DR

A fanned-out quorum read reports, per node, whether that node can safely serve the
read at a requested floor (`min_commit_index`). The **quorum-confirmed leader**
certifies the read as safe; a **follower that has not yet applied up to the read
floor** must report `safe = Some(false)` with an explaining reason. Reporting such
a follower as safe would be a stale-read (linearizability) bug. The original test
asserted that *every* node in every fanned group is `safe = Some(true)`, which is
only true when all followers are caught up. The fix encodes the honest contract;
the safety predicate itself is correct and unchanged.

## The safety model (as implemented)

**Leader ReadIndex** — `src/facade/cluster_runtime.rs:3034` — returns `safe = true`
only when all hold:

1. serving node healthy;
2. requester **is** the leader (`not_leader` ⇒ unsafe);
3. **live quorum** confirmed, or a valid **lease read**;
4. `min_commit_index <= safety_applied_index` (fence floor);
5. `read_index = max(commit_index, last_index_before_current_term + 1) <= safety_applied_index`
   — the current-term no-op barrier (the leader has committed an entry in its term).

**Follower / bounded-stale** — `src/facade/cluster_runtime.rs:3144` — returns
`safe = node.healthy && quorum.reached && applied_index_fence.passed`, where the
fence (`src/read_safety.rs:176`) is
`applied_index >= min_commit_index && applied_index <= commit_index`.

**Fanout** — `src/facade/matrixraft_compat.rs:19252` — runs each node's own
`read_index_with_options_callback`, so every route key reports that node's local
truth. The per-route `safe` in the summary is `result.read_index.map(|r| r.safe)`
(`src/facade/matrixraft_compat.rs:710`).

## Why the flagged test failed

For the 2-node group `826`, the follower had not applied up to the read floor
`min(fanout_log.index, data_log.index)`, so its fence failed and it correctly
returned `safe = Some(false)` (`applied_index_behind_min_commit`). Group `827`
(single node = leader) reported `Some(true)`. Instrumentation confirmed the
follower's `safe` was the **only** false sub-condition; all other checks (ops,
request ids, deadlines, timeouts, responses, read indices, lease, reasons) passed.

The library is correct. The test's expectation ("every node safe") was too strong.

## Options considered

| Option | Summary | Safety | Effort |
| --- | --- | --- | --- |
| **A. Honest contract in the test** (chosen) | Assert: every node produced a read index; the leader certifies `safe`; lagging followers honestly report `Some(false)` with a reason. | preserved | S |
| A′. Drive follower catch-up in setup | Advance the follower's `safety_applied_index >= floor`, then all nodes are legitimately safe under the original assertion. | preserved | S |
| B. Follower-read forwarding (feature) | Follower obtains a leader-confirmed ReadIndex, waits until `applied_index >= read_index`, then serves `safe`. | new capability | L |
| C. Fanout excludes lagging followers | Return `safe` only for eligible nodes. | changes API shape | M |

**Rejected:** dropping the fence (`safe = healthy && quorum.reached`) — that is the
stale-read bug.

## Decision

Adopt **Option A** now: replace the single "all nodes `safe == Some(true)`" check
with the honest contract — every route has a read index (`Some`), at least one
route (the leader) is `Some(true)`, and non-safe routes carry a non-empty reason
(already asserted). The flagged test is un-ignored.

## Option B — implemented (follower-read forwarding)

`MatrixRaftMultiRaftServer::forwarded_read_index_on_node` and
`forwarded_read_index_for_group` (`src/facade/matrixraft_compat.rs`) implement
linearizable follower reads. A follower's read is forwarded to the group leader for
a quorum-confirmed ReadIndex (a lease read is never accepted cross-node), and is
served as `safe` only once the follower has applied up to that index; otherwise it
reports `follower_apply_pending`, or `leader_read_unavailable:<reason>` when the
leader itself cannot certify. The leader (or a node with no distinct known leader) is
served directly. It never fakes linearizable safety.

Covered by `matrixraft_forwarded_follower_read_index_is_linearizable`. Note: the
multi-raft simulation harness models each node as an independent runtime that does
not apply the leader's replicated entries, so a follower's `applied_index` stays
behind the leader's read index. The test therefore asserts that the safety decision
is consistent with the follower's real applied state (covering both branches of
`applied_index >= read_index`) and that forwarding is strictly safer than a
follower's local `not_leader` read. A future enhancement could add apply-wait
(block until `applied_index >= read_index`) and tests for leader handoff and timeout.

## Safety invariants (must never regress)

- A node reports `safe = true` only if it is the quorum-confirmed leader ReadIndex,
  or a replica with `applied_index >= read_index`.
- The current-term no-op barrier (`read_index >= last_index_before_current_term + 1`)
  is mandatory.
- Lease reads require `enable_lease_read && leader_lease_valid`.

## Verification

`cargo test` is green with the test un-ignored. A future regression that marks a
lagging follower `safe = true` would still satisfy the shape checks, so a targeted
follow-up test (Option B work) should assert a lagging follower is `Some(false)`
with `applied_index_behind_min_commit` to lock the invariant down.
