# Independent durable peers

Enable `peer-runtime` to use `matrixraft::peer_runtime::DurablePeer`. This optional
API hosts one voter per instance, backed by a pinned raft-rs core. The existing
`RaftCluster` and `NodeRuntime` APIs continue to model a peer group locally; they
must not be used as evidence that remote peers voted or persisted an entry.

`DurablePeer::campaign`, `tick`, and `step` feed the local consensus state.
Call `process_ready` after inputs. It synchronizes protobuf WAL batches before
returning outgoing protobuf packets or invoking the committed-entry callback.
Deliver packets only to their `to` peer. A successful `propose` allocates a log
position; it does not acknowledge commitment. A serving application acknowledges
only its matching committed log position after successful application.

Hosts may submit several proposals or already-queued incoming packets on the same
exclusive peer worker before calling `process_ready`. Bound each batch by count,
bytes, and elapsed time; do not wait for a batch to fill or starve ticks and Ready
processing. A successful input call is still not a durable acknowledgment. Send
only the packets returned by successful Ready processing and acknowledge client
writes only after their committed-entry callbacks succeed.

Batching proposals alone does not batch follower persistence. A receiver that
processes Ready after every packet can still produce many small WAL records.
The loopback process test compares unbatched inputs, batches of 16 proposals,
and batches of 16 proposals plus up to 16 queued packets per receiving peer.
This is a host scheduling choice, not a relaxation of the persistence barrier.

For an application batch already available on the leader, `propose_batch` submits
all values in one core proposal instead of broadcasting after each individual
`propose` call. It accepts 1..1024 nonempty values with at most 4 MiB of combined
payload and returns contiguous admitted log positions. Invalid batches allocate
no positions. The consensus core's uncommitted-byte budget still applies; a full
budget rejects the batch. Admission is not commitment, and applications must
still wait for the matching successful committed-entry callbacks.

Live Ready installation borrows the core's decoded entries after WAL sync,
instead of decoding the just-encoded journal payload again. Recovery still
decodes the journal, and both paths share the same state and membership checks.
`cargo run --features peer-runtime --example durable_ready_allocations` measures
allocated bytes around single-voter `process_ready`, excluding payload creation
and proposal admission, and verifies exact application values after reopening.
Local debug-build measurements with the old decode call and the borrowed call:

| Entries | Bytes per value | Decode call | Borrowed call |
| ---: | ---: | ---: | ---: |
| 1 | 4096 | 29273 | 20793 |
| 1 | 65536 | 459356 | 327996 |
| 32 | 4096 | 930248 | 663784 |
| 32 | 65536 | 14692904 | 10494280 |

These are allocation volumes (about 29% lower here), not peak memory or
throughput. WAL bytes and persistence ordering are unchanged.

The application callback receives election no-ops as empty payloads. On reopen,
the WAL restores term, vote, log, and commit state, and committed entries replay
from the beginning, or from the latest application checkpoint and its suffix.
Supply a fresh application, or an explicitly idempotent applier. An application or persistence failure stops that peer instance; restart
and recover before serving. Keep the WAL file intact for the life of the group.

The initial voter set, group, and node identity are recorded and checked during
recovery. `open` keeps membership fixed; the opt-in API below supports changes.
Transport threads, authentication, and graceful
worker lifecycle integration remain required before
production use. The WAL is bounded to 512 MiB,
individual frames to 8 MiB, and proposals to 4 MiB. Reaching the WAL bound stops
writes; it does not discard history.

## Dependency decision

The core is pinned to upstream commit
`ad13f3d90780f53aea2488c6a4b76c0d334bf136` with only the Prost codec enabled.
The older released core brings in `protobuf` 2.x, which has
[RUSTSEC-2024-0437](https://rustsec.org/advisories/RUSTSEC-2024-0437.html).
That package is absent from this feature's dependency graph. Packet and WAL
messages use Prost; the tests include excessive protobuf nesting and local-only
message rejection. Do not enable the alternate codec.

The pinned core still uses `fxhash` 0.2.1. Its
[RUSTSEC-2025-0057](https://rustsec.org/advisories/RUSTSEC-2025-0057.html) advisory
reports lack of maintenance, not a known vulnerability. `deny.toml` records this
specific maintenance exception; vulnerability checks remain enabled. Review the
exception whenever the core revision changes and remove it when upstream replaces
the hasher. This dependency maintenance risk remains part of production review.

## Validation scope

Tests exchange encoded packets between independent cores. They prove that an
isolated peer cannot elect itself, a missing quorum cannot commit, a new majority
can replace an isolated leader's uncommitted tail, and restart replays committed
data. Additional tests exercise storage failure, application failure, durable
identity, exclusive locking, every truncated final append, and record corruption.
These packet-exchange tests do not establish socket transport, multi-host scale,
or power-loss safety. Checkpoint process-crash coverage is described below.

`tests/peer_process_transport.rs` separately runs 3, 5, and 9 independent voter
processes over loopback TCP. For each group size it commits 32 values of 64 or
4096 bytes under each of four batching policies (including `propose_batch`), checks every voter, kills
all voter processes, and checks exact replay after reopening every WAL. It also
compares aggregate WAL growth after election settles. The parent routes messages;
this is not a production peer server, a multi-host throughput test, or a
power-loss durability test. Run it with:

```text
cargo test --release --features peer-runtime --test peer_process_transport -- --test-threads=1 --nocapture
```

## Quorum-confirmed reads

A leader calls `begin_read` to obtain a local token, continues processing and
transporting Ready packets, then calls `poll_read`. A true result consumes the
token only after the core confirms a current-term quorum and the corresponding
commit index has applied locally. Serve the read immediately on that same
application worker. Leadership loss, unknown tokens, and failed peers are errors.
This uses the safe quorum mode, not a clock-based lease.

Only one confirmation may be outstanding. Concurrent application reads may share
a confirmation if all were admitted before it began. Do not forget an unresolved
token on client timeout: keep polling until completion or leadership loss. This
bounds the core's pending read contexts even during a partition. Read tokens are
not persistent and must not be reused across peer instances. Confirmations do
not append entries or WAL records; writes and elections still do, and snapshot
compaction remains necessary for a long-running write workload.

## Application checkpoints and lagging voters

After processing all Ready work, call `checkpoint` on the exclusive application
worker with a binary snapshot of the exact applied state. The runtime records its
applied index, term and committed configuration; it retains any persisted uncommitted
suffix without applying it. A synced replacement WAL is published by atomic rename
and its directory is synced. The stable `.runtime.lock` sidecar remains locked
across inode replacement. Do not remove the WAL or lock sidecar; missing or empty
history beside an existing lock is refused. An incomplete first WAL record is
refused without truncation. A later torn final append retains the older prefix.

Use `process_ready_with_snapshot` for every subsequent Ready pass and on reopen.
Its restore callback installs the checkpoint before committed suffix entries are
applied. A lagging voter receives the saved snapshot through the normal peer
packet transport and publishes its new WAL before restoring application state or
acknowledging Raft progress. Failure to persist or restore stops that peer. The
older `process_ready` method refuses a checkpoint that needs restoration.

`MAX_CHECKPOINT_BYTES` is 256 MiB. Checkpoints up to 4 MiB retain the inline
snapshot packet and version-2 WAL encoding. Larger checkpoints use version-2
transport packets containing acknowledged 1 MiB fragments and version-3 WAL
records. Every packet and WAL record still fits the existing 8 MiB frame bound.
The segmented WAL prefix is accepted only when contiguous, complete, and checksum
verified; an interrupted prefix is refused without truncating the file. The old
WAL remains authoritative until the complete replacement is synced and renamed.

Each peer retains at most one outgoing and one incoming large transfer. Other
lagging voters remain on the consensus retry path until the outgoing slot is free.
A sender advances one fragment per acknowledgment and retries after ten local
ticks. A transfer without progress expires after 100 ticks. A receiver that
restarts requests retransmission from offset zero. Repeated delivery after
completion does not restore state twice. Transport completion only resumes Raft
probing: it never advances a follower's matched index or commits a proposal.
Checkpoint acknowledgments leave the runtime only after processing persistence
and application work successfully.

The application snapshot and assembled transfer remain memory-backed; this is
bounded packet transport, not constant-memory application snapshotting. Plan for
snapshot copies during encoding, validation and restoration. Checkpoints larger
than 256 MiB are refused before history replacement. Applications whose state can
exceed that limit need a file-backed snapshot API before enabling automatic
compaction; the runtime must not discard their history to fit the bound.

Upgrade every voter to a runtime supporting segmented checkpoints before enabling
them. Older runtimes refuse the new packet and WAL versions. Storage nodes must
adopt complete application capture/restore and choose a checkpoint policy before
this API reclaims their history automatically. This library change alone does
not enable automatic compaction in those applications.

Tests cover lagging-voter transfer, replay after total restart, preserved undecided
suffixes, repeated history reclamation, replacement locking, failed publication,
restore failure, every truncated inline checkpoint prefix, segmented record
boundaries, and byte corruption. Child processes exit during segmented writing
and immediately before and after the atomic replacement to verify that reopening
sees complete old or new history. These are process-crash tests, not
power-loss or filesystem fault-injection claims.

Large-checkpoint tests transfer 9 MiB state through packet loss, duplicates, a
receiver restart, and multiple lagging voters. They verify a subsequent write and
total restart. Decoder tests cover conflicting senders, reordered extents, stale
terms, and corrupt streams. These tests do not establish deployment-scale memory
or throughput limits.

### Incoming checkpoint staging

An incomplete incoming checkpoint is held in an automatically removed temporary
file in the WAL directory. Only received fragments consume file space; the
advertised total does not preallocate it. Duplicate fragments are compared with
the staged bytes. Creation, read or write errors discard partial state without
acknowledging it, and expiry or peer shutdown releases the file. A retry after
partial state is discarded starts at offset zero.

Staging is expendable transport state. Completion still requires checksum and
canonical-message validation, followed by durable WAL publication and application
restore before completion packets can escape. The final decoder, outgoing
snapshot and installed application checkpoint still use complete in-memory
buffers, and the 256 MiB application bound remains. File staging alone does not
provide larger checkpoints or a fully streaming restore API.

### Checkpoint publication durability

A replacement WAL is written in full, including any retained log suffix, and
synced once before its atomic publication. Individual records in that temporary
file do not each require a sync: recovery continues to use the previous WAL until
the rename. Live WAL appends still sync before returning. Publication retains the
stable runtime lock and the parent-directory sync. A write or sync failure stops
checkpoint publication and leaves the peer failed until restart.

## File-backed checkpoints

`checkpoint_from_reader(reader, length)` captures an application checkpoint with
64 KiB copying buffers. It accepts up to 16 GiB on a 64-bit host and requires the
reader to end at the declared length. Call it on the exclusive application worker
at a settled applied position, just like `checkpoint`. Capture and checksum scans
are synchronous; a very large capture can delay that worker.

Use `process_ready_with_snapshot_reader` to restore through a bounded reader,
consuming exactly the supplied length before returning success. The retained log
suffix applies afterwards. The Raft core retains snapshot metadata only; payloads
live in a directory named `<wal filename>.payloads` beside the WAL. Treat this
directory and the WAL as one durability unit for backups and recovery. Missing or
corrupt committed payloads refuse startup. The legacy byte-slice API still reads
old checkpoints, but refuses file payloads above its 256 MiB allocation bound.

The payload is synced before an atomic WAL replacement publishes a version-4
binary protobuf manifest. That manifest names a bounded local filename, length,
and checksum; it does not contain the application bytes. Previous payload files
remain available to active transfers and are removed after their last user drops.
Orphan capture files are removed on reopen under the exclusive runtime lock.

File transfers use packet version 3 and canonical protobuf fragment metadata.
The stream consists of a bounded length/checksum header, a canonical protobuf
Raft message containing snapshot metadata, and the payload bytes. Each fragment
is at most 1 MiB. The receiver stages bytes on disk, validates the envelope and
payload checksums, then publishes its own local payload and WAL manifest before
restoring application state. Transport acknowledgments do not commit entries.

The final file fragment starts one owned background worker to copy, checksum and
sync the staged payload. `step` returns without waiting for that work. Call
`process_ready_with_snapshot_reader` on a bounded timer, including when no network
packet arrives, to collect completion; `checkpoint_receive_pending` reports this
outstanding work. No completion acknowledgment or application restoration occurs
before successful validation. An older-term result is discarded, and malformed
payloads are rejected without stopping the voter. One worker per peer bounds
concurrency even if other transfers arrive. Shutdown joins the owned worker and
removes unpublished payloads.

Installation reuses the payload validated and synced by the receive worker,
avoiding another full checksum scan on the Raft loop. Received images still
obey the incoming commit-position floor and exact manifest checks; they do not
inherit local-capture compaction rules. Reopening from the WAL continues to scan
and validate the entire payload before recovery.

This moves file finalization off packet processing. Fragment staging, final WAL
publication and application restoration remain synchronous. It does not remove
disk latency from those operations or establish a maximum recovery latency.

Upgrade every voter before using the new capture API. Older readers reject the
new WAL and packet versions; no automatic fallback sends an empty snapshot.
Existing `checkpoint(Vec<u8>)` callers keep their original formats and 256 MiB
bound. This API removes full-payload buffering in the peer runtime; applications
must separately adopt streaming serialization and restoration to benefit.

### Accumulated append packets

Several bounded network packets can accumulate into a Ready larger than the
8 MiB record ceiling. The journal splits those entries into bounded records,
persists a new term and vote before them, and writes the advanced commit index
after them. It syncs the complete append before returning any acknowledgment.
Every complete record prefix can reopen; an incomplete physical record still
fails closed. Record and total WAL bounds are checked before writing the first
part. This uses the existing version-1 records and does not raise the 512 MiB
WAL ceiling or change checkpoint formats.

### Background checkpoint capture

Call `prepare_checkpoint` at a settled applied position and take an immutable
application snapshot at the returned position before processing more mutations.
Move the ticket and snapshot to a worker. `CheckpointCapture::capture` serializes,
checksums and syncs the payload without borrowing the peer. The caller can continue
processing peer traffic and applying writes while the worker runs.

Return the `PreparedCheckpoint` to the originating peer and call
`install_checkpoint` after draining ready work and pending reads. Installation
preserves all log entries after the captured position, including newer committed
writes. A busy peer returns an error without consuming the prepared result; retry
once settled. A result superseded by a newer checkpoint returns `false` and should
be dropped. A result from another open peer is rejected.

Tickets and prepared results hold the runtime storage lock until released. Dropping
an unpublished result removes its payload. Applications should bound concurrent
captures (normally one), arrange worker cancellation or completion at shutdown,
and never claim a worker snapshot includes writes applied after its captured
position. The application owns snapshot consistency; the runtime cannot verify
its contents. This API retains the existing file and packet formats.

Final WAL replacement and sync remain synchronous and their cost grows with the
retained log. Snapshot export, serialization, validation and restore may also block
unless the application explicitly schedules them off its consensus loop. The API
alone does not change an application's threading or latency guarantees.

## Heartbeat commit bounds

Heartbeat packets cannot advance commit beyond the receiving peer's local log.
The boundary includes entries already received and awaiting persistence. A packet
with a larger commit position is rejected before entering the consensus core,
changing term, or writing durable state. A later valid packet can still proceed.

Raft requires durable term, vote and log state for each voter identity. Losing a
voter's storage does not authorize starting an empty voter under the same ID.
Keep the old process fenced and preserve its remaining files. The fixed-membership
runtime does not provide a new-identity replacement or membership-change workflow.
Rejecting an inconsistent heartbeat prevents a core panic; it does not reconstruct
lost votes or make identity reuse safe.


## Opt-in durable membership

New groups may use `DurablePeer::open_with_membership(options, permitted)`.
Supply the same sorted inventory of at most 64 nonzero unique IDs to every peer,
including all bootstrap voters and future learners. Bootstrap groups have one to
seven voters. The inventory, bootstrap voters, group and local ID are bound into
every binary protobuf WAL record. Keep these options unchanged on every reopen.
This API does not migrate an existing fixed-membership WAL. Fixed-mode readers,
older runtimes and a changed inventory refuse membership-enabled history.

Before ticking, campaigning or delivering packets to any newly opened instance,
call the appropriate `process_ready` variant successfully. This restores its
checkpoint and replays committed configuration entries before it can vote.
`membership()` reports the last applied configuration; inspect it only after this
initial recovery. Configuration entries are applied internally and do not reach
the application's normal-entry callback.

Applications must authenticate and route packets for the whole permitted
inventory. An inventory entry is permission to join, not a vote. A fresh identity
outside the bootstrap voter set cannot campaign until promoted. Serialize all
operations on the application worker and use this replacement sequence:

1. Fence the failed process and preserve its durable files. Start the replacement
   with a distinct permitted ID and new storage, then drain its initial recovery.
2. The leader proposes `MembershipChange::AddLearner(id)`. Wait for the returned
   position to apply; proposal acceptance alone does not acknowledge commitment.
3. If `membership_checkpoint_needed()` is true, publish a fresh application
   checkpoint at the current applied position. A compacted image predating the
   learner cannot seed that learner. A staged capture retains its capture-time
   configuration and can therefore still require a newer checkpoint.
4. Transport packets until the learner catches up. `Promote(id)` refuses a
   learner whose matched position is below the leader's commit. Promotion enters
   explicit joint consensus; both old and new majorities must acknowledge writes.
5. After the joint entry applies, propose `LeaveJoint` and wait for it to apply.
   Remove the old voter with `Remove(old_id)`, wait for that joint entry to apply,
   then commit another `LeaveJoint`. Removing a learner needs no joint phase.

Only one configuration proposal may be pending. While joint, only `LeaveJoint`
is admitted. The last voter cannot be removed, and promotion beyond seven voters
is refused. A leader removed from both voter sets steps down and refuses writes
and campaigns. Every applied configuration invalidates outstanding read tokens;
start a fresh quorum-confirmed read under the new membership.

Checkpoint metadata preserves both voter sets and learners. Recovery applies
retained configuration entries in order, including after an older staged capture.
Tests exercise both-majority partitions, learner catch-up, leader removal,
restart during joint consensus, every complete WAL record prefix, and checkpoint
history across configuration changes. Run them in WSL with
`cargo test --locked --features peer-runtime --test peer_membership`.

This library API does not implement endpoint discovery, an administrative network
API, application catalog changes or automatic replacement. Applications must wire
those operations before offering membership changes to operators. Losing a
voter's durable state never makes reuse of its old identity safe.
