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

The application callback receives election no-ops as empty payloads. On reopen,
the WAL restores term, vote, log, and commit state, and committed entries replay
from the beginning, or from the latest application checkpoint and its suffix.
Supply a fresh application, or an explicitly idempotent applier. An application or persistence failure stops that peer instance; restart
and recover before serving. Keep the WAL file intact for the life of the group.

The initial voter set, group, and node identity are recorded and checked during
recovery. Membership is fixed. Transport threads, authentication, and graceful
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
applied index, term and immutable voter set; it retains any persisted uncommitted
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
