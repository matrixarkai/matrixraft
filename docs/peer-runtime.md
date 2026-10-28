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
from the beginning. Supply a fresh application, or an explicitly idempotent
applier. An application or persistence failure stops that peer instance; restart
and recover before serving. Keep the WAL file intact for the life of the group.

The initial voter set, group, and node identity are recorded and checked during
recovery. Membership is fixed. Snapshot installation, log compaction, transport
threads, authentication, and graceful worker lifecycle
integration remain required before production use. The WAL is bounded to 512 MiB,
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
They are not socket, multi-process, scale, or power-loss tests.

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
