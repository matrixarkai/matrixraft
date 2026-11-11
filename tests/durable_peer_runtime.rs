// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(feature = "peer-runtime")]
use matrixraft::{
    peer_runtime::{DurablePeer, PeerOptions, PeerTuning},
    RaftError,
};
use prost::Message as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{collections::BTreeMap, path::PathBuf};
static DIRECTORY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// A directory name for one test, from the clock reading AND a process-local
/// sequence number.
///
/// The clock alone is not enough. Twenty tests share this binary and the harness
/// runs them on parallel threads, so two can take the same reading --
/// `as_nanos()` reports whatever resolution the host clock has, not a promise of
/// distinctness. Same pid plus same reading is the same path, and the
/// `create_dir` below then fails `AlreadyExists`: that is how this surfaced, one
/// CI job red on a tree whose own run had just passed.
///
/// The sequence number is distinct by construction within the process, and the
/// pid keeps it distinct across processes. Taking the reading as an argument is
/// what lets the test below reach the collision deliberately instead of waiting
/// for a slow clock to produce one.
fn directory_name(nanos: u128) -> String {
    format!(
        "matrixraft-peer-{}-{}-{}",
        std::process::id(),
        nanos,
        DIRECTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    )
}

#[test]
fn two_directories_differ_even_on_one_clock_reading() {
    // The same reading twice is exactly the case the clock cannot rule out.
    // `create_dir` is kept rather than `create_dir_all` on purpose: if a path
    // ever does collide, two peers writing one WAL should fail loudly instead of
    // quietly sharing a directory.
    let first = directory_name(1_700_000_000_000_000_000);
    let second = directory_name(1_700_000_000_000_000_000);
    assert_ne!(
        first, second,
        "two directories built from one clock reading must still differ, or parallel \
         tests in this binary can be handed the same path"
    );
}

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(directory_name(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn options(&self, id: u64, voters: Vec<u64>) -> PeerOptions {
        PeerOptions {
            group_id: 17,
            node_id: id,
            voters,
            wal_path: self.0.join(format!("peer-{id}.wal")),
        }
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
type Applied = BTreeMap<u64, Vec<(u64, Vec<u8>)>>;

#[test]
fn bounded_apply_yields_keeps_reads_waiting_and_replays_the_committed_tail() {
    let dir = Directory::new();
    let options = dir.options(1, vec![1]);
    let tuning = PeerTuning {
        max_apply_bytes: Some(1),
        ..PeerTuning::default()
    };
    let mut peer = DurablePeer::open_with_tuning(options.clone(), tuning).unwrap();
    peer.campaign().unwrap();
    while peer.has_ready() {
        peer.process_ready(|_, _| Ok(())).unwrap();
    }
    let mut values: Vec<_> = (0..20u8).map(|value| vec![value; 32]).collect();
    let ids = peer.propose_batch(values.clone()).unwrap();
    let last = ids.last().unwrap().index;
    for _ in 0..2 {
        let mut count = 0;
        peer.process_ready(|_, _| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert!(count <= 1, "one large entry still advances a tiny budget");
    }
    assert_eq!(peer.commit_index(), last);
    assert!(peer.applied_index() < last);
    let token = peer.begin_read().unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    assert!(
        !peer.poll_read(token).unwrap(),
        "a quorum read must wait for application"
    );
    assert!(
        peer.has_ready(),
        "an empty packet list is not an idle application"
    );
    while peer.applied_index() < last {
        assert!(!peer.poll_read(token).unwrap());
        peer.process_ready(|_, _| Ok(())).unwrap();
    }
    assert!(peer.poll_read(token).unwrap());
    let suffix: Vec<_> = (200..204u8).map(|value| vec![value; 32]).collect();
    let last = peer
        .propose_batch(suffix.clone())
        .unwrap()
        .last()
        .unwrap()
        .index;
    values.extend(suffix);
    for _ in 0..2 {
        peer.process_ready(|_, _| Ok(())).unwrap();
    }
    assert_eq!(peer.commit_index(), last);
    assert!(peer.applied_index() < last);
    drop(peer);
    let mut peer = DurablePeer::open_with_tuning(options, tuning).unwrap();
    let mut replay = Vec::new();
    let mut calls = 0;
    while peer.has_ready() {
        let mut count = 0;
        peer.process_ready(|_, value| {
            count += 1;
            if !value.is_empty() {
                replay.push(value.to_vec());
            }
            Ok(())
        })
        .unwrap();
        assert!(count <= 1);
        calls += 1;
        assert!(calls < 100);
    }
    assert_eq!(replay, values);
    assert_eq!(peer.applied_index(), last);
    assert!(calls >= 20);
}

#[test]
fn bounded_apply_restores_a_checkpoint_once_before_its_suffix() {
    let dir = Directory::new();
    let options = dir.options(1, vec![1]);
    let mut peer = DurablePeer::open(options.clone()).unwrap();
    peer.campaign().unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    peer.propose(vec![7]).unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    peer.checkpoint(b"saved image".to_vec()).unwrap();
    let values: Vec<_> = (0..8u8).map(|value| vec![value; 32]).collect();
    let last = peer
        .propose_batch(values.clone())
        .unwrap()
        .last()
        .unwrap()
        .index;
    peer.process_ready_with_snapshot(|_, _| Ok(()), |_, _| Ok(()))
        .unwrap();
    drop(peer);
    let mut peer = DurablePeer::open_with_tuning(
        options,
        PeerTuning {
            max_apply_bytes: Some(1),
            ..PeerTuning::default()
        },
    )
    .unwrap();
    let restored = std::cell::Cell::new(0);
    let mut applied = Vec::new();
    let mut calls = 0;
    while peer.has_ready() {
        let mut count = 0;
        peer.process_ready_with_snapshot(
            |_, value| {
                assert_eq!(restored.get(), 1);
                count += 1;
                applied.push(value.to_vec());
                Ok(())
            },
            |_, image| {
                assert_eq!(image, b"saved image");
                restored.set(restored.get() + 1);
                Ok(())
            },
        )
        .unwrap();
        assert!(count <= 1);
        calls += 1;
        assert!(calls < 100);
    }
    assert_eq!(restored.get(), 1);
    assert_eq!(applied, values);
    assert_eq!(peer.applied_index(), last);
}

#[test]
fn bounded_apply_failure_stops_the_peer_without_consuming_the_failed_entry() {
    let dir = Directory::new();
    let options = dir.options(1, vec![1]);
    let mut peer = DurablePeer::open_with_tuning(
        options.clone(),
        PeerTuning {
            max_apply_bytes: Some(1),
            ..PeerTuning::default()
        },
    )
    .unwrap();
    peer.campaign().unwrap();
    while peer.has_ready() {
        peer.process_ready(|_, _| Ok(())).unwrap();
    }
    let entry = peer.propose(vec![77]).unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    assert!(peer
        .process_ready(|_, _| Err(RaftError::Storage("application rejected".into())))
        .is_err());
    assert!(peer.applied_index() < entry.index);
    assert!(!peer.has_ready());
    assert!(peer.propose(vec![88]).is_err());
    drop(peer);
    let mut peer = DurablePeer::open(options).unwrap();
    let mut replay = Vec::new();
    peer.process_ready(|_, value| {
        if !value.is_empty() {
            replay.push(value.to_vec());
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(replay, vec![vec![77]]);
}

#[test]
fn bounded_apply_replicates_full_entries_in_order_across_independent_voters() {
    let dir = Directory::new();
    let mut group: BTreeMap<_, _> = (1..=3)
        .map(|id| {
            (
                id,
                DurablePeer::open_with_tuning(
                    dir.options(id, vec![1, 2, 3]),
                    PeerTuning {
                        max_apply_bytes: Some(128),
                        ..PeerTuning::default()
                    },
                )
                .unwrap(),
            )
        })
        .collect();
    let mut applied = Applied::new();
    group.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut group, &mut applied, None);
    let mut values: Vec<_> = (0..20u8).map(|value| vec![value; 32]).collect();
    values.insert(7, vec![99; 512]);
    let positions = group
        .get_mut(&1)
        .unwrap()
        .propose_batch(values.clone())
        .unwrap();
    assert!(group[&1].commit_index() < positions.last().unwrap().index);
    for round in 0..200 {
        let mut packets = Vec::new();
        for (id, peer) in &mut group {
            let mut lengths = Vec::new();
            packets.extend(
                peer.process_ready(|log, value| {
                    lengths.push(value.len());
                    applied
                        .entry(*id)
                        .or_default()
                        .push((log.index, value.to_vec()));
                    Ok(())
                })
                .unwrap(),
            );
            assert!(lengths.iter().sum::<usize>() <= 128 || lengths.len() == 1);
        }
        if packets.is_empty() && group.values().all(|peer| !peer.has_ready()) {
            break;
        }
        for packet in packets {
            group
                .get_mut(&packet.to)
                .unwrap()
                .step(&packet.bytes)
                .unwrap();
        }
        assert!(round < 199, "bounded work must continue to make progress");
    }
    for id in 1..=3 {
        let actual: Vec<_> = applied[&id]
            .iter()
            .filter(|(_, value)| !value.is_empty())
            .map(|(_, value)| value.clone())
            .collect();
        assert_eq!(actual, values);
        assert_eq!(group[&id].applied_index(), positions.last().unwrap().index);
        assert_eq!(applied[&id], applied[&1]);
    }
}

#[test]
fn term_fenced_writes_reject_old_leadership_without_allocating_positions() {
    let dir = Directory::new();
    let mut group = peers(&dir);
    let mut applied = Applied::new();
    group.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut group, &mut applied, None);
    let first_term = group[&1].term();
    let first = group
        .get_mut(&1)
        .unwrap()
        .propose_in_term(first_term, b"first".to_vec())
        .unwrap();
    pump(&mut group, &mut applied, None);
    assert_eq!(group[&1].commit_index(), first.index);
    for id in [2, 1] {
        group.get_mut(&id).unwrap().force_campaign().unwrap();
        pump(&mut group, &mut applied, None);
        assert!(group[&id].is_leader());
    }
    let term = group[&1].term();
    assert!(term > first_term);
    let follower = group.get_mut(&2).unwrap();
    assert!(matches!(
        follower.propose_in_term(term, vec![9]),
        Err(RaftError::NotLeader(2))
    ));
    assert!(matches!(
        follower.propose_batch_in_term(term, vec![vec![9]]),
        Err(RaftError::NotLeader(2))
    ));
    let leader = group.get_mut(&1).unwrap();
    let before = leader.commit_index();
    let wal_bytes = leader.persisted_wal_bytes().unwrap();
    for rejected in [0, first_term, term + 1, u64::MAX] {
        assert!(matches!(
            leader.propose_in_term(rejected, vec![9]),
            Err(RaftError::InvalidRequest(_))
        ));
        assert!(matches!(
            leader.propose_batch_in_term(rejected, vec![vec![9], vec![8]]),
            Err(RaftError::InvalidRequest(_))
        ));
    }
    assert!(leader
        .process_ready(|_, _| panic!("rejected writes cannot apply"))
        .unwrap()
        .is_empty());
    assert_eq!(leader.persisted_wal_bytes().unwrap(), wal_bytes);
    assert_eq!(leader.commit_index(), before);
    assert!(leader.propose_in_term(term, Vec::new()).is_err());
    assert!(leader
        .propose_batch_in_term(term, vec![vec![9], Vec::new()])
        .is_err());
    let single = leader.propose_in_term(term, b"current".to_vec()).unwrap();
    let batch = leader
        .propose_batch_in_term(term, vec![b"batch-a".to_vec(), b"batch-b".to_vec()])
        .unwrap();
    assert_eq!(single.index, before + 1);
    assert_eq!(single.term, term);
    assert_eq!(
        batch
            .iter()
            .map(|id| (id.term, id.index))
            .collect::<Vec<_>>(),
        vec![(term, before + 2), (term, before + 3)]
    );
    assert_eq!(leader.commit_index(), before);
    pump(&mut group, &mut applied, None);
    let expected = applied[&1].clone();
    assert_eq!(
        expected
            .iter()
            .filter(|(_, value)| !value.is_empty())
            .map(|(_, value)| value.as_slice())
            .collect::<Vec<_>>(),
        vec![
            b"first".as_slice(),
            b"current".as_slice(),
            b"batch-a".as_slice(),
            b"batch-b".as_slice()
        ]
    );
    for id in 1..=3 {
        assert_eq!(applied[&id], expected);
    }
    drop(group);
    let mut recovered = peers(&dir);
    let mut replay = Applied::new();
    pump(&mut recovered, &mut replay, None);
    for id in 1..=3 {
        assert_eq!(replay[&id], expected);
        assert!(recovered
            .get_mut(&id)
            .unwrap()
            .propose_in_term(term, vec![9])
            .is_err());
    }
}

#[test]
fn forced_campaign_bypasses_lease_inhibition_but_waits_for_received_votes() {
    for forced in [false, true] {
        let dir = Directory::new();
        let mut group = peers(&dir);
        let mut applied = Applied::new();
        group.get_mut(&1).unwrap().campaign().unwrap();
        pump(&mut group, &mut applied, None);
        let term = group.get(&1).unwrap().term();
        group.get_mut(&1).unwrap().force_campaign().unwrap();
        assert_eq!(group.get(&1).unwrap().term(), term);
        let candidate = group.get_mut(&2).unwrap();
        candidate.set_automatic_elections(false).unwrap();
        if forced {
            candidate.force_campaign().unwrap();
            let started_term = candidate.term();
            assert!(candidate.force_campaign().is_err());
            assert_eq!(candidate.term(), started_term);
        } else {
            candidate.campaign().unwrap();
        }
        assert!(
            !candidate.is_leader(),
            "a local request cannot synthesize votes"
        );
        assert!(candidate.propose(vec![9]).is_err());
        pump(&mut group, &mut applied, None);
        if forced {
            assert!(group.get(&2).unwrap().is_leader());
            assert!(group.get(&2).unwrap().term() > term);
            group
                .get_mut(&2)
                .unwrap()
                .propose(b"forced leader".to_vec())
                .unwrap();
            pump(&mut group, &mut applied, None);
            assert_eq!(applied.get(&1), applied.get(&2));
            assert_eq!(applied.get(&2), applied.get(&3));
        } else {
            assert!(!group.get(&2).unwrap().is_leader());
            assert!(group.get(&1).unwrap().is_leader());
            assert_eq!(group.get(&2).unwrap().term(), term);
            assert!(
                group.get_mut(&2).unwrap().force_campaign().is_err(),
                "pre-campaign is already in progress"
            );
        }
    }
}

#[test]
fn forced_campaign_cannot_elect_a_stale_voter() {
    let dir = Directory::new();
    let mut group = peers(&dir);
    let mut applied = Applied::new();
    group.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut group, &mut applied, None);
    for value in 0..10u8 {
        group.get_mut(&1).unwrap().propose(vec![value; 32]).unwrap();
        pump(&mut group, &mut applied, Some(2));
    }
    let committed = group.get(&1).unwrap().commit_index();
    assert!(group.get(&2).unwrap().commit_index() < committed);
    group.get_mut(&2).unwrap().force_campaign().unwrap();
    pump(&mut group, &mut applied, None);
    assert!(!group.get(&2).unwrap().is_leader());
    assert!(group.get_mut(&2).unwrap().propose(vec![9]).is_err());
    assert_eq!(group.get(&1).unwrap().commit_index(), committed);
    assert_eq!(group.get(&3).unwrap().commit_index(), committed);
}

#[test]
fn canceled_read_responses_cannot_confirm_a_newer_token() {
    let dir = Directory::new();
    let mut group = peers(&dir);
    let mut applied = Applied::new();
    group.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut group, &mut applied, None);
    let leader = group.get_mut(&1).unwrap();
    let bytes = leader.persisted_wal_bytes().unwrap();
    let old = leader.begin_read().unwrap();
    let old_packets = leader.process_ready(|_, _| Ok(())).unwrap();
    assert!(leader.cancel_read(old).unwrap());
    assert!(!leader.cancel_read(old).unwrap());
    assert!(leader.poll_read(old).is_err());
    let new = leader.begin_read().unwrap();
    assert!(new > old);
    assert!(!leader.cancel_read(old).unwrap());
    let new_packets = leader.process_ready(|_, _| Ok(())).unwrap();
    for packet in old_packets {
        group
            .get_mut(&packet.to)
            .unwrap()
            .step(&packet.bytes)
            .unwrap();
    }
    pump(&mut group, &mut applied, None);
    assert!(!group.get_mut(&1).unwrap().poll_read(new).unwrap());
    for packet in new_packets {
        group
            .get_mut(&packet.to)
            .unwrap()
            .step(&packet.bytes)
            .unwrap();
    }
    pump(&mut group, &mut applied, None);
    assert!(group.get_mut(&1).unwrap().poll_read(new).unwrap());
    assert_eq!(group.get(&1).unwrap().persisted_wal_bytes().unwrap(), bytes);
}

#[test]
fn repeated_read_cancellation_bounds_queued_heartbeats_and_releases_admission() {
    let dir = Directory::new();
    let mut group = peers(&dir);
    let mut applied = Applied::new();
    group.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut group, &mut applied, None);
    let leader = group.get_mut(&1).unwrap();
    let bytes = leader.persisted_wal_bytes().unwrap();
    for _ in 0..64 {
        let token = leader.begin_read().unwrap();
        assert!(leader.cancel_read(token).unwrap());
    }
    let packets = leader.process_ready(|_, _| Ok(())).unwrap();
    assert_eq!(
        packets.len(),
        2,
        "keep only one liveness heartbeat per follower"
    );
    assert_eq!(leader.persisted_wal_bytes().unwrap(), bytes);
    for packet in packets {
        group
            .get_mut(&packet.to)
            .unwrap()
            .step(&packet.bytes)
            .unwrap();
    }
    pump(&mut group, &mut applied, None);
    let token = group.get_mut(&1).unwrap().begin_read().unwrap();
    pump(&mut group, &mut applied, None);
    assert!(group.get_mut(&1).unwrap().poll_read(token).unwrap());
}

#[test]
fn automatic_elections_can_be_suppressed_and_resumed() {
    let dir = Directory::new();
    let mut peer = DurablePeer::open_with_tuning(
        dir.options(1, vec![1]),
        PeerTuning {
            election_tick: 3,
            heartbeat_tick: 1,
            pre_vote: false,
            ..PeerTuning::default()
        },
    )
    .unwrap();
    peer.set_automatic_elections(false).unwrap();
    for _ in 0..32 {
        peer.tick().unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
    }
    assert_eq!(peer.term(), 0);
    assert!(!peer.is_leader());
    peer.set_automatic_elections(true).unwrap();
    for _ in 0..8 {
        peer.tick().unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
    }
    assert!(peer.is_leader());
    peer.set_automatic_elections(false).unwrap();
    drop(peer);
    let mut reopened = DurablePeer::open_with_tuning(
        dir.options(1, vec![1]),
        PeerTuning {
            election_tick: 3,
            heartbeat_tick: 1,
            pre_vote: false,
            ..PeerTuning::default()
        },
    )
    .unwrap();
    reopened.process_ready(|_, _| Ok(())).unwrap();
    for _ in 0..8 {
        reopened.tick().unwrap();
        reopened.process_ready(|_, _| Ok(())).unwrap();
    }
    assert!(
        reopened.is_leader(),
        "local suppression must not persist across restart"
    );
}

#[test]
fn a_suppressed_follower_still_votes_and_replicates_for_a_new_leader() {
    let dir = Directory::new();
    let mut group = peers(&dir);
    let mut applied = Applied::new();
    group.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut group, &mut applied, None);
    group
        .get_mut(&2)
        .unwrap()
        .set_automatic_elections(false)
        .unwrap();
    for _ in 0..40 {
        group.get_mut(&2).unwrap().tick().unwrap();
        group.get_mut(&3).unwrap().tick().unwrap();
        pump(&mut group, &mut applied, Some(1));
    }
    assert!(group.get(&3).unwrap().is_leader());
    assert!(!group.get(&2).unwrap().is_leader());
    assert_eq!(group.get(&2).unwrap().leader_id(), Some(3));
    group
        .get_mut(&3)
        .unwrap()
        .propose(b"replacement leader".to_vec())
        .unwrap();
    pump(&mut group, &mut applied, Some(1));
    assert_eq!(applied.get(&2), applied.get(&3));
}

#[test]
fn election_suppression_does_not_stop_leader_quorum_checks() {
    let dir = Directory::new();
    let mut group = peers(&dir);
    let mut applied = Applied::new();
    group.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut group, &mut applied, None);
    let leader = group.get_mut(&1).unwrap();
    let term = leader.term();
    leader.set_automatic_elections(false).unwrap();
    for _ in 0..25 {
        leader.tick().unwrap();
        leader.process_ready(|_, _| Ok(())).unwrap();
    }
    assert!(!leader.is_leader());
    assert_eq!(leader.term(), term);
}

#[test]
fn delivery_failure_reports_release_replication_windows_without_quorum_claims() {
    let dir = Directory::new();
    let tuning = PeerTuning {
        max_inflight_msgs: 1,
        ..PeerTuning::default()
    };
    let mut group: BTreeMap<_, _> = (1..=3)
        .map(|id| {
            (
                id,
                DurablePeer::open_with_tuning(dir.options(id, vec![1, 2, 3]), tuning).unwrap(),
            )
        })
        .collect();
    let mut applied = Applied::new();
    group.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut group, &mut applied, None);
    let committed = group.get(&1).unwrap().commit_index();
    let leader = group.get_mut(&1).unwrap();
    leader.propose(vec![10; 64]).unwrap();
    let lost = leader.process_ready(|_, _| Ok(())).unwrap();
    assert!(lost.iter().any(|packet| packet.to == 2));
    assert!(lost.iter().any(|packet| packet.to == 3));
    leader.propose(vec![20; 64]).unwrap();
    assert!(
        leader.process_ready(|_, _| Ok(())).unwrap().is_empty(),
        "full windows must block another append"
    );
    for target in [0, 1, 99] {
        assert!(leader.report_unreachable(target).is_err());
    }
    let bytes = leader.persisted_wal_bytes().unwrap();
    for target in [2, 3, 2, 3] {
        leader.report_unreachable(target).unwrap();
    }
    assert!(leader.process_ready(|_, _| Ok(())).unwrap().is_empty());
    assert_eq!(leader.persisted_wal_bytes().unwrap(), bytes);
    assert_eq!(leader.commit_index(), committed);
    let last = leader.propose(vec![30; 64]).unwrap();
    let retry = leader.process_ready(|_, _| Ok(())).unwrap();
    assert!(retry.iter().any(|packet| packet.to == 2));
    assert!(retry.iter().any(|packet| packet.to == 3));
    assert_eq!(
        leader.commit_index(),
        committed,
        "sending is not acknowledgment"
    );
    for packet in retry {
        group
            .get_mut(&packet.to)
            .unwrap()
            .step(&packet.bytes)
            .unwrap();
    }
    pump(&mut group, &mut applied, None);
    assert_eq!(group.get(&1).unwrap().commit_index(), last.index);
    assert_eq!(applied.get(&1), applied.get(&2));
    assert_eq!(applied.get(&2), applied.get(&3));
    assert_eq!(applied.get(&1).unwrap().last().unwrap().1, vec![30; 64]);
}

#[test]
fn leadership_transfer_uses_messages_and_the_new_leader_commits() {
    let dir = Directory::new();
    let mut group = peers(&dir);
    let mut applied = Applied::new();
    group
        .get_mut(&1)
        .unwrap()
        .set_automatic_elections(false)
        .unwrap();
    group.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut group, &mut applied, None);
    group
        .get_mut(&1)
        .unwrap()
        .propose(b"before transfer".to_vec())
        .unwrap();
    pump(&mut group, &mut applied, None);
    let term = group.get(&1).unwrap().term();
    assert!(group.get_mut(&2).unwrap().transfer_leadership(3).is_err());
    for target in [0, 1, 99] {
        assert!(group
            .get_mut(&1)
            .unwrap()
            .transfer_leadership(target)
            .is_err());
    }
    group
        .get_mut(&2)
        .unwrap()
        .set_automatic_elections(false)
        .unwrap();
    group.get_mut(&1).unwrap().transfer_leadership(2).unwrap();
    assert!(
        group.get(&1).unwrap().is_leader(),
        "acceptance alone cannot transfer leadership"
    );
    assert!(!group.get(&2).unwrap().is_leader());
    pump(&mut group, &mut applied, None);
    assert!(group.get(&2).unwrap().is_leader());
    assert!(group.get(&2).unwrap().term() > term);
    assert!(!group.get(&1).unwrap().is_leader());
    assert!(group.get_mut(&1).unwrap().propose(vec![9]).is_err());
    group
        .get_mut(&2)
        .unwrap()
        .propose(b"after transfer".to_vec())
        .unwrap();
    pump(&mut group, &mut applied, None);
    assert_eq!(applied.get(&1), applied.get(&2));
    assert_eq!(applied.get(&2), applied.get(&3));
    assert_eq!(
        applied.get(&2).unwrap().last().unwrap().1,
        b"after transfer"
    );
}

#[test]
fn lost_transfer_packets_expire_without_forcing_a_new_leader() {
    let dir = Directory::new();
    let mut group = peers(&dir);
    let mut applied = Applied::new();
    group.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut group, &mut applied, None);
    group.get_mut(&1).unwrap().transfer_leadership(3).unwrap();
    pump(&mut group, &mut applied, Some(3));
    assert!(group.get_mut(&1).unwrap().propose(vec![1]).is_err());
    for _ in 0..12 {
        group.get_mut(&1).unwrap().tick().unwrap();
        pump(&mut group, &mut applied, Some(3));
    }
    assert!(group.get(&1).unwrap().is_leader());
    group.get_mut(&1).unwrap().propose(vec![2]).unwrap();
    pump(&mut group, &mut applied, None);
    assert_eq!(applied.get(&1), applied.get(&3));
}

fn peers_with_handoff_timeout(dir: &Directory, timeout: usize) -> BTreeMap<u64, DurablePeer> {
    (1..=3)
        .map(|id| {
            (
                id,
                DurablePeer::open_with_tuning(
                    dir.options(id, vec![1, 2, 3]),
                    PeerTuning {
                        transfer_timeout_tick: Some(timeout),
                        ..Default::default()
                    },
                )
                .unwrap(),
            )
        })
        .collect()
}

fn tick_handoff_leader(
    group: &mut BTreeMap<u64, DurablePeer>,
    applied: &mut Applied,
    isolated: u64,
) -> usize {
    group.get_mut(&1).unwrap().tick().unwrap();
    let packets = group
        .get_mut(&1)
        .unwrap()
        .process_ready(|log, payload| {
            applied
                .entry(1)
                .or_default()
                .push((log.index, payload.to_vec()));
            Ok(())
        })
        .unwrap();
    let heartbeats = packets
        .iter()
        .filter(|packet| {
            let envelope = TestPacket::decode(packet.bytes.as_slice()).unwrap();
            let message = raft_core::eraftpb::Message::decode(envelope.message.as_slice()).unwrap();
            message.get_msg_type() == raft_core::eraftpb::MessageType::MsgHeartbeat
        })
        .count();
    for packet in packets {
        if packet.to != isolated {
            group
                .get_mut(&packet.to)
                .unwrap()
                .step(&packet.bytes)
                .unwrap();
        }
    }
    pump(group, applied, Some(isolated));
    heartbeats
}

#[test]
fn short_handoff_deadlines_do_not_restart_for_repeated_targets() {
    for deadline in [1, 3, 9] {
        let dir = Directory::new();
        let mut group = peers_with_handoff_timeout(&dir, deadline);
        let mut applied = Applied::new();
        group.get_mut(&1).unwrap().campaign().unwrap();
        pump(&mut group, &mut applied, None);
        let term = group[&1].term();
        group.get_mut(&1).unwrap().transfer_leadership(3).unwrap();
        pump(&mut group, &mut applied, Some(3));
        for _ in 1..deadline {
            tick_handoff_leader(&mut group, &mut applied, 3);
            assert!(group.get_mut(&1).unwrap().propose(vec![1]).is_err());
            group.get_mut(&1).unwrap().transfer_leadership(3).unwrap();
        }
        tick_handoff_leader(&mut group, &mut applied, 3);
        assert!(group[&1].is_leader());
        assert_eq!(group[&1].term(), term);
        group.get_mut(&1).unwrap().propose(vec![2]).unwrap();
        pump(&mut group, &mut applied, Some(3));
        assert_eq!(applied.get(&1), applied.get(&2));
        group.get_mut(&1).unwrap().transfer_leadership(3).unwrap();
        pump(&mut group, &mut applied, Some(3));
        for _ in 1..deadline {
            tick_handoff_leader(&mut group, &mut applied, 3);
            assert!(group.get_mut(&1).unwrap().propose(vec![3]).is_err());
        }
        tick_handoff_leader(&mut group, &mut applied, 3);
        group.get_mut(&1).unwrap().propose(vec![4]).unwrap();
    }
}

#[test]
fn extended_handoff_deadlines_preserve_heartbeat_cadence_and_quorum_checks() {
    let dir = Directory::new();
    let mut group = peers_with_handoff_timeout(&dir, 25);
    let mut applied = Applied::new();
    group.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut group, &mut applied, None);
    let term = group[&1].term();
    group.get_mut(&1).unwrap().transfer_leadership(3).unwrap();
    pump(&mut group, &mut applied, Some(3));
    for tick in 1..25 {
        let heartbeats = tick_handoff_leader(&mut group, &mut applied, 3);
        assert_eq!(heartbeats, if tick % 2 == 0 { 2 } else { 0 });
        assert!(group[&1].is_leader());
        assert_eq!(group[&1].term(), term);
        assert!(group.get_mut(&1).unwrap().propose(vec![1]).is_err());
    }
    let token = group.get_mut(&1).unwrap().begin_read().unwrap();
    pump(&mut group, &mut applied, Some(3));
    assert!(group.get_mut(&1).unwrap().poll_read(token).unwrap());
    tick_handoff_leader(&mut group, &mut applied, 3);
    group.get_mut(&1).unwrap().propose(vec![2]).unwrap();
    pump(&mut group, &mut applied, Some(3));
    assert_eq!(applied.get(&1), applied.get(&2));

    let dir = Directory::new();
    let mut group = peers_with_handoff_timeout(&dir, 50);
    let mut applied = Applied::new();
    group.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut group, &mut applied, None);
    group.get_mut(&1).unwrap().transfer_leadership(3).unwrap();
    group
        .get_mut(&1)
        .unwrap()
        .process_ready(|_, _| Ok(()))
        .unwrap();
    for tick in 0..20 {
        let leader = group.get_mut(&1).unwrap();
        leader
            .transfer_leadership(if tick % 2 == 0 { 2 } else { 3 })
            .unwrap();
        leader.tick().unwrap();
        leader.process_ready(|_, _| Ok(())).unwrap();
    }
    assert!(
        !group[&1].is_leader(),
        "the longer handoff must not extend quorum authority"
    );
    assert!(matches!(
        group.get_mut(&1).unwrap().propose(vec![3]),
        Err(RaftError::NotLeader(_))
    ));
}

#[test]
fn delayed_handoff_catches_up_after_the_first_election_interval() {
    let dir = Directory::new();
    let mut group = peers_with_handoff_timeout(&dir, 25);
    let mut applied = Applied::new();
    group.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut group, &mut applied, None);
    for value in 0..12u8 {
        group.get_mut(&1).unwrap().propose(vec![value; 32]).unwrap();
        pump(&mut group, &mut applied, Some(3));
    }
    let term = group[&1].term();
    group.get_mut(&1).unwrap().transfer_leadership(3).unwrap();
    pump(&mut group, &mut applied, Some(3));
    for _ in 0..12 {
        tick_handoff_leader(&mut group, &mut applied, 3);
    }
    assert!(group[&1].is_leader());
    assert!(!group[&3].is_leader());
    assert!(group[&3].applied_index() < group[&1].commit_index());
    group.get_mut(&1).unwrap().report_unreachable(3).unwrap();
    for _ in 0..2 {
        tick_handoff_leader(&mut group, &mut applied, 0);
    }
    assert!(group[&3].is_leader());
    assert!(group[&3].term() > term);
    group.get_mut(&3).unwrap().propose(vec![99; 32]).unwrap();
    pump(&mut group, &mut applied, None);
    assert_eq!(applied.get(&1), applied.get(&3));
    group.get_mut(&1).unwrap().force_campaign().unwrap();
    pump(&mut group, &mut applied, None);
    assert!(group[&1].is_leader());
    for _ in 0..14 {
        tick_handoff_leader(&mut group, &mut applied, 0);
    }
    group.get_mut(&1).unwrap().propose(vec![100; 32]).unwrap();
    pump(&mut group, &mut applied, None);
    group.get_mut(&1).unwrap().transfer_leadership(2).unwrap();
    pump(&mut group, &mut applied, Some(2));
    for _ in 0..24 {
        tick_handoff_leader(&mut group, &mut applied, 2);
        assert!(group[&1].is_leader());
        assert!(group.get_mut(&1).unwrap().propose(vec![1]).is_err());
    }
    tick_handoff_leader(&mut group, &mut applied, 2);
    group.get_mut(&1).unwrap().propose(vec![101; 32]).unwrap();
}

#[test]
fn replacing_a_handoff_target_resets_only_the_handoff_deadline() {
    let dir = Directory::new();
    let mut group = peers_with_handoff_timeout(&dir, 5);
    let mut applied = Applied::new();
    group.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut group, &mut applied, None);
    group.get_mut(&1).unwrap().transfer_leadership(3).unwrap();
    pump(&mut group, &mut applied, Some(3));
    for _ in 0..4 {
        tick_handoff_leader(&mut group, &mut applied, 3);
    }
    group.get_mut(&1).unwrap().transfer_leadership(2).unwrap();
    pump(&mut group, &mut applied, Some(2));
    assert!(group.get_mut(&1).unwrap().transfer_leadership(0).is_err());
    for _ in 0..4 {
        tick_handoff_leader(&mut group, &mut applied, 2);
        group.get_mut(&1).unwrap().transfer_leadership(2).unwrap();
        assert!(group.get_mut(&1).unwrap().propose(vec![1]).is_err());
    }
    tick_handoff_leader(&mut group, &mut applied, 2);
    group.get_mut(&1).unwrap().propose(vec![2]).unwrap();
    pump(&mut group, &mut applied, Some(2));
    assert_eq!(applied.get(&1), applied.get(&3));
}

#[test]
fn reopening_does_not_restore_an_old_handoff_clock_or_its_tuning() {
    let dir = Directory::new();
    let mut group = peers_with_handoff_timeout(&dir, 3);
    let mut applied = Applied::new();
    group.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut group, &mut applied, None);
    group.get_mut(&1).unwrap().propose(vec![42]).unwrap();
    pump(&mut group, &mut applied, None);
    let expected = applied.get(&1).unwrap().clone();
    group.get_mut(&1).unwrap().transfer_leadership(3).unwrap();
    pump(&mut group, &mut applied, Some(3));
    for _ in 0..2 {
        tick_handoff_leader(&mut group, &mut applied, 3);
    }
    group.remove(&1);
    applied.insert(1, Vec::new());
    group.insert(1, DurablePeer::open(dir.options(1, vec![1, 2, 3])).unwrap());
    pump(&mut group, &mut applied, None);
    assert_eq!(applied.get(&1), Some(&expected));
    group.get_mut(&1).unwrap().force_campaign().unwrap();
    pump(&mut group, &mut applied, None);
    group.get_mut(&1).unwrap().propose(vec![43]).unwrap();
    pump(&mut group, &mut applied, None);
    group.get_mut(&1).unwrap().transfer_leadership(3).unwrap();
    pump(&mut group, &mut applied, Some(3));
    for _ in 0..9 {
        tick_handoff_leader(&mut group, &mut applied, 3);
        assert!(group.get_mut(&1).unwrap().propose(vec![1]).is_err());
    }
    tick_handoff_leader(&mut group, &mut applied, 3);
    group.get_mut(&1).unwrap().propose(vec![44]).unwrap();
}

#[test]
fn local_shutdown_cancels_an_extended_handoff() {
    for graceful in [false, true] {
        let dir = Directory::new();
        let mut group = peers_with_handoff_timeout(&dir, 25);
        let mut applied = Applied::new();
        group.get_mut(&1).unwrap().campaign().unwrap();
        pump(&mut group, &mut applied, None);
        group.get_mut(&1).unwrap().transfer_leadership(3).unwrap();
        pump(&mut group, &mut applied, Some(3));
        let peer = group.get_mut(&1).unwrap();
        let term = peer.term();
        if graceful {
            peer.begin_shutdown().unwrap();
            assert!(peer.poll_shutdown().unwrap());
        } else {
            peer.stop();
        }
        assert!(!peer.is_leader());
        assert_eq!(peer.term(), term);
        assert!(peer.tick().is_err());
        assert!(peer.transfer_leadership(2).is_err());
        assert!(peer.propose(vec![1]).is_err());
    }
}

#[test]
fn leadership_transfer_catches_up_its_target_before_election() {
    let dir = Directory::new();
    let mut group = peers(&dir);
    let mut applied = Applied::new();
    group.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut group, &mut applied, None);
    for value in 0..10u8 {
        group.get_mut(&1).unwrap().propose(vec![value; 32]).unwrap();
        pump(&mut group, &mut applied, Some(2));
    }
    assert!(group.get(&2).unwrap().applied_index() < group.get(&1).unwrap().commit_index());
    group.get_mut(&1).unwrap().transfer_leadership(2).unwrap();
    assert!(!group.get(&2).unwrap().is_leader());
    pump(&mut group, &mut applied, None);
    assert!(group.get(&2).unwrap().is_leader());
    assert_eq!(applied.get(&1), applied.get(&2));
    assert_eq!(applied.get(&2), applied.get(&3));
}

#[test]
fn peer_tuning_validates_before_creating_storage_and_controls_election_ticks() {
    let dir = Directory::new();
    let options = dir.options(1, vec![1, 2, 3]);
    for tuning in [
        PeerTuning {
            election_tick: 2,
            heartbeat_tick: 2,
            ..PeerTuning::default()
        },
        PeerTuning {
            heartbeat_tick: 0,
            ..PeerTuning::default()
        },
        PeerTuning {
            election_tick: usize::MAX,
            ..PeerTuning::default()
        },
        PeerTuning {
            max_inflight_msgs: 0,
            ..PeerTuning::default()
        },
        PeerTuning {
            max_uncommitted_size: 1,
            ..PeerTuning::default()
        },
        PeerTuning {
            max_apply_bytes: Some(0),
            ..PeerTuning::default()
        },
        PeerTuning {
            max_apply_bytes: Some(4 * 1024 * 1024 + 1),
            ..PeerTuning::default()
        },
        PeerTuning {
            max_disk_replicate_entries: Some(0),
            ..PeerTuning::default()
        },
        PeerTuning {
            max_disk_replicate_entries: Some(65537),
            ..PeerTuning::default()
        },
        PeerTuning {
            transfer_timeout_tick: Some(0),
            ..PeerTuning::default()
        },
        PeerTuning {
            transfer_timeout_tick: Some(usize::MAX / 2 + 1),
            ..PeerTuning::default()
        },
    ] {
        assert!(DurablePeer::open_with_tuning(options.clone(), tuning).is_err());
        assert!(!options.wal_path.exists());
        assert!(!options.wal_path.with_extension("runtime.lock").exists());
    }
    let mut fast = DurablePeer::open_with_tuning(
        options,
        PeerTuning {
            election_tick: 3,
            heartbeat_tick: 1,
            pre_vote: false,
            ..PeerTuning::default()
        },
    )
    .unwrap();
    for _ in 0..6 {
        fast.tick().unwrap();
        fast.process_ready(|_, _| Ok(())).unwrap();
    }
    assert!(fast.term() > 0);
    assert!(
        !fast.is_leader(),
        "timing cannot substitute for received quorum votes"
    );
    let other = Directory::new();
    let mut default = DurablePeer::open(other.options(1, vec![1, 2, 3])).unwrap();
    for _ in 0..6 {
        default.tick().unwrap();
        default.process_ready(|_, _| Ok(())).unwrap();
    }
    assert_eq!(default.term(), 0);
}

#[test]
fn tuned_uncommitted_budget_refuses_and_releases_admission() {
    let dir = Directory::new();
    let tuning = PeerTuning {
        max_size_per_msg: 128,
        max_uncommitted_size: 256,
        max_inflight_msgs: 1,
        ..PeerTuning::default()
    };
    let mut group: BTreeMap<_, _> = (1..=3)
        .map(|id| {
            (
                id,
                DurablePeer::open_with_tuning(dir.options(id, vec![1, 2, 3]), tuning).unwrap(),
            )
        })
        .collect();
    let mut applied = Applied::new();
    group.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut group, &mut applied, None);
    let first = group.get_mut(&1).unwrap().propose(vec![7; 128]).unwrap();
    let second = group.get_mut(&1).unwrap().propose(vec![8; 128]).unwrap();
    assert_eq!(second.index, first.index + 1);
    assert!(group.get_mut(&1).unwrap().propose(vec![9; 128]).is_err());
    pump(&mut group, &mut applied, None);
    let next = group.get_mut(&1).unwrap().propose(vec![9; 128]).unwrap();
    assert_eq!(next.index, second.index + 1);
    pump(&mut group, &mut applied, None);
    assert_eq!(applied.get(&1), applied.get(&3));
    let expected = applied.get(&1).unwrap().clone();
    drop(group);
    let mut reopened = DurablePeer::open_with_tuning(
        dir.options(1, vec![1, 2, 3]),
        PeerTuning {
            election_tick: 5,
            heartbeat_tick: 1,
            ..PeerTuning::default()
        },
    )
    .unwrap();
    let mut replayed = Vec::new();
    reopened
        .process_ready(|id, bytes| {
            replayed.push((id.index, bytes.to_vec()));
            Ok(())
        })
        .unwrap();
    assert_eq!(replayed, expected);
}

#[test]
fn captured_log_reads_pin_a_durable_generation_across_checkpoint_replacement() {
    let dir = Directory::new();
    let mut peer = DurablePeer::open(dir.options(1, vec![1])).unwrap();
    peer.campaign().unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    let first = peer.propose(b"first".to_vec()).unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    let mut reader = peer.capture_log_reader().unwrap();
    let before = peer.persisted_wal_bytes().unwrap();
    let entries = reader.entries(first.index, first.index + 1, None).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].data.as_slice(), b"first");
    assert_eq!(
        reader
            .entries(first.index, first.index + 1, Some(0))
            .unwrap(),
        entries
    );
    assert_eq!(peer.persisted_wal_bytes().unwrap(), before);
    let path = dir.options(1, vec![1]).wal_path;
    let bytes = std::fs::read(&path).unwrap();
    let mut damaged = bytes.clone();
    *damaged.last_mut().unwrap() ^= 1;
    std::fs::write(&path, &damaged).unwrap();
    assert!(reader.entries(first.index, first.index + 1, None).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), damaged);
    std::fs::write(&path, bytes).unwrap();
    let second = peer.propose(b"second".to_vec()).unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    assert!(reader
        .entries(second.index, second.index + 1, None)
        .is_err());
    peer.checkpoint(b"application checkpoint".to_vec()).unwrap();
    let mut fresh = peer.capture_log_reader().unwrap();
    assert!(fresh.entries(first.index, first.index + 1, None).is_err());
    drop(peer);
    let read = std::thread::spawn(move || reader.entries(first.index, first.index + 1, None));
    assert_eq!(read.join().unwrap().unwrap(), entries);
}

fn pump(peers: &mut BTreeMap<u64, DurablePeer>, applied: &mut Applied, isolated: Option<u64>) {
    for _ in 0..100 {
        let mut packets = Vec::new();
        for (id, peer) in peers.iter_mut() {
            let id = *id;
            let output = peer
                .process_ready(|log, payload| {
                    applied
                        .entry(id)
                        .or_default()
                        .push((log.index, payload.to_vec()));
                    Ok(())
                })
                .unwrap();
            packets.extend(
                output
                    .into_iter()
                    .filter(|packet| isolated != Some(id) && isolated != Some(packet.to)),
            );
        }
        if packets.is_empty() && peers.values().all(|peer| !peer.has_ready()) {
            return;
        }
        for packet in packets {
            peers
                .get_mut(&packet.to)
                .unwrap()
                .step(&packet.bytes)
                .unwrap();
        }
    }
    panic!("peer message exchange did not settle");
}

fn pump_async_reads(peers: &mut BTreeMap<u64, DurablePeer>, applied: &mut Applied) -> usize {
    let mut reads = 0;
    for _ in 0..200 {
        let mut packets = Vec::new();
        let mut active = false;
        for (&id, peer) in peers.iter_mut() {
            let tasks = peer.take_log_read_tasks().unwrap();
            assert!(
                tasks.len() <= 1,
                "the configured outstanding bound must hold"
            );
            for task in tasks {
                reads += 1;
                active = true;
                let completion = std::thread::spawn(move || task.execute()).join().unwrap();
                assert!(peer.complete_log_read(completion).unwrap());
            }
            packets.extend(
                peer.process_ready(|log, payload| {
                    applied
                        .entry(id)
                        .or_default()
                        .push((log.index, payload.to_vec()));
                    Ok(())
                })
                .unwrap(),
            );
        }
        if packets.is_empty() && !active {
            return reads;
        }
        for packet in packets {
            peers
                .get_mut(&packet.to)
                .unwrap()
                .step(&packet.bytes)
                .unwrap();
        }
    }
    panic!("asynchronous replication did not quiesce");
}

#[test]
fn durable_read_completions_resume_replication_to_a_lagging_voter() {
    let dir = Directory::new();
    let mut group = peers(&dir);
    let mut applied = Applied::new();
    group.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut group, &mut applied, None);
    let baseline = applied.get(&1).unwrap().len();
    for value in 0..20u8 {
        group.get_mut(&1).unwrap().propose(vec![value; 64]).unwrap();
        pump(&mut group, &mut applied, Some(3));
    }
    group
        .get_mut(&1)
        .unwrap()
        .enable_async_log_reads(1)
        .unwrap();
    let mut reads = 0;
    for _ in 0..6 {
        group.get_mut(&1).unwrap().tick().unwrap();
        reads += pump_async_reads(&mut group, &mut applied);
    }
    assert!(reads > 0, "catch-up must consume durable read completions");
    assert_eq!(applied.get(&3), applied.get(&1));
    assert_eq!(applied.get(&3).unwrap().len(), baseline + 20);
}

#[test]
fn bounded_journal_reads_resume_until_a_lagging_voter_is_current() {
    for cache in [None, Some(512)] {
        for limit in [1, 3] {
            for asynchronous in [false, true] {
                let dir = Directory::new();
                let tuning = PeerTuning {
                    max_disk_replicate_entries: Some(limit),
                    max_log_cache_bytes: cache,
                    ..Default::default()
                };
                let mut group: BTreeMap<_, _> = (1..=3)
                    .map(|id| {
                        (
                            id,
                            DurablePeer::open_with_tuning(dir.options(id, vec![1, 2, 3]), tuning)
                                .unwrap(),
                        )
                    })
                    .collect();
                let mut applied = Applied::new();
                group.get_mut(&1).unwrap().campaign().unwrap();
                pump(&mut group, &mut applied, None);
                for value in 0..24u8 {
                    group
                        .get_mut(&1)
                        .unwrap()
                        .propose(vec![value; 128])
                        .unwrap();
                    pump(&mut group, &mut applied, Some(3));
                }
                let committed = group.get(&1).unwrap().commit_index();
                assert!(group.get(&3).unwrap().commit_index() < committed);
                if asynchronous {
                    group
                        .get_mut(&1)
                        .unwrap()
                        .enable_async_log_reads(1)
                        .unwrap();
                }
                group.get_mut(&1).unwrap().report_unreachable(3).unwrap();
                let mut reads = 0;
                for _ in 0..6 {
                    group.get_mut(&1).unwrap().tick().unwrap();
                    reads += pump_async_reads(&mut group, &mut applied);
                }
                if asynchronous {
                    assert!(reads >= 24usize.div_ceil(limit));
                } else {
                    assert_eq!(reads, 0);
                }
                assert_eq!(group.get(&3).unwrap().commit_index(), committed);
                assert_eq!(applied.get(&1), applied.get(&3));
                group.get_mut(&1).unwrap().transfer_leadership(3).unwrap();
                for _ in 0..6 {
                    pump_async_reads(&mut group, &mut applied);
                }
                assert!(group.get(&3).unwrap().is_leader());
                group.get_mut(&3).unwrap().propose(vec![99; 128]).unwrap();
                pump_async_reads(&mut group, &mut applied);
                assert_eq!(applied.get(&1), applied.get(&3));
                let expected = applied.get(&3).unwrap().clone();
                drop(group);
                let mut group: BTreeMap<_, _> = (1..=3)
                    .map(|id| {
                        (
                            id,
                            DurablePeer::open_with_tuning(
                                dir.options(id, vec![1, 2, 3]),
                                PeerTuning::default(),
                            )
                            .unwrap(),
                        )
                    })
                    .collect();
                let mut replay = Applied::new();
                pump(&mut group, &mut replay, None);
                assert_eq!(replay.get(&1), Some(&expected));
                assert_eq!(replay.get(&3), Some(&expected));
            }
        }
    }
}

fn peers(dir: &Directory) -> BTreeMap<u64, DurablePeer> {
    (1..=3)
        .map(|id| {
            (
                id,
                DurablePeer::open(dir.options(id, vec![1, 2, 3])).unwrap(),
            )
        })
        .collect()
}
#[test]
fn proposal_groups_are_bounded_contiguous_and_replayed() {
    let dir = Directory::new();
    let mut group = peers(&dir);
    assert!(group
        .get_mut(&2)
        .unwrap()
        .propose_batch(vec![vec![1]])
        .is_err());
    let mut applied = Applied::new();
    group.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut group, &mut applied, None);
    let before = group[&1].commit_index();
    let leader = group.get_mut(&1).unwrap();
    for invalid in [
        Vec::new(),
        vec![vec![1], Vec::new()],
        vec![vec![1]; 1025],
        vec![vec![1; 4 * 1024 * 1024], vec![2]],
    ] {
        assert!(leader.propose_batch(invalid).is_err());
    }
    let payloads: Vec<_> = (0..16u8).map(|n| vec![n; 4096]).collect();
    let ids = leader.propose_batch(payloads.clone()).unwrap();
    assert_eq!(ids.len(), payloads.len());
    for (offset, id) in ids.iter().enumerate() {
        assert_eq!(id.index, before + offset as u64 + 1);
        assert_eq!(id.term, leader.term());
    }
    assert_eq!(leader.commit_index(), before);
    pump(&mut group, &mut applied, None);
    let expected = applied[&1].clone();
    for values in applied.values() {
        assert_eq!(values, &expected);
    }
    drop(group);
    let mut recovered = peers(&dir);
    let mut replay = Applied::new();
    pump(&mut recovered, &mut replay, None);
    for values in replay.values() {
        assert_eq!(values, &expected);
    }
}

#[test]
fn proposal_group_limits_and_uncommitted_backpressure_preserve_positions() {
    let dir = Directory::new();
    let mut group = peers(&dir);
    let mut applied = Applied::new();
    group.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut group, &mut applied, None);
    let leader = group.get_mut(&1).unwrap();
    let before = leader.commit_index();
    let ids = leader.propose_batch(vec![vec![7; 4096]; 1024]).unwrap();
    assert_eq!(ids.len(), 1024);
    assert_eq!(ids[0].index, before + 1);
    assert_eq!(ids[1023].index, before + 1024);
    assert!(leader.propose_batch(vec![vec![8]]).is_err());
    assert_eq!(leader.commit_index(), before);
    pump(&mut group, &mut applied, None);
    for values in applied.values() {
        assert_eq!(
            values.iter().filter(|(_, value)| !value.is_empty()).count(),
            1024
        );
        for (_, value) in values.iter().filter(|(_, value)| !value.is_empty()) {
            assert_eq!(value.len(), 4096);
            assert!(value.iter().all(|byte| *byte == 7));
        }
    }
    let next = group
        .get_mut(&1)
        .unwrap()
        .propose_batch(vec![vec![9]])
        .unwrap();
    assert_eq!(next[0].index, before + 1025);
    pump(&mut group, &mut applied, None);
    assert!(applied
        .values()
        .all(|values| values.last().unwrap().1 == vec![9]));
}

#[test]
fn grouped_proposals_reduce_append_packet_overhead() {
    let mut costs = Vec::new();
    for grouped in [false, true] {
        let dir = Directory::new();
        let mut group = peers(&dir);
        let mut applied = Applied::new();
        group.get_mut(&1).unwrap().campaign().unwrap();
        pump(&mut group, &mut applied, None);
        let leader = group.get_mut(&1).unwrap();
        let values = (0..16u8).map(|n| vec![n; 64]).collect::<Vec<_>>();
        if grouped {
            leader.propose_batch(values).unwrap();
        } else {
            for value in values {
                leader.propose(value).unwrap();
            }
        }
        let packets = leader.process_ready(|_, _| Ok(())).unwrap();
        costs.push((
            packets.len(),
            packets.iter().map(|p| p.bytes.len()).sum::<usize>(),
        ));
    }
    println!("individual={:?} grouped={:?}", costs[0], costs[1]);
    assert!(costs[1].0 < costs[0].0);
    assert!(costs[1].1 < costs[0].1);
}

#[test]
fn isolated_peer_cannot_elect_or_commit_without_received_votes() {
    let dir = Directory::new();
    let mut peer = DurablePeer::open(dir.options(1, vec![1, 2, 3])).unwrap();
    peer.campaign().unwrap();
    let mut applied = 0;
    let packets = peer
        .process_ready(|_, _| {
            applied += 1;
            Ok(())
        })
        .unwrap();
    assert!(!packets.is_empty());
    assert!(!peer.is_leader());
    assert_eq!(peer.commit_index(), 0);
    assert_eq!(applied, 0);
    assert!(peer.propose(b"uncommitted".to_vec()).is_err());
}
#[test]
fn real_message_quorum_commits_and_replays_after_restart() {
    let dir = Directory::new();
    let mut peers = peers(&dir);
    let mut applied = Applied::new();
    peers.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut peers, &mut applied, None);
    assert!(peers[&1].is_leader());
    let log = peers
        .get_mut(&1)
        .unwrap()
        .propose(b"binary outcome".to_vec())
        .unwrap();
    pump(&mut peers, &mut applied, Some(1));
    assert!(peers[&1].commit_index() < log.index);
    assert!(!applied[&1].iter().any(|(_, p)| p == b"binary outcome"));
    for _ in 0..3 {
        peers.get_mut(&1).unwrap().tick().unwrap();
        pump(&mut peers, &mut applied, None);
    }
    for id in 1..=3 {
        assert!(applied[&id]
            .iter()
            .any(|(index, p)| *index == log.index && p == b"binary outcome"));
    }
    let expected = applied[&1].clone();
    drop(peers.remove(&1));
    applied.remove(&1);
    peers.insert(1, DurablePeer::open(dir.options(1, vec![1, 2, 3])).unwrap());
    pump(&mut peers, &mut applied, None);
    assert_eq!(applied[&1], expected);
}
#[test]
fn majority_replaces_an_isolated_leaders_uncommitted_tail() {
    let dir = Directory::new();
    let mut peers = peers(&dir);
    let mut applied = Applied::new();
    peers.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut peers, &mut applied, None);
    peers
        .get_mut(&1)
        .unwrap()
        .propose(b"orphan".to_vec())
        .unwrap();
    pump(&mut peers, &mut applied, Some(1));
    let mut replacement = None;
    for _ in 0..100 {
        for peer in peers.values_mut() {
            peer.tick().unwrap();
        }
        pump(&mut peers, &mut applied, Some(1));
        replacement = (2..=3).find(|id| peers[id].is_leader());
        if replacement.is_some() {
            break;
        }
    }
    let leader = replacement.expect("reachable majority elects a leader");
    peers
        .get_mut(&leader)
        .unwrap()
        .propose(b"majority".to_vec())
        .unwrap();
    pump(&mut peers, &mut applied, Some(1));
    for _ in 0..10 {
        peers.get_mut(&leader).unwrap().tick().unwrap();
        pump(&mut peers, &mut applied, None);
    }
    for id in 1..=3 {
        assert!(!applied[&id].iter().any(|(_, p)| p == b"orphan"));
        assert!(applied[&id].iter().any(|(_, p)| p == b"majority"));
        assert_eq!(applied[&id], applied[&leader]);
    }
}
#[test]
fn durable_identity_lock_and_packet_routing_are_enforced() {
    let dir = Directory::new();
    let options = dir.options(1, vec![1, 2, 3]);
    let mut peer = DurablePeer::open(options.clone()).unwrap();
    assert!(DurablePeer::open(options.clone()).is_err());
    peer.campaign().unwrap();
    let packet = peer.process_ready(|_, _| Ok(())).unwrap().remove(0);
    assert!(peer.step(&packet.bytes).is_err());
    drop(peer);
    let mut wrong = options.clone();
    wrong.group_id += 1;
    assert!(DurablePeer::open(wrong).is_err());
    let mut wrong = options.clone();
    wrong.voters = vec![1, 2];
    assert!(DurablePeer::open(wrong).is_err());
    assert!(DurablePeer::open(options).is_ok());
}
#[test]
fn application_failure_stops_peer_and_committed_data_replays() {
    let dir = Directory::new();
    let options = dir.options(1, vec![1]);
    let mut peer = DurablePeer::open(options.clone()).unwrap();
    peer.campaign().unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    peer.propose(b"retry after restart".to_vec()).unwrap();
    assert!(peer
        .process_ready(|_, payload| if payload.is_empty() {
            Ok(())
        } else {
            Err(RaftError::Storage("apply failed".into()))
        })
        .is_err());
    assert!(!peer.is_leader());
    assert!(peer.tick().is_err());
    assert!(peer.propose(b"later".to_vec()).is_err());
    drop(peer);
    let mut peer = DurablePeer::open(options).unwrap();
    let mut payloads = Vec::new();
    peer.process_ready(|_, p| {
        payloads.push(p.to_vec());
        Ok(())
    })
    .unwrap();
    assert!(payloads.iter().any(|p| p == b"retry after restart"));
}

#[test]
fn quorum_reads_wait_for_messages_without_growing_any_wal() {
    let dir = Directory::new();
    let mut peers = peers(&dir);
    let mut applied = Applied::new();
    peers.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut peers, &mut applied, None);
    peers
        .get_mut(&1)
        .unwrap()
        .propose(b"visible".to_vec())
        .unwrap();
    pump(&mut peers, &mut applied, None);
    for _ in 0..3 {
        peers.get_mut(&1).unwrap().tick().unwrap();
        pump(&mut peers, &mut applied, None);
    }
    assert_eq!(applied[&1], applied[&2]);
    assert_eq!(applied[&1], applied[&3]);
    let before = applied.clone();
    let files: Vec<_> = (1..=3)
        .map(|id| std::fs::read(dir.options(id, vec![1, 2, 3]).wal_path).unwrap())
        .collect();
    let token = peers.get_mut(&1).unwrap().begin_read().unwrap();
    assert!(peers.get_mut(&1).unwrap().begin_read().is_err());
    assert!(peers.get_mut(&2).unwrap().begin_read().is_err());
    pump(&mut peers, &mut applied, Some(1));
    assert!(!peers.get_mut(&1).unwrap().poll_read(token).unwrap());
    assert!(peers.get_mut(&1).unwrap().poll_read(token + 1).is_err());
    for _ in 0..3 {
        peers.get_mut(&1).unwrap().tick().unwrap();
        pump(&mut peers, &mut applied, None);
    }
    assert!(peers.get_mut(&1).unwrap().poll_read(token).unwrap());
    assert!(peers.get_mut(&1).unwrap().poll_read(token).is_err());
    for _ in 0..64 {
        let next = peers.get_mut(&1).unwrap().begin_read().unwrap();
        assert_ne!(next, token);
        assert!(!peers.get_mut(&1).unwrap().poll_read(next).unwrap());
        pump(&mut peers, &mut applied, None);
        assert!(peers.get_mut(&1).unwrap().poll_read(next).unwrap());
    }
    assert_eq!(applied, before);
    for (id, bytes) in (1..=3).zip(files) {
        assert_eq!(
            std::fs::read(dir.options(id, vec![1, 2, 3]).wal_path).unwrap(),
            bytes
        );
    }
}

fn peers_with_read_limit(dir: &Directory, limit: usize) -> BTreeMap<u64, DurablePeer> {
    (1..=3)
        .map(|id| {
            (
                id,
                DurablePeer::open_with_tuning(
                    dir.options(id, vec![1, 2, 3]),
                    PeerTuning {
                        max_pending_reads: limit,
                        ..PeerTuning::default()
                    },
                )
                .unwrap(),
            )
        })
        .collect()
}

#[test]
fn concurrent_reads_share_a_later_quorum_without_wal_growth() {
    let dir = Directory::new();
    let mut peers = peers_with_read_limit(&dir, 1024);
    let mut applied = Applied::new();
    peers.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut peers, &mut applied, None);
    let files: Vec<_> = (1..=3)
        .map(|id| std::fs::read(dir.options(id, vec![1, 2, 3]).wal_path).unwrap())
        .collect();
    let tokens: Vec<_> = (0..1024)
        .map(|_| peers.get_mut(&1).unwrap().begin_read().unwrap())
        .collect();
    assert!(peers.get_mut(&1).unwrap().begin_read().is_err());
    let packets = peers
        .get_mut(&1)
        .unwrap()
        .process_ready(|_, _| Ok(()))
        .unwrap();
    assert_eq!(packets.len(), 2, "one read heartbeat per remote voter");
    // Deliver only the newest read heartbeat to one remote voter. Its quorum
    // confirmation must release the older requests as well, without their ACKs.
    let packet = packets
        .into_iter()
        .filter(|packet| packet.to == 2)
        .find(|packet| {
            let envelope = TestPacket::decode(packet.bytes.as_slice()).unwrap();
            let message = raft_core::eraftpb::Message::decode(envelope.message.as_slice()).unwrap();
            message.get_msg_type() == raft_core::eraftpb::MessageType::MsgHeartbeat
                && message
                    .context
                    .ends_with(&tokens.last().unwrap().to_be_bytes())
        })
        .unwrap();
    peers.get_mut(&2).unwrap().step(&packet.bytes).unwrap();
    let acknowledgments = peers
        .get_mut(&2)
        .unwrap()
        .process_ready(|_, _| Ok(()))
        .unwrap();
    assert!(!acknowledgments.is_empty());
    for packet in acknowledgments {
        peers
            .get_mut(&packet.to)
            .unwrap()
            .step(&packet.bytes)
            .unwrap();
    }
    pump(&mut peers, &mut applied, None);
    assert!(peers.get_mut(&1).unwrap().begin_read().is_err());
    for token in tokens.into_iter().rev() {
        assert!(peers.get_mut(&1).unwrap().poll_read(token).unwrap());
        assert!(peers.get_mut(&1).unwrap().poll_read(token).is_err());
    }
    let token = peers.get_mut(&1).unwrap().begin_read().unwrap();
    assert!(peers.get_mut(&1).unwrap().cancel_read(token).unwrap());
    for (id, bytes) in (1..=3).zip(files) {
        assert_eq!(
            std::fs::read(dir.options(id, vec![1, 2, 3]).wal_path).unwrap(),
            bytes
        );
    }
}

#[test]
fn canceled_and_earlier_read_responses_cannot_confirm_newer_tokens() {
    let dir = Directory::new();
    let mut peers = peers_with_read_limit(&dir, 3);
    let mut applied = Applied::new();
    peers.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut peers, &mut applied, None);
    let first = peers.get_mut(&1).unwrap().begin_read().unwrap();
    let mut packets = peers
        .get_mut(&1)
        .unwrap()
        .process_ready(|_, _| Ok(()))
        .unwrap();
    let canceled = peers.get_mut(&1).unwrap().begin_read().unwrap();
    packets.extend(
        peers
            .get_mut(&1)
            .unwrap()
            .process_ready(|_, _| Ok(()))
            .unwrap(),
    );
    let tail = peers.get_mut(&1).unwrap().begin_read().unwrap();
    packets.extend(
        peers
            .get_mut(&1)
            .unwrap()
            .process_ready(|_, _| Ok(()))
            .unwrap(),
    );
    assert!(peers.get_mut(&1).unwrap().cancel_read(canceled).unwrap());
    let newer = peers.get_mut(&1).unwrap().begin_read().unwrap();
    assert!(!peers.get_mut(&1).unwrap().cancel_read(canceled).unwrap());
    // The old packets have escaped the peer. Replay duplicates as well as the
    // canceled context; none was sent after the newer token was admitted.
    for packet in packets {
        for _ in 0..2 {
            peers
                .get_mut(&packet.to)
                .unwrap()
                .step(&packet.bytes)
                .unwrap();
        }
    }
    let mut responses = Vec::new();
    for id in [2, 3] {
        responses.extend(
            peers
                .get_mut(&id)
                .unwrap()
                .process_ready(|_, _| Ok(()))
                .unwrap(),
        );
    }
    for packet in responses {
        peers
            .get_mut(&packet.to)
            .unwrap()
            .step(&packet.bytes)
            .unwrap();
    }
    // Process the old confirmations without delivering the new request yet.
    peers
        .get_mut(&1)
        .unwrap()
        .process_ready(|_, _| Ok(()))
        .unwrap();
    assert!(peers.get_mut(&1).unwrap().poll_read(first).unwrap());
    assert!(peers.get_mut(&1).unwrap().poll_read(tail).unwrap());
    assert!(peers.get_mut(&1).unwrap().poll_read(canceled).is_err());
    assert!(!peers.get_mut(&1).unwrap().poll_read(newer).unwrap());
    for _ in 0..2 {
        peers.get_mut(&1).unwrap().tick().unwrap();
    }
    pump(&mut peers, &mut applied, None);
    assert!(peers.get_mut(&1).unwrap().poll_read(newer).unwrap());
    let survivor = peers.get_mut(&1).unwrap().begin_read().unwrap();
    let tail = peers.get_mut(&1).unwrap().begin_read().unwrap();
    assert!(peers.get_mut(&1).unwrap().cancel_read(tail).unwrap());
    pump(&mut peers, &mut applied, None);
    assert!(peers.get_mut(&1).unwrap().poll_read(survivor).unwrap());
}

#[test]
fn concurrent_reads_keep_independent_application_fences_during_retry() {
    use matrixraft::peer_runtime::ApplyDecision;
    let dir = Directory::new();
    let mut peer = DurablePeer::open_with_tuning(
        dir.options(1, vec![1]),
        PeerTuning {
            max_pending_reads: 3,
            max_apply_bytes: Some(1),
            max_log_cache_bytes: Some(512),
            ..PeerTuning::default()
        },
    )
    .unwrap();
    peer.campaign().unwrap();
    while peer.has_ready() {
        peer.process_ready(|_, _| Ok(())).unwrap();
    }
    let early = peer.begin_read().unwrap();
    let last = peer
        .propose_batch(vec![vec![7; 512]; 16])
        .unwrap()
        .last()
        .unwrap()
        .index;
    // Persistence and commitment must advance before a read can fence this tail.
    for _ in 0..2 {
        peer.process_ready_retryable(|_, _| Ok(ApplyDecision::Retry))
            .unwrap();
    }
    assert_eq!(peer.commit_index(), last);
    assert!(peer.applied_index() < last);
    let late = peer.begin_read().unwrap();
    let canceled = peer.begin_read().unwrap();
    peer.process_ready_retryable(|_, _| Ok(ApplyDecision::Retry))
        .unwrap();
    assert!(peer.poll_read(early).unwrap());
    assert!(!peer.poll_read(late).unwrap());
    assert!(peer.cancel_read(canceled).unwrap());
    assert!(peer.prepare_checkpoint().is_err());
    let mut values = Vec::new();
    for _ in 0..100 {
        assert!(!peer.poll_read(late).unwrap());
        peer.process_ready(|_, bytes| {
            values.push(bytes.to_vec());
            Ok(())
        })
        .unwrap();
        if peer.applied_index() == last {
            break;
        }
    }
    assert_eq!(values, vec![vec![7; 512]; 16]);
    assert!(peer.poll_read(late).unwrap());
    assert!(peer.prepare_checkpoint().is_ok());
}

#[test]
fn a_new_leadership_term_releases_all_old_read_admission() {
    let dir = Directory::new();
    let mut peers = peers_with_read_limit(&dir, 4);
    let mut applied = Applied::new();
    peers.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut peers, &mut applied, None);
    let old: Vec<_> = (0..4)
        .map(|_| peers.get_mut(&1).unwrap().begin_read().unwrap())
        .collect();
    pump(&mut peers, &mut applied, None);
    peers.get_mut(&2).unwrap().force_campaign().unwrap();
    pump(&mut peers, &mut applied, None);
    assert!(peers[&2].is_leader());
    peers.get_mut(&1).unwrap().force_campaign().unwrap();
    pump(&mut peers, &mut applied, None);
    assert!(peers[&1].is_leader());
    // Starting in the new term must reclaim old slots even if the caller never
    // polled the obsolete tokens while this peer was a follower.
    let fresh: Vec<_> = (0..4)
        .map(|_| peers.get_mut(&1).unwrap().begin_read().unwrap())
        .collect();
    for token in old {
        assert!(peers.get_mut(&1).unwrap().poll_read(token).is_err());
    }
    pump(&mut peers, &mut applied, None);
    for token in fresh {
        assert!(peers.get_mut(&1).unwrap().poll_read(token).unwrap());
    }
}

#[test]
fn concurrent_reads_admitted_during_election_wait_for_current_term_commitment() {
    use raft_core::eraftpb::{Message, MessageType};
    let dir = Directory::new();
    let mut peers = peers_with_read_limit(&dir, 3);
    let mut applied = Applied::new();
    peers.get_mut(&1).unwrap().campaign().unwrap();
    // Deliver votes but not the new leader's entry. Becoming leader is not
    // sufficient to begin safe read confirmation in the consensus core.
    for _ in 0..10 {
        let mut packets = Vec::new();
        for peer in peers.values_mut() {
            packets.extend(peer.process_ready(|_, _| Ok(())).unwrap());
        }
        for packet in packets {
            let envelope = TestPacket::decode(packet.bytes.as_slice()).unwrap();
            let message = Message::decode(envelope.message.as_slice()).unwrap();
            if message.get_msg_type() != MessageType::MsgAppend {
                peers
                    .get_mut(&packet.to)
                    .unwrap()
                    .step(&packet.bytes)
                    .unwrap();
            }
        }
        if peers[&1].is_leader() {
            break;
        }
    }
    assert!(peers[&1].is_leader());
    assert_eq!(peers[&1].commit_index(), 0);
    let mut tokens: Vec<_> = (0..3)
        .map(|_| peers.get_mut(&1).unwrap().begin_read().unwrap())
        .collect();
    assert!(peers.get_mut(&1).unwrap().begin_read().is_err());
    let canceled = tokens.remove(1);
    assert!(peers.get_mut(&1).unwrap().cancel_read(canceled).unwrap());
    tokens.push(peers.get_mut(&1).unwrap().begin_read().unwrap());
    for token in &tokens {
        assert!(!peers.get_mut(&1).unwrap().poll_read(*token).unwrap());
    }
    let packets = peers
        .get_mut(&1)
        .unwrap()
        .process_ready(|_, _| Ok(()))
        .unwrap();
    assert!(!packets.is_empty());
    assert_eq!(peers[&1].commit_index(), 0);
    for packet in packets {
        peers
            .get_mut(&packet.to)
            .unwrap()
            .step(&packet.bytes)
            .unwrap();
    }
    pump(&mut peers, &mut applied, None);
    assert!(peers[&1].commit_index() > 0);
    assert!(peers.get_mut(&1).unwrap().poll_read(canceled).is_err());
    for token in tokens {
        assert!(peers.get_mut(&1).unwrap().poll_read(token).unwrap());
    }
}
#[test]
fn read_confirmation_is_invalid_after_leadership_changes() {
    let dir = Directory::new();
    let mut peers = peers(&dir);
    let mut applied = Applied::new();
    peers.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut peers, &mut applied, None);
    let token = peers.get_mut(&1).unwrap().begin_read().unwrap();
    // Even a completed confirmation cannot survive a later leadership change.
    pump(&mut peers, &mut applied, None);
    let mut replacement = None;
    for _ in 0..100 {
        for peer in peers.values_mut() {
            peer.tick().unwrap();
        }
        pump(&mut peers, &mut applied, Some(1));
        replacement = (2..=3).find(|id| peers[id].is_leader());
        if replacement.is_some() {
            break;
        }
    }
    let leader = replacement.unwrap();
    peers
        .get_mut(&leader)
        .unwrap()
        .propose(b"new term".to_vec())
        .unwrap();
    pump(&mut peers, &mut applied, None);
    for _ in 0..3 {
        peers.get_mut(&leader).unwrap().tick().unwrap();
        pump(&mut peers, &mut applied, None);
    }
    assert!(!peers[&1].is_leader());
    assert_eq!(peers[&1].term(), peers[&leader].term());
    assert!(peers.get_mut(&1).unwrap().poll_read(token).is_err());
    let token = peers.get_mut(&leader).unwrap().begin_read().unwrap();
    pump(&mut peers, &mut applied, None);
    assert!(peers.get_mut(&leader).unwrap().poll_read(token).unwrap());
}
#[test]
fn read_confirmation_requires_successful_local_application() {
    let dir = Directory::new();
    let mut peer = DurablePeer::open(dir.options(1, vec![1])).unwrap();
    peer.campaign().unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    peer.propose(b"must apply".to_vec()).unwrap();
    let token = peer.begin_read().unwrap();
    assert!(!peer.poll_read(token).unwrap());
    assert!(peer
        .process_ready(|_, payload| {
            if payload.is_empty() {
                Ok(())
            } else {
                Err(RaftError::Storage("injected apply failure".into()))
            }
        })
        .is_err());
    assert!(peer.poll_read(token).is_err());
    assert!(peer.begin_read().is_err());
}

#[derive(Default)]
struct CheckpointApp {
    value: u64,
    restored: usize,
}
#[derive(Clone, PartialEq, prost::Message)]
struct TestPacket {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, tag = "2")]
    group: u64,
    #[prost(bytes = "vec", tag = "3")]
    message: Vec<u8>,
}
fn pump_checkpoints(
    peers: &mut BTreeMap<u64, DurablePeer>,
    apps: &mut BTreeMap<u64, CheckpointApp>,
    isolated: Option<u64>,
) {
    pump_checkpoint_packets(peers, apps, isolated, None);
}
fn pump_checkpoint_packets(
    peers: &mut BTreeMap<u64, DurablePeer>,
    apps: &mut BTreeMap<u64, CheckpointApp>,
    isolated: Option<u64>,
    drop_kind: Option<raft_core::eraftpb::MessageType>,
) -> usize {
    let mut dropped = 0;
    for _ in 0..100 {
        let mut packets = Vec::new();
        for (id, peer) in peers.iter_mut() {
            let app = std::cell::RefCell::new(apps.entry(*id).or_default());
            let output = peer
                .process_ready_with_snapshot(
                    |_, bytes| {
                        if !bytes.is_empty() {
                            app.borrow_mut().value = u64::from_be_bytes(bytes.try_into().unwrap());
                        }
                        Ok(())
                    },
                    |_, bytes| {
                        let mut app = app.borrow_mut();
                        app.value = u64::from_be_bytes(bytes.try_into().unwrap());
                        app.restored += 1;
                        Ok(())
                    },
                )
                .unwrap();
            packets.extend(output.into_iter().filter(|p| {
                let packet = TestPacket::decode(p.bytes.as_slice()).unwrap();
                let message =
                    raft_core::eraftpb::Message::decode(packet.message.as_slice()).unwrap();
                if drop_kind == Some(message.get_msg_type())
                    && (message.to == 3 || message.from == 3)
                {
                    dropped += 1;
                    return false;
                }
                isolated != Some(*id) && isolated != Some(p.to)
            }));
        }
        if packets.is_empty() {
            return dropped;
        }
        for packet in packets {
            peers
                .get_mut(&packet.to)
                .unwrap()
                .step(&packet.bytes)
                .unwrap();
        }
    }
    panic!("checkpoint exchange did not settle");
}
#[test]
fn checkpoint_catches_up_a_lagging_voter_and_all_peers_recover() {
    let dir = Directory::new();
    let mut peers = peers(&dir);
    let mut apps = BTreeMap::new();
    peers.get_mut(&1).unwrap().campaign().unwrap();
    pump_checkpoints(&mut peers, &mut apps, None);
    for value in 1u64..=32 {
        peers
            .get_mut(&1)
            .unwrap()
            .propose(value.to_be_bytes().to_vec())
            .unwrap();
        pump_checkpoints(&mut peers, &mut apps, Some(3));
    }
    assert_eq!(apps[&1].value, 32);
    assert_eq!(apps[&3].value, 0);
    let path = dir.options(1, vec![1, 2, 3]).wal_path;
    let before = std::fs::metadata(&path).unwrap().len();
    peers
        .get_mut(&1)
        .unwrap()
        .checkpoint(32u64.to_be_bytes().to_vec())
        .unwrap();
    assert!(std::fs::metadata(&path).unwrap().len() < before);
    assert!(
        DurablePeer::open(dir.options(1, vec![1, 2, 3])).is_err(),
        "replacement WAL must remain exclusively locked"
    );
    for _ in 0..10 {
        peers.get_mut(&1).unwrap().tick().unwrap();
        pump_checkpoints(&mut peers, &mut apps, None);
    }
    assert_eq!(apps[&3].value, 32);
    assert_eq!(apps[&3].restored, 1);
    peers
        .get_mut(&1)
        .unwrap()
        .propose(33u64.to_be_bytes().to_vec())
        .unwrap();
    pump_checkpoints(&mut peers, &mut apps, None);
    for _ in 0..3 {
        peers.get_mut(&1).unwrap().tick().unwrap();
        pump_checkpoints(&mut peers, &mut apps, None);
    }
    drop(peers);
    apps.clear();
    let mut peers = self::peers(&dir);
    pump_checkpoints(&mut peers, &mut apps, None);
    for id in 1..=3 {
        assert_eq!(apps[&id].value, 33);
    }
    assert_eq!(apps[&1].restored, 1);
    assert_eq!(apps[&3].restored, 1);
}
#[test]
fn checkpoint_preserves_an_uncommitted_suffix_without_applying_it_on_restart() {
    let dir = Directory::new();
    let mut peers = peers(&dir);
    let mut apps = BTreeMap::new();
    peers.get_mut(&1).unwrap().campaign().unwrap();
    pump_checkpoints(&mut peers, &mut apps, None);
    peers
        .get_mut(&1)
        .unwrap()
        .propose(1u64.to_be_bytes().to_vec())
        .unwrap();
    pump_checkpoints(&mut peers, &mut apps, None);
    let undecided = peers
        .get_mut(&1)
        .unwrap()
        .propose(999u64.to_be_bytes().to_vec())
        .unwrap();
    pump_checkpoints(&mut peers, &mut apps, Some(1));
    peers
        .get_mut(&1)
        .unwrap()
        .checkpoint(1u64.to_be_bytes().to_vec())
        .unwrap();
    drop(peers);
    apps.clear();
    let mut peers = self::peers(&dir);
    pump_checkpoints(&mut peers, &mut apps, Some(1));
    assert_eq!(apps[&1].value, 1);
    assert!(peers[&1].commit_index() < undecided.index);
    peers.get_mut(&1).unwrap().campaign().unwrap();
    pump_checkpoints(&mut peers, &mut apps, None);
    assert_eq!(apps[&1].value, 999);
}
#[test]
fn checkpoint_publication_failure_preserves_the_old_wal_and_stops_the_peer() {
    let dir = Directory::new();
    let options = dir.options(1, vec![1]);
    let mut peer = DurablePeer::open(options.clone()).unwrap();
    peer.campaign().unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    peer.propose(7u64.to_be_bytes().to_vec()).unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    let before = std::fs::read(&options.wal_path).unwrap();
    assert!(peer
        .checkpoint(vec![0; matrixraft::peer_runtime::MAX_CHECKPOINT_BYTES + 1])
        .is_err());
    assert!(peer.is_leader());
    let temporary = options.wal_path.with_extension("checkpoint.tmp");
    std::fs::create_dir(&temporary).unwrap();
    assert!(peer.checkpoint(7u64.to_be_bytes().to_vec()).is_err());
    assert!(!peer.is_leader());
    assert!(peer.tick().is_err());
    assert_eq!(std::fs::read(&options.wal_path).unwrap(), before);
    drop(peer);
    let mut reopened = DurablePeer::open(options).unwrap();
    let mut seen = false;
    reopened
        .process_ready(|_, bytes| {
            seen |= bytes == 7u64.to_be_bytes();
            Ok(())
        })
        .unwrap();
    assert!(seen);
}
#[test]
fn checkpoint_recovery_requires_restoration_and_refuses_every_truncated_prefix() {
    let dir = Directory::new();
    let options = dir.options(1, vec![1]);
    let mut peer = DurablePeer::open(options.clone()).unwrap();
    peer.campaign().unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    peer.checkpoint(4u64.to_be_bytes().to_vec()).unwrap();
    drop(peer);
    let full = std::fs::read(&options.wal_path).unwrap();
    for cut in 0..full.len() {
        std::fs::write(&options.wal_path, &full[..cut]).unwrap();
        assert!(DurablePeer::open(options.clone()).is_err(), "cut {cut}");
        assert_eq!(std::fs::read(&options.wal_path).unwrap(), full[..cut]);
    }
    for index in 0..full.len() {
        let mut damaged = full.clone();
        damaged[index] ^= 1;
        std::fs::write(&options.wal_path, &damaged).unwrap();
        assert!(DurablePeer::open(options.clone()).is_err(), "byte {index}");
        assert_eq!(std::fs::read(&options.wal_path).unwrap(), damaged);
    }
    std::fs::write(&options.wal_path, &full).unwrap();
    let mut peer = DurablePeer::open(options.clone()).unwrap();
    assert!(peer.process_ready(|_, _| Ok(())).is_err());
    assert!(peer.tick().is_err());
    drop(peer);
    let mut peer = DurablePeer::open(options).unwrap();
    assert!(peer
        .process_ready_with_snapshot(
            |_, _| Ok(()),
            |_, _| Err(RaftError::Storage("restore failed".into()))
        )
        .is_err());
    assert!(peer.tick().is_err());
}

#[test]
fn repeated_checkpoints_bound_history_and_missing_wal_is_not_reinitialized() {
    let dir = Directory::new();
    let options = dir.options(1, vec![1]);
    let mut peer = DurablePeer::open(options.clone()).unwrap();
    peer.campaign().unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    for value in 1u64..=64 {
        peer.propose(value.to_be_bytes().to_vec()).unwrap();
        peer.process_ready_with_snapshot(|_, _| Ok(()), |_, _| Ok(()))
            .unwrap();
        peer.checkpoint(value.to_be_bytes().to_vec()).unwrap();
        assert!(std::fs::metadata(&options.wal_path).unwrap().len() < 256);
    }
    drop(peer);
    let mut peer = DurablePeer::open(options.clone()).unwrap();
    let mut restored = 0;
    peer.process_ready_with_snapshot(
        |_, _| Ok(()),
        |_, bytes| {
            restored = u64::from_be_bytes(bytes.try_into().unwrap());
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(restored, 64);
    drop(peer);
    std::fs::remove_file(&options.wal_path).unwrap();
    assert!(DurablePeer::open(options.clone()).is_err());
    assert!(!options.wal_path.exists());
}

#[test]
fn lost_snapshots_and_acknowledgments_are_retried_without_reapplying_state() {
    for kind in [
        raft_core::eraftpb::MessageType::MsgSnapshot,
        raft_core::eraftpb::MessageType::MsgAppendResponse,
    ] {
        let dir = Directory::new();
        let mut peers = peers(&dir);
        let mut apps = BTreeMap::new();
        peers.get_mut(&1).unwrap().campaign().unwrap();
        pump_checkpoints(&mut peers, &mut apps, None);
        peers
            .get_mut(&1)
            .unwrap()
            .propose(32u64.to_be_bytes().to_vec())
            .unwrap();
        pump_checkpoints(&mut peers, &mut apps, Some(3));
        peers
            .get_mut(&1)
            .unwrap()
            .checkpoint(32u64.to_be_bytes().to_vec())
            .unwrap();
        let mut dropped = 0;
        for _ in 0..24 {
            peers.get_mut(&1).unwrap().tick().unwrap();
            dropped += pump_checkpoint_packets(&mut peers, &mut apps, None, Some(kind));
        }
        assert!(dropped > 0);
        if kind == raft_core::eraftpb::MessageType::MsgSnapshot {
            assert_eq!(apps[&3].value, 0);
        }
        for _ in 0..24 {
            peers.get_mut(&1).unwrap().tick().unwrap();
            pump_checkpoints(&mut peers, &mut apps, None);
        }
        assert_eq!(apps[&3].value, 32);
        assert_eq!(apps[&3].restored, 1);
        peers
            .get_mut(&1)
            .unwrap()
            .propose(33u64.to_be_bytes().to_vec())
            .unwrap();
        pump_checkpoints(&mut peers, &mut apps, None);
        for _ in 0..3 {
            peers.get_mut(&1).unwrap().tick().unwrap();
            pump_checkpoints(&mut peers, &mut apps, None);
        }
        assert_eq!(apps[&3].value, 33);
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct TestFragment {
    #[prost(uint64, tag = "1")]
    from: u64,
    #[prost(uint64, tag = "2")]
    to: u64,
    #[prost(uint64, tag = "3")]
    term: u64,
    #[prost(uint64, tag = "4")]
    index: u64,
    #[prost(uint64, tag = "5")]
    total: u64,
    #[prost(fixed64, tag = "6")]
    checksum: u64,
    #[prost(uint64, tag = "7")]
    offset: u64,
    #[prost(bytes = "vec", tag = "8")]
    data: Vec<u8>,
    #[prost(bool, tag = "9")]
    ack: bool,
}
#[derive(Default)]
struct LargeApp {
    bytes: Vec<u8>,
    restored: usize,
}
struct TransferFault {
    kind: u8,
    fired: bool,
    duplicate: bool,
    fragments: usize,
    zero_fragments: usize,
}
fn pump_large(
    dir: &Directory,
    peers: &mut BTreeMap<u64, DurablePeer>,
    apps: &mut BTreeMap<u64, LargeApp>,
    isolated: &[u64],
    fault: &mut TransferFault,
) {
    for _ in 0..500 {
        let mut packets = Vec::new();
        for (id, peer) in peers.iter_mut() {
            let app = std::cell::RefCell::new(apps.entry(*id).or_default());
            for packet in peer
                .process_ready_with_snapshot(
                    |_, bytes| {
                        app.borrow_mut().bytes.extend_from_slice(bytes);
                        Ok(())
                    },
                    |_, bytes| {
                        let mut app = app.borrow_mut();
                        app.bytes = bytes.to_vec();
                        app.restored += 1;
                        Ok(())
                    },
                )
                .unwrap()
            {
                assert!(packet.bytes.len() <= 8 * 1024 * 1024);
                if !isolated.contains(id) && !isolated.contains(&packet.to) {
                    packets.push(packet);
                }
            }
        }
        if packets.is_empty() {
            return;
        }
        for packet in packets {
            let envelope = TestPacket::decode(packet.bytes.as_slice()).unwrap();
            if envelope.version == 2 {
                let part = TestFragment::decode(envelope.message.as_slice()).unwrap();
                assert!(part.data.len() <= 1024 * 1024);
                if !part.ack {
                    fault.fragments += 1;
                    if part.offset == 0 {
                        fault.zero_fragments += 1;
                    }
                }
                if fault.kind == 7 && part.ack && part.from == 4 && part.offset == 2 * 1024 * 1024 {
                    fault.fired = true;
                    continue;
                }
                if fault.kind == 6 && part.ack && part.offset == part.total {
                    fault.fired = true;
                    continue;
                }
                let middle = !part.ack && part.offset == 2 * 1024 * 1024;
                if !fault.fired
                    && ((fault.kind == 1 && !part.ack && part.offset == 0)
                        || (fault.kind == 2 && middle)
                        || (fault.kind == 3 && part.ack && part.offset == part.total))
                {
                    fault.fired = true;
                    continue;
                }
                if !fault.fired && fault.kind == 4 && middle {
                    assert_eq!(packet.to, 3);
                    peers.remove(&3);
                    apps.remove(&3);
                    peers.insert(3, DurablePeer::open(dir.options(3, vec![1, 2, 3])).unwrap());
                    fault.fired = true;
                }
                if fault.duplicate {
                    peers
                        .get_mut(&packet.to)
                        .unwrap()
                        .step(&packet.bytes)
                        .unwrap();
                }
            } else if (fault.kind == 5 && !fault.fired) || fault.kind == 6 {
                let message =
                    raft_core::eraftpb::Message::decode(envelope.message.as_slice()).unwrap();
                if message.from == 3
                    && message.get_msg_type() == raft_core::eraftpb::MessageType::MsgAppendResponse
                    && apps.get(&3).is_some_and(|a| a.restored != 0)
                {
                    fault.fired = true;
                    continue;
                }
            }
            peers
                .get_mut(&packet.to)
                .unwrap()
                .step(&packet.bytes)
                .unwrap();
        }
    }
    panic!("large checkpoint exchange did not settle");
}

#[test]
fn checkpoint_quotas_pause_inline_and_framed_transfers_without_stalling_quorum_work() {
    use matrixraft::{ByteQuotaLimiter, RateLimiter};
    use std::sync::{Arc, Mutex};
    for (limit_send, limit_receive) in [(true, false), (false, true), (true, true)] {
        for length in [64, 2 * 1024 * 1024 + 17] {
            let dir = Directory::new();
            let mut peers = peers(&dir);
            let mut apps = BTreeMap::new();
            let mut fault = TransferFault {
                kind: 0,
                fired: false,
                duplicate: false,
                fragments: 0,
                zero_fragments: 0,
            };
            peers.get_mut(&1).unwrap().campaign().unwrap();
            pump_large(&dir, &mut peers, &mut apps, &[], &mut fault);
            let mut expected = vec![31; length];
            peers
                .get_mut(&1)
                .unwrap()
                .propose(expected.clone())
                .unwrap();
            pump_large(&dir, &mut peers, &mut apps, &[3], &mut fault);
            peers
                .get_mut(&1)
                .unwrap()
                .checkpoint(expected.clone())
                .unwrap();
            let send = Arc::new(Mutex::new(ByteQuotaLimiter::with_available(1024 * 1024, 0)));
            let receive = Arc::new(Mutex::new(ByteQuotaLimiter::with_available(1024 * 1024, 0)));
            peers
                .get_mut(&1)
                .unwrap()
                .set_checkpoint_transfer_limiters(limit_send.then(|| Arc::clone(&send)), None)
                .unwrap();
            peers
                .get_mut(&3)
                .unwrap()
                .set_checkpoint_transfer_limiters(None, limit_receive.then(|| Arc::clone(&receive)))
                .unwrap();
            for _ in 0..250 {
                for peer in peers.values_mut() {
                    peer.tick().unwrap();
                }
                pump_large(&dir, &mut peers, &mut apps, &[], &mut fault);
            }
            assert!(peers[&1].is_leader());
            assert_eq!(apps[&3].restored, 0);
            assert!(apps[&3].bytes.is_empty());
            assert_eq!(send.lock().unwrap().available_bytes(), 0);
            assert_eq!(receive.lock().unwrap().available_bytes(), 0);
            if limit_send {
                assert_eq!(
                    fault.fragments, 0,
                    "no checkpoint payload without send quota"
                );
            }
            let read = peers.get_mut(&1).unwrap().begin_read().unwrap();
            pump_large(&dir, &mut peers, &mut apps, &[], &mut fault);
            assert!(peers.get_mut(&1).unwrap().poll_read(read).unwrap());
            peers
                .get_mut(&1)
                .unwrap()
                .propose(b"suffix".to_vec())
                .unwrap();
            expected.extend_from_slice(b"suffix");
            pump_large(&dir, &mut peers, &mut apps, &[], &mut fault);
            assert_eq!(apps[&1].bytes, expected);
            assert_eq!(apps[&2].bytes, expected);
            assert!(apps[&3].bytes.is_empty());
            for _ in 0..60 {
                send.lock().unwrap().refill_bytes(1024 * 1024);
                receive.lock().unwrap().refill_bytes(1024 * 1024);
                for peer in peers.values_mut() {
                    peer.tick().unwrap();
                }
                pump_large(&dir, &mut peers, &mut apps, &[], &mut fault);
                if apps[&3].bytes == expected {
                    break;
                }
            }
            assert_eq!(apps[&3].restored, 1, "send={limit_send} receive={limit_receive} length={length} fragments={} applied={} commit={} leader={:?}", fault.fragments, peers[&3].applied_index(), peers[&3].commit_index(), peers[&1].leader_id());
            for id in 1..=3 {
                assert_eq!(apps[&id].bytes, expected);
            }
            drop(peers);
            apps.clear();
            let mut peers = self::peers(&dir);
            pump_large(&dir, &mut peers, &mut apps, &[], &mut fault);
            for id in 1..=3 {
                assert_eq!(apps[&id].bytes, expected);
            }
        }
    }
}

#[test]
fn paused_checkpoint_quota_cannot_release_payload_after_leadership_changes() {
    use matrixraft::{ByteQuotaLimiter, RateLimiter};
    use std::sync::{Arc, Mutex};
    let dir = Directory::new();
    let mut peers = peers(&dir);
    let mut apps = BTreeMap::new();
    let mut fault = TransferFault {
        kind: 0,
        fired: false,
        duplicate: false,
        fragments: 0,
        zero_fragments: 0,
    };
    peers.get_mut(&1).unwrap().campaign().unwrap();
    pump_large(&dir, &mut peers, &mut apps, &[], &mut fault);
    let expected = vec![7; 64];
    peers
        .get_mut(&1)
        .unwrap()
        .propose(expected.clone())
        .unwrap();
    pump_large(&dir, &mut peers, &mut apps, &[3], &mut fault);
    peers
        .get_mut(&1)
        .unwrap()
        .checkpoint(expected.clone())
        .unwrap();
    let shared = Arc::new(Mutex::new(ByteQuotaLimiter::with_available(1024 * 1024, 0)));
    peers
        .get_mut(&1)
        .unwrap()
        .set_checkpoint_transfer_limiters(Some(Arc::clone(&shared)), None)
        .unwrap();
    for _ in 0..4 {
        peers.get_mut(&1).unwrap().tick().unwrap();
        pump_large(&dir, &mut peers, &mut apps, &[], &mut fault);
    }
    assert!(apps[&3].bytes.is_empty());
    peers.get_mut(&2).unwrap().force_campaign().unwrap();
    pump_large(&dir, &mut peers, &mut apps, &[], &mut fault);
    assert!(peers[&2].is_leader());
    shared.lock().unwrap().refill_bytes(1024 * 1024);
    // Do not tick the old leader: Ready processing itself must discard its
    // obsolete transfer before a newly available quota can release old bytes.
    pump_large(&dir, &mut peers, &mut apps, &[], &mut fault);
    assert_eq!(shared.lock().unwrap().available_bytes(), 1024 * 1024);
    for id in 1..=3 {
        assert_eq!(apps[&id].bytes, expected);
    }
}

#[test]
fn streamed_checkpoints_survive_packet_loss_duplicates_and_receiver_restart() {
    for kind in 0..=6 {
        let dir = Directory::new();
        let mut peers = peers(&dir);
        let mut apps = BTreeMap::new();
        let mut fault = TransferFault {
            kind: 0,
            fired: false,
            duplicate: false,
            fragments: 0,
            zero_fragments: 0,
        };
        peers.get_mut(&1).unwrap().campaign().unwrap();
        pump_large(&dir, &mut peers, &mut apps, &[], &mut fault);
        let mut expected = Vec::new();
        for value in 1..=9u8 {
            let data = vec![value; 1024 * 1024];
            expected.extend_from_slice(&data);
            peers.get_mut(&1).unwrap().propose(data).unwrap();
            pump_large(&dir, &mut peers, &mut apps, &[3], &mut fault);
        }
        assert_eq!(apps[&1].bytes, expected);
        assert!(apps[&3].bytes.is_empty());
        peers
            .get_mut(&1)
            .unwrap()
            .checkpoint(expected.clone())
            .unwrap();
        fault.kind = kind;
        fault.duplicate = true;
        for tick in 0..35 {
            if kind == 6 && tick == 24 {
                fault.kind = 0;
            }
            peers.get_mut(&1).unwrap().tick().unwrap();
            pump_large(&dir, &mut peers, &mut apps, &[], &mut fault);
        }
        assert_eq!(apps[&3].bytes, expected, "scenario {kind}");
        assert_eq!(apps[&3].restored, 1, "scenario {kind}");
        assert!(fault.fragments >= 10);
        assert!(kind == 0 || fault.fired, "fault not exercised: {kind}");
        peers
            .get_mut(&1)
            .unwrap()
            .propose(b"suffix".to_vec())
            .unwrap();
        pump_large(&dir, &mut peers, &mut apps, &[], &mut fault);
        for _ in 0..3 {
            peers.get_mut(&1).unwrap().tick().unwrap();
            pump_large(&dir, &mut peers, &mut apps, &[], &mut fault);
        }
        expected.extend_from_slice(b"suffix");
        drop(peers);
        apps.clear();
        let mut peers = self::peers(&dir);
        pump_large(&dir, &mut peers, &mut apps, &[], &mut fault);
        for id in 1..=3 {
            assert_eq!(apps[&id].bytes, expected, "scenario {kind} voter {id}");
        }
    }
}

#[test]
fn segmented_checkpoint_recovery_refuses_missing_records_and_corruption() {
    let dir = Directory::new();
    let options = dir.options(1, vec![1]);
    let mut peer = DurablePeer::open(options.clone()).unwrap();
    peer.campaign().unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    let data = vec![77; 9 * 1024 * 1024];
    peer.checkpoint(data.clone()).unwrap();
    drop(peer);
    let complete = std::fs::read(&options.wal_path).unwrap();
    let mut boundaries = vec![0];
    let mut pos = 0;
    while pos < complete.len() {
        let len = u32::from_be_bytes(complete[pos + 4..pos + 8].try_into().unwrap()) as usize;
        assert!(len <= 8 * 1024 * 1024);
        pos += 24 + len;
        boundaries.push(pos);
    }
    assert!(boundaries.len() > 9);
    for boundary in boundaries {
        for cut in [
            boundary.saturating_sub(1),
            boundary,
            (boundary + 1).min(complete.len()),
        ] {
            if cut == complete.len() {
                continue;
            }
            std::fs::write(&options.wal_path, &complete[..cut]).unwrap();
            assert!(DurablePeer::open(options.clone()).is_err(), "cut {cut}");
            assert_eq!(std::fs::read(&options.wal_path).unwrap(), complete[..cut]);
        }
    }
    for offset in [24, 1024 * 1024 + 100, complete.len() - 1] {
        let mut damaged = complete.clone();
        damaged[offset] ^= 1;
        std::fs::write(&options.wal_path, &damaged).unwrap();
        assert!(DurablePeer::open(options.clone()).is_err());
        assert_eq!(std::fs::read(&options.wal_path).unwrap(), damaged);
    }
    std::fs::write(&options.wal_path, &complete).unwrap();
    let mut peer = DurablePeer::open(options).unwrap();
    let mut restored = false;
    peer.process_ready_with_snapshot(
        |_, _| Ok(()),
        |_, bytes| {
            assert_eq!(bytes, data);
            restored = true;
            Ok(())
        },
    )
    .unwrap();
    assert!(restored);
}

#[test]
fn streamed_checkpoint_slot_serves_multiple_lagging_voters() {
    let dir = Directory::new();
    let voters = vec![1, 2, 3, 4, 5];
    let mut peers: BTreeMap<_, _> = voters
        .iter()
        .map(|id| {
            (
                *id,
                DurablePeer::open(dir.options(*id, voters.clone())).unwrap(),
            )
        })
        .collect();
    let mut apps = BTreeMap::new();
    let mut fault = TransferFault {
        kind: 0,
        fired: false,
        duplicate: false,
        fragments: 0,
        zero_fragments: 0,
    };
    peers.get_mut(&1).unwrap().campaign().unwrap();
    pump_large(&dir, &mut peers, &mut apps, &[], &mut fault);
    let mut expected = Vec::new();
    for value in 1..=9u8 {
        let data = vec![value; 1024 * 1024];
        expected.extend_from_slice(&data);
        peers.get_mut(&1).unwrap().propose(data).unwrap();
        pump_large(&dir, &mut peers, &mut apps, &[4, 5], &mut fault);
    }
    peers
        .get_mut(&1)
        .unwrap()
        .checkpoint(expected.clone())
        .unwrap();
    for _ in 0..150 {
        peers.get_mut(&1).unwrap().tick().unwrap();
        pump_large(&dir, &mut peers, &mut apps, &[4], &mut fault);
    }
    assert_eq!(
        apps[&5].bytes, expected,
        "an unavailable voter must not block another repair"
    );
    assert_eq!(apps[&5].restored, 1);
    assert!(apps[&4].bytes.is_empty());
    for _ in 0..35 {
        peers.get_mut(&1).unwrap().tick().unwrap();
        pump_large(&dir, &mut peers, &mut apps, &[], &mut fault);
    }
    for id in [4, 5] {
        assert_eq!(apps[&id].bytes, expected);
        assert_eq!(apps[&id].restored, 1);
    }
    assert!(fault.fragments >= 20);
}

#[test]
fn parallel_checkpoint_sends_isolate_a_stalled_voter_and_resume_its_staged_prefix() {
    let dir = Directory::new();
    let voters = vec![1, 2, 3, 4, 5];
    let mut peers: BTreeMap<_, _> = voters
        .iter()
        .map(|id| {
            (
                *id,
                DurablePeer::open_with_tuning(
                    dir.options(*id, voters.clone()),
                    PeerTuning {
                        max_checkpoint_sends: Some(2),
                        ..PeerTuning::default()
                    },
                )
                .unwrap(),
            )
        })
        .collect();
    let mut apps = BTreeMap::new();
    let mut fault = TransferFault {
        kind: 7,
        fired: false,
        duplicate: false,
        fragments: 0,
        zero_fragments: 0,
    };
    peers.get_mut(&1).unwrap().campaign().unwrap();
    pump_large(&dir, &mut peers, &mut apps, &[], &mut fault);
    let mut expected = Vec::new();
    for value in 1..=3u8 {
        let bytes = vec![value; 1024 * 1024];
        peers.get_mut(&1).unwrap().propose(bytes.clone()).unwrap();
        expected.extend_from_slice(&bytes);
        pump_large(&dir, &mut peers, &mut apps, &[4, 5], &mut fault);
    }
    peers
        .get_mut(&1)
        .unwrap()
        .checkpoint(expected.clone())
        .unwrap();
    for _ in 0..10 {
        for peer in peers.values_mut() {
            peer.tick().unwrap();
        }
        pump_large(&dir, &mut peers, &mut apps, &[], &mut fault);
    }
    assert!(fault.fired);
    assert_eq!(apps[&4].restored, 0);
    assert!(apps[&4].bytes.is_empty());
    assert_eq!(
        apps[&5].restored, 1,
        "the other active sender must not wait for voter 4"
    );
    assert_eq!(apps[&5].bytes, expected);
    peers
        .get_mut(&1)
        .unwrap()
        .propose(b"suffix".to_vec())
        .unwrap();
    expected.extend_from_slice(b"suffix");
    let read = peers.get_mut(&1).unwrap().begin_read().unwrap();
    pump_large(&dir, &mut peers, &mut apps, &[], &mut fault);
    assert!(peers.get_mut(&1).unwrap().poll_read(read).unwrap());
    for _ in 0..115 {
        for peer in peers.values_mut() {
            peer.tick().unwrap();
        }
        pump_large(&dir, &mut peers, &mut apps, &[], &mut fault);
    }
    assert!(
        fault.zero_fragments >= 3,
        "the expired send must have retried from zero"
    );
    assert_eq!(apps[&4].restored, 0);
    fault.kind = 0;
    for _ in 0..30 {
        for peer in peers.values_mut() {
            peer.tick().unwrap();
        }
        pump_large(&dir, &mut peers, &mut apps, &[], &mut fault);
        if apps[&4].bytes == expected {
            break;
        }
    }
    assert_eq!(apps[&4].restored, 1);
    assert_eq!(apps[&5].restored, 1);
    for id in 1..=5 {
        assert_eq!(apps[&id].bytes, expected);
    }
    drop(peers);
    apps.clear();
    let mut peers: BTreeMap<_, _> = voters
        .iter()
        .map(|id| {
            (
                *id,
                DurablePeer::open(dir.options(*id, voters.clone())).unwrap(),
            )
        })
        .collect();
    pump_large(&dir, &mut peers, &mut apps, &[], &mut fault);
    for id in 1..=5 {
        assert_eq!(apps[&id].bytes, expected);
    }
}
