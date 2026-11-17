// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#![cfg(feature = "peer-runtime")]
use matrixraft::{
    peer_runtime::{
        ApplyDecision, DurablePeer, MembershipChange, PeerOptions, PeerTuning, RestoreDecision,
    },
    RaftError,
};
use prost::Message as _;
use std::{cell::RefCell, collections::BTreeMap};

#[derive(Clone, PartialEq, prost::Message)]
struct ApplyPacket {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, tag = "2")]
    group: u64,
    #[prost(bytes = "vec", tag = "3")]
    message: Vec<u8>,
}

struct AsyncCluster {
    root: tempfile::TempDir,
    peers: BTreeMap<u64, DurablePeer>,
    values: BTreeMap<u64, Vec<u8>>,
    tasks: BTreeMap<u64, std::collections::VecDeque<matrixraft::peer_runtime::PeerApplyTask>>,
    restores: BTreeMap<u64, usize>,
    tuning: PeerTuning,
    hold_ack_above: Option<u64>,
    held_packets: Vec<matrixraft::peer_runtime::PeerPacket>,
}
impl AsyncCluster {
    fn new(capacity: usize, bytes: u64, cache: Option<u64>) -> Self {
        let mut c = Self {
            root: tempfile::tempdir().unwrap(),
            peers: BTreeMap::new(),
            values: BTreeMap::new(),
            tasks: BTreeMap::new(),
            restores: BTreeMap::new(),
            tuning: PeerTuning {
                max_inflight_apply_tasks: Some(capacity),
                max_apply_bytes: Some(bytes),
                max_log_cache_bytes: cache,
                ..Default::default()
            },
            hold_ack_above: None,
            held_packets: Vec::new(),
        };
        for id in 1..=3 {
            c.open(id);
        }
        c.drive(None);
        c.peers.get_mut(&1).unwrap().campaign().unwrap();
        c.drive(None);
        assert!(c.peers[&1].is_leader());
        c
    }
    fn options(&self, id: u64) -> PeerOptions {
        PeerOptions {
            group_id: 91,
            node_id: id,
            voters: vec![1, 2, 3],
            wal_path: self.root.path().join(format!("async-{id}.wal")),
        }
    }
    fn open(&mut self, id: u64) {
        self.peers.insert(
            id,
            DurablePeer::open_with_membership_and_tuning(
                self.options(id),
                vec![1, 2, 3, 4],
                self.tuning,
            )
            .unwrap(),
        );
        self.values.insert(id, Vec::new());
        self.tasks.insert(id, Default::default());
    }
    fn drive(&mut self, blocked: Option<u64>) {
        for _ in 0..64 {
            let mut packets = Vec::new();
            for (&id, peer) in &mut self.peers {
                let values = self.values.entry(id).or_default();
                let restores = self.restores.entry(id).or_default();
                let outgoing = peer
                    .process_ready_async_apply(|_, reader, _| {
                        let mut data = Vec::new();
                        reader
                            .read_to_end(&mut data)
                            .map_err(|error| RaftError::Storage(error.to_string()))?;
                        *values = data;
                        *restores += 1;
                        Ok(RestoreDecision::Restored)
                    })
                    .unwrap();
                for packet in outgoing {
                    let hold = self.hold_ack_above.is_some_and(|index| {
                        let envelope = ApplyPacket::decode(packet.bytes.as_slice()).unwrap();
                        if envelope.version != 1 {
                            return false;
                        }
                        let message =
                            raft_core::eraftpb::Message::decode(envelope.message.as_slice())
                                .unwrap();
                        packet.to == 1
                            && message.get_msg_type()
                                == raft_core::eraftpb::MessageType::MsgAppendResponse
                            && !message.reject
                            && message.index > index
                    });
                    if hold {
                        self.held_packets.push(packet);
                    } else {
                        packets.push(packet);
                    }
                }
                let tasks = self.tasks.entry(id).or_default();
                tasks.extend(peer.take_application_tasks().unwrap());
                if blocked != Some(id) {
                    while let Some(task) = tasks.pop_front() {
                        for entry in task.entries().iter() {
                            values.extend_from_slice(entry.payload);
                        }
                        peer.complete_application_task(task.finish()).unwrap();
                    }
                }
            }
            for packet in packets {
                self.peers
                    .get_mut(&packet.to)
                    .unwrap()
                    .step(&packet.bytes)
                    .unwrap();
            }
        }
    }
    fn tick(&mut self, blocked: Option<u64>) {
        for peer in self.peers.values_mut() {
            peer.tick().unwrap();
        }
        self.drive(blocked);
    }
    fn propose(&mut self, leader: u64, values: std::ops::RangeInclusive<u8>) {
        self.peers
            .get_mut(&leader)
            .unwrap()
            .propose_batch(values.map(|value| vec![value]).collect())
            .unwrap();
    }
}

#[test]
fn asynchronous_application_is_bounded_and_completions_advance_only_a_prefix() {
    for cache in [None, Some(512)] {
        let mut c = AsyncCluster::new(3, 1, cache);
        let base = c.peers[&1].applied_index();
        c.propose(1, 1..=9);
        c.drive(Some(1));
        assert_eq!(c.tasks[&1].len(), 3);
        assert_eq!(c.peers[&1].applied_index(), base);
        assert!(c.peers[&1].commit_index() >= base + 9);
        let token = c.peers.get_mut(&1).unwrap().begin_read().unwrap();
        c.drive(Some(1));
        assert!(!c.peers.get_mut(&1).unwrap().poll_read(token).unwrap());
        assert!(!c.peers[&1].has_ready());
        assert!(c.peers[&1].prepare_checkpoint().is_err());
        let mut completions = Vec::new();
        for task in c.tasks.get_mut(&1).unwrap().drain(..) {
            for entry in task.entries().iter() {
                c.values
                    .get_mut(&1)
                    .unwrap()
                    .extend_from_slice(entry.payload);
            }
            completions.push(task.finish());
        }
        let first = completions.remove(0);
        for completion in completions.into_iter().rev() {
            c.peers
                .get_mut(&1)
                .unwrap()
                .complete_application_task(completion)
                .unwrap();
            assert_eq!(c.peers[&1].applied_index(), base);
        }
        c.peers
            .get_mut(&1)
            .unwrap()
            .complete_application_task(first)
            .unwrap();
        assert_eq!(c.peers[&1].applied_index(), base + 3);
        c.drive(None);
        assert!(c.peers.get_mut(&1).unwrap().poll_read(token).unwrap());
        for id in 1..=3 {
            assert_eq!(c.values[&id], (1..=9).collect::<Vec<_>>());
            assert_eq!(c.peers[&id].applied_index(), c.peers[&id].commit_index());
        }
    }
}

#[test]
fn rejected_and_abandoned_tasks_wait_for_workers_before_checkpoint_restore() {
    for file in [false, true] {
        let mut c = AsyncCluster::new(3, 1, Some(512));
        c.propose(1, 1..=6);
        c.drive(Some(2));
        assert_eq!(c.tasks[&2].len(), 3);
        let bytes = c.values[&1].clone();
        if file {
            c.peers
                .get_mut(&1)
                .unwrap()
                .checkpoint_from_reader(&mut std::io::Cursor::new(&bytes), bytes.len() as u64)
                .unwrap();
        } else {
            c.peers
                .get_mut(&1)
                .unwrap()
                .checkpoint(bytes.clone())
                .unwrap();
        }
        let task = c.tasks.get_mut(&2).unwrap().pop_front().unwrap();
        if file {
            drop(task);
        } else {
            c.peers
                .get_mut(&2)
                .unwrap()
                .complete_application_task(task.reject_after(0).unwrap())
                .unwrap();
        }
        for _ in 0..20 {
            c.tick(Some(2));
            if c.peers[&2]
                .capture_log_reader()
                .unwrap()
                .entries(1, 2, None)
                .is_err()
            {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(c.peers[&2]
            .capture_log_reader()
            .unwrap()
            .entries(1, 2, None)
            .is_err());
        assert_eq!(*c.restores.get(&2).unwrap_or(&0), 0);
        assert!(c.values[&2].is_empty());
        assert!(c.peers[&2].required_application_checkpoint().is_some());
        c.tasks.get_mut(&2).unwrap().clear();
        for _ in 0..40 {
            c.tick(None);
            if c.restores.get(&2).is_some_and(|count| *count > 0) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert_eq!(c.peers[&2].required_application_checkpoint(), None);
        assert_eq!(c.values[&2], bytes);
        c.propose(1, 7..=8);
        c.drive(None);
        assert_eq!(c.values[&2], (1..=8).collect::<Vec<_>>());
    }
}

#[test]
fn a_rejected_leader_hands_off_and_repairs_its_partial_application() {
    let mut c = AsyncCluster::new(3, 128, Some(512));
    let base = c.peers[&1].applied_index();
    c.propose(1, 1..=6);
    c.drive(Some(1));
    let task = c.tasks.get_mut(&1).unwrap().pop_front().unwrap();
    assert!(task.entries().len() > 2);
    for entry in task.entries().iter().take(2) {
        c.values
            .get_mut(&1)
            .unwrap()
            .extend_from_slice(entry.payload);
    }
    c.peers
        .get_mut(&1)
        .unwrap()
        .complete_application_task(task.reject_after(2).unwrap())
        .unwrap();
    assert_eq!(c.peers[&1].applied_index(), base + 2);
    assert_eq!(
        c.peers[&1].required_application_checkpoint(),
        Some(base + 3)
    );
    assert!(!c.peers[&1].is_leader());
    assert!(c.peers.get_mut(&1).unwrap().campaign().is_err());
    assert!(c.peers.get_mut(&1).unwrap().force_campaign().is_err());
    assert!(c.peers.get_mut(&1).unwrap().propose(vec![99]).is_err());
    let mut leader = None;
    for _ in 0..40 {
        c.tick(Some(1));
        leader = [2, 3].into_iter().find(|id| c.peers[id].is_leader());
        if leader.is_some() {
            break;
        }
    }
    let leader = leader.unwrap();
    for _ in 0..20 {
        c.tick(Some(1));
        if c.peers[&leader].snapshot_wanted() {
            break;
        }
    }
    assert!(c.peers[&leader].snapshot_wanted());
    c.tasks.get_mut(&1).unwrap().clear();
    c.peers
        .get_mut(&leader)
        .unwrap()
        .checkpoint(c.values[&leader].clone())
        .unwrap();
    for _ in 0..20 {
        c.tick(None);
        if c.peers[&1].required_application_checkpoint().is_none() {
            break;
        }
    }
    assert_eq!(c.peers[&1].required_application_checkpoint(), None);
    assert_eq!(c.values[&1], (1..=6).collect::<Vec<_>>());
    c.propose(leader, 7..=9);
    c.drive(None);
    for id in 1..=3 {
        assert_eq!(c.values[&id], (1..=9).collect::<Vec<_>>());
    }
}

#[test]
fn asynchronous_graceful_shutdown_drains_its_committed_fence() {
    let mut c = AsyncCluster::new(3, 1, None);
    c.propose(1, 1..=9);
    c.drive(Some(1));
    let fence = c.peers.get_mut(&1).unwrap().begin_shutdown().unwrap();
    assert!(!c.peers.get_mut(&1).unwrap().poll_shutdown().unwrap());
    for _ in 0..40 {
        let peer = c.peers.get_mut(&1).unwrap();
        if peer.poll_shutdown().unwrap() {
            break;
        }
        assert!(peer
            .process_ready_async_apply(|_, _, _| panic!("no checkpoint pending"))
            .unwrap()
            .is_empty());
        c.tasks
            .get_mut(&1)
            .unwrap()
            .extend(peer.take_application_tasks().unwrap());
        while let Some(task) = c.tasks.get_mut(&1).unwrap().pop_front() {
            for entry in task.entries().iter() {
                c.values
                    .get_mut(&1)
                    .unwrap()
                    .extend_from_slice(entry.payload);
            }
            peer.complete_application_task(task.finish()).unwrap();
        }
    }
    assert!(c.peers.get_mut(&1).unwrap().poll_shutdown().unwrap());
    assert_eq!(c.peers[&1].applied_index(), fence);
    assert_eq!(c.values[&1], (1..=9).collect::<Vec<_>>());
    c.peers.remove(&1);
    c.open(1);
    c.drive(None);
    assert_eq!(c.values[&1], (1..=9).collect::<Vec<_>>());
}

#[test]
fn asynchronous_task_handles_keep_storage_locked_and_reject_other_peers() {
    let mut c = AsyncCluster::new(2, 1, Some(512));
    c.propose(1, 1..=4);
    c.drive(Some(2));
    let task = c.tasks.get_mut(&2).unwrap().pop_front().unwrap();
    assert!(c
        .peers
        .get_mut(&3)
        .unwrap()
        .complete_application_task(task.finish())
        .is_err());
    assert!(c.peers[&2].required_application_checkpoint().is_some());
    c.peers.get_mut(&2).unwrap().stop();
    c.peers.remove(&2);
    assert!(
        DurablePeer::open_with_membership_and_tuning(c.options(2), vec![1, 2, 3, 4], c.tuning)
            .is_err()
    );
    c.tasks.get_mut(&2).unwrap().clear();
    c.open(2);
    c.drive(None);
    assert_eq!(c.values[&2], (1..=4).collect::<Vec<_>>());
}

#[test]
fn asynchronous_application_validates_before_storage_and_excludes_sync_callbacks() {
    let dir = tempfile::tempdir().unwrap();
    let options = PeerOptions {
        group_id: 91,
        node_id: 1,
        voters: vec![1],
        wal_path: dir.path().join("peer.wal"),
    };
    for capacity in [0, 65] {
        assert!(DurablePeer::open_with_tuning(
            options.clone(),
            PeerTuning {
                max_inflight_apply_tasks: Some(capacity),
                ..Default::default()
            }
        )
        .is_err());
        assert!(!options.wal_path.exists());
        assert!(!options.wal_path.with_extension("runtime.lock").exists());
    }
    let mut peer = DurablePeer::open_with_tuning(
        options,
        PeerTuning {
            max_inflight_apply_tasks: Some(2),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(peer
        .process_ready(|_, _| panic!("synchronous callback must not run"))
        .is_err());
    peer.process_ready_async_apply(|_, _, _| panic!("no checkpoint pending"))
        .unwrap();
    peer.campaign().unwrap();
    peer.process_ready_async_apply(|_, _, _| panic!("no checkpoint pending"))
        .unwrap();
    for task in peer.take_application_tasks().unwrap() {
        peer.complete_application_task(task.finish()).unwrap();
    }
    assert_eq!(peer.applied_index(), peer.commit_index());
}

#[test]
fn same_position_application_repairs_obey_the_inline_receive_quota() {
    use matrixraft::{ByteQuotaLimiter, RateLimiter};
    let mut c = AsyncCluster::new(2, 1, Some(512));
    c.propose(1, 1..=3);
    c.drive(Some(2));
    c.peers
        .get_mut(&1)
        .unwrap()
        .checkpoint(c.values[&1].clone())
        .unwrap();
    let quota = std::sync::Arc::new(std::sync::Mutex::new(ByteQuotaLimiter::with_available(
        1024 * 1024,
        0,
    )));
    c.peers
        .get_mut(&2)
        .unwrap()
        .set_checkpoint_transfer_limiters(None, Some(std::sync::Arc::clone(&quota)))
        .unwrap();
    let task = c.tasks.get_mut(&2).unwrap().pop_front().unwrap();
    c.peers
        .get_mut(&2)
        .unwrap()
        .complete_application_task(task.reject_after(0).unwrap())
        .unwrap();
    c.tasks.get_mut(&2).unwrap().clear();
    for _ in 0..3 {
        c.tick(None);
    }
    assert!(c.peers[&2].required_application_checkpoint().is_some());
    assert_eq!(*c.restores.get(&2).unwrap_or(&0), 0);
    assert!(c.peers[&2]
        .capture_log_reader()
        .unwrap()
        .entries(1, 2, None)
        .is_ok());
    quota.lock().unwrap().refill_bytes(1024 * 1024);
    for _ in 0..20 {
        c.tick(None);
        if c.peers[&2].required_application_checkpoint().is_none() {
            break;
        }
    }
    assert_eq!(c.peers[&2].required_application_checkpoint(), None);
    assert!(quota.lock().unwrap().available_bytes() < 1024 * 1024);
    assert_eq!(c.values[&2], vec![1, 2, 3]);
}

#[test]
fn a_healthy_leader_repairs_two_rejected_application_followers() {
    let mut c = AsyncCluster::new(2, 1, None);
    c.propose(1, 1..=4);
    c.drive(Some(2));
    let task = c.tasks.get_mut(&2).unwrap().pop_front().unwrap();
    c.peers
        .get_mut(&2)
        .unwrap()
        .complete_application_task(task.reject_after(0).unwrap())
        .unwrap();
    c.tasks.get_mut(&2).unwrap().clear();
    c.drive(Some(2));
    c.propose(1, 5..=5);
    c.drive(Some(3));
    let task = c.tasks.get_mut(&3).unwrap().pop_front().unwrap();
    c.peers
        .get_mut(&3)
        .unwrap()
        .complete_application_task(task.reject_after(0).unwrap())
        .unwrap();
    c.tasks.get_mut(&3).unwrap().clear();
    assert!(c.peers[&2].required_application_checkpoint().is_some());
    assert!(c.peers[&3].required_application_checkpoint().is_some());
    c.drive(None);
    assert!(c.peers[&1].snapshot_wanted());
    c.peers
        .get_mut(&1)
        .unwrap()
        .checkpoint(c.values[&1].clone())
        .unwrap();
    for _ in 0..30 {
        c.tick(None);
        if [2, 3]
            .into_iter()
            .all(|id| c.peers[&id].required_application_checkpoint().is_none())
        {
            break;
        }
    }
    for id in [2, 3] {
        assert_eq!(c.peers[&id].required_application_checkpoint(), None);
        assert_eq!(c.values[&id], (1..=5).collect::<Vec<_>>());
    }
    c.propose(1, 6..=7);
    c.drive(None);
    for id in 1..=3 {
        assert_eq!(c.values[&id], (1..=7).collect::<Vec<_>>());
    }
}

#[test]
fn rejecting_application_during_shutdown_cannot_claim_a_completed_drain() {
    let mut c = AsyncCluster::new(2, 1, None);
    c.propose(1, 1..=4);
    c.drive(Some(1));
    c.peers.get_mut(&1).unwrap().begin_shutdown().unwrap();
    let task = c.tasks.get_mut(&1).unwrap().pop_front().unwrap();
    assert!(c
        .peers
        .get_mut(&1)
        .unwrap()
        .complete_application_task(task.reject_after(0).unwrap())
        .is_err());
    assert!(c.peers.get_mut(&1).unwrap().poll_shutdown().is_err());
    c.peers.get_mut(&1).unwrap().stop();
    c.tasks.get_mut(&1).unwrap().clear();
    c.peers.remove(&1);
    c.open(1);
    c.drive(None);
    assert_eq!(c.values[&1], (1..=4).collect::<Vec<_>>());
}

#[test]
fn old_repair_requests_cannot_block_a_new_terms_real_quorum() {
    let mut c = AsyncCluster::new(2, 1, None);
    c.propose(1, 1..=4);
    c.drive(Some(2));
    let mut second_tasks = std::mem::take(c.tasks.get_mut(&2).unwrap());
    c.propose(1, 5..=5);
    c.drive(Some(3));
    let committed = c.peers[&1].commit_index();
    let old_term = c.peers[&1].term();
    c.hold_ack_above = Some(committed);
    c.propose(1, 99..=99);
    c.drive(Some(3));
    assert_eq!(c.peers[&1].commit_index(), committed);
    assert!(!c.held_packets.is_empty());
    let task = second_tasks.pop_front().unwrap();
    c.peers
        .get_mut(&2)
        .unwrap()
        .complete_application_task(task.reject_after(0).unwrap())
        .unwrap();
    drop(second_tasks);
    let task = c.tasks.get_mut(&3).unwrap().pop_front().unwrap();
    c.peers
        .get_mut(&3)
        .unwrap()
        .complete_application_task(task.reject_after(0).unwrap())
        .unwrap();
    c.tasks.get_mut(&3).unwrap().clear();
    c.drive(None);
    assert!(c.peers[&2].required_application_checkpoint().is_some());
    assert!(c.peers[&3].required_application_checkpoint().is_some());
    c.peers.get_mut(&1).unwrap().stop();
    c.peers.remove(&1);
    c.held_packets.clear();
    c.hold_ack_above = None;
    c.open(1);
    {
        let peer = c.peers.get_mut(&1).unwrap();
        for _ in 0..40 {
            assert!(peer
                .process_ready_async_apply(|_, _, _| panic!("no checkpoint pending"))
                .unwrap()
                .is_empty());
            for task in peer.take_application_tasks().unwrap() {
                for entry in task.entries().iter() {
                    c.values
                        .get_mut(&1)
                        .unwrap()
                        .extend_from_slice(entry.payload);
                }
                peer.complete_application_task(task.finish()).unwrap();
            }
            if !peer.has_ready() {
                break;
            }
        }
        assert_eq!(peer.commit_index(), committed);
    }
    assert_eq!(c.values[&1], (1..=5).collect::<Vec<_>>());
    c.peers.get_mut(&1).unwrap().force_campaign().unwrap();
    for _ in 0..30 {
        c.tick(None);
        if c.peers[&1].commit_index() > committed {
            break;
        }
    }
    assert!(c.peers[&1].is_leader());
    assert!(c.peers[&1].term() > old_term);
    assert!(c.peers[&1].commit_index() > committed);
    let expected: Vec<_> = (1..=5).chain(std::iter::once(99)).collect();
    assert_eq!(c.values[&1], expected);
    c.peers
        .get_mut(&1)
        .unwrap()
        .checkpoint(c.values[&1].clone())
        .unwrap();
    for _ in 0..30 {
        c.tick(None);
        if [2, 3]
            .into_iter()
            .all(|id| c.peers[&id].required_application_checkpoint().is_none())
        {
            break;
        }
    }
    for id in 1..=3 {
        assert_eq!(c.peers[&id].required_application_checkpoint(), None);
        assert_eq!(c.values[&id], expected);
    }
}

struct Cluster {
    root: tempfile::TempDir,
    peers: BTreeMap<u64, DurablePeer>,
    values: BTreeMap<u64, Vec<u8>>,
    restores: BTreeMap<u64, usize>,
}

#[test]
fn asynchronous_membership_progress_cannot_publish_ahead_of_application_state() {
    let mut c = AsyncCluster::new(3, 1, Some(512));
    let base = c.peers[&1].applied_index();
    c.propose(1, 1..=9);
    c.drive(Some(1));
    c.open(4);
    c.peers
        .get_mut(&1)
        .unwrap()
        .propose_membership(MembershipChange::AddLearner(4))
        .unwrap();
    c.drive(Some(1));
    assert_eq!(c.peers[&1].membership().learners, vec![4]);
    assert_eq!(c.peers[&1].applied_index(), base);
    assert!(c.peers.get_mut(&1).unwrap().checkpoint(Vec::new()).is_err());
    assert!(c.peers[&1].prepare_checkpoint().is_err());
    assert!(c.peers.get_mut(&4).unwrap().campaign().is_err());
    c.drive(None);
    for id in 1..=4 {
        assert_eq!(c.values[&id], (1..=9).collect::<Vec<_>>());
        assert_eq!(c.peers[&id].applied_index(), c.peers[&id].commit_index());
    }
    c.peers
        .get_mut(&1)
        .unwrap()
        .checkpoint(c.values[&1].clone())
        .unwrap();
}

#[test]
fn asynchronous_application_tasks_execute_on_host_owned_workers() {
    let mut c = AsyncCluster::new(2, 1, None);
    c.propose(1, 1..=4);
    c.drive(Some(2));
    let task = c.tasks.get_mut(&2).unwrap().pop_front().unwrap();
    let application = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let worker_application = std::sync::Arc::clone(&application);
    let completion = std::thread::spawn(move || {
        for entry in task.entries().iter() {
            worker_application
                .lock()
                .unwrap()
                .extend_from_slice(entry.payload);
        }
        task.finish()
    })
    .join()
    .unwrap();
    *c.values.get_mut(&2).unwrap() = application.lock().unwrap().clone();
    c.peers
        .get_mut(&2)
        .unwrap()
        .complete_application_task(completion)
        .unwrap();
    c.drive(None);
    assert_eq!(c.values[&2], (1..=4).collect::<Vec<_>>());
}
impl Cluster {
    fn options(&self, id: u64) -> PeerOptions {
        PeerOptions {
            group_id: 31,
            node_id: id,
            voters: vec![1, 2, 3],
            wal_path: self.root.path().join(format!("peer-{id}.wal")),
        }
    }
    fn new() -> Self {
        let mut cluster = Self {
            root: tempfile::tempdir().unwrap(),
            peers: BTreeMap::new(),
            values: BTreeMap::new(),
            restores: BTreeMap::new(),
        };
        for id in 1..=3 {
            let peer = DurablePeer::open_with_membership_and_tuning(
                cluster.options(id),
                vec![1, 2, 3, 4],
                PeerTuning {
                    max_apply_bytes: Some(128),
                    max_log_cache_bytes: Some(512),
                    ..PeerTuning::default()
                },
            )
            .unwrap();
            cluster.peers.insert(id, peer);
        }
        cluster.drive(None, None);
        cluster.peers.get_mut(&1).unwrap().campaign().unwrap();
        cluster.drive(None, None);
        assert!(cluster.peers[&1].is_leader());
        cluster
    }
    fn drive(&mut self, blocked: Option<u64>, isolated: Option<u64>) {
        self.drive_restore(blocked, isolated, None);
    }
    fn drive_restore(
        &mut self,
        blocked: Option<u64>,
        isolated: Option<u64>,
        restore_blocked: Option<u64>,
    ) {
        // Fixed rounds let consensus settle without treating a deferred application
        // as idle or repeatedly entering an unbounded host drain loop.
        for _ in 0..64 {
            let mut packets = Vec::new();
            for (id, peer) in &mut self.peers {
                let values = RefCell::new(self.values.remove(id).unwrap_or_default());
                let mut restores = 0;
                let outgoing = peer
                    .process_ready_retryable_with_restore(
                        |_, data| {
                            if blocked == Some(*id) && !data.is_empty() {
                                return Ok(ApplyDecision::Retry);
                            }
                            values.borrow_mut().extend_from_slice(data);
                            Ok(ApplyDecision::Applied)
                        },
                        |_, reader, _| {
                            if restore_blocked == Some(*id) {
                                let mut first = [0];
                                reader.read_exact(&mut first).unwrap();
                                assert_eq!(first, [1]);
                                return Ok(RestoreDecision::Retry);
                            }
                            let mut restored = Vec::new();
                            reader
                                .read_to_end(&mut restored)
                                .map_err(|e| RaftError::Storage(e.to_string()))?;
                            *values.borrow_mut() = restored;
                            restores += 1;
                            Ok(RestoreDecision::Restored)
                        },
                    )
                    .unwrap();
                self.values.insert(*id, values.into_inner());
                *self.restores.entry(*id).or_default() += restores;
                if isolated != Some(*id) {
                    packets.extend(outgoing.into_iter().filter(|p| isolated != Some(p.to)));
                }
            }
            for packet in packets {
                if let Some(peer) = self.peers.get_mut(&packet.to) {
                    peer.step(&packet.bytes).unwrap();
                }
            }
            for peer in self.peers.values_mut() {
                for task in peer.take_log_read_tasks().unwrap() {
                    peer.complete_log_read(task.execute()).unwrap();
                }
            }
        }
    }
    fn propose(&mut self, values: std::ops::RangeInclusive<u8>) -> u64 {
        self.peers
            .get_mut(&1)
            .unwrap()
            .propose_batch(values.map(|value| vec![value]).collect())
            .unwrap()
            .last()
            .unwrap()
            .index
    }
}

#[test]
fn a_lagging_voter_catches_up_from_evicted_history_without_a_checkpoint() {
    let mut cluster = Cluster::new();
    cluster.propose(1..=32);
    cluster.drive(None, Some(2));
    assert_eq!(cluster.values[&2], Vec::<u8>::new());
    cluster
        .peers
        .get_mut(&1)
        .unwrap()
        .enable_async_log_reads(8)
        .unwrap();
    cluster
        .peers
        .get_mut(&1)
        .unwrap()
        .report_unreachable(2)
        .unwrap();
    for _ in 0..4 {
        cluster.peers.get_mut(&1).unwrap().tick().unwrap();
        cluster.drive(None, None);
    }
    assert_eq!(cluster.values[&2], (1..=32).collect::<Vec<_>>());
    cluster.peers.get_mut(&3).unwrap().force_campaign().unwrap();
    cluster.drive(None, None);
    assert!(cluster.peers[&3].is_leader());
    cluster
        .peers
        .get_mut(&3)
        .unwrap()
        .propose(vec![99])
        .unwrap();
    cluster.drive(None, None);
    let expected: Vec<_> = (1..=32).chain(std::iter::once(99)).collect();
    for id in 1..=3 {
        assert_eq!(cluster.values[&id], expected);
    }
}

#[test]
fn cache_pressure_releases_after_real_commit_and_uncommitted_overwrite_replays_cleanly() {
    let mut cluster = Cluster::new();
    cluster.propose(1..=16);
    cluster.drive(None, None);
    cluster
        .peers
        .get_mut(&1)
        .unwrap()
        .propose(vec![200; 4096])
        .unwrap();
    cluster.drive(None, Some(1));
    assert!(cluster
        .peers
        .get_mut(&1)
        .unwrap()
        .propose(vec![201])
        .is_err());
    assert!(cluster
        .peers
        .get_mut(&1)
        .unwrap()
        .propose_batch(vec![vec![201]])
        .is_err());
    let mut leader = None;
    for _ in 0..100 {
        for id in [2, 3] {
            cluster.peers.get_mut(&id).unwrap().tick().unwrap();
        }
        cluster.drive(None, Some(1));
        leader = [2, 3].into_iter().find(|id| cluster.peers[id].is_leader());
        if leader.is_some() {
            break;
        }
    }
    let leader = leader.unwrap();
    cluster
        .peers
        .get_mut(&leader)
        .unwrap()
        .propose(vec![99])
        .unwrap();
    cluster.drive(None, Some(1));
    cluster
        .peers
        .get_mut(&leader)
        .unwrap()
        .report_unreachable(1)
        .unwrap();
    for _ in 0..4 {
        cluster.peers.get_mut(&leader).unwrap().tick().unwrap();
        cluster.drive(None, None);
    }
    let expected: Vec<_> = (1..=16).chain(std::iter::once(99)).collect();
    for id in 1..=3 {
        assert_eq!(cluster.values[&id], expected);
    }
    cluster.peers.clear();
    cluster.values.clear();
    for id in 1..=3 {
        let peer = DurablePeer::open_with_membership_and_tuning(
            cluster.options(id),
            vec![1, 2, 3, 4],
            PeerTuning {
                max_log_cache_bytes: Some(512),
                max_apply_bytes: Some(128),
                ..Default::default()
            },
        )
        .unwrap();
        cluster.peers.insert(id, peer);
    }
    cluster.drive(None, None);
    for id in 1..=3 {
        assert_eq!(cluster.values[&id], expected);
    }
}

#[test]
fn restore_retries_reopen_the_reader_and_keep_recovery_and_suffix_application_waiting() {
    let mut cluster = Cluster::new();
    cluster.propose(1..=3);
    cluster.drive(None, None);
    cluster
        .peers
        .get_mut(&1)
        .unwrap()
        .checkpoint(vec![1, 2, 3])
        .unwrap();
    let last = cluster.propose(4..=6);
    cluster.drive(None, None);
    let options = cluster.options(1);
    cluster.peers.remove(&1);
    let mut peer = DurablePeer::open_with_membership(options.clone(), vec![1, 2, 3, 4]).unwrap();
    assert_eq!(peer.applied_index(), 0);
    let bytes = peer.persisted_wal_bytes().unwrap();
    let mut attempts = 0;
    for _ in 0..5 {
        peer.process_ready_batch_with_restore(
            |_| panic!("suffix before image"),
            |_, reader, length| {
                attempts += 1;
                assert_eq!(length, 3);
                let mut first = [0];
                reader.read_exact(&mut first).unwrap();
                assert_eq!(first, [1]);
                Ok(RestoreDecision::Retry)
            },
        )
        .unwrap();
        assert_eq!(peer.applied_index(), 0);
        assert!(peer.has_ready());
        assert!(peer.campaign().is_err());
        assert!(peer.tick().is_err());
    }
    assert_eq!(attempts, 5);
    assert_eq!(peer.persisted_wal_bytes().unwrap(), bytes);
    drop(peer);
    let mut peer = DurablePeer::open_with_membership(options, vec![1, 2, 3, 4]).unwrap();
    let values = RefCell::new(Vec::new());
    let restored = std::cell::Cell::new(false);
    peer.process_ready_batch_with_restore(
        |batch| {
            assert!(restored.get());
            for entry in batch.iter() {
                values.borrow_mut().extend_from_slice(entry.payload);
            }
            Ok(batch.len())
        },
        |_, reader, _| {
            reader.read_to_end(&mut values.borrow_mut()).unwrap();
            restored.set(true);
            Ok(RestoreDecision::Restored)
        },
    )
    .unwrap();
    assert_eq!(values.into_inner(), (1..=6).collect::<Vec<_>>());
    assert_eq!(peer.applied_index(), last);
    assert!(!peer.has_ready());
    peer.tick().unwrap();
}

#[test]
fn pending_restore_keeps_quorum_progress_and_a_newer_image_supersedes_it() {
    let mut cluster = Cluster::new();
    cluster.propose(1..=3);
    cluster.drive(None, None);
    let applied = cluster.peers[&2].applied_index();
    cluster.propose(4..=9);
    cluster.drive(None, Some(2));
    cluster
        .peers
        .get_mut(&1)
        .unwrap()
        .checkpoint((1..=9).collect())
        .unwrap();
    cluster.propose(10..=10);
    cluster.drive_restore(None, None, Some(2));
    assert_eq!(cluster.peers[&2].applied_index(), applied);
    assert_eq!(cluster.values[&2], vec![1, 2, 3]);
    assert_eq!(cluster.restores[&2], 0);
    let last = cluster.propose(11..=11);
    cluster.drive_restore(None, Some(3), Some(2));
    assert_eq!(
        cluster.peers[&1].commit_index(),
        last,
        "the paused loader must still acknowledge durable replication"
    );
    assert_eq!(cluster.peers[&2].commit_index(), last);
    assert_eq!(cluster.peers[&2].applied_index(), applied);
    cluster.peers.get_mut(&2).unwrap().tick().unwrap();
    for _ in 0..2 {
        cluster.peers.get_mut(&1).unwrap().tick().unwrap();
        cluster.drive_restore(None, None, Some(2));
    }
    cluster.propose(12..=15);
    cluster.drive_restore(None, Some(2), Some(2));
    cluster
        .peers
        .get_mut(&1)
        .unwrap()
        .checkpoint((1..=15).collect())
        .unwrap();
    let last = cluster.propose(16..=16);
    cluster.drive_restore(None, None, Some(2));
    for _ in 0..4 {
        cluster.peers.get_mut(&1).unwrap().tick().unwrap();
        cluster.drive_restore(None, None, Some(2));
    }
    assert_eq!(cluster.peers[&2].applied_index(), applied);
    assert_eq!(cluster.restores[&2], 0);
    cluster.drive(None, None);
    assert_eq!(cluster.values[&2], (1..=16).collect::<Vec<_>>());
    assert_eq!(cluster.peers[&2].applied_index(), last);
    assert_eq!(cluster.restores[&2], 1);
}

#[test]
fn read_confirmation_waits_for_the_reopened_image_and_its_committed_suffix() {
    let root = tempfile::tempdir().unwrap();
    let options = PeerOptions {
        group_id: 1,
        node_id: 1,
        voters: vec![1],
        wal_path: root.path().join("peer.wal"),
    };
    let mut peer = DurablePeer::open(options.clone()).unwrap();
    peer.campaign().unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    peer.propose(vec![1]).unwrap();
    peer.process_ready(|_, _| Ok(())).unwrap();
    peer.checkpoint(vec![1]).unwrap();
    peer.propose(vec![2]).unwrap();
    peer.process_ready_with_snapshot_reader(
        |_, _| Ok(()),
        |_, reader, _| {
            std::io::copy(reader, &mut std::io::sink()).unwrap();
            Ok(())
        },
    )
    .unwrap();
    drop(peer);
    let mut peer = DurablePeer::open(options).unwrap();
    peer.process_ready_retryable_with_restore(
        |_, _| panic!("suffix before image"),
        |_, _, _| Ok(RestoreDecision::Retry),
    )
    .unwrap();
    let before_election = peer.commit_index();
    peer.campaign().unwrap();
    for _ in 0..5 {
        peer.process_ready_retryable_with_restore(
            |_, _| panic!("suffix before image"),
            |_, _, _| Ok(RestoreDecision::Retry),
        )
        .unwrap();
    }
    assert!(peer.is_leader());
    assert!(peer.commit_index() > before_election);
    let read = peer.begin_read().unwrap();
    peer.process_ready_retryable_with_restore(
        |_, _| panic!("suffix before image"),
        |_, _, _| Ok(RestoreDecision::Retry),
    )
    .unwrap();
    assert_eq!(peer.applied_index(), 0);
    assert!(!peer.poll_read(read).unwrap());
    let values = RefCell::new(Vec::new());
    peer.process_ready_retryable_with_restore(
        |_, data| {
            values.borrow_mut().extend_from_slice(data);
            Ok(ApplyDecision::Applied)
        },
        |_, reader, _| {
            reader.read_to_end(&mut values.borrow_mut()).unwrap();
            Ok(RestoreDecision::Restored)
        },
    )
    .unwrap();
    assert_eq!(values.into_inner(), vec![1, 2]);
    assert!(peer.poll_read(read).unwrap());
}

#[test]
fn restore_errors_and_incomplete_success_still_stop_the_peer() {
    let mut cluster = Cluster::new();
    cluster.propose(1..=3);
    cluster.drive(None, None);
    cluster
        .peers
        .get_mut(&1)
        .unwrap()
        .checkpoint(vec![1, 2, 3])
        .unwrap();
    let options = cluster.options(1);
    cluster.peers.remove(&1);
    for incomplete in [false, true] {
        let mut peer =
            DurablePeer::open_with_membership(options.clone(), vec![1, 2, 3, 4]).unwrap();
        assert!(peer
            .process_ready_batch_with_restore(
                |_| panic!("suffix before image"),
                |_, _, _| {
                    if incomplete {
                        Ok(RestoreDecision::Restored)
                    } else {
                        Err(RaftError::Storage("restore failed".into()))
                    }
                }
            )
            .is_err());
        assert_eq!(peer.applied_index(), 0);
        assert!(!peer.has_ready());
        assert!(peer.process_ready(|_, _| Ok(())).is_err());
    }
    let mut peer = DurablePeer::open_with_membership(options, vec![1, 2, 3, 4]).unwrap();
    let mut restored = Vec::new();
    peer.process_ready_batch_with_restore(
        |_| panic!("unexpected suffix"),
        |_, reader, _| {
            reader.read_to_end(&mut restored).unwrap();
            Ok(RestoreDecision::Restored)
        },
    )
    .unwrap();
    assert_eq!(restored, vec![1, 2, 3]);
}

#[test]
fn borrowed_batches_reduce_callback_count_without_changing_positions_or_values() {
    let root = tempfile::tempdir().unwrap();
    let mut outcomes = Vec::new();
    let mut counts = Vec::new();
    for batched in [false, true] {
        let options = PeerOptions {
            group_id: 1,
            node_id: 1,
            voters: vec![1],
            wal_path: root.path().join(format!("peer-{batched}.wal")),
        };
        let mut peer = DurablePeer::open(options).unwrap();
        peer.campaign().unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
        let values: Vec<_> = (0..32u8).map(|value| vec![value; 32]).collect();
        let ids = peer.propose_batch(values.clone()).unwrap();
        let mut applied = Vec::new();
        let mut calls = 0;
        if batched {
            peer.process_ready_batch(|batch| {
                calls += 1;
                assert!(!batch.is_empty());
                assert_eq!(batch.iter().len(), batch.len());
                assert!(batch.get(batch.len()).is_none());
                assert_eq!(
                    batch.iter().next_back().unwrap().position,
                    batch.get(batch.len() - 1).unwrap().position
                );
                for entry in batch.iter() {
                    applied.push((entry.position, entry.payload.to_vec()));
                }
                Ok(batch.len())
            })
            .unwrap();
        } else {
            peer.process_ready(|id, data| {
                calls += 1;
                applied.push((id, data.to_vec()));
                Ok(())
            })
            .unwrap();
        }
        assert_eq!(peer.applied_index(), ids.last().unwrap().index);
        assert_eq!(
            applied.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>(),
            ids
        );
        assert_eq!(
            applied
                .iter()
                .map(|(_, value)| value.clone())
                .collect::<Vec<_>>(),
            values
        );
        counts.push(calls);
        outcomes.push(applied);
    }
    assert_eq!(outcomes[0], outcomes[1]);
    assert_eq!(counts, vec![32, 1]);
}

#[test]
fn batch_prefix_consumption_defers_only_the_suffix_and_keeps_reads_ordered() {
    for limit in [None, Some(64)] {
        let root = tempfile::tempdir().unwrap();
        let options = PeerOptions {
            group_id: 1,
            node_id: 1,
            voters: vec![1],
            wal_path: root.path().join("peer.wal"),
        };
        let mut peer = DurablePeer::open_with_tuning(
            options.clone(),
            PeerTuning {
                max_apply_bytes: limit,
                ..PeerTuning::default()
            },
        )
        .unwrap();
        peer.campaign().unwrap();
        for _ in 0..20 {
            if !peer.has_ready() {
                break;
            }
            peer.process_ready(|_, _| Ok(())).unwrap();
        }
        let ids = peer
            .propose_batch((1..=10u8).map(|value| vec![value]).collect())
            .unwrap();
        let mut values = Vec::new();
        for _ in 0..20 {
            peer.process_ready_batch(|batch| {
                let take = (2 - values.len()).min(batch.len());
                for entry in batch.iter().take(take) {
                    values.extend_from_slice(entry.payload);
                }
                Ok(take)
            })
            .unwrap();
            if values.len() == 2 {
                break;
            }
        }
        assert_eq!(values, vec![1, 2]);
        assert_eq!(peer.applied_index(), ids[1].index);
        let read = peer.begin_read().unwrap();
        let tail = peer.propose(vec![11]).unwrap();
        let mut attempts = 0;
        for _ in 0..20 {
            peer.process_ready_batch(|batch| {
                attempts += 1;
                assert_eq!(batch.get(0).unwrap().position, ids[2]);
                Ok(0)
            })
            .unwrap();
        }
        assert!(attempts > 0 && attempts <= 20);
        assert_eq!(peer.commit_index(), tail.index);
        assert_eq!(peer.applied_index(), ids[1].index);
        assert!(!peer.poll_read(read).unwrap());
        for _ in 0..100 {
            if !peer.has_ready() {
                break;
            }
            peer.process_ready_batch(|batch| {
                let take = batch.len().min(3);
                for entry in batch.iter().take(take) {
                    values.extend_from_slice(entry.payload);
                }
                Ok(take)
            })
            .unwrap();
        }
        assert_eq!(values, (1..=11).collect::<Vec<_>>());
        assert_eq!(peer.applied_index(), tail.index);
        assert!(peer.poll_read(read).unwrap());
        drop(peer);
        let mut peer = DurablePeer::open(options).unwrap();
        let mut replay = Vec::new();
        peer.process_ready_batch(|batch| {
            for entry in batch.iter() {
                replay.extend_from_slice(entry.payload);
            }
            Ok(batch.len())
        })
        .unwrap();
        assert_eq!(replay, values);
    }
}

#[test]
fn invalid_batch_progress_and_errors_stop_without_marking_the_batch_applied() {
    for invalid_count in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let options = PeerOptions {
            group_id: 1,
            node_id: 1,
            voters: vec![1],
            wal_path: root.path().join("peer.wal"),
        };
        let mut peer = DurablePeer::open(options.clone()).unwrap();
        peer.campaign().unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
        let before = peer.applied_index();
        peer.propose_batch(vec![vec![1], vec![2]]).unwrap();
        assert!(peer
            .process_ready_batch(|batch| if invalid_count {
                Ok(batch.len() + 1)
            } else {
                Err(RaftError::Storage("batch failed".into()))
            })
            .is_err());
        assert_eq!(peer.applied_index(), before);
        assert!(!peer.has_ready());
        assert!(peer.process_ready(|_, _| Ok(())).is_err());
        drop(peer);
        let mut peer = DurablePeer::open(options).unwrap();
        let mut replay = Vec::new();
        peer.process_ready(|_, data| {
            replay.extend_from_slice(data);
            Ok(())
        })
        .unwrap();
        assert_eq!(replay, vec![1, 2]);
    }
}

#[test]
fn batch_replay_splits_at_membership_and_does_not_skip_a_deferred_prefix() {
    let mut cluster = Cluster::new();
    cluster.propose(1..=3);
    cluster.drive(None, None);
    let change = cluster
        .peers
        .get_mut(&1)
        .unwrap()
        .propose_membership(MembershipChange::AddLearner(4))
        .unwrap();
    cluster.drive(None, None);
    cluster.propose(4..=6);
    cluster.drive(None, None);
    let options = cluster.options(2);
    cluster.peers.remove(&2);
    let mut peer = DurablePeer::open_with_membership(options, vec![1, 2, 3, 4]).unwrap();
    let mut values = Vec::new();
    peer.process_ready_batch(|batch| {
        assert_eq!(
            batch.iter().next_back().unwrap().position.index,
            change.index - 1
        );
        for entry in batch.iter().take(batch.len() - 1) {
            values.extend_from_slice(entry.payload);
        }
        Ok(batch.len() - 1)
    })
    .unwrap();
    assert_eq!(values, vec![1, 2]);
    assert!(!peer.membership().learners.contains(&4));
    let mut starts = Vec::new();
    peer.process_ready_batch(|batch| {
        starts.push(batch.get(0).unwrap().position.index);
        for entry in batch.iter() {
            values.extend_from_slice(entry.payload);
        }
        Ok(batch.len())
    })
    .unwrap();
    assert_eq!(starts, vec![change.index - 1, change.index + 1]);
    assert_eq!(values, (1..=6).collect::<Vec<_>>());
    assert!(peer.membership().learners.contains(&4));
}

#[test]
fn batch_application_restores_the_checkpoint_before_its_suffix() {
    let mut cluster = Cluster::new();
    cluster.propose(1..=3);
    cluster.drive(None, None);
    cluster
        .peers
        .get_mut(&1)
        .unwrap()
        .checkpoint(vec![1, 2, 3])
        .unwrap();
    cluster.propose(4..=6);
    cluster.drive(None, None);
    let options = cluster.options(1);
    cluster.peers.remove(&1);
    let mut peer = DurablePeer::open_with_membership(options, vec![1, 2, 3, 4]).unwrap();
    let values = RefCell::new(Vec::new());
    let restored = std::cell::Cell::new(0);
    peer.process_ready_batch_with_snapshot_reader(
        |batch| {
            assert_eq!(restored.get(), 1);
            for entry in batch.iter() {
                values.borrow_mut().extend_from_slice(entry.payload);
            }
            Ok(batch.len())
        },
        |_, reader, _| {
            reader
                .read_to_end(&mut values.borrow_mut())
                .map_err(|e| RaftError::Storage(e.to_string()))?;
            restored.set(restored.get() + 1);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(restored.get(), 1);
    assert_eq!(values.into_inner(), (1..=6).collect::<Vec<_>>());
}

#[test]
fn retry_preserves_partial_progress_and_read_order_without_poisoning_the_peer() {
    for bytes in [None, Some(32)] {
        let root = tempfile::tempdir().unwrap();
        let options = PeerOptions {
            group_id: 1,
            node_id: 1,
            voters: vec![1],
            wal_path: root.path().join("peer.wal"),
        };
        let mut peer = DurablePeer::open_with_tuning(
            options.clone(),
            PeerTuning {
                max_apply_bytes: bytes,
                ..PeerTuning::default()
            },
        )
        .unwrap();
        peer.campaign().unwrap();
        for _ in 0..20 {
            if !peer.has_ready() {
                break;
            }
            peer.process_ready(|_, _| Ok(())).unwrap();
        }
        let ids = peer
            .propose_batch((1..=8u8).map(|value| vec![value]).collect())
            .unwrap();
        let mut values = Vec::new();
        let mut attempts = Vec::new();
        for _ in 0..20 {
            peer.process_ready_retryable(|id, data| {
                if data == [4] {
                    attempts.push(id.index);
                    return Ok(ApplyDecision::Retry);
                }
                values.extend_from_slice(data);
                Ok(ApplyDecision::Applied)
            })
            .unwrap();
        }
        assert_eq!(values, vec![1, 2, 3]);
        assert!(!attempts.is_empty());
        assert!(
            attempts.len() <= 20,
            "Retry cannot busy-loop within a Ready call"
        );
        assert!(attempts.iter().all(|index| *index == ids[3].index));
        assert_eq!(peer.applied_index(), ids[2].index);
        assert_eq!(peer.commit_index(), ids[7].index);
        assert!(peer.has_ready());
        assert!(peer.prepare_checkpoint().is_err());
        assert!(peer.checkpoint(vec![1, 2, 3]).is_err());
        let token = peer.begin_read().unwrap();
        let tail = peer.propose(vec![9]).unwrap();
        for _ in 0..10 {
            peer.process_ready_retryable(|_, _| Ok(ApplyDecision::Retry))
                .unwrap();
        }
        assert_eq!(peer.commit_index(), tail.index);
        assert!(!peer.poll_read(token).unwrap());
        for _ in 0..20 {
            if !peer.has_ready() {
                break;
            }
            peer.process_ready_retryable(|_, data| {
                values.extend_from_slice(data);
                Ok(ApplyDecision::Applied)
            })
            .unwrap();
        }
        assert_eq!(values, (1..=9).collect::<Vec<_>>());
        assert_eq!(peer.applied_index(), tail.index);
        assert!(peer.poll_read(token).unwrap());
        let failed = peer.propose(vec![55]).unwrap();
        let mut failed_call = false;
        for _ in 0..10 {
            if peer
                .process_ready_retryable(|_, _| {
                    Err(RaftError::Storage("application failed".into()))
                })
                .is_err()
            {
                failed_call = true;
                break;
            }
        }
        assert!(failed_call);
        assert!(peer.applied_index() < failed.index);
        assert!(!peer.has_ready());
        drop(peer);
        let mut peer = DurablePeer::open(options).unwrap();
        let mut replay = Vec::new();
        peer.process_ready(|_, data| {
            replay.extend_from_slice(data);
            Ok(())
        })
        .unwrap();
        values.push(55);
        assert_eq!(replay, values);
    }
}

#[test]
fn a_deferred_follower_still_supplies_durable_quorum_acknowledgments() {
    let mut cluster = Cluster::new();
    let last = cluster.propose(1..=8);
    cluster.drive(Some(2), None);
    assert_eq!(cluster.peers[&2].commit_index(), last);
    assert!(cluster.peers[&2].applied_index() < last);
    assert!(cluster.values[&2].is_empty());
    let last = cluster.propose(9..=9);
    cluster.drive(Some(2), Some(3));
    assert_eq!(
        cluster.peers[&1].commit_index(),
        last,
        "the leader needs the deferred voter's durable acknowledgment"
    );
    assert_eq!(cluster.peers[&2].commit_index(), last);
    assert!(cluster.values[&2].is_empty());
    cluster.drive(None, None);
    for _ in 0..2 {
        cluster.peers.get_mut(&1).unwrap().tick().unwrap();
        cluster.drive(None, None);
    }
    for id in 1..=3 {
        assert_eq!(cluster.values[&id], (1..=9).collect::<Vec<_>>());
    }
}

#[test]
fn deferred_work_replays_after_restart_and_holds_membership_recovery_closed() {
    let mut cluster = Cluster::new();
    let last = cluster.propose(1..=8);
    cluster.drive(Some(2), None);
    let options = cluster.options(2);
    cluster.peers.remove(&2);
    let mut peer = DurablePeer::open_with_membership_and_tuning(
        options,
        vec![1, 2, 3, 4],
        PeerTuning {
            max_apply_bytes: Some(32),
            ..PeerTuning::default()
        },
    )
    .unwrap();
    for _ in 0..8 {
        peer.process_ready_retryable(|_, data| {
            Ok(if data.is_empty() {
                ApplyDecision::Applied
            } else {
                ApplyDecision::Retry
            })
        })
        .unwrap();
        assert!(peer.applied_index() < last);
        assert!(peer.campaign().is_err());
        assert!(peer.tick().is_err());
    }
    let mut values = Vec::new();
    for _ in 0..100 {
        if !peer.has_ready() {
            break;
        }
        peer.process_ready_retryable(|_, data| {
            values.extend_from_slice(data);
            Ok(ApplyDecision::Applied)
        })
        .unwrap();
    }
    assert!(!peer.has_ready());
    assert_eq!(peer.applied_index(), last);
    assert_eq!(values, (1..=8).collect::<Vec<_>>());
    peer.tick().unwrap();
}

#[test]
fn committed_membership_waits_behind_a_deferred_application_prefix() {
    let mut cluster = Cluster::new();
    cluster.propose(1..=3);
    cluster.drive(Some(2), None);
    let change = cluster
        .peers
        .get_mut(&1)
        .unwrap()
        .propose_membership(MembershipChange::AddLearner(4))
        .unwrap();
    cluster.drive(Some(2), None);
    assert_eq!(cluster.peers[&1].commit_index(), change.index);
    assert!(!cluster.peers[&2].membership().learners.contains(&4));
    assert!(cluster.peers[&1].membership().learners.contains(&4));
    cluster.drive(None, None);
    assert!(cluster.peers[&2].membership().learners.contains(&4));
    assert_eq!(cluster.values[&2], vec![1, 2, 3]);
}

#[test]
fn a_newer_checkpoint_supersedes_deferred_entries_before_suffix_application() {
    let mut cluster = Cluster::new();
    cluster.propose(1..=3);
    cluster.drive(Some(2), None);
    cluster.propose(4..=9);
    cluster.drive(Some(2), Some(2));
    let image = cluster.values[&1].clone();
    cluster
        .peers
        .get_mut(&1)
        .unwrap()
        .checkpoint(image)
        .unwrap();
    let last = cluster.propose(10..=10);
    cluster.drive(Some(2), None);
    for _ in 0..4 {
        cluster.peers.get_mut(&1).unwrap().tick().unwrap();
        cluster.drive(Some(2), None);
    }
    assert_eq!(cluster.restores[&2], 1);
    assert_eq!(cluster.values[&2], (1..=9).collect::<Vec<_>>());
    assert_eq!(cluster.peers[&2].commit_index(), last);
    assert!(cluster.peers[&2].applied_index() < last);
    cluster.drive(None, None);
    assert_eq!(cluster.values[&2], (1..=10).collect::<Vec<_>>());
    assert_eq!(cluster.peers[&2].applied_index(), last);
}
