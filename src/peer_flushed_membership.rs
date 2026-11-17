// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

#[derive(Clone, PartialEq, prost::Message)]
struct FloorConfiguration {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, tag = "2")]
    index: u64,
    #[prost(bytes = "vec", tag = "3")]
    configuration: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    promotions: Vec<u8>,
    #[prost(uint64, tag = "5")]
    checkpoint_index: u64,
}

pub(super) fn encode(index: u64, conf: ConfState, promotions: &BTreeSet<u64>) -> Vec<u8> {
    encode_covered(index, 0, conf, promotions)
}
pub(super) fn encode_covered(
    index: u64,
    checkpoint_index: u64,
    conf: ConfState,
    promotions: &BTreeSet<u64>,
) -> Vec<u8> {
    FloorConfiguration {
        version: 1,
        index,
        configuration: conf.encode_to_vec(),
        promotions: membership::encode_promotions(promotions),
        checkpoint_index,
    }
    .encode_to_vec()
}
pub(super) fn decode(
    bytes: &[u8],
    index: u64,
    permitted: &[u64],
) -> Result<(ConfState, BTreeSet<u64>), RaftError> {
    if bytes.is_empty() || bytes.len() > 4096 || index == 0 || permitted.is_empty() {
        return Err(storage_error("missing or oversized flushed configuration"));
    }
    let record = FloorConfiguration::decode(bytes).map_err(storage_error)?;
    if record.version != 1
        || record.index != index
        || record.encode_to_vec() != bytes
        || (record.checkpoint_index != 0 && record.checkpoint_index <= index)
    {
        return Err(storage_error("invalid flushed configuration encoding"));
    }
    let conf = ConfState::decode(record.configuration.as_slice()).map_err(storage_error)?;
    if conf.encode_to_vec() != record.configuration {
        return Err(storage_error("invalid flushed membership encoding"));
    }
    membership::validate_conf(&conf, permitted)?;
    let promotions = membership::decode_promotions(&record.promotions, &conf)?;
    Ok((conf, promotions))
}
pub(super) fn covering_checkpoint(bytes: &[u8]) -> Result<u64, RaftError> {
    FloorConfiguration::decode(bytes)
        .map(|record| record.checkpoint_index)
        .map_err(storage_error)
}
pub(super) fn validate_record(record: &WalBatch, permitted: &[u64]) -> Result<(), RaftError> {
    if record.flushed_index == 0 {
        if !record.flushed_membership.is_empty() {
            return Err(storage_error(
                "flushed configuration has no application floor",
            ));
        }
    } else if !permitted.is_empty() {
        if record.version != 1 || !record.entries.is_empty() || !record.snapshot.is_empty() {
            return Err(storage_error(
                "flushed configuration requires a floor record",
            ));
        }
        decode(&record.flushed_membership, record.flushed_index, permitted)?;
    } else if !record.flushed_membership.is_empty() {
        return Err(storage_error(
            "flushed configuration requires durable membership",
        ));
    }
    Ok(())
}

impl DurablePeer {
    pub(super) fn membership_at(
        &self,
        index: u64,
    ) -> Result<(ConfState, BTreeSet<u64>), RaftError> {
        if index > self.applied_index() {
            return Err(invalid("membership position has not applied"));
        }
        let checkpoint = self.store.checkpoint.lock().unwrap().clone();
        let mut start = checkpoint
            .as_ref()
            .map_or(0, |image| image.get_metadata().index);
        if index < start {
            return Err(invalid("membership position has been compacted"));
        }
        let mut conf = checkpoint.as_ref().map_or_else(
            || ConfState {
                voters: self.options.voters.clone(),
                ..Default::default()
            },
            |image| image.get_metadata().get_conf_state().clone(),
        );
        let mut promotions = membership::decode_promotions(
            &self.store.checkpoint_promotions.lock().unwrap(),
            &conf,
        )?;
        if self.journal.flushed_index > start && self.journal.flushed_index <= index {
            (conf, promotions) = decode(
                &self.journal.flushed_membership,
                self.journal.flushed_index,
                &self.store.permitted,
            )?;
            start = self.journal.flushed_index;
        }
        if start == index {
            return Ok((conf, promotions));
        }
        let memory = MemStorage::new_with_conf_state(conf.clone());
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let mut replay = RawNode::new(
            &Config {
                id: self.node_id(),
                ..Default::default()
            },
            memory,
            &logger,
        )
        .map_err(invalid)?;
        let mut next = start + 1;
        while next <= index {
            let entries = self
                .store
                .entries_sync(next, index + 1, Some(65536))
                .map_err(storage_error)?;
            if entries.is_empty() {
                return Err(storage_error("missing flushed configuration history"));
            }
            for entry in entries {
                next = entry.index + 1;
                if entry.entry_type != EntryType::EntryNormal as i32 {
                    let change = membership::decode_change(&entry, &self.store.permitted)?;
                    conf = replay.apply_conf_change(&change).map_err(invalid)?;
                    membership::validate_conf(&conf, &self.store.permitted)?;
                    membership::update_promotions(&mut promotions, &change, &conf);
                }
            }
        }
        Ok((conf, promotions))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(path: PathBuf) -> PeerOptions {
        PeerOptions {
            group_id: 31,
            node_id: 1,
            voters: vec![1],
            wal_path: path,
        }
    }
    fn settle(peer: &mut DurablePeer) {
        for _ in 0..32 {
            peer.process_ready(|_, _| Ok(())).unwrap();
            if !peer.has_ready() {
                return;
            }
        }
        panic!("local membership work did not settle");
    }

    #[test]
    fn flushed_configuration_requires_canonical_membership_and_a_matching_floor() {
        let conf = ConfState {
            voters: vec![1],
            learners: vec![2],
            ..Default::default()
        };
        let targets = BTreeSet::from([2]);
        let bytes = encode(7, conf.clone(), &targets);
        let no_policy = encode(7, conf.clone(), &BTreeSet::new());
        assert_eq!(decode(&bytes, 7, &[1, 2, 3]).unwrap(), (conf, targets));
        assert!(decode(&bytes, 8, &[1, 2, 3]).is_err());
        assert!(decode(&bytes, 7, &[1]).is_err());
        assert!(decode(&bytes, 7, &[]).is_err());
        for n in 0..bytes.len() {
            if bytes[..n] == no_policy {
                assert!(decode(&bytes[..n], 7, &[1, 2, 3]).is_ok());
            } else {
                assert!(decode(&bytes[..n], 7, &[1, 2, 3]).is_err());
            }
        }
        let mut extra = bytes.clone();
        extra.extend_from_slice(&[48, 1]);
        assert!(decode(&extra, 7, &[1, 2, 3]).is_err());
        let mut record = batch(&options(PathBuf::new()), None, &[]);
        record.permitted = vec![1, 2, 3];
        record.flushed_index = 7;
        record.flushed_membership = bytes;
        validate_record(&record, &record.permitted).unwrap();
        let wire = record.encode_to_vec();
        let mut prior_shape = record.clone();
        prior_shape.flushed_membership.clear();
        assert!(!exact_batch_encoding(&prior_shape, &wire, &mut Vec::new()));
        assert!(validate_record(&prior_shape, &record.permitted).is_err());
        record.flushed_index = 0;
        assert!(validate_record(&record, &record.permitted).is_err());
    }

    #[test]
    fn flushed_history_skips_old_images_and_preserves_later_configuration_entries() {
        for cache in [None, Some(128)] {
            for trim in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let options = options(dir.path().join("peer.wal"));
                let tuning = PeerTuning {
                    max_log_cache_bytes: cache,
                    wal_zero_fill_bytes: Some(4096),
                    ..Default::default()
                };
                let mut peer = DurablePeer::open_with_membership_and_tuning(
                    options.clone(),
                    vec![1, 2, 3],
                    tuning,
                )
                .unwrap();
                settle(&mut peer);
                peer.campaign().unwrap();
                settle(&mut peer);
                peer.checkpoint(b"old image".to_vec()).unwrap();
                peer.propose(vec![11]).unwrap();
                settle(&mut peer);
                let floor = peer
                    .propose_membership(MembershipChange::AddLearnerAuto(2))
                    .unwrap()
                    .index;
                settle(&mut peer);
                peer.propose(vec![12]).unwrap();
                settle(&mut peer);
                peer.propose_membership(MembershipChange::AddLearner(3))
                    .unwrap();
                settle(&mut peer);
                let expected = peer.membership();
                peer.set_flushed_index(floor).unwrap();
                assert_eq!(
                    peer.membership(),
                    expected,
                    "capturing history cannot change live membership"
                );
                if trim {
                    peer.trim_log_to(floor).unwrap();
                }
                drop(peer);
                let mut peer =
                    DurablePeer::open_with_membership_and_tuning(options, vec![1, 2, 3], tuning)
                        .unwrap();
                assert_eq!(peer.membership().learners, vec![2]);
                assert_eq!(peer.membership().automatic_learners, vec![2]);
                let mut values = Vec::new();
                while peer.has_ready() {
                    peer.process_ready_with_snapshot(
                        |_, data| {
                            if !data.is_empty() {
                                values.push(data.to_vec());
                            }
                            Ok(())
                        },
                        |_, _| {
                            Err(storage_error(
                                "must not restore an image older than the durable floor",
                            ))
                        },
                    )
                    .unwrap();
                }
                assert_eq!(values, vec![vec![12]]);
                assert_eq!(peer.membership(), expected);
                assert_eq!(peer.flushed_index().unwrap(), floor);
                assert_eq!(peer.applied_index(), peer.commit_index());
            }
        }
    }

    #[test]
    fn a_floor_before_compacted_history_uses_a_covered_configuration_anchor() {
        let dir = tempfile::tempdir().unwrap();
        let options = options(dir.path().join("peer.wal"));
        let mut peer = DurablePeer::open_with_membership(options.clone(), vec![1, 2]).unwrap();
        settle(&mut peer);
        peer.campaign().unwrap();
        settle(&mut peer);
        let floor = peer.applied_index();
        peer.propose_membership(MembershipChange::AddLearnerAuto(2))
            .unwrap();
        settle(&mut peer);
        peer.propose(vec![29]).unwrap();
        settle(&mut peer);
        let image_index = peer.applied_index();
        peer.checkpoint(b"covered image".to_vec()).unwrap();
        let expected = peer.membership();
        peer.set_flushed_index(floor).unwrap();
        assert_eq!(
            covering_checkpoint(&peer.journal.flushed_membership).unwrap(),
            image_index
        );
        drop(peer);
        let mut peer = DurablePeer::open_with_membership(options.clone(), vec![1, 2]).unwrap();
        assert_eq!(peer.membership(), expected);
        let restored = std::cell::Cell::new(false);
        peer.process_ready_with_snapshot(
            |_, _| Err(storage_error("unexpected compacted replay")),
            |_, bytes| {
                assert_eq!(bytes, b"covered image");
                restored.set(true);
                Ok(())
            },
        )
        .unwrap();
        assert!(restored.get());
        assert_eq!(peer.flushed_index().unwrap(), floor);
        let next_floor = floor + 1;
        let conf = peer.store.initial_state().unwrap().conf_state;
        let mut invalid = batch(&peer.options, None, &[]);
        invalid.flushed_index = next_floor;
        invalid.flushed_membership =
            encode_covered(next_floor, image_index + 1, conf, &peer.automatic_learners);
        peer.journal.append(&invalid).unwrap();
        drop(peer);
        assert!(DurablePeer::open_with_membership(options, vec![1, 2]).is_err());
    }

    #[test]
    fn newer_images_restore_beyond_the_floor_without_losing_its_configuration() {
        let dir = tempfile::tempdir().unwrap();
        let options = options(dir.path().join("peer.wal"));
        let mut peer = DurablePeer::open_with_membership(options.clone(), vec![1, 2, 3]).unwrap();
        settle(&mut peer);
        peer.campaign().unwrap();
        settle(&mut peer);
        peer.propose_membership(MembershipChange::AddLearnerAuto(2))
            .unwrap();
        settle(&mut peer);
        let floor = peer.applied_index();
        peer.set_flushed_index(floor).unwrap();
        peer.propose_membership(MembershipChange::AddLearner(3))
            .unwrap();
        settle(&mut peer);
        peer.propose(vec![19]).unwrap();
        settle(&mut peer);
        let expected = peer.membership();
        peer.checkpoint(b"new image".to_vec()).unwrap();
        drop(peer);
        let mut peer = DurablePeer::open_with_membership(options, vec![1, 2, 3]).unwrap();
        assert_eq!(peer.membership(), expected);
        let restored = std::cell::Cell::new(false);
        peer.process_ready_with_snapshot(
            |_, _| Err(storage_error("checkpoint already contains the tail")),
            |_, bytes| {
                assert_eq!(bytes, b"new image");
                restored.set(true);
                Ok(())
            },
        )
        .unwrap();
        assert!(restored.get());
        assert_eq!(peer.flushed_index().unwrap(), floor);
        assert_eq!(peer.membership(), expected);
    }

    #[test]
    fn every_interrupted_floor_record_preserves_replayable_configuration() {
        let dir = tempfile::tempdir().unwrap();
        let options = options(dir.path().join("peer.wal"));
        let mut peer = DurablePeer::open_with_membership(options.clone(), vec![1, 2]).unwrap();
        settle(&mut peer);
        peer.campaign().unwrap();
        settle(&mut peer);
        peer.propose_membership(MembershipChange::AddLearnerAuto(2))
            .unwrap();
        settle(&mut peer);
        peer.propose(vec![23]).unwrap();
        settle(&mut peer);
        let expected = peer.membership();
        let base = std::fs::read(&options.wal_path).unwrap();
        let floor = peer.applied_index();
        peer.set_flushed_index(floor).unwrap();
        let complete = std::fs::read(&options.wal_path).unwrap();
        drop(peer);
        for size in base.len()..=complete.len() {
            std::fs::write(&options.wal_path, &complete[..size]).unwrap();
            let mut peer = DurablePeer::open_with_membership(options.clone(), vec![1, 2]).unwrap();
            let mut values = Vec::new();
            while peer.has_ready() {
                peer.process_ready(|_, bytes| {
                    if !bytes.is_empty() {
                        values.push(bytes.to_vec());
                    }
                    Ok(())
                })
                .unwrap();
            }
            if size == complete.len() {
                assert_eq!(peer.flushed_index().unwrap(), floor);
                assert!(values.is_empty());
            } else {
                assert_eq!(peer.flushed_index().unwrap(), 0);
                assert_eq!(values, vec![vec![23]]);
            }
            assert_eq!(peer.membership(), expected);
            peer.tick().unwrap();
        }
    }

    #[test]
    fn floor_configuration_cannot_claim_uncommitted_recovery_progress() {
        let dir = tempfile::tempdir().unwrap();
        let options = options(dir.path().join("peer.wal"));
        let mut peer = DurablePeer::open_with_membership(options.clone(), vec![1, 2]).unwrap();
        settle(&mut peer);
        peer.campaign().unwrap();
        settle(&mut peer);
        let floor = peer.commit_index() + 1;
        let conf = peer.store.initial_state().unwrap().conf_state;
        let mut record = batch(&peer.options, None, &[]);
        record.flushed_index = floor;
        record.flushed_membership = encode(floor, conf, &BTreeSet::new());
        peer.journal.append(&record).unwrap();
        drop(peer);
        assert!(DurablePeer::open_with_membership(options, vec![1, 2]).is_err());
    }
}
