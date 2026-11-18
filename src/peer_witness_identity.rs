// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

pub(super) struct OpenRole {
    pub(super) local_witness: bool,
    pub(super) bootstrap_witnesses: Vec<u64>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RoleRecord {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint32, tag = "2")]
    local_role: u32,
    #[prost(uint64, repeated, packed = "true", tag = "3")]
    bootstrap_witnesses: Vec<u64>,
}

pub(super) struct Identity {
    encoded: Vec<u8>,
    record: RoleRecord,
    wire_bootstrap: Vec<u8>,
}

impl Identity {
    pub(super) fn from_role(
        role: Option<OpenRole>,
        options: &PeerOptions,
        permitted: &[u64],
    ) -> Result<Option<Arc<Self>>, RaftError> {
        let Some(mut role) = role else {
            return Ok(None);
        };
        role.bootstrap_witnesses.sort_unstable();
        if permitted.is_empty()
            || permitted.len() > 64
            || permitted.contains(&0)
            || permitted.windows(2).any(|pair| pair[0] >= pair[1])
            || !permitted.contains(&options.node_id)
            || options.voters.len() > 7
            || options.voters.iter().any(|id| !permitted.contains(id))
            || role.bootstrap_witnesses.len() >= options.voters.len()
            || role
                .bootstrap_witnesses
                .windows(2)
                .any(|pair| pair[0] == pair[1])
            || role
                .bootstrap_witnesses
                .iter()
                .any(|id| !options.voters.contains(id))
            || (options.voters.contains(&options.node_id)
                && role.local_witness != role.bootstrap_witnesses.contains(&options.node_id))
        {
            return Err(invalid("invalid witness bootstrap identity"));
        }
        let record = RoleRecord {
            version: 1,
            local_role: if role.local_witness { 2 } else { 1 },
            bootstrap_witnesses: role.bootstrap_witnesses,
        };
        let encoded = record.encode_to_vec();
        let mut wire_bootstrap = Vec::new();
        prost::encoding::uint64::encode_packed(3, &record.bootstrap_witnesses, &mut wire_bootstrap);
        Ok(Some(Arc::new(Self {
            encoded,
            record,
            wire_bootstrap,
        })))
    }

    pub(super) fn can_adopt_full_history(&self) -> bool {
        self.record.local_role == 1 && self.record.bootstrap_witnesses.is_empty()
    }
    pub(super) fn local_witness(&self) -> bool {
        self.record.local_role == 2
    }
    pub(super) fn bootstrap_witnesses(&self) -> &[u64] {
        &self.record.bootstrap_witnesses
    }
    pub(super) fn wire_bootstrap(&self) -> &[u8] {
        &self.wire_bootstrap
    }
}

pub(super) fn bytes(identity: &Option<Arc<Identity>>) -> &[u8] {
    identity
        .as_ref()
        .map_or(&[], |identity| identity.encoded.as_slice())
}

pub(super) fn matches(identity: &Option<Arc<Identity>>, marker: &[u8]) -> bool {
    marker.is_empty() || marker == bytes(identity)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(path: &Path, node_id: u64, voters: &[u64]) -> PeerOptions {
        PeerOptions {
            group_id: 79,
            node_id,
            voters: voters.to_vec(),
            wal_path: path.to_path_buf(),
        }
    }

    fn open(
        path: &Path,
        node_id: u64,
        voters: &[u64],
        local_witness: bool,
        bootstrap_witnesses: &[u64],
        tuning: PeerTuning,
    ) -> Result<DurablePeer, RaftError> {
        DurablePeer::open_inner_with_role(
            options(path, node_id, voters),
            vec![1, 2, 3, 4],
            tuning,
            Some(OpenRole {
                local_witness,
                bootstrap_witnesses: bootstrap_witnesses.to_vec(),
            }),
        )
    }

    fn plain(path: &Path, node_id: u64, voters: &[u64]) -> Result<DurablePeer, RaftError> {
        DurablePeer::open_with_membership(options(path, node_id, voters), vec![1, 2, 3, 4])
    }

    fn settle(peer: &mut DurablePeer) {
        for _ in 0..32 {
            peer.process_ready(|_, _| Ok(())).unwrap();
            if !peer.has_ready() {
                return;
            }
        }
        panic!("witness identity peer did not settle");
    }

    fn records(path: &Path) -> Vec<WalBatch> {
        let data = std::fs::read(path).unwrap();
        let mut position = 0;
        let mut output = Vec::new();
        while position + 24 <= data.len() && data[position..position + 24].iter().any(|b| *b != 0) {
            let header = &data[position..position + 24];
            let size = u32::from_be_bytes(header[4..8].try_into().unwrap()) as usize;
            let wire = &data[position + 24..position + 24 + size];
            assert_eq!(
                digest(&header[..8]),
                u64::from_be_bytes(header[8..16].try_into().unwrap())
            );
            assert_eq!(
                digest(wire),
                u64::from_be_bytes(header[16..24].try_into().unwrap())
            );
            let record = WalBatch::decode(wire).unwrap();
            assert!(exact_batch_encoding(&record, wire, &mut Vec::new()));
            assert_eq!(record.encode_to_vec(), wire);
            if !record.witness_identity.is_empty() {
                let mut without_role = record.clone();
                without_role.witness_identity.clear();
                assert!(!exact_batch_encoding(&without_role, wire, &mut Vec::new()));
            }
            output.push(record);
            position += 24 + size + usize::from(&header[..4] == MAGIC_ZERO_TAIL);
        }
        output
    }

    #[test]
    fn invalid_bootstrap_roles_refuse_before_creating_storage() {
        let directory = tempfile::tempdir().unwrap();
        for (local, witnesses) in [
            (false, vec![1]),
            (true, vec![2]),
            (false, vec![2, 2]),
            (false, vec![0]),
            (false, vec![4]),
            (true, vec![1, 2, 3]),
        ] {
            let path = directory.path().join("invalid.wal");
            assert!(open(
                &path,
                1,
                &[1, 2, 3],
                local,
                &witnesses,
                PeerTuning::default()
            )
            .is_err());
            assert!(!path.exists());
            assert!(!path.with_extension("runtime.lock").exists());
        }
        assert_eq!(
            std::mem::size_of::<Option<Arc<Identity>>>(),
            std::mem::size_of::<usize>()
        );
    }

    #[test]
    fn witness_identity_survives_reopen_and_rejects_role_or_bootstrap_changes() {
        let directory = tempfile::tempdir().unwrap();
        for (node, witnesses, local) in
            [(1, vec![3], false), (3, vec![3], true), (4, vec![3], true)]
        {
            let path = directory.path().join(format!("{node}.wal"));
            let peer = open(
                &path,
                node,
                &[1, 2, 3],
                local,
                &witnesses,
                PeerTuning::default(),
            )
            .unwrap();
            let marker = bytes(&peer.store.witness_identity).to_vec();
            assert!(!marker.is_empty());
            drop(peer);
            let original = std::fs::read(&path).unwrap();
            assert!(plain(&path, node, &[1, 2, 3]).is_err());
            assert!(open(&path, node, &[1, 2, 3], false, &[2], PeerTuning::default()).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), original);
            let mut reopened = open(
                &path,
                node,
                &[1, 2, 3],
                local,
                &witnesses,
                PeerTuning::default(),
            )
            .unwrap();
            assert_eq!(bytes(&reopened.store.witness_identity), marker);
            reopened.capture_log_reader().unwrap().verify().unwrap();
            settle(&mut reopened);
            assert_eq!(records(&path)[0].witness_identity, marker);
        }
    }

    #[test]
    fn existing_full_history_can_adopt_mode_once_without_changing_entries() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("peer.wal");
        let mut peer = plain(&path, 1, &[1]).unwrap();
        peer.campaign().unwrap();
        settle(&mut peer);
        let proposal = peer.propose(b"saved".to_vec()).unwrap();
        settle(&mut peer);
        assert!(records(&path)
            .iter()
            .all(|record| record.witness_identity.is_empty()));
        drop(peer);
        let original = std::fs::read(&path).unwrap();
        let mut adopted = open(&path, 1, &[1], false, &[], PeerTuning::default()).unwrap();
        assert_eq!(adopted.journal.barriers, 1);
        assert!(std::fs::read(&path).unwrap().starts_with(&original));
        adopted.capture_log_reader().unwrap().verify().unwrap();
        assert_eq!(
            adopted
                .capture_log_reader()
                .unwrap()
                .entries(proposal.index, proposal.index + 1, None)
                .unwrap()[0]
                .data,
            b"saved"
        );
        settle(&mut adopted);
        drop(adopted);
        assert!(plain(&path, 1, &[1]).is_err());
        let reopened = open(&path, 1, &[1], false, &[], PeerTuning::default()).unwrap();
        assert_eq!(reopened.journal.barriers, 0);
        assert_eq!(
            records(&path)
                .iter()
                .filter(|record| !record.witness_identity.is_empty())
                .count(),
            1
        );
    }

    #[test]
    fn old_full_history_cannot_be_reinterpreted_as_witness_history() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("peer.wal");
        drop(plain(&path, 3, &[1, 2, 3]).unwrap());
        let original = std::fs::read(&path).unwrap();
        assert!(open(&path, 3, &[1, 2, 3], true, &[3], PeerTuning::default()).is_err());
        assert!(open(&path, 3, &[1, 2, 3], false, &[2], PeerTuning::default()).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), original);
    }

    #[test]
    fn every_torn_initial_identity_refuses_to_open() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.wal");
        drop(open(&path, 3, &[1, 2, 3], true, &[3], PeerTuning::default()).unwrap());
        let original = std::fs::read(&path).unwrap();
        let truncated = directory.path().join("truncated.wal");
        for length in 0..original.len() {
            std::fs::write(&truncated, &original[..length]).unwrap();
            assert!(
                open(&truncated, 3, &[1, 2, 3], true, &[3], PeerTuning::default()).is_err(),
                "prefix {length}"
            );
        }
        std::fs::write(&truncated, &original).unwrap();
        drop(open(&truncated, 3, &[1, 2, 3], true, &[3], PeerTuning::default()).unwrap());
        assert!(plain(&truncated, 3, &[1, 2, 3]).is_err());
    }

    #[test]
    fn checkpoint_forms_keep_identity_flushed_floors_and_old_reader_generations() {
        for form in 0..3 {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("peer.wal");
            let tuning = PeerTuning {
                wal_zero_fill_bytes: Some(1024 * 1024),
                ..Default::default()
            };
            let mut peer = open(&path, 1, &[1], false, &[], tuning).unwrap();
            peer.campaign().unwrap();
            settle(&mut peer);
            let proposal = peer.propose(b"saved".to_vec()).unwrap();
            settle(&mut peer);
            let mut old = peer.capture_log_reader().unwrap();
            peer.set_flushed_index(proposal.index).unwrap();
            let marker = bytes(&peer.store.witness_identity).to_vec();
            match form {
                0 => peer.checkpoint(b"image".to_vec()).unwrap(),
                1 => peer
                    .checkpoint(vec![7; INLINE_CHECKPOINT_BYTES + 17])
                    .unwrap(),
                _ => peer
                    .checkpoint_from_reader(&mut std::io::Cursor::new(vec![7; 65536]), 65536)
                    .unwrap(),
            }
            old.verify().unwrap();
            assert_eq!(
                old.entries(proposal.index, proposal.index + 1, None)
                    .unwrap()[0]
                    .data,
                b"saved"
            );
            let saved = records(&path);
            assert_eq!(saved[0].witness_identity, marker);
            assert_eq!(saved[0].version, [2, 3, 4][form]);
            assert!(saved
                .iter()
                .any(|record| record.flushed_index == proposal.index));
            if form == 1 {
                assert!(saved.iter().filter(|record| record.version == 3).count() > 1);
            }
            drop(old);
            drop(peer);
            assert!(plain(&path, 1, &[1]).is_err());
            let mut reopened = open(&path, 1, &[1], false, &[], tuning).unwrap();
            assert_eq!(reopened.flushed_index().unwrap(), proposal.index);
            reopened
                .process_ready(|_, _| panic!("flushed application was delivered again"))
                .unwrap();
            reopened.capture_log_reader().unwrap().verify().unwrap();
        }
    }

    #[test]
    fn decoded_and_encoded_segmented_writes_keep_the_identity_fence() {
        for encoded in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("peer.wal");
            let mut peer = open(&path, 1, &[1], false, &[], PeerTuning::default()).unwrap();
            let entries: Vec<_> = (1..=3)
                .map(|index| Entry {
                    term: 1,
                    index,
                    data: vec![index as u8; 3 * 1024 * 1024],
                    ..Default::default()
                })
                .collect();
            let hs = HardState {
                term: 1,
                vote: 1,
                commit: 3,
            };
            let marker = bytes(&peer.store.witness_identity).to_vec();
            let mut record = batch(
                &peer.options,
                Some(&hs),
                if encoded { &entries } else { &[] },
            );
            record.witness_identity = marker.clone();
            if encoded {
                peer.journal
                    .append_ready(&mut record, &HardState::default())
                    .unwrap();
            } else {
                peer.journal
                    .append_decoded_ready(&mut record, &entries, &HardState::default())
                    .unwrap();
            }
            let saved = records(&path);
            assert!(
                saved
                    .iter()
                    .filter(|record| !record.entries.is_empty())
                    .count()
                    > 1
            );
            assert!(saved.iter().all(|record| record.witness_identity == marker));
            drop(peer);
            let mut reopened = open(&path, 1, &[1], false, &[], PeerTuning::default()).unwrap();
            let mut delivered = Vec::new();
            reopened
                .process_ready(|index, value| {
                    delivered.push((index.index, value.len()));
                    Ok(())
                })
                .unwrap();
            assert_eq!(
                delivered,
                vec![
                    (1, 3 * 1024 * 1024),
                    (2, 3 * 1024 * 1024),
                    (3, 3 * 1024 * 1024)
                ]
            );
            reopened.capture_log_reader().unwrap().verify().unwrap();
        }
    }

    #[test]
    fn captured_readers_reject_conflicting_role_markers() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("peer.wal");
        let mut peer = open(&path, 1, &[1], false, &[], PeerTuning::default()).unwrap();
        peer.campaign().unwrap();
        settle(&mut peer);
        peer.propose(b"saved".to_vec()).unwrap();
        settle(&mut peer);
        let mut reader = peer.capture_log_reader().unwrap();
        reader.witness_identity = None;
        assert!(reader.verify().is_err());
        assert!(reader.scrub_step(0, u64::MAX).is_err());
        reader.index = Default::default();
        assert!(reader.entries(1, peer.applied_index() + 1, None).is_err());
    }
}
