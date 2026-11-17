// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

/// Admission settings for one proposal or one atomic proposal batch.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PeerProposalOptions {
    /// Admit only while leading in this exact term. None accepts the current term.
    pub expected_term: Option<u64>,
    /// Sync the committed position before exposing these entries to application.
    pub durable_command: bool,
}

const COMMAND_CONTEXT: &[u8] = b"matrixraft-command-v1";

pub(super) fn is_command(entry: &Entry) -> bool {
    entry.entry_type == EntryType::EntryNormal as i32 && entry.context == COMMAND_CONTEXT
}

impl PeerProposalOptions {
    pub(super) fn context(self) -> Vec<u8> {
        if self.durable_command {
            COMMAND_CONTEXT.to_vec()
        } else {
            Vec::new()
        }
    }
}

impl DurablePeer {
    pub(super) fn sync_application_commands(&mut self, entries: &[Entry]) -> Result<(), RaftError> {
        let Some(index) = entries
            .iter()
            .rev()
            .find(|entry| is_command(entry))
            .map(|entry| entry.index)
        else {
            return Ok(());
        };
        if index <= self.command_commit_synced {
            return Ok(());
        }
        // Storage installs a hard state only after its journal record is written.
        // Commit-only records may still be unsynced; sync before application escapes.
        let commit = self
            .store
            .initial_state()
            .map_err(storage_error)?
            .hard_state
            .commit;
        if commit < index {
            return Err(storage_error(
                "command application exceeds journaled commitment",
            ));
        }
        self.journal.file.sync_all().map_err(storage_error)?;
        self.journal.barriers = self.journal.barriers.saturating_add(1);
        self.command_commit_synced = commit;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(path: PathBuf) -> PeerOptions {
        PeerOptions {
            group_id: 29,
            node_id: 1,
            voters: vec![1],
            wal_path: path,
        }
    }
    fn command() -> PeerProposalOptions {
        PeerProposalOptions {
            durable_command: true,
            ..Default::default()
        }
    }
    fn leader(path: PathBuf, tuning: PeerTuning) -> DurablePeer {
        let mut peer = DurablePeer::open_with_tuning(options(path), tuning).unwrap();
        peer.campaign().unwrap();
        if peer.apply_pipeline.is_some() {
            for _ in 0..8 {
                peer.process_ready_async_apply(|_, _, _| Ok(RestoreDecision::Restored))
                    .unwrap();
                for task in peer.take_application_tasks().unwrap() {
                    peer.complete_application_task(task.finish()).unwrap();
                }
            }
        } else {
            peer.process_ready(|_, _| Ok(())).unwrap();
        }
        peer
    }
    fn records(path: &Path) -> Vec<WalBatch> {
        let bytes = std::fs::read(path).unwrap();
        let mut offset = 0;
        let mut records = Vec::new();
        while offset < bytes.len() {
            assert_eq!(&bytes[offset..offset + 4], MAGIC);
            let size =
                u32::from_be_bytes(bytes[offset + 4..offset + 8].try_into().unwrap()) as usize;
            let wire = &bytes[offset + 24..offset + 24 + size];
            records.push(WalBatch::decode(wire).unwrap());
            offset += 24 + size;
        }
        records
    }
    fn journal_commit(path: &Path) -> u64 {
        records(path)
            .iter()
            .filter(|record| !record.hard_state.is_empty())
            .map(|record| {
                HardState::decode(record.hard_state.as_slice())
                    .unwrap()
                    .commit
            })
            .max()
            .unwrap_or(0)
    }

    #[test]
    fn command_admission_preserves_term_fences_limits_and_default_entries() {
        let dir = tempfile::tempdir().unwrap();
        let mut peer = leader(
            dir.path().join("peer.wal"),
            PeerTuning {
                max_pending_proposal_receipts: Some(2),
                ..Default::default()
            },
        );
        let before = peer.raw.raft.raft_log.last_index();
        let stale = PeerProposalOptions {
            expected_term: Some(peer.term() + 1),
            ..command()
        };
        assert!(peer.propose_with_options(stale, vec![1]).is_err());
        assert!(peer
            .propose_batch_with_options(stale, vec![vec![1]])
            .is_err());
        assert!(peer
            .propose_tracked_with_options(stale, vec![1], None)
            .is_err());
        assert!(peer
            .propose_batch_tracked_with_options(command(), vec![vec![1]; 3], None)
            .is_err());
        assert!(peer.propose_with_options(command(), Vec::new()).is_err());
        assert!(peer
            .propose_batch_with_options(command(), vec![Vec::new()])
            .is_err());
        assert_eq!(peer.raw.raft.raft_log.last_index(), before);
        let receipts = peer
            .propose_batch_tracked_with_options(
                PeerProposalOptions {
                    expected_term: Some(peer.term()),
                    ..command()
                },
                vec![vec![1], vec![2]],
                None,
            )
            .unwrap();
        assert_eq!(receipts[1].position().index, before + 2);
        assert!(peer
            .propose_tracked_with_options(command(), vec![3], None)
            .is_err());
        peer.process_ready_batch(|batch| {
            assert!(batch.iter().all(|entry| entry.is_command));
            Ok(batch.len())
        })
        .unwrap();
        assert!(receipts
            .iter()
            .all(|receipt| receipt.status() == PeerProposalStatus::Applied));
        peer.propose(vec![4]).unwrap();
        peer.process_ready_batch(|batch| {
            assert!(batch.iter().all(|entry| !entry.is_command));
            Ok(batch.len())
        })
        .unwrap();
    }

    #[test]
    fn command_batches_sync_the_commit_before_callbacks_and_deduplicate_retries() {
        let dir = tempfile::tempdir().unwrap();
        for durable in [false, true] {
            let path = dir.path().join(format!("{durable}.wal"));
            let mut peer = leader(path.clone(), PeerTuning::default());
            let barriers = peer.persisted_wal_barriers().unwrap();
            let positions = peer
                .propose_batch_with_options(
                    PeerProposalOptions {
                        durable_command: durable,
                        ..Default::default()
                    },
                    vec![vec![1], vec![2]],
                )
                .unwrap();
            let mut callbacks = 0;
            peer.process_ready_batch(|batch| {
                callbacks += 1;
                assert!(batch.iter().all(|entry| entry.is_command == durable));
                assert!(journal_commit(&path) >= positions[1].index);
                Ok(0)
            })
            .unwrap();
            assert_eq!(callbacks, 1);
            assert_eq!(
                peer.persisted_wal_barriers().unwrap() - barriers,
                if durable { 2 } else { 1 }
            );
            let barriers = peer.persisted_wal_barriers().unwrap();
            peer.process_ready(|_, _| Ok(())).unwrap();
            assert_eq!(peer.persisted_wal_barriers().unwrap(), barriers);
            assert_eq!(peer.applied_index(), positions[1].index);
        }
    }

    fn blocked_command(path: PathBuf) -> (DurablePeer, PeerApplyTask, PeerProposalReceipt) {
        let mut peer = leader(
            path,
            PeerTuning {
                max_inflight_apply_tasks: Some(1),
                max_pending_proposal_receipts: Some(4),
                ..Default::default()
            },
        );
        peer.propose(vec![7]).unwrap();
        for _ in 0..8 {
            peer.process_ready_async_apply(|_, _, _| Ok(RestoreDecision::Restored))
                .unwrap();
        }
        let mut tasks = peer.take_application_tasks().unwrap();
        assert_eq!(tasks.len(), 1);
        let prior = tasks.pop().unwrap();
        let receipt = peer
            .propose_tracked_with_options(command(), vec![8], None)
            .unwrap();
        for _ in 0..8 {
            peer.process_ready_async_apply(|_, _, _| Ok(RestoreDecision::Restored))
                .unwrap();
        }
        assert!(peer.command_commit_synced < receipt.position().index);
        assert!(peer.commit_index() >= receipt.position().index);
        assert!(peer.take_application_tasks().unwrap().is_empty());
        (peer, prior, receipt)
    }

    #[test]
    fn worker_commands_sync_before_issuance_after_capacity_is_released() {
        let dir = tempfile::tempdir().unwrap();
        let (mut peer, prior, receipt) = blocked_command(dir.path().join("peer.wal"));
        peer.complete_application_task(prior.finish()).unwrap();
        let barriers = peer.persisted_wal_barriers().unwrap();
        peer.process_ready_async_apply(|_, _, _| Ok(RestoreDecision::Restored))
            .unwrap();
        assert_eq!(peer.persisted_wal_barriers().unwrap(), barriers + 1);
        assert!(peer.command_commit_synced >= receipt.position().index);
        let mut tasks = peer.take_application_tasks().unwrap();
        assert_eq!(tasks.len(), 1);
        assert!(tasks[0].entries().get(0).unwrap().is_command);
        assert_eq!(receipt.status(), PeerProposalStatus::Pending);
        peer.complete_application_task(tasks.pop().unwrap().finish())
            .unwrap();
        assert_eq!(receipt.status(), PeerProposalStatus::Applied);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn command_barrier_failure_issues_no_worker_task_and_fails_receipts() {
        let dir = tempfile::tempdir().unwrap();
        let (mut peer, prior, receipt) = blocked_command(dir.path().join("peer.wal"));
        peer.complete_application_task(prior.finish()).unwrap();
        let original = std::mem::replace(&mut peer.journal.file, File::open("/dev/null").unwrap());
        assert!(peer
            .process_ready_async_apply(|_, _, _| Ok(RestoreDecision::Restored))
            .is_err());
        peer.journal.file = original;
        assert!(peer.failed);
        assert_eq!(receipt.status(), PeerProposalStatus::Failed);
        assert!(peer.applied_index() < receipt.position().index);
    }

    #[test]
    fn command_packets_and_records_require_explicit_format_fences() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = options(dir.path().join("peer.wal"));
        config.node_id = 2;
        config.voters = vec![1, 2, 3];
        let mut peer = DurablePeer::open(config.clone()).unwrap();
        let message = Message {
            from: 1,
            to: 2,
            term: 1,
            msg_type: MessageType::MsgAppend as i32,
            entries: vec![Entry {
                index: 1,
                term: 1,
                data: vec![9],
                context: command().context(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let bytes = peer.persisted_wal_bytes().unwrap();
        assert!(peer
            .step(&encode_envelope(1, config.group_id, &message).unwrap())
            .is_err());
        assert_eq!(peer.term(), 0);
        assert_eq!(peer.persisted_wal_bytes().unwrap(), bytes);
        peer.step(&encode_envelope(7, config.group_id, &message).unwrap())
            .unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
        let record = batch(
            &config,
            Some(&HardState {
                term: 1,
                commit: 1,
                ..Default::default()
            }),
            &message.entries,
        );
        let wire = record.encode_to_vec();
        let mut older_shape = record.clone();
        older_shape.durable_commands = false;
        assert!(!exact_batch_encoding(&older_shape, &wire, &mut Vec::new()));
        assert!(install_recovered_entries(&peer.store, older_shape).is_err());
        let mut ordinary = message;
        ordinary.entries[0].context.clear();
        assert!(peer
            .step(&encode_envelope(7, config.group_id, &ordinary).unwrap())
            .is_err());
        drop(peer);
        assert!(DurablePeer::open(config).is_ok());
    }

    #[test]
    fn command_history_survives_segmented_writes_and_truncated_tail_frames() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.wal");
        let tuning = PeerTuning {
            max_uncommitted_size: 32 * 1024 * 1024,
            ..Default::default()
        };
        let mut peer = leader(path.clone(), tuning);
        for value in 1..=3 {
            peer.propose_with_options(command(), vec![value; 3 * 1024 * 1024])
                .unwrap();
        }
        peer.process_ready_batch(|batch| {
            assert!(batch.iter().all(|entry| entry.is_command));
            Ok(batch.len())
        })
        .unwrap();
        drop(peer);
        let bytes = std::fs::read(&path).unwrap();
        let mut cuts = BTreeSet::from([bytes.len()]);
        let mut offset = 0;
        while offset < bytes.len() {
            let size =
                u32::from_be_bytes(bytes[offset + 4..offset + 8].try_into().unwrap()) as usize;
            let end = offset + 24 + size;
            if offset > 0 {
                cuts.extend([offset, offset + 23, end - 1, end]);
            }
            offset = end;
        }
        for cut in cuts {
            let target = dir.path().join(format!("{cut}.wal"));
            std::fs::write(&target, &bytes[..cut]).unwrap();
            let mut reopened = DurablePeer::open_with_tuning(options(target), tuning).unwrap();
            let mut count = 0;
            reopened
                .process_ready_batch(|batch| {
                    for entry in batch.iter().filter(|entry| !entry.payload.is_empty()) {
                        assert!(entry.is_command);
                        count += 1;
                    }
                    Ok(batch.len())
                })
                .unwrap();
            if cut == bytes.len() {
                assert_eq!(count, 3);
            }
        }
    }

    #[test]
    fn checkpoint_rewrite_preserves_command_suffix_and_cold_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("peer.wal");
        let tuning = PeerTuning {
            max_log_cache_bytes: Some(1),
            ..Default::default()
        };
        let mut peer = leader(path.clone(), tuning);
        let prepared = peer
            .prepare_checkpoint()
            .unwrap()
            .capture(&mut &b"image"[..], 5)
            .unwrap();
        peer.propose_batch_with_options(
            command(),
            vec![vec![31; 4096], vec![32; 4096], vec![33; 4096]],
        )
        .unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
        assert!(peer.install_checkpoint(&prepared).unwrap());
        drop(prepared);
        assert!(records(&path)
            .iter()
            .filter(|record| !record.entries.is_empty())
            .all(|record| record.durable_commands));
        drop(peer);
        let mut peer = DurablePeer::open_with_tuning(options(path), tuning).unwrap();
        let mut count = 0;
        peer.process_ready_batch_with_restore(
            |batch| {
                for entry in batch.iter() {
                    assert!(entry.is_command);
                    count += 1;
                }
                Ok(batch.len())
            },
            |_, reader, length| {
                let mut bytes = Vec::new();
                reader.read_to_end(&mut bytes).unwrap();
                assert_eq!(length, 5);
                assert_eq!(bytes, b"image");
                Ok(RestoreDecision::Restored)
            },
        )
        .unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn commands_keep_their_contract_in_zero_filled_journals_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("peer.wal");
        let mut peer = leader(
            path.clone(),
            PeerTuning {
                wal_zero_fill_bytes: Some(4096),
                max_pending_proposal_receipts: Some(4),
                ..Default::default()
            },
        );
        let receipts = peer
            .propose_batch_tracked_with_options(command(), vec![vec![41], vec![42]], None)
            .unwrap();
        peer.process_ready_batch(|batch| {
            assert!(batch.iter().all(|entry| entry.is_command));
            Ok(batch.len())
        })
        .unwrap();
        assert!(receipts
            .iter()
            .all(|receipt| receipt.status() == PeerProposalStatus::Applied));
        let logical = peer.persisted_wal_bytes().unwrap();
        assert!(std::fs::metadata(&path).unwrap().len() > logical);
        assert_eq!(&std::fs::read(&path).unwrap()[..4], MAGIC_ZERO_TAIL);
        drop(peer);
        let mut peer = DurablePeer::open(options(path)).unwrap();
        let mut values = Vec::new();
        peer.process_ready_batch(|batch| {
            for entry in batch.iter().filter(|entry| !entry.payload.is_empty()) {
                assert!(entry.is_command);
                values.push(entry.payload.to_vec());
            }
            Ok(batch.len())
        })
        .unwrap();
        assert_eq!(values, vec![vec![41], vec![42]]);
    }
}
