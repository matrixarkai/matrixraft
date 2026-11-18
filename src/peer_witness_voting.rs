// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use raft_core::eraftpb::{ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2};

const ENABLE: &[u8] = b"matrixraft-witness-voting-on-v1";
const DISABLE: &[u8] = b"matrixraft-witness-voting-off-v1";

pub(super) fn target(change: &ConfChangeV2) -> Option<bool> {
    if change.context == ENABLE {
        Some(true)
    } else if change.context == DISABLE {
        Some(false)
    } else {
        None
    }
}

pub(super) fn valid_shape(change: &ConfChangeV2) -> bool {
    let Some(enabled) = target(change) else {
        return false;
    };
    (1..=7).contains(&change.changes.len())
        && change
            .changes
            .windows(2)
            .all(|pair| pair[0].node_id < pair[1].node_id)
        && matches!(change.transition, x if x == ConfChangeTransition::Explicit as i32 || x == ConfChangeTransition::Implicit as i32)
        && change.changes.iter().all(|member| {
            member.change_type == ConfChangeType::AddNode as i32
                || (!enabled && member.change_type == ConfChangeType::AddLearnerNode as i32)
        })
}

pub(super) fn changes(
    history: &membership_roles::History,
    conf: &ConfState,
    enabled: bool,
) -> Result<Vec<ConfChangeSingle>, RaftError> {
    if !conf.voters_outgoing.is_empty() {
        return Err(invalid("witness voting requires settled membership"));
    }
    let anchor = conf
        .voters
        .iter()
        .filter(|id| history.data.contains(id))
        .min()
        .copied()
        .ok_or_else(|| invalid("witness voting requires a data voter"))?;
    let witnesses = if enabled {
        &conf.learners
    } else {
        &conf.voters
    };
    let mut changes: Vec<_> = witnesses
        .iter()
        .copied()
        .filter(|id| history.witnesses.contains(id))
        .map(|node_id| ConfChangeSingle {
            node_id,
            change_type: if enabled {
                ConfChangeType::AddNode
            } else {
                ConfChangeType::AddLearnerNode
            } as i32,
        })
        .collect();
    if enabled && conf.voters.len() + changes.len() > 7 {
        return Err(invalid("witness voting exceeds voter capacity"));
    }
    // A stable data identity also carries policy changes in groups without witnesses.
    changes.push(ConfChangeSingle {
        node_id: anchor,
        change_type: ConfChangeType::AddNode as i32,
    });
    changes.sort_unstable_by_key(|member| member.node_id);
    Ok(changes)
}

pub(super) fn proposal(
    history: &membership_roles::History,
    conf: &ConfState,
    enabled: bool,
    automatic: bool,
) -> Result<ConfChangeV2, RaftError> {
    Ok(ConfChangeV2 {
        context: if enabled { ENABLE } else { DISABLE }.to_vec(),
        transition: if automatic {
            ConfChangeTransition::Implicit
        } else {
            ConfChangeTransition::Explicit
        } as i32,
        changes: changes(history, conf, enabled)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn history() -> membership_roles::History {
        membership_roles::History {
            data: BTreeSet::from([1, 2]),
            witnesses: BTreeSet::from([3]),
            witness_voting_disabled: false,
        }
    }
    fn conf() -> ConfState {
        ConfState {
            voters: vec![1, 2, 3],
            ..Default::default()
        }
    }
    fn identity() -> Option<Arc<witness_identity::Identity>> {
        witness_identity::Identity::from_role(
            Some(witness_identity::OpenRole {
                local_witness: false,
                bootstrap_witnesses: vec![3],
            }),
            &PeerOptions {
                group_id: 37,
                node_id: 1,
                voters: vec![1, 2, 3],
                wal_path: PathBuf::new(),
            },
            &[1, 2, 3, 4, 5, 6, 7, 8],
        )
        .unwrap()
    }

    #[test]
    fn voting_policy_encodes_only_disabled_state_in_the_extended_format() {
        let mut history = history();
        let before = membership_roles::encode(&BTreeSet::new(), Some(&history));
        assert_eq!(&before[..2], &[8, 2]);
        let change = proposal(&history, &conf(), false, true).unwrap();
        history.apply(&change, &conf()).unwrap();
        let conf = ConfState {
            voters: vec![1, 2],
            learners: vec![3],
            ..Default::default()
        };
        let bytes = membership_roles::encode(&BTreeSet::new(), Some(&history));
        assert_eq!(&bytes[..2], &[8, 3]);
        let recovered =
            membership_roles::decode(&bytes, &conf, &[1, 2, 3, 4, 5, 6, 7, 8], &identity())
                .unwrap();
        assert!(recovered.roles.unwrap().witness_voting_disabled);
        assert!(membership_roles::decode(&bytes, &conf, &[1, 2, 3], &None).is_err());
        let mut wrong_version = bytes.clone();
        wrong_version[1] = 2;
        assert!(membership_roles::decode(&wrong_version, &conf, &[1, 2, 3], &identity()).is_err());
        assert!(membership_roles::decode(
            &bytes[..bytes.len() - 2],
            &conf,
            &[1, 2, 3],
            &identity()
        )
        .is_err());
        let promotion = membership_roles::encode(&BTreeSet::from([3]), Some(&history));
        assert!(membership_roles::decode(&promotion, &conf, &[1, 2, 3], &identity()).is_err());
        let change = proposal(&history, &conf, true, false).unwrap();
        history.apply(&change, &conf).unwrap();
        assert_eq!(
            membership_roles::encode(&BTreeSet::new(), Some(&history)),
            before
        );
    }

    #[test]
    fn voting_changes_require_complete_ordered_joint_transitions() {
        let history = history();
        let change = proposal(&history, &conf(), false, false).unwrap();
        assert!(valid_shape(&change));
        assert_eq!(
            change
                .changes
                .iter()
                .map(|member| member.node_id)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
        for mutate in 0..5 {
            let mut invalid = change.clone();
            match mutate {
                0 => invalid.changes.clear(),
                1 => invalid.changes.reverse(),
                2 => invalid.changes[1].node_id = 1,
                3 => invalid.transition = ConfChangeTransition::Auto as i32,
                _ => invalid.changes[1].change_type = ConfChangeType::RemoveNode as i32,
            }
            assert!(!valid_shape(&invalid));
        }
        let mut incomplete = change.clone();
        incomplete.changes.pop();
        assert!(valid_shape(&incomplete));
        let mut next = history.clone();
        assert!(next.apply(&incomplete, &conf()).is_err());
        assert_eq!(next, history);
        let entry = Entry {
            entry_type: EntryType::EntryConfChangeV2 as i32,
            data: change.encode_to_vec(),
            ..Default::default()
        };
        assert!(membership::decode_change(&entry, &[1, 2, 3]).is_err());
        assert!(membership::decode_change_for(&entry, &[1, 2, 3], &identity()).is_ok());
    }

    #[test]
    fn reenabling_voting_respects_capacity_and_no_witness_groups_persist_policy() {
        let mut history = history();
        history.witnesses = (3..=8).collect();
        history.witness_voting_disabled = true;
        let conf = ConfState {
            voters: vec![1, 2],
            learners: (3..=8).collect(),
            ..Default::default()
        };
        assert!(proposal(&history, &conf, true, true).is_err());
        let repeated = proposal(&history, &conf, false, true).unwrap();
        assert_eq!(repeated.changes.len(), 1);
        history.apply(&repeated, &conf).unwrap();
        assert!(history.witness_voting_disabled);
        history.witnesses.clear();
        history.witness_voting_disabled = false;
        let conf = ConfState {
            voters: vec![1, 2],
            ..Default::default()
        };
        let change = proposal(&history, &conf, false, true).unwrap();
        assert_eq!(change.changes.len(), 1);
        history.apply(&change, &conf).unwrap();
        assert!(history.witness_voting_disabled);
        let change = proposal(&history, &conf, true, true).unwrap();
        history.apply(&change, &conf).unwrap();
        assert!(!history.witness_voting_disabled);
    }

    #[test]
    fn ordinary_membership_refuses_voting_policy_changes_before_appending() {
        let directory = tempfile::tempdir().unwrap();
        let mut peer = DurablePeer::open_with_membership(
            PeerOptions {
                group_id: 41,
                node_id: 1,
                voters: vec![1],
                wal_path: directory.path().join("peer.wal"),
            },
            vec![1, 2],
        )
        .unwrap();
        peer.campaign().unwrap();
        peer.process_ready(|_, _| Ok(())).unwrap();
        let before = peer.raw.raft.raft_log.last_index();
        for enabled in [false, true] {
            assert!(peer
                .propose_membership_auto(MembershipChange::SetWitnessVoting(enabled))
                .is_err());
            assert_eq!(peer.raw.raft.raft_log.last_index(), before);
        }
        assert!(peer.roles.is_none());
        let path = directory.path().join("unsupported-bootstrap.wal");
        assert!(DurablePeer::open_with_tuning(
            PeerOptions {
                group_id: 42,
                node_id: 1,
                voters: vec![1],
                wal_path: path.clone()
            },
            PeerTuning {
                bootstrap_witness_voting_disabled: true,
                ..Default::default()
            }
        )
        .is_err());
        assert!(!path.exists());
        assert!(!path.with_extension("runtime.lock").exists());
    }
}
