// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

/// Immutable storage role of this replica, including while it is outside membership.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PeerRole {
    #[default]
    Data,
    Witness,
}

/// Opt-in replica roles shared by every member of a group.
/// All peers must use role-aware construction and the same bootstrap witnesses.
/// The local role cannot change when reopening the same journal.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PeerRoles {
    pub local: PeerRole,
    pub bootstrap_witnesses: Vec<u64>,
}

pub(super) const ADD_WITNESS: &[u8] = b"matrixraft-add-witness-v1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct History {
    pub(super) data: BTreeSet<u64>,
    pub(super) witnesses: BTreeSet<u64>,
    pub(super) witness_voting_disabled: bool,
}

pub(super) struct Policy {
    pub(super) promotions: BTreeSet<u64>,
    pub(super) roles: Option<Box<History>>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Record {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, repeated, packed = "true", tag = "2")]
    promotions: Vec<u64>,
    #[prost(uint64, repeated, packed = "true", tag = "3")]
    witnesses: Vec<u64>,
    #[prost(uint64, repeated, packed = "true", tag = "4")]
    data: Vec<u64>,
    #[prost(bool, tag = "5")]
    witness_voting_disabled: bool,
}

pub(super) fn encode(promotions: &BTreeSet<u64>, roles: Option<&History>) -> Vec<u8> {
    let Some(roles) = roles else {
        return membership::encode_promotions(promotions);
    };
    Record {
        version: if roles.witness_voting_disabled { 3 } else { 2 },
        promotions: promotions.iter().copied().collect(),
        witnesses: roles.witnesses.iter().copied().collect(),
        data: roles.data.iter().copied().collect(),
        witness_voting_disabled: roles.witness_voting_disabled,
    }
    .encode_to_vec()
}

pub(super) fn decode(
    bytes: &[u8],
    conf: &ConfState,
    permitted: &[u64],
    identity: &Option<Arc<witness_identity::Identity>>,
) -> Result<Policy, RaftError> {
    let Some(identity) = identity else {
        return Ok(Policy {
            promotions: membership::decode_promotions(bytes, conf)?,
            roles: None,
        });
    };
    if bytes.is_empty() {
        let roles = History::bootstrap(identity);
        roles.validate(conf, permitted, identity)?;
        return Ok(Policy {
            promotions: BTreeSet::new(),
            roles: Some(Box::new(roles)),
        });
    }
    if bytes.len() > 4096 {
        return Err(invalid("oversized membership role policy"));
    }
    let record = Record::decode(bytes).map_err(invalid)?;
    let ordered =
        |values: &[u64]| values.len() <= 64 && values.windows(2).all(|pair| pair[0] < pair[1]);
    if record.version != if record.witness_voting_disabled { 3 } else { 2 }
        || !ordered(&record.promotions)
        || !ordered(&record.witnesses)
        || !ordered(&record.data)
        || record.encode_to_vec() != bytes
        || record
            .promotions
            .iter()
            .any(|id| !conf.learners.contains(id) || record.witnesses.contains(id))
    {
        return Err(invalid("invalid membership role policy"));
    }
    let roles = History {
        data: record.data.into_iter().collect(),
        witnesses: record.witnesses.into_iter().collect(),
        witness_voting_disabled: record.witness_voting_disabled,
    };
    roles.validate(conf, permitted, identity)?;
    Ok(Policy {
        promotions: record.promotions.into_iter().collect(),
        roles: Some(Box::new(roles)),
    })
}

impl History {
    pub(super) fn extends(&self, previous: &Self) -> bool {
        previous.data.is_subset(&self.data) && previous.witnesses.is_subset(&self.witnesses)
    }
    fn bootstrap(identity: &witness_identity::Identity) -> Self {
        let witnesses: BTreeSet<_> = identity.bootstrap_witnesses().iter().copied().collect();
        Self {
            data: identity
                .bootstrap_voters()
                .iter()
                .copied()
                .filter(|id| !witnesses.contains(id))
                .collect(),
            witnesses,
            witness_voting_disabled: identity.bootstrap_voting_disabled(),
        }
    }

    pub(super) fn validate(
        &self,
        conf: &ConfState,
        permitted: &[u64],
        identity: &witness_identity::Identity,
    ) -> Result<(), RaftError> {
        if self.data.len() + self.witnesses.len() > 64
            || !self.data.is_disjoint(&self.witnesses)
            || self
                .data
                .iter()
                .chain(&self.witnesses)
                .any(|id| !permitted.contains(id))
            || identity.bootstrap_voters().iter().any(|id| {
                if identity.bootstrap_witnesses().contains(id) {
                    !self.witnesses.contains(id)
                } else {
                    !self.data.contains(id)
                }
            })
            || conf
                .voters
                .iter()
                .chain(&conf.voters_outgoing)
                .chain(&conf.learners)
                .chain(&conf.learners_next)
                .any(|id| !self.data.contains(id) && !self.witnesses.contains(id))
            || (!self.witness_voting_disabled
                && conf
                    .learners
                    .iter()
                    .chain(&conf.learners_next)
                    .any(|id| self.witnesses.contains(id)))
            || (self.witness_voting_disabled
                && conf.voters.iter().any(|id| self.witnesses.contains(id)))
            || conf.voters.iter().all(|id| !self.data.contains(id))
            || (!conf.voters_outgoing.is_empty()
                && conf
                    .voters_outgoing
                    .iter()
                    .all(|id| !self.data.contains(id)))
            || (self.data.contains(&identity.node_id()) && identity.local_witness())
            || (self.witnesses.contains(&identity.node_id()) && !identity.local_witness())
        {
            return Err(invalid(
                "membership roles conflict with durable replica identities",
            ));
        }
        Ok(())
    }

    pub(super) fn apply(
        &mut self,
        change: &raft_core::eraftpb::ConfChangeV2,
        conf: &ConfState,
    ) -> Result<(), RaftError> {
        use raft_core::eraftpb::ConfChangeType;
        if let Some(enabled) = witness_voting::target(change) {
            if change.changes != witness_voting::changes(self, conf, enabled)? {
                return Err(invalid(
                    "witness voting change does not match committed membership",
                ));
            }
            self.witness_voting_disabled = !enabled;
            return Ok(());
        }
        for member in &change.changes {
            if change.context == ADD_WITNESS {
                let kind = if self.witness_voting_disabled {
                    ConfChangeType::AddLearnerNode
                } else {
                    ConfChangeType::AddNode
                };
                if member.change_type != kind as i32 {
                    return Err(invalid("witness addition conflicts with voting policy"));
                }
                if self.data.contains(&member.node_id) {
                    return Err(invalid("a data identity cannot be reused as a witness"));
                }
                self.witnesses.insert(member.node_id);
            } else if member.change_type == ConfChangeType::AddLearnerNode as i32 {
                if self.witnesses.contains(&member.node_id) {
                    return Err(invalid(
                        "a witness identity cannot be reused as a data learner",
                    ));
                }
                self.data.insert(member.node_id);
            } else if member.change_type == ConfChangeType::AddNode as i32
                && !self.witnesses.contains(&member.node_id)
            {
                self.data.insert(member.node_id);
            }
        }
        Ok(())
    }
}

impl DurablePeer {
    pub fn open_with_roles(
        options: PeerOptions,
        permitted: Vec<u64>,
        roles: PeerRoles,
    ) -> Result<Self, RaftError> {
        Self::open_with_roles_and_tuning(options, permitted, roles, PeerTuning::default())
    }

    pub fn open_with_roles_and_tuning(
        options: PeerOptions,
        mut permitted: Vec<u64>,
        roles: PeerRoles,
        tuning: PeerTuning,
    ) -> Result<Self, RaftError> {
        permitted.sort_unstable();
        Self::open_inner_with_role(
            options,
            permitted,
            tuning,
            Some(witness_identity::OpenRole {
                local_witness: roles.local == PeerRole::Witness,
                bootstrap_witnesses: roles.bootstrap_witnesses,
            }),
        )
    }

    pub(super) fn membership_policy(&self) -> Vec<u8> {
        encode(&self.automatic_learners, self.roles.as_deref())
    }

    pub(super) fn install_membership_policy(&mut self, policy: Policy) {
        self.automatic_learners = policy.promotions;
        self.roles = policy.roles;
        self.synchronize_witness_roles();
    }

    pub(super) fn synchronize_witness_roles(&mut self) {
        let Some(roles) = &self.roles else {
            return;
        };
        if let Some(policy) = &mut self.witness_commit {
            policy.set_witnesses(&roles.witnesses);
        } else {
            self.witness_commit = witness::CommitPolicy::from_witnesses(roles.witnesses.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(node: u64, witness: bool) -> Option<Arc<witness_identity::Identity>> {
        witness_identity::Identity::from_role(
            Some(witness_identity::OpenRole {
                local_witness: witness,
                bootstrap_witnesses: vec![3],
            }),
            &PeerOptions {
                group_id: 19,
                node_id: node,
                voters: vec![1, 2, 3],
                wal_path: PathBuf::new(),
            },
            &[1, 2, 3, 4, 5, 6],
        )
        .unwrap()
    }

    fn conf() -> ConfState {
        ConfState {
            voters: vec![1, 2, 3],
            learners: vec![4],
            ..Default::default()
        }
    }

    fn record() -> Record {
        Record {
            version: 2,
            promotions: vec![4],
            witnesses: vec![3, 5],
            data: vec![1, 2, 4, 6],
            witness_voting_disabled: false,
        }
    }

    #[test]
    fn role_policy_keeps_retired_identities_and_preserves_the_default_format() {
        let record = record();
        let bytes = record.encode_to_vec();
        let policy = decode(&bytes, &conf(), &[1, 2, 3, 4, 5, 6], &identity(1, false)).unwrap();
        assert_eq!(policy.promotions, BTreeSet::from([4]));
        let roles = policy.roles.unwrap();
        assert_eq!(roles.witnesses, BTreeSet::from([3, 5]));
        assert_eq!(roles.data, BTreeSet::from([1, 2, 4, 6]));
        assert_eq!(encode(&policy.promotions, Some(&roles)), bytes);
        assert!(decode(&bytes, &conf(), &[1, 2, 3, 4, 5, 6], &identity(5, true)).is_ok());
        assert!(decode(&bytes, &conf(), &[1, 2, 3, 4, 5, 6], &identity(5, false)).is_err());
        assert!(decode(&bytes, &conf(), &[1, 2, 3, 4, 5, 6], &None).is_err());
        let legacy = encode(&policy.promotions, None);
        assert_eq!(legacy, membership::encode_promotions(&policy.promotions));
        let policy = decode(&legacy, &conf(), &[1, 2, 3, 4], &None).unwrap();
        assert!(policy.roles.is_none());
        assert_eq!(
            std::mem::size_of::<Option<Box<History>>>(),
            std::mem::size_of::<usize>()
        );
    }

    #[test]
    fn role_policy_rejects_noncanonical_uncovered_and_conflicting_histories() {
        let base = record();
        let mut invalid = Vec::new();
        for version in [0, 1, 3] {
            let mut r = base.clone();
            r.version = version;
            invalid.push(r);
        }
        for witnesses in [vec![3, 3], vec![5, 3], vec![3, 7], vec![3, 4, 5], vec![5]] {
            let mut r = base.clone();
            r.witnesses = witnesses;
            invalid.push(r);
        }
        for data in [
            vec![1, 2, 3, 4, 6],
            vec![1, 2, 6],
            vec![1, 4, 6],
            vec![2, 1, 4, 6],
            vec![1, 2, 4, 4, 6],
        ] {
            let mut r = base.clone();
            r.data = data;
            invalid.push(r);
        }
        for promotions in [vec![1], vec![4, 4], vec![5], vec![4; 65]] {
            let mut r = base.clone();
            r.promotions = promotions;
            invalid.push(r);
        }
        for r in invalid {
            assert!(decode(
                &r.encode_to_vec(),
                &conf(),
                &[1, 2, 3, 4, 5, 6],
                &identity(1, false)
            )
            .is_err());
        }
        let mut bytes = base.encode_to_vec();
        prost::encoding::uint32::encode(5, &1, &mut bytes);
        assert!(decode(&bytes, &conf(), &[1, 2, 3, 4, 5, 6], &identity(1, false)).is_err());
        for conf in [
            ConfState {
                voters: vec![3, 5],
                learners: vec![4],
                ..Default::default()
            },
            ConfState {
                voters: vec![1, 2],
                voters_outgoing: vec![3, 5],
                learners: vec![4],
                ..Default::default()
            },
        ] {
            assert!(decode(
                &base.encode_to_vec(),
                &conf,
                &[1, 2, 3, 4, 5, 6],
                &identity(1, false)
            )
            .is_err());
        }
    }

    #[test]
    fn witness_changes_require_role_support_and_joint_consensus() {
        use raft_core::eraftpb::{
            ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2,
        };
        let mut change = ConfChangeV2 {
            transition: ConfChangeTransition::Explicit as i32,
            changes: vec![ConfChangeSingle {
                node_id: 5,
                change_type: ConfChangeType::AddNode as i32,
            }],
            context: ADD_WITNESS.to_vec(),
        };
        let entry = |change: &ConfChangeV2| Entry {
            entry_type: EntryType::EntryConfChangeV2 as i32,
            data: change.encode_to_vec(),
            ..Default::default()
        };
        let permitted = [1, 2, 3, 4, 5, 6];
        assert!(membership::decode_change(&entry(&change), &permitted).is_err());
        assert!(
            membership::decode_change_for(&entry(&change), &permitted, &identity(1, false)).is_ok()
        );
        change.transition = ConfChangeTransition::Implicit as i32;
        assert!(
            membership::decode_change_for(&entry(&change), &permitted, &identity(1, false)).is_ok()
        );
        change.transition = ConfChangeTransition::Auto as i32;
        assert!(
            membership::decode_change_for(&entry(&change), &permitted, &identity(1, false))
                .is_err()
        );
        change.transition = ConfChangeTransition::Explicit as i32;
        change.changes[0].change_type = ConfChangeType::AddLearnerNode as i32;
        assert!(
            membership::decode_change_for(&entry(&change), &permitted, &identity(1, false)).is_ok()
        );
        let identity = identity(1, false).unwrap();
        assert!(History::bootstrap(&identity)
            .apply(
                &change,
                &ConfState {
                    voters: vec![1, 2, 3],
                    ..Default::default()
                }
            )
            .is_err());
    }
}
