// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! MatrixRaft-style leader and follower lease helpers.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

use crate::{NodeId, ReplicaRole, Term};

pub type LeaseEpochId = u64;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeasePeer {
    pub node_id: NodeId,
    pub role: ReplicaRole,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeaderLeaseStatus {
    pub in_lease: bool,
    pub lease_end_ms: Option<u64>,
    pub quorum_size: usize,
    pub voting_peer_count: usize,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PeerLeaseInfo {
    role: ReplicaRole,
    max_send_epoch_ms: LeaseEpochId,
    lease_end_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeaderLease {
    peer_id: NodeId,
    lease_duration_ms: u64,
    current_term: Term,
    peers: BTreeMap<NodeId, PeerLeaseInfo>,
    last_active_lease_end_ms: Option<u64>,
    have_new_confirm: bool,
}

impl LeaderLease {
    pub fn new(peer_id: NodeId, lease_duration_ms: u64) -> Self {
        Self {
            peer_id,
            lease_duration_ms,
            current_term: 0,
            peers: BTreeMap::new(),
            last_active_lease_end_ms: None,
            have_new_confirm: false,
        }
    }

    pub fn reset(&mut self, new_term: Term) {
        self.current_term = new_term;
        self.have_new_confirm = false;
        for info in self.peers.values_mut() {
            info.max_send_epoch_ms = 0;
            info.lease_end_ms = None;
        }
    }

    pub fn update_members<I>(&mut self, peers: I)
    where
        I: IntoIterator<Item = LeasePeer>,
    {
        let mut live = BTreeSet::new();
        for peer in peers {
            live.insert(peer.node_id);
            let reset_role = self
                .peers
                .get(&peer.node_id)
                .map(|info| self.need_reset_node_role(info.role, peer.role))
                .unwrap_or(true);
            if reset_role {
                self.peers.insert(
                    peer.node_id,
                    PeerLeaseInfo {
                        role: peer.role,
                        max_send_epoch_ms: 0,
                        lease_end_ms: None,
                    },
                );
            } else if let Some(info) = self.peers.get_mut(&peer.node_id) {
                info.role = peer.role;
            }
        }
        self.peers.retain(|node_id, _| live.contains(node_id));
    }

    pub fn epoch_id_from_send_time(send_time_ms: u64) -> LeaseEpochId {
        send_time_ms
    }

    pub fn on_recv_lease_confirm(
        &mut self,
        term: Term,
        from: NodeId,
        epoch_id: LeaseEpochId,
        lease_duration_ms: u64,
    ) -> bool {
        // A confirmation carrying a term we have moved past, or one we appear
        // to have sent ourselves, is a message to drop -- the same answer this
        // function already gives for a peer it does not know and for an epoch
        // it has already seen, a few lines below.
        if term != self.current_term || from == self.peer_id {
            return false;
        }
        let Some(info) = self.peers.get_mut(&from) else {
            return false;
        };
        if epoch_id <= info.max_send_epoch_ms {
            return false;
        }

        info.max_send_epoch_ms = epoch_id;
        let new_end = epoch_id.saturating_add(lease_duration_ms);
        if info.lease_end_ms.map(|old| new_end > old).unwrap_or(true) {
            info.lease_end_ms = Some(new_end);
        }
        self.have_new_confirm = true;
        true
    }

    pub fn in_lease(&mut self, term: Term, now_ms: u64) -> bool {
        self.status(term, now_ms).in_lease
    }

    pub fn status(&mut self, term: Term, now_ms: u64) -> LeaderLeaseStatus {
        // No lease is held for a term that is not the current one. Reported
        // rather than asserted: this is reachable from any caller holding a
        // stale term, which in Raft is ordinary.
        if term != self.current_term {
            return self.status_for_mismatched_term();
        }
        if self
            .last_active_lease_end_ms
            .map(|lease_end| now_ms < lease_end)
            .unwrap_or(false)
        {
            return self.status_with_reason(now_ms, "active");
        }
        if self.maybe_renew_lease_point(now_ms) {
            return self.status_with_reason(now_ms, "renewed");
        }
        self.status_with_reason(now_ms, "insufficient_confirmations")
    }

    pub fn last_active_lease_end_ms(&self) -> Option<u64> {
        self.last_active_lease_end_ms
    }

    fn maybe_renew_lease_point(&mut self, now_ms: u64) -> bool {
        if self.peers.len() > 1 && !self.have_new_confirm {
            return false;
        }
        self.last_active_lease_end_ms = self.valid_lease_end_ms(now_ms);
        self.have_new_confirm = false;
        true
    }

    fn valid_lease_end_ms(&self, now_ms: u64) -> Option<u64> {
        let mut end_times = Vec::new();
        for (node_id, info) in &self.peers {
            if !info.role.participates_in_quorum() {
                continue;
            }
            if *node_id == self.peer_id {
                end_times.push(now_ms.saturating_add(self.lease_duration_ms));
            } else {
                end_times.push(info.lease_end_ms.unwrap_or(0));
            }
        }
        if end_times.is_empty() {
            return None;
        }
        end_times.sort_unstable();
        let quorum = end_times.len() / 2 + 1;
        let safe_idx = end_times.len() - quorum;
        Some(end_times[safe_idx])
    }

    /// Status for a term this lease does not track: never in lease, and with
    /// no lease end to report. The quorum figures still describe the current
    /// peer set, so a caller inspecting the status can tell the two apart.
    fn status_for_mismatched_term(&self) -> LeaderLeaseStatus {
        let voting_peer_count = self
            .peers
            .values()
            .filter(|info| info.role.participates_in_quorum())
            .count();
        LeaderLeaseStatus {
            in_lease: false,
            lease_end_ms: None,
            quorum_size: voting_peer_count / 2 + 1,
            voting_peer_count,
            reason: "term_mismatch".to_string(),
        }
    }

    fn status_with_reason(&self, now_ms: u64, reason: &str) -> LeaderLeaseStatus {
        let voting_peer_count = self
            .peers
            .values()
            .filter(|info| info.role.participates_in_quorum())
            .count();
        LeaderLeaseStatus {
            in_lease: self
                .last_active_lease_end_ms
                .map(|lease_end| now_ms < lease_end)
                .unwrap_or(false),
            lease_end_ms: self.last_active_lease_end_ms,
            quorum_size: voting_peer_count / 2 + 1,
            voting_peer_count,
            reason: reason.to_string(),
        }
    }

    fn need_reset_node_role(&self, before: ReplicaRole, after: ReplicaRole) -> bool {
        before != after
            && (!before.participates_in_quorum() || !after.participates_in_quorum())
            && (before == ReplicaRole::Learner || after == ReplicaRole::Learner)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FollowerLease {
    peer_id: NodeId,
    lease_duration_ms: u64,
    current_term: Term,
    received_lease_end_ms: Option<u64>,
    max_received_epoch_id: LeaseEpochId,
}

impl FollowerLease {
    pub fn new(
        peer_id: NodeId,
        lease_duration_ms: u64,
        last_lease_duration_ms: u64,
        now_ms: u64,
    ) -> Self {
        Self {
            peer_id,
            lease_duration_ms,
            current_term: 0,
            received_lease_end_ms: (last_lease_duration_ms > 0)
                .then_some(now_ms.saturating_add(last_lease_duration_ms)),
            max_received_epoch_id: 0,
        }
    }

    pub fn reset(&mut self, new_term: Term) {
        self.current_term = new_term;
        self.max_received_epoch_id = 0;
    }

    pub fn in_lease(&self, term: Term, now_ms: u64) -> bool {
        // Not in lease for a term that is not the current one.
        if term != self.current_term {
            return false;
        }
        self.received_lease_end_ms
            .map(|lease_end| now_ms < lease_end)
            .unwrap_or(false)
    }

    pub fn on_recv_lease_item(&mut self, term: Term, epoch_id: LeaseEpochId, now_ms: u64) -> bool {
        // A stale term is a rejected message, exactly as a stale epoch is on
        // the very next line.
        if term != self.current_term {
            return false;
        }
        if epoch_id <= self.max_received_epoch_id {
            return false;
        }
        self.max_received_epoch_id = epoch_id;
        self.received_lease_end_ms = Some(now_ms.saturating_add(self.lease_duration_ms));
        true
    }

    /// Highest lease epoch met for `term`. Nothing has been met for a term
    /// this lease does not track, which is the initial value anyway.
    pub fn max_met_epoch_id(&self, term: Term) -> LeaseEpochId {
        if term != self.current_term {
            return 0;
        }
        self.max_received_epoch_id
    }

    pub fn received_lease_end_ms(&self) -> Option<u64> {
        self.received_lease_end_ms
    }

    pub fn lease_duration_ms(&self) -> u64 {
        self.lease_duration_ms
    }

    pub fn peer_id(&self) -> NodeId {
        self.peer_id
    }
}
