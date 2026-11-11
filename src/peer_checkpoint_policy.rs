// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;
use std::sync::Weak;

pub(super) struct Request {
    index: u64,
}

pub(super) struct Policy {
    limit: Option<u64>,
    total: u128,
    published: u128,
    request: Weak<Request>,
}
impl Policy {
    pub(super) fn new(limit: Option<u64>) -> Self {
        Self {
            limit,
            total: 0,
            published: 0,
            request: Weak::new(),
        }
    }
    pub(super) fn charge(&mut self, entries: &[Entry]) {
        if self.limit.is_some() {
            // Index and entry-size bounds keep a full peer lifetime within u128.
            self.total += entries
                .iter()
                .map(|entry| entry.encoded_len() as u128)
                .sum::<u128>();
        }
    }
    pub(super) fn total(&self) -> u128 {
        self.total
    }
    pub(super) fn due(&self) -> bool {
        self.limit
            .is_some_and(|limit| self.total - self.published >= u128::from(limit))
            && self.request.upgrade().is_none()
    }
    pub(super) fn requested(&mut self, index: u64) -> Arc<Request> {
        let request = Arc::new(Request { index });
        self.request = Arc::downgrade(&request);
        request
    }
    pub(super) fn published(&mut self, index: u64, total: u128) {
        self.published = total;
        if self
            .request
            .upgrade()
            .is_some_and(|request| request.index <= index)
        {
            self.request = Weak::new();
        }
    }
    pub(super) fn restored(&mut self) {
        self.total = 0;
        self.published = 0;
        self.request = Weak::new();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry() -> Entry {
        Entry {
            term: 1,
            index: 1,
            data: vec![7; 32],
            ..Default::default()
        }
    }

    #[test]
    fn disabled_policy_does_not_charge_or_request_work() {
        let mut policy = Policy::new(None);
        policy.charge(&[entry()]);
        assert_eq!(policy.total(), 0);
        assert!(!policy.due());
    }

    #[test]
    fn only_consumed_application_prefixes_add_to_the_capture_budget() {
        let root = tempfile::tempdir().unwrap();
        let mut peer = DurablePeer::open_with_tuning(
            PeerOptions {
                group_id: 17,
                node_id: 1,
                voters: vec![1],
                wal_path: root.path().join("peer.wal"),
            },
            PeerTuning {
                checkpoint_after_bytes: Some(1),
                max_apply_bytes: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
        peer.campaign().unwrap();
        while peer.has_ready() {
            peer.process_ready(|_, _| Ok(())).unwrap();
        }
        let before = peer.checkpoint_policy.total();
        peer.checkpoint(Vec::new()).unwrap();
        let positions = peer.propose_batch(vec![vec![7], vec![8], vec![9]]).unwrap();
        let mut consumed = false;
        for _ in 0..8 {
            if consumed {
                break;
            }
            peer.process_ready_batch(|batch| {
                consumed = !batch.is_empty();
                Ok(batch.len().min(1))
            })
            .unwrap();
        }
        assert!(consumed);
        let entries = peer
            .capture_log_reader()
            .unwrap()
            .entries(
                positions[0].index,
                positions.last().unwrap().index + 1,
                None,
            )
            .unwrap();
        assert_eq!(
            peer.checkpoint_policy.total(),
            before + entries[0].encoded_len() as u128
        );
        let consumed = peer.checkpoint_policy.total();
        peer.process_ready_batch(|_| Ok(0)).unwrap();
        assert_eq!(peer.checkpoint_policy.total(), consumed);
        while peer.has_ready() {
            peer.process_ready_batch(|batch| Ok(batch.len())).unwrap();
        }
        assert_eq!(
            peer.checkpoint_policy.total(),
            before
                + entries
                    .iter()
                    .map(|entry| entry.encoded_len() as u128)
                    .sum::<u128>()
        );
    }

    #[test]
    fn exact_threshold_and_dropped_request_admit_one_capture() {
        let entry = entry();
        let bytes = entry.encoded_len() as u64;
        let mut policy = Policy::new(Some(bytes * 2));
        policy.charge(std::slice::from_ref(&entry));
        assert!(!policy.due());
        policy.charge(std::slice::from_ref(&entry));
        assert!(policy.due());
        let request = policy.requested(2);
        policy.charge(&[entry]);
        assert!(!policy.due());
        drop(request);
        assert!(policy.due());
    }

    #[test]
    fn publishing_an_older_capture_preserves_later_work_and_newer_requests() {
        let entry = entry();
        let mut policy = Policy::new(Some(entry.encoded_len() as u64));
        policy.charge(std::slice::from_ref(&entry));
        let captured = policy.total();
        let old = policy.requested(1);
        policy.charge(std::slice::from_ref(&entry));
        policy.published(1, captured);
        assert!(policy.due());
        let new = policy.requested(2);
        policy.published(1, captured);
        drop(old);
        assert!(!policy.due());
        drop(new);
        assert!(policy.due());
    }

    #[test]
    fn restored_images_reset_work_and_lifetime_totals_cross_u64_without_wrapping() {
        let first = entry();
        let mut policy = Policy::new(Some(first.encoded_len() as u64));
        policy.total = u128::from(u64::MAX);
        policy.published = policy.total;
        policy.charge(&[first]);
        assert!(policy.total() > u128::from(u64::MAX));
        assert!(policy.due());
        let old = policy.requested(1);
        policy.restored();
        assert!(!policy.due());
        policy.charge(&[entry()]);
        assert!(policy.due());
        let new = policy.requested(2);
        drop(old);
        assert!(!policy.due());
        drop(new);
        assert!(policy.due());
    }
}
