// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

struct Job {
    id: u64,
    entries: Arc<Vec<Entry>>,
    claimed: bool,
    consumed: Option<usize>,
}
struct Inner {
    capacity: usize,
    sequence: u64,
    safe: u64,
    next: u64,
    jobs: VecDeque<Job>,
    active: usize,
    required: Option<u64>,
    dirty: bool,
    closed: bool,
}
#[repr(transparent)]
pub(super) struct Pipeline(Arc<Mutex<Inner>>);
struct Permit {
    state: Arc<Mutex<Inner>>,
    id: u64,
    _lock: Arc<File>,
}
impl Drop for Permit {
    fn drop(&mut self) {
        let mut inner = self.state.lock().unwrap();
        inner.active -= 1;
        if !inner.closed {
            if let Some(job) = inner.jobs.iter_mut().find(|job| job.id == self.id) {
                if job.consumed.is_none() {
                    job.consumed = Some(0);
                    let index = job.entries[0].index;
                    inner.required = Some(inner.required.map_or(index, |old| old.min(index)));
                    inner.dirty = true;
                }
            }
        }
    }
}

/// Owned committed entries for a host's ordered application worker.
/// Tasks for one peer must mutate the application in issuance order. Preparation
/// may run in parallel and completion delivery may arrive out of order.
pub struct PeerApplyTask {
    entries: Arc<Vec<Entry>>,
    permit: Permit,
}
/// Return this opaque completion to the originating open peer.
pub struct PeerApplyCompletion {
    consumed: usize,
    permit: Permit,
}
impl PeerApplyTask {
    pub fn entries(&self) -> ApplicationBatch<'_> {
        ApplicationBatch {
            entries: &self.entries,
        }
    }
    pub fn finish(self) -> PeerApplyCompletion {
        PeerApplyCompletion {
            consumed: self.entries.len(),
            permit: self.permit,
        }
    }
    /// Report an applied prefix and reject its remaining entries. Do not apply
    /// later tasks; report or drop them before allowing a replacement checkpoint.
    pub fn reject_after(self, consumed: usize) -> Result<PeerApplyCompletion, RaftError> {
        if consumed >= self.entries.len() {
            return Err(invalid(
                "rejected application task requires an unconsumed suffix",
            ));
        }
        Ok(PeerApplyCompletion {
            consumed,
            permit: self.permit,
        })
    }
}
impl Pipeline {
    pub(super) fn new(capacity: usize, applied: u64) -> Self {
        Self(Arc::new(Mutex::new(Inner {
            capacity,
            sequence: 0,
            safe: applied,
            next: applied + 1,
            jobs: VecDeque::new(),
            active: 0,
            required: None,
            dirty: false,
            closed: false,
        })))
    }
    pub(super) fn safe(&self) -> u64 {
        self.0.lock().unwrap().safe
    }
    pub(super) fn active(&self) -> bool {
        self.0.lock().unwrap().active != 0
    }
    pub(super) fn required(&self) -> Option<u64> {
        self.0.lock().unwrap().required
    }
    pub(super) fn dirty(&self) -> bool {
        self.0.lock().unwrap().dirty
    }
    pub(super) fn next(&self, upper: u64) -> Option<u64> {
        let inner = self.0.lock().unwrap();
        (!inner.closed
            && inner.required.is_none()
            && inner.jobs.len() < inner.capacity
            && inner.next <= upper)
            .then_some(inner.next)
    }
    pub(super) fn settled(&self, upper: u64) -> bool {
        let inner = self.0.lock().unwrap();
        inner.required.is_none()
            && inner.active == 0
            && inner.jobs.is_empty()
            && inner.safe >= upper
    }
    fn push(&self, entries: Vec<Entry>, internal: bool) -> Result<(), RaftError> {
        let mut inner = self.0.lock().unwrap();
        if entries.is_empty()
            || entries[0].index != inner.next
            || inner.jobs.len() >= inner.capacity
        {
            return Err(invalid("invalid application task boundary"));
        }
        inner.sequence = inner
            .sequence
            .checked_add(1)
            .ok_or_else(|| invalid("application task identities exhausted"))?;
        inner.next = entries.last().unwrap().index + 1;
        let id = inner.sequence;
        let consumed = internal.then_some(entries.len());
        inner.jobs.push_back(Job {
            id,
            entries: Arc::new(entries),
            claimed: internal,
            consumed,
        });
        Ok(())
    }
    fn drain(&self) -> Vec<(Arc<Vec<Entry>>, usize)> {
        let mut inner = self.0.lock().unwrap();
        let mut consumed = Vec::new();
        while let Some(job) = inner.jobs.front() {
            let Some(count) = job.consumed else {
                break;
            };
            if inner
                .required
                .is_some_and(|index| job.entries[0].index >= index)
            {
                break;
            }
            let job = inner.jobs.pop_front().unwrap();
            if count > 0 {
                inner.safe = job.entries[count - 1].index;
            }
            let partial = count < job.entries.len();
            consumed.push((job.entries, count));
            if partial {
                break;
            }
        }
        inner.dirty = false;
        consumed
    }
    fn reset(&self, index: u64) {
        let mut inner = self.0.lock().unwrap();
        assert_eq!(inner.active, 0);
        inner.jobs.clear();
        inner.safe = index;
        inner.next = index + 1;
        inner.required = None;
        inner.dirty = false;
    }
    pub(super) fn stop(&self) {
        let mut inner = self.0.lock().unwrap();
        inner.closed = true;
        inner.jobs.clear();
    }
}

impl DurablePeer {
    /// Persist and exchange consensus packets while bounded application tasks
    /// run on host-owned workers. Restore waits for all issued handles to settle.
    pub fn process_ready_async_apply(
        &mut self,
        restore: impl FnMut(LogId, &mut dyn Read, u64) -> Result<RestoreDecision, RaftError>,
    ) -> Result<Vec<PeerPacket>, RaftError> {
        if self.apply_pipeline.is_none() {
            return Err(invalid("asynchronous application is disabled"));
        }
        self.process_application_inner(
            |_| ApplyProgress {
                consumed: 0,
                error: None,
            },
            restore,
        )
    }
    pub fn take_application_tasks(&mut self) -> Result<Vec<PeerApplyTask>, RaftError> {
        self.processing_allowed()?;
        if self.pending_restore.is_some() || self.raw.raft.raft_log.unstable_snapshot().is_some() {
            return Ok(Vec::new());
        }
        let Some(pipeline) = &self.apply_pipeline else {
            return Err(invalid("asynchronous application is disabled"));
        };
        let mut inner = pipeline.0.lock().unwrap();
        if inner.closed || inner.required.is_some() {
            return Ok(Vec::new());
        }
        let mut tasks = Vec::new();
        let state = Arc::clone(&pipeline.0);
        for job in &mut inner.jobs {
            if !job.claimed {
                job.claimed = true;
                tasks.push(PeerApplyTask {
                    entries: Arc::clone(&job.entries),
                    permit: Permit {
                        state: Arc::clone(&state),
                        id: job.id,
                        _lock: Arc::clone(&self.journal._lock),
                    },
                });
            }
        }
        inner.active += tasks.len();
        Ok(tasks)
    }
    pub fn complete_application_task(
        &mut self,
        completion: PeerApplyCompletion,
    ) -> Result<(), RaftError> {
        self.processing_allowed()?;
        let Some(pipeline) = &self.apply_pipeline else {
            return Err(invalid("asynchronous application is disabled"));
        };
        if !Arc::ptr_eq(&pipeline.0, &completion.permit.state) {
            return Err(invalid(
                "application completion belongs to another open peer",
            ));
        }
        {
            let mut inner = pipeline.0.lock().unwrap();
            let job = inner
                .jobs
                .iter_mut()
                .find(|job| job.id == completion.permit.id)
                .ok_or_else(|| invalid("obsolete application task completion"))?;
            job.consumed = Some(completion.consumed);
            if completion.consumed < job.entries.len() {
                let index = job.entries[completion.consumed].index;
                inner.required = Some(inner.required.map_or(index, |old| old.min(index)));
            }
            inner.dirty = true;
        }
        drop(completion);
        self.service_apply_pipeline()?;
        self.service_proposal_receipts();
        self.finish_drain();
        Ok(())
    }
    /// A rejected or abandoned task requires a leader checkpoint covering this index.
    pub fn required_application_checkpoint(&self) -> Option<u64> {
        self.apply_pipeline
            .as_ref()
            .and_then(|pipeline| pipeline.required())
    }
    pub(super) fn application_settled(&self) -> bool {
        self.apply_pipeline
            .as_ref()
            .is_none_or(|pipeline| pipeline.settled(self.commit_index()))
    }
    pub(super) fn application_repair_needs_input(&self) -> bool {
        self.required_application_checkpoint().is_some()
            && self.pending_restore.is_none()
            && self.raw.raft.raft_log.unstable_snapshot().is_none()
    }
    pub(super) fn service_apply_pipeline(&mut self) -> Result<(), RaftError> {
        let Some(pipeline) = &self.apply_pipeline else {
            return Ok(());
        };
        for (entries, count) in pipeline.drain() {
            self.checkpoint_policy.charge(&entries[..count]);
            if let Some(tracker) = &mut self.proposal_receipts {
                tracker.applied(&entries[..count]);
            }
        }
        if pipeline.required().is_some() {
            if self.drain_through.is_some() && self.application_repair_needs_input() {
                return Err(invalid(
                    "application checkpoint cannot be requested after shutdown admission closes",
                ));
            }
            if self.raw.raft.state == StateRole::Leader {
                // Only an already caught-up voter can be handed off immediately.
                let target = self
                    .membership()
                    .voters
                    .into_iter()
                    .filter(|id| *id != self.node_id())
                    .filter(|id| {
                        self.raw.raft.prs().get(*id).is_some_and(|p| {
                            p.recent_active && p.matched == self.raw.raft.raft_log.last_index()
                        })
                    })
                    .min();
                if let Some(target) = target {
                    self.raw.transfer_leader(target);
                }
            }
            if self.raw.raft.state != StateRole::Follower {
                self.raw.raft.become_follower(self.term(), 0);
                self.transfer_clock = None;
            }
            if self.pending_restore.is_none() && self.leader_id().is_some() {
                match self.raw.request_snapshot() {
                    Ok(()) | Err(raft_core::Error::RequestSnapshotDropped) => {}
                    Err(error) => return Err(invalid(error)),
                }
            }
        }
        self.align_replica_contacts();
        Ok(())
    }
    pub(super) fn apply_async_consensus(&mut self, entries: &[Entry]) -> Result<(), RaftError> {
        if self.apply_pipeline.is_none() {
            return Ok(());
        }
        let previous = self.raw.raft.raft_log.applied;
        for entry in entries.iter().filter(|entry| {
            entry.index > previous && entry.entry_type != EntryType::EntryNormal as i32
        }) {
            self.apply_committed_membership(entry)?;
        }
        if let Some(last) = entries.last().filter(|entry| entry.index > previous) {
            self.raw.advance_apply_to(last.index);
        }
        Ok(())
    }
    pub(super) fn issue_application_tasks(
        &mut self,
        entries: Vec<Entry>,
    ) -> Result<bool, RaftError> {
        let mut available: VecDeque<_> = entries.into();
        loop {
            let upper = self.raw.raft.raft_log.applied;
            let next = self.apply_pipeline.as_ref().unwrap().next(upper);
            let Some(low) = next else {
                break;
            };
            if available.front().is_none_or(|entry| entry.index != low) {
                available = self
                    .store
                    .entries_sync(
                        low,
                        upper.saturating_add(1).min(low.saturating_add(1024)),
                        self.application_fetch_bytes,
                    )
                    .map_err(storage_error)?
                    .into();
            }
            if available.is_empty() {
                return Err(storage_error("committed application range is empty"));
            }
            let internal = available[0].entry_type != EntryType::EntryNormal as i32;
            let mut batch = Vec::new();
            while batch.len() < 1024
                && available.front().is_some_and(|entry| {
                    (entry.entry_type != EntryType::EntryNormal as i32) == internal
                })
            {
                batch.push(available.pop_front().unwrap());
                if internal {
                    break;
                }
            }
            self.sync_application_commands(&batch)?;
            self.apply_pipeline
                .as_ref()
                .unwrap()
                .push(batch, internal)?;
            self.service_apply_pipeline()?;
        }
        Ok(false)
    }
    pub(super) fn reset_application_pipeline(&mut self, index: u64) {
        if let Some(pipeline) = &self.apply_pipeline {
            pipeline.reset(index);
        }
    }
    pub(super) fn retire_application_snapshot_request(&mut self, message: &Message) {
        if self.required_application_checkpoint().is_some()
            && matches!(
                message.get_msg_type(),
                MessageType::MsgAppend | MessageType::MsgHeartbeat
            )
            && message.term >= self.term()
            && self.raw.raft.pending_request_snapshot != 0
            && message.term > self.raw.raft.raft_log.last_term()
        {
            // Let a new leader replicate before requesting its replacement image.
            self.raw.raft.pending_request_snapshot = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn entries(index: u64) -> Vec<Entry> {
        vec![Entry {
            index,
            term: 1,
            data: vec![7; 128],
            ..Default::default()
        }]
    }
    #[test]
    fn queued_payloads_move_without_copying_and_slots_wait_for_prefix_completion() {
        assert_eq!(
            std::mem::size_of::<Option<Pipeline>>(),
            std::mem::size_of::<usize>()
        );
        let pipeline = Pipeline::new(2, 0);
        let first = entries(1);
        let pointer = first[0].data.as_ptr();
        pipeline.push(first, false).unwrap();
        pipeline.push(entries(2), false).unwrap();
        assert!(pipeline.push(entries(3), false).is_err());
        {
            let mut inner = pipeline.0.lock().unwrap();
            assert_eq!(inner.jobs[0].entries[0].data.as_ptr(), pointer);
            inner.jobs[1].consumed = Some(1);
        }
        assert!(pipeline.drain().is_empty());
        assert_eq!(pipeline.safe(), 0);
        assert!(pipeline.next(10).is_none());
        pipeline.0.lock().unwrap().jobs[0].consumed = Some(1);
        assert_eq!(pipeline.drain().len(), 2);
        assert_eq!(pipeline.safe(), 2);
        assert!(pipeline.settled(2));
        pipeline.push(entries(3), false).unwrap();
    }
    #[test]
    fn an_abandoned_handle_blocks_until_a_checkpoint_resets_the_pipeline() {
        let pipeline = Pipeline::new(2, 0);
        pipeline.push(entries(1), false).unwrap();
        let (id, entries) = {
            let mut inner = pipeline.0.lock().unwrap();
            inner.active = 1;
            inner.jobs[0].claimed = true;
            (inner.jobs[0].id, Arc::clone(&inner.jobs[0].entries))
        };
        drop(PeerApplyTask {
            entries,
            permit: Permit {
                state: Arc::clone(&pipeline.0),
                id,
                _lock: Arc::new(tempfile::tempfile().unwrap()),
            },
        });
        assert!(!pipeline.active());
        assert!(pipeline.dirty());
        assert_eq!(pipeline.required(), Some(1));
        assert!(pipeline.next(10).is_none());
        assert!(pipeline.drain().is_empty());
        assert_eq!(pipeline.safe(), 0);
        pipeline.reset(2);
        assert_eq!(pipeline.required(), None);
        assert_eq!(pipeline.next(10), Some(3));
        pipeline.stop();
        assert_eq!(pipeline.next(10), None);
    }
}
