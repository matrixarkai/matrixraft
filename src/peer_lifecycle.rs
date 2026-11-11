// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

impl DurablePeer {
    /// Stop immediately, canceling local admission and unsent transport work.
    /// Committed entries may remain unapplied. Drop this instance before reopening.
    /// Already returned packets and host-owned tasks cannot be recalled.
    pub fn stop(&mut self) {
        self.cancel_local_requests();
        self.incoming_payload = None;
        self.stopped = true;
    }

    /// Close admission and capture the current committed index for local draining.
    /// Continue Ready processing with the application's normal callbacks. No
    /// packets are returned and no new peer inputs are accepted during the drain.
    /// Repeated calls retain the original fence, including after successful stop.
    pub fn begin_shutdown(&mut self) -> Result<u64, RaftError> {
        self.storage_healthy()?;
        if let Some(fence) = self.drain_through {
            if self.stopped && !self.drain_complete() {
                return Err(invalid(
                    "shutdown was stopped before its committed fence applied",
                ));
            }
            return Ok(fence);
        }
        if self.stopped {
            return Err(invalid("peer is already stopped"));
        }
        let fence = self.commit_index();
        self.cancel_local_requests();
        self.drain_through = Some(fence);
        self.finish_drain();
        Ok(fence)
    }

    /// True only after a requested shutdown has applied its committed fence.
    /// Application or storage failures, and an immediate stop before the fence,
    /// remain errors rather than successful graceful completion.
    pub fn poll_shutdown(&mut self) -> Result<bool, RaftError> {
        self.storage_healthy()?;
        if self.drain_through.is_none() {
            return Err(invalid("graceful shutdown has not been requested"));
        }
        if self.stopped && !self.drain_complete() {
            return Err(invalid(
                "shutdown was stopped before its committed fence applied",
            ));
        }
        self.finish_drain();
        Ok(self.stopped)
    }

    pub(super) fn drain_complete(&self) -> bool {
        self.drain_through
            .is_some_and(|fence| self.pending_restore.is_none() && self.applied_index() >= fence)
    }
    pub(super) fn finish_drain(&mut self) {
        if !self.stopped && self.drain_complete() {
            self.stop();
        }
    }
    fn cancel_local_requests(&mut self) {
        self.raw.raft.become_follower(self.term(), 0);
        self.transfer_clock = None;
        self.pending_reads.clear();
        self.deferred_reads.clear();
        self.raw.raft.read_only.pending_read_index.clear();
        self.raw.raft.read_only.read_index_queue.clear();
        self.raw.raft.read_states.clear();
        self.raw.raft.msgs.clear();
        *self.store.async_reads.lock().unwrap() = None;
        self.snapshot_flights.clear();
        self.stream.cancel_transfers();
    }
}
