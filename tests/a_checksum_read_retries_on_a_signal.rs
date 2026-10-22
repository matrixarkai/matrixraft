// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A checksum read interrupted by a signal is retried, not reported.
//!
//! `Read::read` surfaces `ErrorKind::Interrupted` to its caller rather than
//! retrying. That is unlike the other IO this crate does on the durable path --
//! `write_all`, `sync_data` and `set_len` all retry internally -- so the
//! asymmetry is easy to miss: a single `read(..)?` abandons a whole file's
//! checksum because a signal arrived, and reports a failure for something that
//! did not fail.
//!
//! The loop lives in `matrixraft_checksum_extend_from_reader` rather than inside
//! `FileChecksum::start` for exactly this test: a reader can be made to
//! interrupt on demand, where arranging a real signal at the right instant
//! cannot be done reliably.

use matrixraft::{matrixraft_checksum_extend_from_reader, ChecksumContext, ChecksumType};
use std::io::{self, Read};

/// Yields its data in one-byte reads, returning `Interrupted` before each one.
///
/// Interrupting *every* read is deliberate: a retry that only survives a single
/// interruption would pass a gentler fixture.
struct InterruptsEveryRead {
    data: Vec<u8>,
    at: usize,
    interrupt_next: bool,
    interruptions: usize,
}

impl InterruptsEveryRead {
    fn new(data: &[u8]) -> Self {
        Self {
            data: data.to_vec(),
            at: 0,
            interrupt_next: true,
            interruptions: 0,
        }
    }
}

impl Read for InterruptsEveryRead {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.interrupt_next {
            self.interrupt_next = false;
            self.interruptions += 1;
            return Err(io::Error::new(io::ErrorKind::Interrupted, "signal"));
        }
        if self.at >= self.data.len() {
            return Ok(0);
        }
        buf[0] = self.data[self.at];
        self.at += 1;
        self.interrupt_next = true;
        Ok(1)
    }
}

fn checksum_of(data: &[u8]) -> matrixraft::ChecksumResult {
    let mut context = ChecksumContext::new(ChecksumType::Crc32);
    context.extend(data).expect("extend");
    context.finalize()
}

#[test]
fn an_interrupted_read_is_retried_and_the_checksum_is_whole() {
    let data = b"the quick brown fox jumps over the lazy dog".to_vec();

    let mut reader = InterruptsEveryRead::new(&data);
    let mut context = ChecksumContext::new(ChecksumType::Crc32);
    let mut buffer = vec![0u8; 8];

    matrixraft_checksum_extend_from_reader(&mut reader, &mut context, &mut buffer)
        .expect("an interrupted read must not fail the checksum");

    assert!(
        reader.interruptions > data.len(),
        "the premise: the reader interrupted every read, {} times for {} bytes",
        reader.interruptions,
        data.len()
    );
    // The value and the byte count must match a single bulk pass; `chunks`
    // legitimately does not, because this reader delivers one byte at a time
    // and `ChecksumResult` records how it was fed. Comparing the whole struct
    // would be asserting the feeding pattern, not the checksum.
    let retried = context.finalize();
    let bulk = checksum_of(&data);
    assert_eq!(
        retried.value, bulk.value,
        "the checksum is the same as over the whole slice at once"
    );
    assert_eq!(
        retried.bytes, bulk.bytes,
        "and every byte was fed exactly once"
    );
    assert_eq!(
        retried.chunks,
        data.len() as u64,
        "one chunk per one-byte read, which is why the structs differ"
    );
}

#[test]
fn a_real_error_is_still_reported() {
    // The control: the retry must not swallow errors that are not Interrupted.
    struct Fails;
    impl Read for Fails {
        fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::PermissionDenied, "nope"))
        }
    }

    let mut context = ChecksumContext::new(ChecksumType::Crc32);
    let mut buffer = vec![0u8; 8];
    let error = matrixraft_checksum_extend_from_reader(&mut Fails, &mut context, &mut buffer)
        .expect_err("a permission error is not an interruption");
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
}
