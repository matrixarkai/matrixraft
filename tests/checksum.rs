// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    rustraft_checksum_file_list, rustraft_crc32c, rustraft_murmur32, RustRaftChecksumContext,
    RustRaftChecksumType, RustRaftFileChecksumContext,
};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "rustraft-checksum-{name}-{}-{nonce}",
        std::process::id()
    ))
}

#[test]
fn checksum_context_matches_known_crc32c_and_murmur32_vectors() {
    assert_eq!(rustraft_crc32c(b"123456789"), 0xe306_9283);
    assert_eq!(rustraft_murmur32(b"hello"), 0x248b_fa47);

    let mut crc = RustRaftChecksumContext::new(RustRaftChecksumType::Crc32);
    crc.extend(b"123").expect("extend");
    crc.extend(b"456").expect("extend");
    crc.extend(b"789").expect("extend");
    let crc_result = crc.finalize();
    assert_eq!(crc_result.value, 0xe306_9283);
    assert_eq!(crc_result.bytes, 9);
    assert_eq!(crc_result.chunks, 3);
    assert_eq!(crc_result.checksum_name, "crc32");

    let mut murmur = RustRaftChecksumContext::from_name("murmur32");
    murmur.extend(b"he").expect("extend");
    murmur.extend(b"llo").expect("extend");
    let murmur_result = murmur.finalize();
    assert_eq!(murmur_result.value, 0x248b_fa47);
    assert_eq!(murmur_result.bytes, 5);
    assert_eq!(murmur_result.chunks, 2);
}

#[test]
fn checksum_context_rejects_invalid_type_and_extend_after_finalize() {
    let mut invalid = RustRaftChecksumContext::from_name("unknown");
    assert_eq!(
        invalid.extend(b"data").expect_err("invalid type"),
        "checksum type is invalid"
    );

    let mut crc = RustRaftChecksumContext::new(RustRaftChecksumType::Crc32);
    crc.extend(b"data").expect("extend");
    crc.finalize();
    assert_eq!(
        crc.extend(b"again").expect_err("already finished"),
        "checksum already finished"
    );
}

#[test]
fn file_checksum_context_reads_file_in_blocks_like_matrixraft() {
    let dir = temp_dir("file");
    fs::create_dir_all(&dir).expect("create dir");
    let file = dir.join("segment.log");
    fs::write(&file, b"abcdefghi").expect("write");

    let result = RustRaftFileChecksumContext::with_type(&file, RustRaftChecksumType::Crc32, 3)
        .start()
        .expect("checksum file");

    assert_eq!(result.files, vec![file.clone()]);
    assert_eq!(result.block_size, 3);
    assert_eq!(result.checksum.value, rustraft_crc32c(b"abcdefghi"));
    assert_eq!(result.checksum.chunks, 3);
    assert_eq!(result.checksum.bytes, 9);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn directory_checksum_uses_recursive_sorted_file_order() {
    let dir = temp_dir("dir");
    let nested = dir.join("nested");
    fs::create_dir_all(&nested).expect("create nested");
    fs::write(dir.join("b.log"), b"b").expect("write b");
    fs::write(dir.join("a.log"), b"a").expect("write a");
    fs::write(nested.join("c.log"), b"c").expect("write c");

    let files = rustraft_checksum_file_list(&dir).expect("list files");
    assert_eq!(
        files
            .iter()
            .map(|path| path.file_name().unwrap().to_string_lossy().to_string())
            .collect::<Vec<_>>(),
        vec!["a.log", "b.log", "c.log"]
    );

    let result = RustRaftFileChecksumContext::with_type(&dir, RustRaftChecksumType::Murmur32, 2)
        .start()
        .expect("checksum dir");
    let mut expected = RustRaftChecksumContext::new(RustRaftChecksumType::Murmur32);
    expected.extend(b"a").expect("a");
    expected.extend(b"b").expect("b");
    expected.extend(b"c").expect("c");
    assert_eq!(result.checksum.value, expected.finalize().value);
    assert_eq!(result.files, files);

    let _ = fs::remove_dir_all(dir);
}
