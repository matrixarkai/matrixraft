// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Durable payload files for streamed independent-peer checkpoints.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
pub(super) const MAX_FILE_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const BUFFER: usize = 64 * 1024;
#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct Manifest {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(string, tag = "2")]
    pub name: String,
    #[prost(uint64, tag = "3")]
    pub length: u64,
    #[prost(fixed64, tag = "4")]
    pub checksum: u64,
}
pub(super) struct Blob {
    path: PathBuf,
    pub manifest: Manifest,
    retained: AtomicBool,
}
fn extend_digest(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash = (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
    }
    hash
}
pub(super) fn scan(
    reader: &mut dyn Read,
    length: u64,
    mut output: Option<&mut dyn Write>,
) -> Result<u64, RaftError> {
    let mut remaining = length;
    let mut buffer = [0u8; BUFFER];
    let mut hash = 0xcbf29ce484222325;
    while remaining != 0 {
        let size = remaining.min(BUFFER as u64) as usize;
        reader
            .read_exact(&mut buffer[..size])
            .map_err(storage_error)?;
        hash = extend_digest(hash, &buffer[..size]);
        if let Some(writer) = output.as_mut() {
            writer.write_all(&buffer[..size]).map_err(storage_error)?;
        }
        remaining -= size as u64;
    }
    Ok(hash)
}
impl Manifest {
    fn validate(&self) -> Result<(), RaftError> {
        if self.version != 1
            || self.length > MAX_FILE_BYTES
            || self.length > usize::MAX.saturating_sub(8192) as u64
            || self.name.len() > 128
            || !self.name.starts_with("blob-")
            || self.name.len() <= 5
            || !self
                .name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(storage_error("invalid checkpoint payload manifest"));
        }
        Ok(())
    }
}
impl Blob {
    pub(super) fn capture(
        directory: &Path,
        reader: &mut dyn Read,
        length: u64,
    ) -> Result<Arc<Self>, RaftError> {
        if length > MAX_FILE_BYTES || length > usize::MAX.saturating_sub(8192) as u64 {
            return Err(invalid("file checkpoint exceeds size bound"));
        }
        std::fs::create_dir_all(directory).map_err(storage_error)?;
        let mut temporary = tempfile::Builder::new()
            .prefix("blob-")
            .tempfile_in(directory)
            .map_err(storage_error)?;
        let checksum = scan(reader, length, Some(temporary.as_file_mut()))?;
        let mut extra = [0];
        if reader.read(&mut extra).map_err(storage_error)? != 0 {
            return Err(invalid("checkpoint exceeds declared length"));
        }
        temporary.as_file().sync_all().map_err(storage_error)?;
        let (_, path) = temporary.keep().map_err(storage_error)?;
        let manifest = Manifest {
            version: 1,
            name: path
                .file_name()
                .unwrap()
                .to_str()
                .ok_or_else(|| invalid("checkpoint name is not UTF-8"))?
                .into(),
            length,
            checksum,
        };
        let blob = Arc::new(Self {
            path,
            manifest,
            retained: AtomicBool::new(false),
        });
        blob.manifest.validate()?;
        sync_directory(directory)?;
        Ok(blob)
    }
    pub(super) fn open(directory: &Path, bytes: &[u8]) -> Result<Arc<Self>, RaftError> {
        let manifest = Manifest::decode(bytes).map_err(storage_error)?;
        manifest.validate()?;
        if manifest.encode_to_vec() != bytes {
            return Err(storage_error("noncanonical checkpoint manifest"));
        }
        let path = directory.join(&manifest.name);
        let metadata = std::fs::symlink_metadata(&path).map_err(storage_error)?;
        if !metadata.is_file() || metadata.len() != manifest.length {
            return Err(storage_error("checkpoint payload length or type mismatch"));
        }
        let mut file = File::open(&path).map_err(storage_error)?;
        if scan(&mut file, manifest.length, None)? != manifest.checksum {
            return Err(storage_error("checkpoint payload checksum mismatch"));
        }
        Ok(Arc::new(Self {
            path,
            manifest,
            retained: AtomicBool::new(true),
        }))
    }
    pub(super) fn reader(&self) -> Result<File, RaftError> {
        File::open(&self.path).map_err(storage_error)
    }
    pub(super) fn retain(&self) {
        self.retained.store(true, Ordering::SeqCst);
    }
    pub(super) fn retire(&self) {
        self.retained.store(false, Ordering::SeqCst);
    }
}
impl Drop for Blob {
    fn drop(&mut self) {
        if !self.retained.load(Ordering::SeqCst) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}
pub(super) fn directory(wal: &Path) -> PathBuf {
    let mut name = wal.file_name().unwrap().to_os_string();
    name.push(".payloads");
    wal.with_file_name(name)
}
pub(super) fn cleanup(directory: &Path, keep: Option<&str>) -> Result<(), RaftError> {
    if !directory.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(directory).map_err(storage_error)? {
        let entry = entry.map_err(storage_error)?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.starts_with("blob-")
            && Some(name) != keep
            && entry.file_type().map_err(storage_error)?.is_file()
        {
            std::fs::remove_file(entry.path()).map_err(storage_error)?;
        }
    }
    sync_directory(directory)
}
fn sync_directory(directory: &Path) -> Result<(), RaftError> {
    #[cfg(unix)]
    File::open(directory)
        .and_then(|f| f.sync_all())
        .map_err(storage_error)?;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    struct Generated {
        remaining: u64,
        largest: usize,
    }
    impl Read for Generated {
        fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
            self.largest = self.largest.max(out.len());
            let n = self.remaining.min(out.len() as u64) as usize;
            out[..n].fill(13);
            self.remaining -= n as u64;
            Ok(n)
        }
    }
    #[test]
    fn file_capture_is_bounded_and_reopen_checks_the_complete_payload() {
        let dir = tempfile::tempdir().unwrap();
        let length = 9 * 1024 * 1024;
        let mut input = Generated {
            remaining: length,
            largest: 0,
        };
        let blob = Blob::capture(dir.path(), &mut input, length).unwrap();
        assert!(input.largest <= BUFFER);
        let manifest = blob.manifest.encode_to_vec();
        let path = blob.path.clone();
        blob.retain();
        drop(blob);
        assert!(path.exists());
        let reopened = Blob::open(dir.path(), &manifest).unwrap();
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(length / 2)).unwrap();
        file.write_all(&[14]).unwrap();
        file.sync_all().unwrap();
        assert!(Blob::open(dir.path(), &manifest).is_err());
        reopened.retire();
        drop(reopened);
        assert!(!path.exists());
        assert!(Blob::capture(dir.path(), &mut &b"short"[..], 6).is_err());
        assert!(Blob::capture(dir.path(), &mut &b"extra"[..], 4).is_err());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }
    #[test]
    fn orphan_cleanup_preserves_the_committed_payload_and_unrelated_files() {
        let dir = tempfile::tempdir().unwrap();
        let committed = Blob::capture(dir.path(), &mut &b"current"[..], 7).unwrap();
        committed.retain();
        let orphan = Blob::capture(dir.path(), &mut &b"orphan"[..], 6).unwrap();
        orphan.retain();
        let abandoned = orphan.path.clone();
        drop(orphan);
        let unrelated = dir.path().join("keep.txt");
        std::fs::write(&unrelated, b"keep").unwrap();
        cleanup(dir.path(), Some(&committed.manifest.name)).unwrap();
        assert!(!abandoned.exists());
        assert!(unrelated.exists());
        assert!(committed.path.exists());
        let mut bad = committed.manifest.clone();
        bad.name = "../elsewhere".into();
        assert!(Blob::open(dir.path(), &bad.encode_to_vec()).is_err());
        committed.retire();
    }
}
