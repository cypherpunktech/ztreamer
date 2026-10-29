//! The source side of a snapshot: [`SnapshotWriter`] writes a snapshot directory from an index,
//! in the layout of [`crate::snapshot`].

use std::{
    fs::{self, File},
    io::{self, Write as _},
    path::PathBuf,
};

use chrono::{DateTime, Utc};
use ed25519_dalek::SigningKey;
use tracing::warn;
use ztreamer_indexer::{
    index::{Index, IndexError},
    manifest_entry::{BlobMismatch, ManifestEntry},
};

use crate::{
    manifest::Manifest,
    snapshot::{Latest, RangePath, SignatureFile},
};

/// Writes a snapshot directory in the layout of [`crate::snapshot`]. One key signs the directory.
///
/// The writer writes each file to a temporary file next to its path. Then it renames the
/// temporary file to the path. Thus a reader sees the old file or the new file, never a part of
/// one.
///
/// The writer writes a range file only after it checks the blob against the length and the
/// digest of its entry. Thus a file with the name and the length of its entry holds the range.
/// [`Self::export`] does not read or write this file again: a daily export writes only the new
/// ranges.
///
/// The writer never replaces a manifest. Each manifest has a new name. The writer writes it,
/// then `latest.json`, which points at it.
///
/// The `Debug` output shows only the verifying key: `SigningKey` does not show its secret key.
#[derive(Debug)]
pub struct SnapshotWriter {
    root: PathBuf,
    key: SigningKey,
}

/// What [`SnapshotWriter::export`] wrote.
#[derive(Debug, Eq, PartialEq)]
pub struct Exported {
    /// The path of the manifest, relative to the snapshot directory.
    pub manifest: String,
    /// The number of range files that the export wrote. The other range files were present,
    /// with the length of their entry.
    pub written: usize,
}

/// Why a [`SnapshotWriter`] stopped.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotWriteError {
    /// A file operation failed.
    #[error("write the snapshot directory")]
    Io(#[from] io::Error),
    /// The index read failed.
    #[error("read the index")]
    Index(#[from] IndexError),
    /// The index has no blob for the sealed range at this start height.
    #[error("sealed range {0} is not in the index")]
    MissingRange(u32),
    /// The blob of the range at this start height does not have the length or the digest of
    /// its entry.
    #[error("the blob of sealed range {0} does not match its manifest entry")]
    BlobMismatch(u32, #[source] BlobMismatch),
    /// A manifest with this path is present.
    #[error("{0} exists, and a manifest is never replaced")]
    ManifestExists(String),
}

impl SnapshotWriter {
    /// A writer for the snapshot directory at `root`, which signs with `key`.
    pub fn new(root: impl Into<PathBuf>, key: SigningKey) -> Self {
        Self { root: root.into(), key }
    }

    /// Writes the range files of `manifest` from `index`. Then it writes `manifest` and its
    /// [`SignatureFile`] at `manifests/<now>.json`, and a `latest.json` that points at the
    /// manifest. Thus `latest.json` points only at files that are present.
    ///
    /// It checks the manifest name before it writes a range file. If a file has this name, it
    /// stops with [`SnapshotWriteError::ManifestExists`].
    pub fn export(
        &self,
        index: &Index,
        manifest: &Manifest,
        now: DateTime<Utc>,
    ) -> Result<Exported, SnapshotWriteError> {
        let path = self.new_manifest_path(now)?;
        let written = manifest.entries().iter().try_fold(0, |written, entry| {
            if self.has_range(entry)? {
                return Ok(written);
            }
            let start = entry.start_height;
            let blob = index
                .latest()?
                .sealed_range(start)?
                .ok_or(SnapshotWriteError::MissingRange(start))?;
            self.put_range(entry, &blob)?;
            Ok::<_, SnapshotWriteError>(written + 1)
        })?;
        self.put_manifest_and_latest(&path, manifest)?;
        Ok(Exported { manifest: path, written })
    }

    /// Checks `blob` against the length and the digest in `entry`. Then it writes `blob` to the
    /// range file of `entry`.
    pub fn put_range(&self, entry: &ManifestEntry, blob: &[u8]) -> Result<(), SnapshotWriteError> {
        entry
            .check_blob(blob)
            .map_err(|mismatch| SnapshotWriteError::BlobMismatch(entry.start_height, mismatch))?;
        Ok(self.put(RangePath::from(entry).as_str(), blob)?)
    }

    /// The path of the manifest for `now`. It refuses the path if a file has it.
    pub(crate) fn new_manifest_path(
        &self,
        now: DateTime<Utc>,
    ) -> Result<String, SnapshotWriteError> {
        let path = format!("manifests/{}.json", now.format("%Y-%m-%dT%H%M%SZ"));
        if self.root.join(&path).exists() {
            return Err(SnapshotWriteError::ManifestExists(path));
        }
        Ok(path)
    }

    /// Returns `true` if the range file of `entry` is present with the length of `entry`. For a
    /// file with a different length, it logs a warning and returns `false`. Then the caller
    /// writes the file again.
    fn has_range(&self, entry: &ManifestEntry) -> io::Result<bool> {
        let path = self.root.join(RangePath::from(entry).as_str());
        match fs::metadata(&path) {
            Ok(file) if file.is_file() && file.len() == entry.blob_len => Ok(true),
            Ok(_) => {
                warn!(path = %path.display(), "range file has the wrong length; writing it again");
                Ok(false)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Writes the manifest at `path`, then its signature, then `latest.json`. For a short time,
    /// the manifest is present without its signature. But `latest.json` points at the manifest
    /// only after the signature is present.
    pub(crate) fn put_manifest_and_latest(
        &self,
        path: &str,
        manifest: &Manifest,
    ) -> io::Result<()> {
        let bytes = serde_json::to_vec(manifest).expect("a Manifest always serializes");
        self.put(path, &bytes)?;
        let signature = SignatureFile::sign(&bytes, &self.key);
        self.put(&SignatureFile::path_of(path), signature.to_hex().as_bytes())?;
        let latest =
            Latest { manifest: path.to_owned(), sealed_through: manifest.sealed_through() };
        self.put(Latest::PATH, &latest.to_file(&self.key))
    }

    /// Writes `bytes` to a temporary file next to `relative`. Then it renames the temporary file
    /// to `relative`.
    fn put(&self, relative: &str, bytes: &[u8]) -> io::Result<()> {
        let path = self.root.join(relative);
        fs::create_dir_all(path.parent().expect("a file below the root has a parent"))?;
        let temp = path.with_added_extension("tmp");
        let mut file = File::create(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(temp, path)
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;
    use zakura_chain::parameters::Network;

    use super::*;
    use crate::{
        epoch::synthetic_entries,
        fetch::{HttpSource, RangeSource as _},
        test_utils::{FileServer, SEED, SourceIndex},
    };

    #[test]
    fn a_blob_that_does_not_match_its_entry_is_not_written() {
        let dir = tempfile::tempdir().unwrap();
        let writer = SnapshotWriter::new(dir.path(), SigningKey::from_bytes(&SEED));
        let mut entry = synthetic_entries(&[Default::default()]).pop().unwrap();
        entry.blob_len = 5;

        let error = writer.put_range(&entry, b"other").unwrap_err();

        assert!(
            matches!(error, SnapshotWriteError::BlobMismatch(0, BlobMismatch::Digest)),
            "{error:?}"
        );
        assert!(!dir.path().join(RangePath::from(&entry).as_str()).exists());
    }

    /// Twelve ranges: the filler closes an epoch only at range 9. The first export has the first
    /// epoch. The second export, on the next day, has all ranges.
    #[tokio::test]
    async fn http_source_reads_the_manifest_of_each_export() {
        let source = SourceIndex::new(12).await;
        let dir = tempfile::tempdir().unwrap();
        let key = SigningKey::from_bytes(&SEED);
        let http = HttpSource::new(FileServer::start(dir.path()).await.base, key.verifying_key());
        let (http, writer) = (http.unwrap(), SnapshotWriter::new(dir.path(), key));
        let entries = source.entries();
        let manifest = |ranges| {
            let entries = entries[..ranges].to_vec();
            Manifest::new(Network::Mainnet.to_string(), Network::Mainnet.genesis_hash(), entries)
                .unwrap()
        };
        let now = DateTime::from_timestamp(1_792_972_800, 0).unwrap();

        for (ranges, written, now) in [(10, 10, now), (12, 2, now + TimeDelta::days(1))] {
            let exported = writer.export(&source.index, &manifest(ranges), now).unwrap();

            assert_eq!(exported.written, written);
            assert_eq!(http.manifest().await.unwrap(), manifest(ranges));
        }
    }
}
