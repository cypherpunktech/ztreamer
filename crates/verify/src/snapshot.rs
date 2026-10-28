//! The layout of a snapshot directory: what `ztreamerd export-snapshot` writes and a replica
//! reads.
//!
//! ```text
//! latest.json                         the signed pointer to the current manifest: Latest
//! manifests/<time>.json               one entry per sealed range; never replaced
//! manifests/<time>.json.sig           SignatureFile of the manifest bytes
//! ranges/<start>-<digest prefix>.bin  range blob, at RangePath; never replaced
//! ```
//!
//! `<time>` is UTC, for example `2026-10-26T143005Z`. [`SnapshotWriter`] writes the directory.
//!
//! `latest.json` is the only file that an export replaces. It holds its own signature, so that a
//! reader never sees a new pointer with an old signature:
//!
//! ```json
//! {"latest":{"manifest":"manifests/2026-10-26T143005Z.json","sealed_through":1785999},
//!   "signature":"<hex>"}
//! ```
//!
//! The signature covers the exact bytes of the `latest` object. A reader verifies it with a key
//! that it trusts. The file does not hold the key: a source that can change the file can also
//! change a key in it.

use std::{
    fs::{self, File},
    io::{self, Write as _},
    path::PathBuf,
};

use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Signer as _, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_with::{hex::Hex, serde_as};
use tracing::warn;
use ztreamer_indexer::index::{
    Index, IndexError,
    range_meta::{BlobMismatch, ManifestEntry},
};

use crate::manifest::Manifest;

/// The Ed25519 public key that signs the snapshots that a replica accepts.
///
/// This is the production key. Its secret seed was made on the publishing host and does not
/// leave it. A replica refuses a manifest or a `latest.json` that this key does not verify. A
/// peer source has no signature.
pub const SNAPSHOT_PUBKEY: [u8; 32] = [
    0xf8, 0x92, 0x06, 0x28, 0x05, 0xbc, 0xd4, 0xa6, 0xc8, 0x8f, 0x7f, 0x32, 0x88, 0xd2, 0xdd, 0xe2,
    0x48, 0x7d, 0x67, 0xf8, 0x77, 0x5b, 0x26, 0x32, 0xe8, 0x56, 0x0c, 0xbd, 0x48, 0xe3, 0xce, 0xa8,
];

/// The pointer to the current manifest of a snapshot, in `latest.json`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Latest {
    /// The path of the manifest, relative to the snapshot directory.
    pub manifest: String,
    /// The last height of the last range in the manifest.
    pub sealed_through: u32,
}

/// The JSON of `latest.json`: the pointer and the signature of its exact bytes.
#[serde_as]
#[derive(Deserialize, Serialize)]
struct LatestFile<'a> {
    #[serde(borrow)]
    latest: &'a RawValue,
    #[serde_as(as = "Hex")]
    signature: [u8; 64],
}

/// Why `latest.json` was refused.
#[derive(Debug, thiserror::Error)]
pub enum LatestError {
    /// The file does not parse. A file without a signature does not parse.
    #[error("latest.json is not valid")]
    Json(#[from] serde_json::Error),
    /// The signature does not verify with the trusted key.
    #[error("signature of latest.json does not verify")]
    Signature,
}

impl Latest {
    /// The path of the pointer, relative to the snapshot directory.
    pub const PATH: &str = "latest.json";

    /// The bytes of `latest.json`, signed with `key`.
    pub fn to_file(&self, key: &SigningKey) -> Vec<u8> {
        let latest = serde_json::to_string(self).expect("a Latest always serializes");
        let signature = key.sign(latest.as_bytes()).to_bytes();
        let latest = RawValue::from_string(latest).expect("serde_json wrote valid JSON");
        serde_json::to_vec(&LatestFile { latest: &latest, signature })
            .expect("a LatestFile always serializes")
    }

    /// Reads `latest.json` and verifies its signature with `key`, the key that the reader
    /// trusts.
    pub fn from_file(bytes: &[u8], key: &VerifyingKey) -> Result<Self, LatestError> {
        let file: LatestFile<'_> = serde_json::from_slice(bytes)?;
        key.verify_strict(file.latest.get().as_bytes(), &Signature::from_bytes(&file.signature))
            .map_err(|_| LatestError::Signature)?;
        Ok(serde_json::from_str(file.latest.get())?)
    }
}

/// A detached signature. The file `<file>.sig` holds the Ed25519 signature of the bytes of
/// `<file>`: 64 bytes in hex.
///
/// ```
/// use ed25519_dalek::SigningKey;
/// use ztreamer_verify::snapshot::SignatureFile;
///
/// let key = SigningKey::from_bytes(&[7; 32]);
/// let file = SignatureFile::sign(b"manifest", &key).to_hex();
/// let signature = SignatureFile::try_from(file.as_bytes()).unwrap();
/// assert!(signature.verify(b"manifest", &key.verifying_key()).is_ok());
/// assert_eq!(SignatureFile::path_of("manifests/a.json"), "manifests/a.json.sig");
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SignatureFile(Signature);

/// Why a reader refused a [`SignatureFile`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SignatureFileError {
    /// The file is not 64 bytes in hex.
    #[error("the signature file is not 64 bytes in hex")]
    Encoding,
    /// The signature does not verify with the key of the reader.
    #[error("the signature does not verify")]
    Mismatch,
}

impl SignatureFile {
    /// The end of the path of a signature file.
    pub const SUFFIX: &str = ".sig";

    /// The signature of `bytes` with `key`.
    pub fn sign(bytes: &[u8], key: &SigningKey) -> Self {
        Self(key.sign(bytes))
    }

    /// The path of the signature file for the file at `path`: `path`, then [`Self::SUFFIX`].
    pub fn path_of(path: &str) -> String {
        format!("{path}{}", Self::SUFFIX)
    }

    /// The contents of the signature file: the signature in hex.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0.to_bytes())
    }

    /// Verifies that this signature signs `bytes` with `key`.
    pub fn verify(&self, bytes: &[u8], key: &VerifyingKey) -> Result<(), SignatureFileError> {
        key.verify_strict(bytes, &self.0).map_err(|_| SignatureFileError::Mismatch)
    }
}

impl TryFrom<&[u8]> for SignatureFile {
    type Error = SignatureFileError;

    /// Decodes the contents of a signature file. The hex can have ASCII whitespace before and
    /// after it.
    fn try_from(file: &[u8]) -> Result<Self, Self::Error> {
        let bytes = hex::decode(file.trim_ascii()).map_err(|_| SignatureFileError::Encoding)?;
        Signature::from_slice(&bytes).map(Self).map_err(|_| SignatureFileError::Encoding)
    }
}

/// The path of the blob of a range, relative to the snapshot directory:
/// `ranges/<start>-<digest prefix>.bin`. The prefix is the first 4 bytes of the BLAKE3 digest,
/// in hex.
///
/// ```
/// # use ztreamer_indexer::index::range_meta::ManifestEntry;
/// use ztreamer_verify::snapshot::RangePath;
///
/// # fn run(entry: &ManifestEntry) {
/// let path = RangePath::from(entry);
/// assert!(path.as_str().starts_with("ranges/"));
/// # }
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangePath(String);

impl RangePath {
    /// The path as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&ManifestEntry> for RangePath {
    fn from(entry: &ManifestEntry) -> Self {
        let prefix = hex::encode(&entry.blob_digest.0[..4]);
        Self(format!("ranges/{}-{prefix}.bin", entry.start_height))
    }
}

/// Writes a snapshot directory in the layout of this module. One key signs the directory.
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
                .read_sealed_range_bytes(start)?
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
    use ztreamer_indexer::index::range_meta::BlobDigest;

    use super::*;
    use crate::{
        epoch::synthetic_entries,
        fetch::{HttpSource, RangeSource as _},
        test_utils::{FileServer, SEED, SourceIndex},
    };

    #[test]
    fn the_snapshot_key_is_a_valid_key_that_is_not_weak() {
        let key = VerifyingKey::from_bytes(&SNAPSHOT_PUBKEY).unwrap();
        assert!(!key.is_weak());
    }

    fn latest() -> Latest {
        Latest { manifest: "manifests/2026-10-26T000000Z.json".to_owned(), sealed_through: 2_999 }
    }

    #[test]
    fn a_signed_latest_verifies_with_the_trusted_key_only() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let bytes = latest().to_file(&key);

        assert_eq!(Latest::from_file(&bytes, &key.verifying_key()).unwrap(), latest());
        let other = SigningKey::from_bytes(&[8; 32]).verifying_key();
        assert!(matches!(Latest::from_file(&bytes, &other), Err(LatestError::Signature)));
        let changed = String::from_utf8(bytes).unwrap().replace("2999", "3999");
        let error = Latest::from_file(changed.as_bytes(), &key.verifying_key()).unwrap_err();
        assert!(matches!(error, LatestError::Signature), "{error:?}");
    }

    #[test]
    fn an_unsigned_latest_is_refused() {
        let key = SigningKey::from_bytes(&[7; 32]).verifying_key();
        let unsigned = serde_json::json!({ "latest": latest() }).to_string();

        let error = Latest::from_file(unsigned.as_bytes(), &key).unwrap_err();

        assert!(matches!(error, LatestError::Json(_)), "{error:?}");
    }

    #[test]
    fn a_signature_file_verifies_its_bytes_with_its_key_only() {
        let key = SigningKey::from_bytes(&SEED);
        let file = format!("{}\n", SignatureFile::sign(b"manifest", &key).to_hex());
        let signature = SignatureFile::try_from(file.as_bytes()).unwrap();

        assert_eq!(signature.verify(b"manifest", &key.verifying_key()), Ok(()));
        let other = SigningKey::from_bytes(&[8; 32]).verifying_key();
        assert_eq!(signature.verify(b"manifest", &other), Err(SignatureFileError::Mismatch));
        let error = signature.verify(b"other bytes", &key.verifying_key());
        assert_eq!(error, Err(SignatureFileError::Mismatch));
        for file in [&file[..126], "zz"] {
            let error = SignatureFile::try_from(file.as_bytes());
            assert_eq!(error, Err(SignatureFileError::Encoding));
        }
    }

    #[test]
    fn a_range_path_names_the_start_and_the_digest_prefix() {
        let mut entry = synthetic_entries(&[Default::default(); 2]).pop().unwrap();
        entry.blob_digest = BlobDigest([0xab; 32]);

        assert_eq!(RangePath::from(&entry).as_str(), "ranges/1000-abababab.bin");
    }

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
