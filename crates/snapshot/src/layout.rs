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
//! `<time>` is UTC, for example `2026-10-26T143005Z`.
//! `ztreamer_verify::snapshot_writer::SnapshotWriter` writes the directory.
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

use crate::manifest_entry::ManifestEntry;
use ed25519_dalek::{Signature, Signer as _, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_with::{hex::Hex, serde_as};

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
/// use ztreamer_snapshot::layout::SignatureFile;
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
/// # use ztreamer_snapshot::manifest_entry::ManifestEntry;
/// use ztreamer_snapshot::layout::RangePath;
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

#[cfg(test)]
mod tests {
    use crate::manifest_entry::BlobDigest;

    use super::*;
    use crate::test_utils::synthetic_entries;

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
        let key = SigningKey::from_bytes(&[7; 32]);
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
}
