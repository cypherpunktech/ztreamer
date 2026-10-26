//! The layout of a snapshot directory: what `ztreamerd export-snapshot` writes and a replica
//! reads.
//!
//! ```text
//! latest.json                         the signed pointer to the current manifest: Latest
//! manifests/<time>.json               one entry per sealed range; never replaced
//! manifests/<time>.json.sig           Ed25519 signature of the manifest bytes, hex
//! ranges/<start>-<digest prefix>.bin  range blob, at RangePath; never replaced
//! ```
//!
//! `latest.json` is the only file that an export replaces. It holds its own signature, so that a
//! reader never sees a new pointer with an old signature:
//!
//! ```json
//! {"latest":{"manifest":"manifests/2026-10-26T143005Z.json","sealed_through":1785999,
//!   "published_at":"2026-10-26T14:30:05Z","public_key":"<hex>"},"signature":"<hex>"}
//! ```
//!
//! The signature covers the exact bytes of the `latest` object. A reader verifies it with a key
//! that it trusts, never with `public_key`: a source that can change the file can also change
//! that key.

use std::fmt;

use ed25519_dalek::{Signature, Signer as _, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_with::{hex::Hex, serde_as};
use ztreamer_indexer::index::range_meta::ManifestEntry;

/// The Ed25519 public key that signs the snapshots that a replica accepts.
///
/// Plan PR 8.1 sets the production key. Until then, this key is a placeholder with no known
/// secret key: the SHA-256 hash of `ztreamer placeholder snapshot key 0` with the top bit clear.
/// Thus a replica refuses each snapshot over HTTP. A peer source has no signature.
pub const SNAPSHOT_PUBKEY: [u8; 32] = [
    0x42, 0xa7, 0x01, 0xa5, 0x38, 0x19, 0x3f, 0x17, 0xe5, 0x6b, 0xf6, 0xb5, 0x85, 0xea, 0x66, 0xe1,
    0x3f, 0x1f, 0xd7, 0xb1, 0xdd, 0x78, 0xfb, 0x15, 0x7d, 0xf5, 0x8e, 0x11, 0xb8, 0x70, 0xbc, 0x2b,
];

/// The end of the path of a signature file: `<file>.sig` signs `<file>`.
pub const SIGNATURE_SUFFIX: &str = ".sig";

/// The pointer to the current manifest of a snapshot, in `latest.json`.
#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Latest {
    /// The path of the manifest, relative to the snapshot directory.
    pub manifest: String,
    /// The last height of the last range in the manifest.
    pub sealed_through: u32,
    /// The export time, RFC 3339 in UTC.
    pub published_at: String,
    /// The key that signed the manifest and this pointer. Absent if the export is not signed.
    /// A reader must not trust it.
    #[serde_as(as = "Option<Hex>")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_key: Option<[u8; 32]>,
}

/// The JSON of `latest.json`: the pointer and the signature of its exact bytes.
#[serde_as]
#[derive(Deserialize, Serialize)]
struct LatestFile<'a> {
    #[serde(borrow)]
    latest: &'a RawValue,
    #[serde_as(as = "Option<Hex>")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    signature: Option<[u8; 64]>,
}

/// Why `latest.json` was refused.
#[derive(Debug, thiserror::Error)]
pub enum LatestError {
    /// The file does not parse.
    #[error("latest.json is not valid")]
    Json(#[from] serde_json::Error),
    /// The file has no signature.
    #[error("latest.json has no signature")]
    Unsigned,
    /// The signature does not verify with the trusted key.
    #[error("signature of latest.json does not verify")]
    Signature,
}

impl Latest {
    /// The path of the pointer, relative to the snapshot directory.
    pub const PATH: &str = "latest.json";

    /// The bytes of `latest.json`, signed with `key` when it is set.
    pub fn to_file(&self, key: Option<&SigningKey>) -> Vec<u8> {
        let latest = serde_json::to_string(self).expect("a Latest always serializes");
        let signature = key.map(|key| key.sign(latest.as_bytes()).to_bytes());
        let latest = RawValue::from_string(latest).expect("serde_json wrote valid JSON");
        serde_json::to_vec(&LatestFile { latest: &latest, signature })
            .expect("a LatestFile always serializes")
    }

    /// Reads `latest.json` and verifies its signature with `key`, the key that the reader
    /// trusts.
    pub fn from_file(bytes: &[u8], key: &VerifyingKey) -> Result<Self, LatestError> {
        let file: LatestFile<'_> = serde_json::from_slice(bytes)?;
        let signature = file.signature.ok_or(LatestError::Unsigned)?;
        key.verify_strict(file.latest.get().as_bytes(), &Signature::from_bytes(&signature))
            .map_err(|_| LatestError::Signature)?;
        Ok(serde_json::from_str(file.latest.get())?)
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

impl fmt::Display for RangePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use ztreamer_indexer::index::range_meta::BlobDigest;

    use super::*;
    use crate::epoch::synthetic_entries;

    #[test]
    fn the_snapshot_key_is_a_valid_key_that_is_not_weak() {
        let key = VerifyingKey::from_bytes(&SNAPSHOT_PUBKEY).unwrap();
        assert!(!key.is_weak());
    }

    fn latest() -> Latest {
        Latest {
            manifest: "manifests/2026-10-26T000000Z.json".to_owned(),
            sealed_through: 2_999,
            published_at: "2026-10-26T00:00:00Z".to_owned(),
            public_key: Some([9; 32]),
        }
    }

    #[test]
    fn a_signed_latest_verifies_with_the_trusted_key_only() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let bytes = latest().to_file(Some(&key));

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
        let unsigned = Latest { public_key: None, ..latest() }.to_file(None);

        assert!(matches!(Latest::from_file(&unsigned, &key), Err(LatestError::Unsigned)));
        let json: serde_json::Value = serde_json::from_slice(&unsigned).unwrap();
        assert_eq!(json["latest"]["sealed_through"], 2_999);
        assert!(json["latest"].get("public_key").is_none());
    }

    #[test]
    fn a_range_path_names_the_start_and_the_digest_prefix() {
        let mut entry = synthetic_entries(&[Default::default(); 2]).pop().unwrap();
        entry.blob_digest = BlobDigest([0xab; 32]);

        assert_eq!(RangePath::from(&entry).as_str(), "ranges/1000-abababab.bin");
    }
}
