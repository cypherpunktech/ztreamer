//! The paths of the files in a snapshot directory.
//!
//! A path is relative to the snapshot directory and uses `/`. A writer joins it to the directory.
//! An HTTP reader joins it to the base URL.

use std::{fmt, path::Path};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::manifest_entry::ManifestEntry;

/// The path of a manifest: `manifests/<time>.json`. `<time>` is UTC, for example
/// `2026-10-26T143005Z`.
///
/// Serde refuses a path that does not have this form. Thus a `latest.json` cannot point at a
/// file out of `manifests/`.
///
/// ```
/// use chrono::DateTime;
/// use ztreamer_snapshot::layout::ManifestPath;
///
/// let time = DateTime::from_timestamp(1_792_972_800, 0).unwrap();
/// assert_eq!(ManifestPath::at(time).as_str(), "manifests/2026-10-26T000000Z.json");
/// ```
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "String", into = "String")]
pub struct ManifestPath(String);

impl ManifestPath {
    /// The directory of the manifests.
    const DIR: &str = "manifests/";
    /// The extension of a manifest.
    const EXTENSION: &str = ".json";

    /// The path of the manifest that is published at `time`.
    pub fn at(time: DateTime<Utc>) -> Self {
        Self(format!("{}{}{}", Self::DIR, time.format("%Y-%m-%dT%H%M%SZ"), Self::EXTENSION))
    }

    /// The path as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<Path> for ManifestPath {
    fn as_ref(&self) -> &Path {
        Path::new(&self.0)
    }
}

impl fmt::Display for ManifestPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<ManifestPath> for String {
    fn from(path: ManifestPath) -> Self {
        path.0
    }
}

impl TryFrom<String> for ManifestPath {
    type Error = ManifestPathError;

    /// Refuses a path that is not `manifests/<name>.json`, with a name that is not empty and
    /// has no `/`.
    fn try_from(path: String) -> Result<Self, Self::Error> {
        let name = path
            .strip_prefix(Self::DIR)
            .and_then(|name| name.strip_suffix(Self::EXTENSION))
            .filter(|name| !name.is_empty() && !name.contains('/'));
        match name {
            Some(_) => Ok(Self(path)),
            None => Err(ManifestPathError(path)),
        }
    }
}

/// The path is not `manifests/<name>.json`.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("manifest path {0:?} is not manifests/<name>.json")]
pub struct ManifestPathError(String);

/// The path of the blob of a range: `ranges/<start>-<digest prefix>.bin`. The prefix is the
/// first 4 bytes of the BLAKE3 digest, in hex.
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

impl AsRef<Path> for RangePath {
    fn as_ref(&self) -> &Path {
        Path::new(&self.0)
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
    use super::*;
    use crate::{manifest_entry::BlobDigest, test_utils::entries_from_genesis};

    #[test]
    fn a_manifest_path_has_the_form_of_the_layout() {
        let path = ManifestPath::try_from("manifests/2026-10-26T000000Z.json".to_owned());
        assert_eq!(path.unwrap().as_str(), "manifests/2026-10-26T000000Z.json");

        for wrong in [
            "manifests/.json",
            "manifests/a/b.json",
            "manifests/../../latest.json",
            "ranges/0-abababab.bin",
            "/manifests/a.json",
            "manifests/a.json.sig",
        ] {
            let error = ManifestPath::try_from(wrong.to_owned());
            assert_eq!(error, Err(ManifestPathError(wrong.to_owned())), "{wrong}");
        }
    }

    #[test]
    fn serde_refuses_a_manifest_path_out_of_the_layout() {
        let path: ManifestPath = serde_json::from_str("\"manifests/a.json\"").unwrap();
        assert_eq!(serde_json::to_string(&path).unwrap(), "\"manifests/a.json\"");

        assert!(serde_json::from_str::<ManifestPath>("\"../a.json\"").is_err());
    }

    #[test]
    fn a_range_path_names_the_start_and_the_digest_prefix() {
        let mut entry = entries_from_genesis(&[Default::default(); 2]).pop().unwrap();
        entry.blob_digest = BlobDigest([0xab; 32]);

        assert_eq!(RangePath::from(&entry).as_str(), "ranges/1000-abababab.bin");
    }
}
