//! The paths of the files in a snapshot directory.
//!
//! Each path type holds the values that its file name encodes. Its `Display` form is the path
//! relative to the snapshot directory, with `/`. An HTTP reader joins this form to the base URL.
//! [`ManifestPath::to_path`] and [`RangePath::to_path`] give the same path for the disk.

use std::{fmt, path::PathBuf, str::FromStr};

use chrono::{DateTime, NaiveDateTime, SubsecRound as _, Utc};
use serde_with::{DeserializeFromStr, SerializeDisplay};

use crate::manifest_entry::ManifestEntry;

/// The path of a manifest: `manifests/<time>.json`. `<time>` is the publish time in UTC, to
/// the second, for example `2026-10-26T143005Z`.
///
/// The JSON form is the path. Serde refuses a path that is not a manifest path. Thus a
/// `latest.json` cannot point at a file out of `manifests/`.
///
/// ```
/// use chrono::DateTime;
/// use ztreamer_snapshot::layout::ManifestPath;
///
/// let time = DateTime::from_timestamp(1_792_972_800, 0).unwrap();
/// let path = ManifestPath::at(time);
/// assert_eq!(path.to_string(), "manifests/2026-10-26T000000Z.json");
/// assert_eq!("manifests/2026-10-26T000000Z.json".parse(), Ok(path));
/// ```
#[derive(
    Clone, Copy, Debug, DeserializeFromStr, Eq, Hash, Ord, PartialEq, PartialOrd, SerializeDisplay,
)]
pub struct ManifestPath {
    published_at: DateTime<Utc>,
}

impl ManifestPath {
    /// The directory of the manifests.
    const DIR: &str = "manifests/";
    /// The extension of a manifest.
    const EXTENSION: &str = ".json";
    /// The format of the publish time in the file name.
    const TIME_FORMAT: &str = "%Y-%m-%dT%H%M%SZ";

    /// The path of the manifest that is published at `time`. The path removes the parts of a
    /// second.
    pub fn at(time: DateTime<Utc>) -> Self {
        Self { published_at: time.trunc_subsecs(0) }
    }

    /// The publish time of the manifest, to the second.
    pub const fn published_at(&self) -> DateTime<Utc> {
        self.published_at
    }

    /// The path relative to the snapshot directory.
    pub fn to_path(&self) -> PathBuf {
        PathBuf::from(self.to_string())
    }

    /// The path of the [`SignatureFile`](super::SignatureFile) of the manifest, with `/`.
    pub fn signature_path(&self) -> String {
        super::SignatureFile::path_of(&self.to_string())
    }
}

impl fmt::Display for ManifestPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let time = self.published_at.format(Self::TIME_FORMAT);
        write!(f, "{}{time}{}", Self::DIR, Self::EXTENSION)
    }
}

impl FromStr for ManifestPath {
    type Err = ManifestPathError;

    /// Reads the publish time from `manifests/<time>.json`. Refuses a path that
    /// [`ManifestPath`] does not write in the same form, for example a time without zero
    /// padding.
    fn from_str(path: &str) -> Result<Self, Self::Err> {
        let error = || ManifestPathError(path.to_owned());
        let time = path
            .strip_prefix(Self::DIR)
            .and_then(|name| name.strip_suffix(Self::EXTENSION))
            .ok_or_else(error)?;
        let manifest = NaiveDateTime::parse_from_str(time, Self::TIME_FORMAT)
            .map(|time| Self { published_at: time.and_utc() })
            .map_err(|_| error())?;
        (manifest.to_string() == path).then_some(manifest).ok_or_else(error)
    }
}

/// The path is not `manifests/<time>.json`.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("{0:?} is not manifests/<time>.json with a time such as 2026-10-26T143005Z")]
pub struct ManifestPathError(String);

/// The path of the blob of a range: `ranges/<start>-<digest prefix>.bin`. The prefix is the
/// first 4 bytes of the BLAKE3 digest of the blob, in hex.
///
/// ```
/// # use ztreamer_snapshot::manifest_entry::ManifestEntry;
/// use ztreamer_snapshot::layout::RangePath;
///
/// # fn run(entry: &ManifestEntry) {
/// let path = RangePath::from(entry);
/// assert!(path.to_string().starts_with("ranges/"));
/// # }
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RangePath {
    start: u32,
    digest_prefix: [u8; 4],
}

impl RangePath {
    /// The path relative to the snapshot directory.
    pub fn to_path(&self) -> PathBuf {
        PathBuf::from(self.to_string())
    }
}

impl fmt::Display for RangePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ranges/{}-{}.bin", self.start, hex::encode(self.digest_prefix))
    }
}

impl From<&ManifestEntry> for RangePath {
    fn from(entry: &ManifestEntry) -> Self {
        let [a, b, c, d, ..] = entry.blob_digest.0;
        Self { start: entry.start_height, digest_prefix: [a, b, c, d] }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{manifest_entry::BlobDigest, test_utils::entries_from_genesis};

    #[test]
    fn a_manifest_path_round_trips_and_drops_the_parts_of_a_second() {
        let time = DateTime::from_timestamp(1_792_972_805, 999_000_000).unwrap();
        let path = ManifestPath::at(time);

        assert_eq!(path.to_string(), "manifests/2026-10-26T000005Z.json");
        assert_eq!(path.published_at(), DateTime::from_timestamp(1_792_972_805, 0).unwrap());
        assert_eq!(path.to_string().parse(), Ok(path));
        assert_eq!(path.to_path(), PathBuf::from("manifests/2026-10-26T000005Z.json"));
        assert_eq!(path.signature_path(), "manifests/2026-10-26T000005Z.json.sig");
    }

    #[test]
    fn a_path_that_is_not_a_manifest_path_is_refused() {
        for wrong in [
            "manifests/.json",
            "manifests/a.json",
            "manifests/2026-10-26T000000Z.json.sig",
            "manifests/2026-1-26T000000Z.json",
            "manifests/../2026-10-26T000000Z.json",
            "/manifests/2026-10-26T000000Z.json",
            "ranges/2026-10-26T000000Z.json",
        ] {
            let error = wrong.parse::<ManifestPath>();
            assert_eq!(error, Err(ManifestPathError(wrong.to_owned())), "{wrong}");
        }
    }

    #[test]
    fn serde_uses_the_path_and_refuses_another_path() {
        let json = "\"manifests/2026-10-26T000000Z.json\"";
        let path: ManifestPath = serde_json::from_str(json).unwrap();

        assert_eq!(serde_json::to_string(&path).unwrap(), json);
        assert!(serde_json::from_str::<ManifestPath>("\"../latest.json\"").is_err());
    }

    #[test]
    fn manifest_paths_sort_by_publish_time() {
        let [old, new] = [0, 86_400].map(|offset| {
            ManifestPath::at(DateTime::from_timestamp(1_792_972_800 + offset, 0).unwrap())
        });

        assert!(old < new);
    }

    #[test]
    fn a_range_path_names_the_start_and_the_digest_prefix() {
        let mut entry = entries_from_genesis(&[Default::default(); 2]).pop().unwrap();
        entry.blob_digest = BlobDigest([0xab; 32]);

        assert_eq!(RangePath::from(&entry).to_string(), "ranges/1000-abababab.bin");
    }
}
