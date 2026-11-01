//! The snapshot manifest: the JSON document that lists every sealed range of a snapshot.
//!
//! ```json
//! {
//!   "network": "Mainnet",
//!   "genesis_hash": "00040fe8ec8471911baa1db1266ea15dd06b4a8a5c453883c000b031973dce08",
//!   "sealed_through": 1785999,
//!   "manifest_version": 1,
//!   "entries": [{
//!     "range_version": 1,
//!     "start_height": 1785000, "end_height": 1785999,
//!     "end_hash": "<32 bytes hex, display order>",
//!     "end_tree_sizes": { "sapling": 61231877, "orchard": 0, "ironwood": 0 },
//!     "sapling_frontier": "<hex>", "orchard_frontier": "<hex>", "ironwood_frontier": "<hex>",
//!     "subtree_roots": [{ "pool": "sapling", "index": 934, "root": "<32 bytes hex>" }],
//!     "blob_digest": "<BLAKE3 hex>", "blob_len": 121034512,
//!     "epoch_end": true
//!   }]
//! }
//! ```
//!
//! Block hashes are hex in display order, as the node prints them. A frontier is the hex of the
//! `zcashd` RPC tree encoding. An empty string means that the pool is not active; an active pool
//! with an empty tree has the encoding of the empty tree. A subtree root is the hex of the pool's
//! byte order, as `z_getsubtreesbyindex` prints it.

use std::borrow::Cow;

use serde::{Deserialize, Serialize, Serializer};
use serde_with::{DisplayFromStr, serde_as};
use zakura_chain::block;
use ztreamer_chain::codec::RANGE_SIZE;

use crate::{
    epoch::Epochs,
    manifest_entry::{MANIFEST_VERSION, ManifestEntry},
};

/// A snapshot manifest that obeys the rules in the module docs.
///
/// The fields are private. Each way to get a manifest checks the rules: [`Manifest::new`],
/// [`Manifest::from_entries_to_gap`], [`Manifest::from_json`] and serde. Thus a `Manifest`
/// is always valid, and it has at least one entry.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(try_from = "ManifestJson<'static>")]
pub struct Manifest {
    network: String,
    genesis_hash: block::Hash,
    entries: Vec<ManifestEntry>,
}

impl Manifest {
    /// The manifest of `entries`, one per sealed range in height order from genesis, for
    /// `network` with the genesis block hash. Its JSON form is at [`MANIFEST_VERSION`], with
    /// `sealed_through` at the end of the last entry. It checks that the entries obey the rules.
    ///
    /// ```
    /// use zakura_chain::block;
    /// use ztreamer_snapshot::manifest_entry::ManifestEntry;
    /// use ztreamer_snapshot::manifest::{Manifest, ManifestError};
    ///
    /// fn manifest(genesis: block::Hash, entries: Vec<ManifestEntry>) -> Result<Manifest, ManifestError> {
    ///     Manifest::new("Mainnet", genesis, entries)
    /// }
    /// ```
    pub fn new(
        network: impl Into<String>,
        genesis_hash: block::Hash,
        entries: Vec<ManifestEntry>,
    ) -> Result<Self, ManifestError> {
        let manifest = Self { network: network.into(), genesis_hash, entries };
        manifest.check_entries()?;
        Ok(manifest)
    }

    /// The manifest of the `range_meta` entries of an index, for `network` with the genesis
    /// block hash. `entries` are in height order from genesis. The manifest takes the entries
    /// that follow each other from genesis, and stops at the first gap. It sets `epoch_end` on
    /// its last entry, because [`Epochs`] closes an epoch at the last entry of a manifest. The
    /// other entries keep their flag, and [`Manifest::new`] checks it with the epoch rule.
    ///
    /// `ztreamerd export-snapshot` and a peer source make their manifests with this
    /// constructor. Thus an export and a peer give the same manifest for the same rows.
    ///
    /// ```
    /// use zakura_chain::block;
    /// use ztreamer_snapshot::manifest_entry::ManifestEntry;
    /// use ztreamer_snapshot::manifest::{Manifest, ManifestError};
    ///
    /// fn manifest(genesis: block::Hash, rows: Vec<ManifestEntry>) -> Result<Manifest, ManifestError> {
    ///     Manifest::from_entries_to_gap("Mainnet", genesis, rows)
    /// }
    /// ```
    pub fn from_entries_to_gap(
        network: impl Into<String>,
        genesis_hash: block::Hash,
        entries: impl IntoIterator<Item = ManifestEntry>,
    ) -> Result<Self, ManifestError> {
        let mut entries: Vec<_> = entries
            .into_iter()
            .scan(0, |next_start, entry| {
                (entry.start_height == *next_start).then(|| {
                    *next_start = entry.end_height.saturating_add(1);
                    entry
                })
            })
            .collect();
        if let Some(last) = entries.last_mut() {
            last.epoch_end = true;
        }
        Self::new(network, genesis_hash, entries)
    }

    /// Parses a manifest and checks the rules.
    pub fn from_json(json: &str) -> Result<Self, ManifestError> {
        serde_json::from_str::<ManifestJson<'_>>(json)?.try_into()
    }

    /// The network name, for example `Mainnet`.
    pub fn network(&self) -> &str {
        &self.network
    }

    /// The hash of the genesis block.
    pub const fn genesis_hash(&self) -> block::Hash {
        self.genesis_hash
    }

    /// The last height of the last sealed range.
    pub fn sealed_through(&self) -> u32 {
        self.entries.last().map_or(0, |entry| entry.end_height)
    }

    /// One entry per sealed range, in height order from genesis.
    pub fn entries(&self) -> &[ManifestEntry] {
        &self.entries
    }

    /// The verification epochs of the manifest.
    pub fn epochs(&self) -> Epochs<'_> {
        Epochs::new(&self.entries)
    }

    /// Checks that:
    ///
    /// - there is at least one entry;
    /// - the entries are one aligned range each, adjacent from genesis, with tree sizes that
    ///   do not decrease, and with no pool that becomes inactive;
    /// - `epoch_end` agrees with [`Epochs`].
    fn check_entries(&self) -> Result<(), ManifestError> {
        if self.entries.is_empty() {
            return Err(ManifestError::Empty);
        }
        self.entries
            .iter()
            .enumerate()
            .try_for_each(|(index, entry)| self.check_entry(index, entry))?;
        self.check_epoch_ends()
    }

    /// Checks that entry `index` is one aligned range, and that it follows the entry before it
    /// or starts at genesis.
    fn check_entry(&self, index: usize, entry: &ManifestEntry) -> Result<(), ManifestError> {
        let previous = index.checked_sub(1).map(|previous| &self.entries[previous]);
        let expected_start =
            previous.map_or(Some(0), |previous| previous.end_height.checked_add(1));
        if expected_start != Some(entry.start_height) {
            return Err(ManifestError::Start { index, start_height: entry.start_height });
        }
        if entry.start_height.checked_add(RANGE_SIZE - 1) != Some(entry.end_height) {
            return Err(ManifestError::RangeLength { index });
        }
        let Some(previous) = previous else {
            return Ok(());
        };
        if !entry.end_tree_sizes.follows(previous.end_tree_sizes) {
            return Err(ManifestError::TreeSizes { index });
        }
        if !entry.frontiers.follows(&previous.frontiers) {
            return Err(ManifestError::PoolInactive { index });
        }
        Ok(())
    }

    /// Checks that each entry has `epoch_end` set when it is the last entry of its epoch.
    fn check_epoch_ends(&self) -> Result<(), ManifestError> {
        Epochs::new(&self.entries)
            .flat_map(|epoch| {
                let last = epoch.entries().len() - 1;
                epoch.entries().iter().enumerate().map(move |(at, entry)| (entry, at == last))
            })
            .enumerate()
            .find(|(_, (entry, expected))| entry.epoch_end != *expected)
            .map_or(Ok(()), |(index, (_, expected))| {
                Err(ManifestError::EpochEnd { index, expected })
            })
    }
}

impl Serialize for Manifest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        ManifestJson::from(self).serialize(serializer)
    }
}

impl TryFrom<ManifestJson<'_>> for Manifest {
    type Error = ManifestError;

    /// Checks that the version is [`MANIFEST_VERSION`], then the rules of [`Manifest::new`], then
    /// that `sealed_through` is the last height of the last entry.
    fn try_from(json: ManifestJson<'_>) -> Result<Self, ManifestError> {
        if json.manifest_version != MANIFEST_VERSION {
            return Err(ManifestError::Version { version: json.manifest_version });
        }
        let manifest = Self::new(json.network, json.genesis_hash, json.entries.into_owned())?;
        let last_end = manifest.sealed_through();
        if last_end != json.sealed_through {
            return Err(ManifestError::SealedThrough {
                sealed_through: json.sealed_through,
                last_end,
            });
        }
        Ok(manifest)
    }
}

/// A manifest that does not parse or does not obey the rules in the module docs.
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    /// The JSON does not parse into a manifest.
    #[error("manifest JSON is not valid")]
    Json(#[from] serde_json::Error),
    /// The manifest format version is not [`MANIFEST_VERSION`].
    #[error("unsupported manifest version {version}")]
    Version {
        /// The version in the manifest.
        version: u32,
    },
    /// The manifest has no entries.
    #[error("the manifest has no entries")]
    Empty,
    /// `sealed_through` is not the last height of the last entry.
    #[error("sealed_through {sealed_through} is not the end of the last entry ({last_end})")]
    SealedThrough {
        /// The `sealed_through` of the manifest.
        sealed_through: u32,
        /// The last height of the last entry.
        last_end: u32,
    },
    /// The entry does not start one height after the previous entry, or at genesis.
    #[error("entry {index} starts at {start_height}, not after the previous entry")]
    Start {
        /// The position of the entry in the manifest.
        index: usize,
        /// The start height of the entry.
        start_height: u32,
    },
    /// The entry is not one aligned range.
    #[error("entry {index} does not hold {RANGE_SIZE} blocks")]
    RangeLength {
        /// The position of the entry in the manifest.
        index: usize,
    },
    /// A tree size of the entry is smaller than in the previous entry.
    #[error("entry {index} has a smaller tree than the previous entry")]
    TreeSizes {
        /// The position of the entry in the manifest.
        index: usize,
    },
    /// A frontier of the entry is empty, but the same frontier of the previous entry is not. An
    /// empty frontier means that the pool is not active, and a pool does not become inactive.
    #[error("entry {index} has an empty frontier for a pool that is active")]
    PoolInactive {
        /// The position of the entry in the manifest.
        index: usize,
    },
    /// The `epoch_end` flag of the entry does not agree with the epoch rule.
    #[error("entry {index} has the wrong epoch_end flag; the epoch rule gives {expected}")]
    EpochEnd {
        /// The position of the entry in the manifest.
        index: usize,
        /// The flag that the epoch rule gives.
        expected: bool,
    },
}

/// The JSON form of a [`Manifest`], before the rules are checked. It borrows a manifest to
/// serialize it, and owns its fields after it deserializes.
#[serde_as]
#[derive(Deserialize, Serialize)]
struct ManifestJson<'a> {
    network: Cow<'a, str>,
    #[serde_as(as = "DisplayFromStr")]
    genesis_hash: block::Hash,
    sealed_through: u32,
    manifest_version: u32,
    entries: Cow<'a, [ManifestEntry]>,
}

impl<'a> From<&'a Manifest> for ManifestJson<'a> {
    fn from(manifest: &'a Manifest) -> Self {
        Self {
            network: Cow::Borrowed(&manifest.network),
            genesis_hash: manifest.genesis_hash,
            sealed_through: manifest.sealed_through(),
            manifest_version: MANIFEST_VERSION,
            entries: Cow::Borrowed(&manifest.entries),
        }
    }
}

#[cfg(test)]
mod tests;
