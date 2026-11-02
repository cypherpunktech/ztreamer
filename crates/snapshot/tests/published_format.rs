//! The format of the published mainnet snapshot, from `https://snapshot.ztreamer.sh/mainnet`.
//!
//! The fixtures are the bytes that the source wrote on 2026-11-01:
//!
//! - `latest.json`: the file as published, signed with the production key.
//! - `manifest-prefix.json`: the header and the first 20 entries of the manifest that
//!   `latest.json` names, with `sealed_through` at the end of entry 19. The entries close two
//!   epochs.
//! - `entries-with-subtree-roots.json`: entries 558, 1707 and 3451 of that manifest. They hold
//!   the first subtree root of the Sapling, the Orchard and the Ironwood pool.
//!
//! The tests read each fixture and write it again. The bytes must not change: replicas read
//! the published files, and the signatures cover their exact bytes.

use ed25519_zebra::VerificationKey;
use serde::Deserialize;
use serde_json::value::RawValue;
use ztreamer_snapshot::{
    layout::{ManifestPointer, SNAPSHOT_PUBKEY},
    manifest::Manifest,
    manifest_entry::ManifestEntry,
    pool::Pool,
};

const LATEST: &[u8] = include_bytes!("fixtures/latest.json");
const MANIFEST_PREFIX: &str = include_str!("fixtures/manifest-prefix.json");
const ENTRIES_WITH_SUBTREE_ROOTS: &str = include_str!("fixtures/entries-with-subtree-roots.json");

/// The `latest` object of `latest.json`, as its exact bytes.
#[derive(Deserialize)]
struct LatestObject<'a> {
    #[serde(borrow)]
    latest: &'a RawValue,
}

#[test]
fn the_published_latest_verifies_and_writes_the_same_bytes() {
    let key = VerificationKey::try_from(SNAPSHOT_PUBKEY).unwrap();

    let latest = ManifestPointer::from_signed_json(LATEST, &key).unwrap();

    assert_eq!(latest.manifest.to_string(), "manifests/2026-11-01T104237Z.json");
    assert_eq!(latest.sealed_through, 3_536_999);
    let signed: LatestObject<'_> = serde_json::from_slice(LATEST).unwrap();
    assert_eq!(serde_json::to_string(&latest).unwrap(), signed.latest.get());
}

#[test]
fn the_published_manifest_obeys_the_rules_and_writes_the_same_bytes() {
    let manifest = Manifest::from_json(MANIFEST_PREFIX).unwrap();

    assert_eq!(manifest.network(), "Mainnet");
    assert_eq!(manifest.sealed_through(), 19_999);
    let epochs: Vec<_> = manifest.epochs().map(|epoch| epoch.end_height()).collect();
    assert_eq!(epochs, [9_999, 19_999]);
    assert_eq!(serde_json::to_string(&manifest).unwrap(), MANIFEST_PREFIX);
}

#[test]
fn published_entries_with_subtree_roots_write_the_same_bytes() {
    let entries: Vec<ManifestEntry> = serde_json::from_str(ENTRIES_WITH_SUBTREE_ROOTS).unwrap();

    let first_pools: Vec<_> =
        entries.iter().map(|entry| entry.subtree_roots[0].root.pool()).collect();
    assert_eq!(first_pools, [Pool::Sapling, Pool::Orchard, Pool::Ironwood]);
    assert_eq!(serde_json::to_string(&entries).unwrap(), ENTRIES_WITH_SUBTREE_ROOTS);
}
