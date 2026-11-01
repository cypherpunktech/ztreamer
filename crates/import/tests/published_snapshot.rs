//! The published mainnet snapshot at `https://snapshot.ztreamer.sh/mainnet`.
//!
//! The test reads the current snapshot over the network, so it is ignored by default. Run it
//! with `cargo t -p ztreamer-import --test published_snapshot -- --ignored`.

use ed25519_dalek::VerifyingKey;
use http::Uri;
use zakura_chain::parameters::Network;
use ztreamer_import::fetch::{HttpSource, RangeSource as _};
use ztreamer_snapshot::layout::SNAPSHOT_PUBKEY;

#[tokio::test]
#[ignore = "reads https://snapshot.ztreamer.sh"]
async fn the_published_snapshot_verifies_with_the_production_key_and_obeys_the_rules() {
    let key = VerifyingKey::from_bytes(&SNAPSHOT_PUBKEY).unwrap();
    let base = Uri::from_static("https://snapshot.ztreamer.sh/mainnet");
    let source = HttpSource::new(base, key).unwrap();

    // `manifest` verifies `latest.json` and the manifest signature, checks the manifest rules,
    // and checks that `latest.json` and the manifest give the same sealed height.
    let manifest = source.manifest().await.unwrap();

    assert_eq!(manifest.network(), Network::Mainnet.to_string());
    assert_eq!(manifest.genesis_hash(), Network::Mainnet.genesis_hash());
    assert!(manifest.sealed_through() > 3_000_000, "{}", manifest.sealed_through());
}
