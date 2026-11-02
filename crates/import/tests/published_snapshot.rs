//! The published mainnet snapshot at `https://snapshot.ztreamer.sh/mainnet`.
//!
//! The tests read the current snapshot over the network, so they are ignored by default. The
//! `Published snapshot` CI job runs them. Run them with
//! `cargo t -p ztreamer-import --test published_snapshot -- --ignored`.

use std::{num::NonZeroUsize, sync::Arc};

use async_trait::async_trait;
use ed25519_zebra::VerificationKey;
use http::{StatusCode, Uri};
use serde_json::json;
use tokio::sync::watch;
use zakura_chain::parameters::Network;
use ztreamer_import::{
    attest::JsonRpcAttestor,
    fetch::{BlobStream, FetchError, HttpSource, RangeSource},
    importer::{ImportConfig, ImportReport, Importer, TrustedNode},
    sample::{JsonRpcSampler, SamplePlan},
};
use ztreamer_index::index::{Index, range_meta::NodeId};
use ztreamer_remote::{
    RpcClient, ZakuraClient,
    test_utils::{MockNode, Reply},
};
use ztreamer_snapshot::{
    layout::SNAPSHOT_PUBKEY, manifest::Manifest, manifest_entry::ManifestEntry,
};

/// The hash of the Mainnet block at height 999, the end of range 0.
const END_HASH: &str = "0000000aeb86e32aeb49ef9e40bfbd69585bd67ebd62d535e7e728ed2ba42064";

#[tokio::test]
#[ignore = "reads https://snapshot.ztreamer.sh"]
async fn the_published_snapshot_verifies_with_the_production_key_and_obeys_the_rules() {
    let key = VerificationKey::try_from(SNAPSHOT_PUBKEY).unwrap();
    let base = Uri::from_static("https://snapshot.ztreamer.sh/mainnet");
    let source = HttpSource::new(base, key).unwrap();

    // `manifest` verifies `latest.json` and the manifest signature, checks the manifest rules,
    // and checks that `latest.json` and the manifest give the same sealed height.
    let manifest = source.manifest().await.unwrap();

    assert_eq!(manifest.network(), Network::Mainnet.to_string());
    assert_eq!(manifest.genesis_hash(), Network::Mainnet.genesis_hash());
    assert!(manifest.sealed_through() > 3_000_000, "{}", manifest.sealed_through());
}

/// The published snapshot with only its first range.
#[derive(Debug)]
struct FirstRange(HttpSource);

#[async_trait]
impl RangeSource for FirstRange {
    async fn manifest(&self) -> Result<Manifest, FetchError> {
        let manifest = self.0.manifest().await?;
        let first = manifest.entries()[0].clone();
        Ok(Manifest::from_entries_to_gap(manifest.network(), manifest.genesis_hash(), [first])?)
    }

    async fn fetch_blob(
        &self,
        entry: &ManifestEntry,
        offset: u64,
    ) -> Result<BlobStream, FetchError> {
        self.0.fetch_blob(entry, offset).await
    }
}

/// An export writes only the new ranges. Thus the published blobs keep the bytes of the version
/// that wrote them, and each new version must read them. The test imports range 0 from the
/// published blob. A mock node gives the answers of zakura for height 999, recorded from a
/// Mainnet node.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "reads https://snapshot.ztreamer.sh"]
async fn the_first_published_range_imports_and_verifies() {
    let node = MockNode::start(|_, request| match request.body["method"].as_str() {
        Some("getblockhash") => Reply::Result(json!(END_HASH)),
        Some("z_gettreestate") => Reply::Result(json!({
            "hash": END_HASH,
            "height": 999,
            "time": 1_477_750_672,
            "sapling": { "commitments": {} },
            "orchard": { "commitments": {} },
            "ironwood": { "commitments": {} },
        })),
        _ => Reply::Status { status: StatusCode::NOT_FOUND, body: "no recorded answer" },
    })
    .await;
    let zakura = ZakuraClient::new(RpcClient::new(node.config()).unwrap());
    let trusted = TrustedNode {
        attestor: Arc::new(JsonRpcAttestor::new(zakura.clone())),
        sampler: Arc::new(JsonRpcSampler::new(zakura)),
        id: NodeId { url: node.url().to_string(), version: "recorded".to_owned() },
    };
    let key = VerificationKey::try_from(SNAPSHOT_PUBKEY).unwrap();
    let base = Uri::from_static("https://snapshot.ztreamer.sh/mainnet");
    let source = FirstRange(HttpSource::new(base, key).unwrap());
    let dir = tempfile::tempdir().unwrap();
    let network = Network::Mainnet;
    let index = Index::open(dir.path(), 1 << 30, &network.to_string(), network.genesis_hash());
    // The mock node has no full blocks, thus the import samples no height.
    let config = ImportConfig {
        network,
        verify_sample: SamplePlan { per_range: 0, seed: 0 },
        import_workers: NonZeroUsize::MIN,
    };
    let importer = Importer::new(Box::new(source), trusted, Arc::new(index.unwrap()), config);
    let (_stop, shutdown) = watch::channel(false);

    let report = importer.run(shutdown).await.unwrap();

    let expected = ImportReport {
        epochs: 1,
        ranges: 1,
        sealed_through: Some(999),
        verified_through: Some(999),
        stopped: false,
    };
    assert_eq!(report, expected);
}
