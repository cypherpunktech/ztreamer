//! Imports the first range of the published Mainnet snapshot with the code of this commit.
//!
//! The deployed `ztreamerd` writes the published snapshot. An export writes only the new
//! ranges, thus the published blobs keep the bytes of the version that wrote them. Each new
//! version must read them. The test reads `latest.json` and the manifest that it names with the
//! production key, then imports range 0 from the published blob. A mock node gives the answers of
//! zakura for height 999, recorded from a Mainnet node.
//!
//! The test downloads about 7 MB, thus it is ignored by default. The `Published snapshot` CI job
//! runs it: `cargo test -p ztreamer-import --test published_snapshot -- --ignored`.

use std::{num::NonZeroUsize, sync::Arc};

use async_trait::async_trait;
use ed25519_dalek::VerifyingKey;
use http::StatusCode;
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

const SNAPSHOT_URL: &str = "https://snapshot.ztreamer.sh/mainnet";

/// The hash of the Mainnet block at height 999, the end of range 0.
const END_HASH: &str = "0000000aeb86e32aeb49ef9e40bfbd69585bd67ebd62d535e7e728ed2ba42064";

/// The published snapshot with only its first range.
#[derive(Debug)]
struct FirstRange(HttpSource);

#[async_trait]
impl RangeSource for FirstRange {
    async fn manifest(&self) -> Result<Manifest, FetchError> {
        let manifest = self.0.manifest().await?;
        let first = manifest.entries()[0].clone();
        Ok(Manifest::from_contiguous_entries(manifest.network(), manifest.genesis_hash(), [first])?)
    }

    async fn fetch_blob(
        &self,
        entry: &ManifestEntry,
        offset: u64,
    ) -> Result<BlobStream, FetchError> {
        self.0.fetch_blob(entry, offset).await
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "downloads the published snapshot"]
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
    let key = VerifyingKey::from_bytes(&SNAPSHOT_PUBKEY).unwrap();
    let source = FirstRange(HttpSource::new(SNAPSHOT_URL.parse().unwrap(), key).unwrap());
    let dir = tempfile::tempdir().unwrap();
    let network = Network::Mainnet;
    let index = Arc::new(
        Index::open(dir.path(), 1 << 30, &network.to_string(), network.genesis_hash()).unwrap(),
    );
    // The mock node has no full blocks, thus the import samples no height.
    let config = ImportConfig {
        network,
        verify_sample: SamplePlan { per_range: 0, seed: 0 },
        import_workers: NonZeroUsize::MIN,
    };
    let (_stop, shutdown) = watch::channel(false);

    let report =
        Importer::new(Box::new(source), trusted, index, config).run(shutdown).await.unwrap();

    let expected = ImportReport {
        epochs: 1,
        ranges: 1,
        sealed_through: Some(999),
        verified_through: Some(999),
        stopped: false,
    };
    assert_eq!(report, expected);
}
