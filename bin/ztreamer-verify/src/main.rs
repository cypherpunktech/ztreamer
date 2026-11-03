//! `ztreamer-verify`: checks a published snapshot with the checks of a replica import, and
//! writes no index. [`ztreamer_import::verifier`] does the checks. [`Cli::run`] gives them the
//! snapshot of `--snapshot-url` and the node of `--node`.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use std::sync::Arc;

use clap::Parser;
use ed25519_zebra::VerificationKey;
use eyre::{Result, WrapErr, eyre};
use http::Uri;
use tracing::info;
use tracing_subscriber::EnvFilter;
use ztreamer_import::{
    attest::JsonRpcAttestor,
    fetch::HttpSource,
    importer::TrustedNode,
    sample::{JsonRpcSampler, SamplePlan},
    verifier::{NodeCheck, SnapshotVerifier},
};
use ztreamer_index::index::range_meta::NodeId;
use ztreamer_remote::{Auth, NodeUrl, RpcClient, ZakuraClient};
use ztreamer_snapshot::layout::SNAPSHOT_PUBKEY;

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Checks a published ztreamer snapshot with the checks of a replica import, and \
             writes no index. Exits with 0 only if each epoch passes"
)]
struct Cli {
    /// Base URL of the snapshot: the directory of `latest.json`, for example
    /// `https://snapshot.ztreamer.sh/mainnet`.
    #[arg(long, value_name = "URL")]
    snapshot_url: Uri,

    /// JSON-RPC URL of a zakura node, for example `http://user:password@127.0.0.1:8232`. The node
    /// attests and samples each epoch. Without this flag, the check does not prove that the
    /// snapshot is the chain.
    #[arg(long, value_name = "URL")]
    node: Option<NodeUrl>,

    /// Log filter, in RUST_LOG syntax.
    #[arg(long, env = "RUST_LOG", default_value = "info", value_name = "DIRECTIVES")]
    log_filter: String,
}

impl Cli {
    /// Checks the snapshot, and logs the report.
    async fn run(self) -> Result<()> {
        let key = VerificationKey::try_from(SNAPSHOT_PUBKEY).expect("the built-in key is valid");
        let source = HttpSource::new(self.snapshot_url.clone(), key)
            .wrap_err("configure the --snapshot-url client")?;
        let report = SnapshotVerifier::new(Box::new(source), self.node_check()?)
            .run()
            .await
            .wrap_err_with(|| format!("{} does not verify", self.snapshot_url))?;
        info!(
            sealed_through = report.sealed_through,
            epochs = report.epochs,
            ranges = report.ranges,
            blob_bytes = report.blob_bytes,
            attested_by = self.node.as_ref().map(|node| node.uri().to_string()),
            sampled_heights = report.sampled_heights,
            "snapshot verified"
        );
        Ok(())
    }

    /// The checks of `--node`, with a random sample seed.
    fn node_check(&self) -> Result<Option<NodeCheck>> {
        let Some(url) = &self.node else {
            return Ok(None);
        };
        let auth = url.credentials().cloned().unwrap_or(Auth::None);
        let config = ZakuraClient::config(url.uri().clone()).with_auth(auth);
        let client =
            ZakuraClient::new(RpcClient::new(config).wrap_err("configure the --node client")?);
        let seed = getrandom::u64().map_err(|error| eyre!("random sample seed: {error}"))?;
        let node = TrustedNode {
            attestor: Arc::new(JsonRpcAttestor::new(client.clone())),
            sampler: Arc::new(JsonRpcSampler::new(client)),
            id: NodeId { url: url.uri().to_string(), version: String::new() },
        };
        Ok(Some(NodeCheck { node, sample: SamplePlan { per_range: SamplePlan::PER_RANGE, seed } }))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let filter = EnvFilter::try_new(&cli.log_filter).wrap_err("parse --log-filter")?;
    tracing_subscriber::fmt().with_env_filter(filter).init();
    cli.run().await
}
