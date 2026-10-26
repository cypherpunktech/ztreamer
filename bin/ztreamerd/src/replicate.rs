//! The `--replicate-from` startup path: a replica imports the sealed ranges of a snapshot or of a
//! peer, and a node verifies them. The node is the `--remote-rpc` node, or the embedded node when
//! it is near the chain tip. Then the daemon indexes the tail and follows the head, as it does
//! without an import.
//!
//! Before it fetches a range, [`Replica::import`] refuses:
//!
//! - a remote node that is out of reach, or that runs another chain or genesis block than the
//!   index;
//! - a manifest whose signature does not verify with `--snapshot-pubkey`;
//! - a manifest for another network or genesis block than the index;
//! - a node that prunes the blocks of the heights that the import attests.
//!
//! With `--serve-unverified`, the import installs each range of the reference after the chain
//! check only. Then [`Reattest`] runs the importer while the daemon serves: it proves these
//! epochs from the blobs in the index.

use std::{
    fmt, fs,
    num::NonZeroUsize,
    ops::ControlFlow,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use clap::Args;
use ed25519_dalek::VerifyingKey;
use eyre::{Result, WrapErr, bail, ensure, eyre};
use http::Uri;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::info;
use zakura_chain::parameters::Network;
use zakura_state::ReadStateService;
use ztreamer_indexer::index::{Index, range_meta::NodeId};
use ztreamer_remote::{RpcCode, ZakuraClient};
use ztreamer_service::{
    CompactService, RemoteBackend,
    replica::{ImportSource, ImportState, ReplicaStatus},
};
use ztreamer_verify::{
    attest::{JsonRpcAttestor, ReadStateAttestor},
    chain_ok::ChainOkImport,
    fetch::{Fetcher, HttpSource, PeerSource, RangeSource as _, Source},
    importer::{ImportConfig, ImportError, Importer, TrustedNode},
    manifest::Manifest,
    sample::{JsonRpcSampler, ReadStateSampler, SamplePlan},
};

/// The time between two `getblockchaininfo` calls while the node is in warm-up.
const WARMUP_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The command-line flags of the import. All are off by default.
#[derive(Debug, Args)]
pub(crate) struct ReplicateArgs {
    /// Import the sealed ranges from this source before the tail: an http:// or https:// base
    /// URL of a snapshot, or the host:port of a peer that serves the Replicator gRPC service
    /// without TLS. Give it again for more sources: the first is the reference. The
    /// --remote-rpc node or the embedded node verifies the ranges. The embedded node starts the
    /// import when it is near the chain tip.
    #[arg(long, value_name = "URL|HOST:PORT")]
    replicate_from: Vec<ReplicaSource>,

    /// The epochs that the import fetches at the same time, and the epochs that it verifies at
    /// the same time. Defaults to the cores minus 2.
    #[arg(long, value_name = "N", requires = "replicate_from")]
    import_workers: Option<NonZeroUsize>,

    /// Heights in each imported range that the import compares with the full blocks of the
    /// node.
    #[arg(long, value_name = "N", default_value_t = 2)]
    verify_sample: usize,

    /// File with the Ed25519 public key, in hex, that signs latest.json and the manifest of an
    /// http(s) --replicate-from snapshot. ztreamerd has no built-in key yet.
    #[arg(long, value_name = "FILE", requires = "replicate_from")]
    snapshot_pubkey: Option<PathBuf>,

    /// Serve each imported range after the chain check: the blob links to the range before it,
    /// and the node attests its end hash. The tree proofs of the epochs run in the background,
    /// and the replica reports import_state=verifying until they end.
    #[arg(long, requires = "replicate_from")]
    serve_unverified: bool,
}

impl ReplicateArgs {
    /// The import of `--replicate-from`, or `None` without that flag. It reads the key file,
    /// but it does not contact the source.
    pub(crate) fn replica(&self) -> Result<Option<Replica>> {
        if self.replicate_from.is_empty() {
            return Ok(None);
        }
        let key = self.snapshot_pubkey.as_deref().map(Replica::read_key).transpose()?;
        let snapshot =
            self.replicate_from.iter().find(|source| matches!(source, ReplicaSource::Snapshot(_)));
        if let Some(source) = snapshot
            && key.is_none()
        {
            bail!(
                "--replicate-from {source} needs --snapshot-pubkey: ztreamerd has no built-in key"
            );
        }
        Ok(Some(Replica {
            sources: self.replicate_from.clone(),
            key,
            workers: self.import_workers.unwrap_or_else(ImportConfig::default_import_workers),
            verify_sample: self.verify_sample,
            serve_unverified: self.serve_unverified,
        }))
    }
}

/// A `--replicate-from` value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ReplicaSource {
    /// A snapshot directory below an http:// or https:// base URL.
    Snapshot(Uri),
    /// A peer that serves the Replicator gRPC service at `http://host:port`.
    Peer(Uri),
}

impl FromStr for ReplicaSource {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, String> {
        let wrong = || format!("{value} is not an http(s) URL or a host:port");
        if value.starts_with("http://") || value.starts_with("https://") {
            return value.parse().map(Self::Snapshot).map_err(|_| wrong());
        }
        let uri: Uri = format!("http://{value}").parse().map_err(|_| wrong())?;
        let host_port = uri.port().is_some() && !value.contains('/');
        host_port.then_some(Self::Peer(uri)).ok_or_else(wrong)
    }
}

impl fmt::Display for ReplicaSource {
    /// The URL of a snapshot, or the host:port of a peer.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Snapshot(base) => write!(f, "{base}"),
            Self::Peer(endpoint) => match endpoint.authority() {
                Some(authority) => write!(f, "{authority}"),
                None => write!(f, "{endpoint}"),
            },
        }
    }
}

impl ReplicaSource {
    /// The source of ranges at this address. A snapshot needs `key`. A peer gets a connection.
    async fn connect(&self, key: Option<VerifyingKey>) -> Result<Source> {
        match self {
            Self::Snapshot(base) => {
                let key = key.ok_or_else(|| eyre!("--replicate-from {self} needs a key"))?;
                Ok(Source::Http(Box::new(HttpSource::new(base.clone(), key)?)))
            }
            Self::Peer(endpoint) => PeerSource::connect(endpoint.clone())
                .await
                .map(Source::Peer)
                .wrap_err_with(|| format!("connect to the peer {self}")),
        }
    }
}

/// The import that `--replicate-from` asks for.
#[derive(Clone, Debug)]
pub(crate) struct Replica {
    sources: Vec<ReplicaSource>,
    key: Option<VerifyingKey>,
    workers: NonZeroUsize,
    verify_sample: usize,
    serve_unverified: bool,
}

impl Replica {
    /// A new readiness status of the replica, in [`ImportState::Fetching`]. Make it after the
    /// Prometheus endpoint starts: its gauges register with the recorder of that time.
    ///
    /// [`ImportState::Fetching`]: ztreamer_service::replica::ImportState::Fetching
    pub(crate) fn new_status(&self) -> ReplicaStatus {
        let sources =
            self.sources.iter().map(|source| ImportSource { name: source.to_string(), strikes: 0 });
        ReplicaStatus::new(sources.collect())
    }

    /// Reads an Ed25519 public key in hex from `path`.
    fn read_key(path: &Path) -> Result<VerifyingKey> {
        let text = fs::read_to_string(path)
            .wrap_err_with(|| format!("read snapshot public key {}", path.display()))?;
        let bytes = hex::decode(text.trim_ascii())
            .ok()
            .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
            .ok_or_else(|| eyre!("{} does not hold a 32-byte key in hex", path.display()))?;
        VerifyingKey::from_bytes(&bytes)
            .wrap_err_with(|| format!("{} does not hold an Ed25519 public key", path.display()))
    }

    /// Imports the epochs of the reference manifest above the verified tip of `index`, checked
    /// against `node`. Breaks on shutdown. With `--serve-unverified`, it gives the
    /// [`Reattest`] to run while the daemon serves.
    ///
    /// The refusals of the module docs come first. An index that is verified at or above the
    /// reference manifest needs no import. On shutdown, the import completes the epoch or the
    /// range that it installs, and stops.
    pub(crate) async fn import<N: ImportNode>(
        &self,
        node: &N,
        network: &Network,
        index: &Arc<Index>,
        status: &ReplicaStatus,
        shutdown: &CancellationToken,
    ) -> Result<ControlFlow<(), Option<Reattest>>> {
        let ControlFlow::Continue(prune_height) = node.prune_height(network, shutdown).await?
        else {
            return Ok(ControlFlow::Break(()));
        };
        let mut sources = Vec::with_capacity(self.sources.len());
        let mut manifests = Vec::with_capacity(self.sources.len());
        for address in &self.sources {
            let source = address.connect(self.key).await?;
            let manifest = source
                .manifest()
                .await
                .wrap_err_with(|| format!("read the manifest of {address}"))?;
            Self::check_identity(&manifest, network)?;
            sources.push(source);
            manifests.push(manifest);
        }
        let state = {
            let index = Arc::clone(index);
            tokio::task::spawn_blocking(move || index.state())
                .await
                .wrap_err("index state task panicked")??
        };
        let (sealed, verified) = (state.sealed_through(), state.verified_through());
        let tip = manifests[0].sealed_through();
        if verified.is_some_and(|verified| verified >= tip) {
            info!(?verified, manifest = tip, "the index needs no import");
            return Ok(ControlFlow::Continue(None));
        }
        Self::check_prune_height(&manifests[0], verified, prune_height, N::NAME)?;

        let node = node.trusted_node().await?;
        let seed = getrandom::u64().map_err(|error| eyre!("random sample seed: {error}"))?;
        let plan = SamplePlan { per_range: self.verify_sample, seed };
        let config = ImportConfig {
            network: network.clone(),
            verify_sample: plan,
            import_workers: self.workers,
        };
        info!(
            sources = self.sources.len(),
            from = ?verified,
            to = tip,
            workers = self.workers,
            serve_unverified = self.serve_unverified,
            "importing sealed ranges"
        );
        if !self.serve_unverified {
            let import = Importer::new(sources, node, Arc::clone(index), config);
            let report = Self::until_shutdown(|stop| import.run(stop), shutdown).await?;
            return Ok(if report.stopped {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(None)
            });
        }
        // An index sealed through the tip holds the `ChainOk` rows of an earlier start.
        let mut chain_ok_through = sealed.filter(|&sealed| sealed >= tip).map(|_| tip);
        if chain_ok_through.is_none() {
            let fetcher = Fetcher::new(self.sources[0].connect(self.key).await?);
            let manifest = manifests.swap_remove(0);
            let attestor = Arc::clone(&node.attestor);
            let import = ChainOkImport::new(fetcher, manifest, attestor, Arc::clone(index));
            let report = Self::until_shutdown(|stop| import.run(stop), shutdown).await?;
            if report.stopped {
                return Ok(ControlFlow::Break(()));
            }
            chain_ok_through = report.chain_ok_through;
        }
        status.update(|progress| progress.chain_ok_through = chain_ok_through);
        let importer = Importer::new(sources, node, Arc::clone(index), config);
        let (index, status) = (Arc::clone(index), status.clone());
        Ok(ControlFlow::Continue(Some(Reattest { importer, index, status })))
    }

    /// Spawns the import that `run` makes from a stop signal. Sends the signal on shutdown,
    /// and waits for the import to stop.
    async fn until_shutdown<T, F>(
        run: impl FnOnce(watch::Receiver<bool>) -> F,
        shutdown: &CancellationToken,
    ) -> Result<T>
    where
        T: fmt::Debug + Send + 'static,
        F: Future<Output = Result<T, ImportError>> + Send + 'static,
    {
        let (stop, stopped) = watch::channel(false);
        let mut import = tokio::spawn(run(stopped));
        let report = tokio::select! {
            result = &mut import => result,
            _ = shutdown.cancelled() => {
                info!("stopping the import before shutdown");
                let _ = stop.send(true);
                import.await
            }
        }
        .wrap_err("import task panicked")?
        .wrap_err("import the sealed ranges")?;
        info!(?report, "import complete");
        Ok(report)
    }

    /// Refuses a manifest for another network or genesis block than the index.
    fn check_identity(manifest: &Manifest, network: &Network) -> Result<()> {
        let name = network.to_string();
        ensure!(
            manifest.network() == name,
            "the snapshot is for network {}, the index for {name}",
            manifest.network()
        );
        ensure!(
            manifest.genesis_hash() == network.genesis_hash(),
            "the snapshot has genesis block {}, the index needs {}",
            manifest.genesis_hash(),
            network.genesis_hash()
        );
        Ok(())
    }

    /// Refuses a node that prunes the first height that the import attests: the end of the
    /// first epoch above `verified`. `node` names the node in the message. zakura v1.5.1 gives
    /// no tree state below its prune height (zakura-core/zakura#1725), and the tail needs the
    /// blocks above the snapshot.
    fn check_prune_height(
        manifest: &Manifest,
        verified: Option<u32>,
        prune_height: Option<u32>,
        node: &str,
    ) -> Result<()> {
        let Some(prune_height) = prune_height else {
            return Ok(());
        };
        let first = manifest
            .epochs()
            .map(|epoch| epoch.end_height())
            .find(|&end| verified.is_none_or(|verified| end > verified))
            .unwrap_or(manifest.sealed_through());
        ensure!(
            first >= prune_height,
            "the import attests the tree states from height {first} on, but {node} keeps block \
             bodies only from height {prune_height}: use a node that does not prune, or an index \
             that is verified through height {} or above",
            prune_height - 1
        );
        Ok(())
    }
}

/// The proof of the epochs that `--serve-unverified` serves before their proof.
#[derive(Debug)]
pub(crate) struct Reattest {
    importer: Importer,
    index: Arc<Index>,
    status: ReplicaStatus,
}

impl Reattest {
    /// Runs the importer until `stop`, while `compact` serves. The importer proves the
    /// `ChainOk` epochs from the blobs in the index.
    ///
    /// At the end, `compact` takes the verified prefix, and then the replica reports
    /// [`ImportState::Following`]. Thus the two change together. Between two epochs, the
    /// head follower takes the new generation of the index at its next poll.
    pub(crate) async fn run(
        self,
        compact: CompactService,
        stop: watch::Receiver<bool>,
    ) -> Result<()> {
        let report = self
            .importer
            .run(stop)
            .await
            .wrap_err("prove the ranges that the replica serves unverified")?;
        info!(?report, "proved the ranges that the replica served unverified");
        if report.stopped {
            return Ok(());
        }
        let index = self.index;
        let state = tokio::task::spawn_blocking(move || index.state())
            .await
            .wrap_err("index state task panicked")??;
        compact.publish_verified(&state);
        self.status.update(|progress| progress.import_state = ImportState::Following);
        Ok(())
    }
}

/// The node that verifies an import: the `--remote-rpc` node or the embedded node.
///
/// # Implementation
///
/// [`ImportNode::prune_height`] refuses a node that cannot verify the import of the index of
/// `network`, and waits while the node cannot answer yet. [`ImportNode::trusted_node`] gives the
/// attestor and the sampler of the same node. [`Replica::import`] calls the second method only
/// when it imports.
pub(crate) trait ImportNode {
    /// The node in a refusal message.
    const NAME: &'static str;

    /// The prune height of the node, `None` for a node that does not prune. Breaks on shutdown.
    async fn prune_height(
        &self,
        network: &Network,
        shutdown: &CancellationToken,
    ) -> Result<ControlFlow<(), Option<u32>>>;

    /// The attestor, the sampler and the id of the node.
    async fn trusted_node(&self) -> Result<TrustedNode>;
}

impl ImportNode for ZakuraClient {
    const NAME: &'static str = "the remote zakura node";

    /// Reads `getblockchaininfo` again while the node is in warm-up. Refuses a node that gives
    /// another error, for example a node out of reach, and a node of another chain or genesis
    /// block than `network`.
    async fn prune_height(
        &self,
        network: &Network,
        shutdown: &CancellationToken,
    ) -> Result<ControlFlow<(), Option<u32>>> {
        let info = loop {
            let Some(result) = shutdown.run_until_cancelled(self.get_blockchain_info()).await
            else {
                return Ok(ControlFlow::Break(()));
            };
            match result {
                Err(error) if error.rpc_code() == Some(RpcCode::IN_WARMUP) => {
                    info!(%error, "waiting for remote zakura to end its warm-up");
                }
                Err(error) if error.is_unsent() => {
                    let url = self.rpc().url();
                    return Err(eyre!(error)).wrap_err(format!(
                        "the remote zakura node at {}:{} is unreachable",
                        url.host().unwrap_or_default(),
                        url.port_u16().unwrap_or_default()
                    ));
                }
                result => break result.wrap_err("read the chain of the remote zakura node")?,
            }
            if shutdown
                .run_until_cancelled(tokio::time::sleep(WARMUP_POLL_INTERVAL))
                .await
                .is_none()
            {
                return Ok(ControlFlow::Break(()));
            }
        };
        RemoteBackend::connect(self.clone(), network.clone())
            .await
            .wrap_err("check the chain of the remote zakura node")?;
        Ok(ControlFlow::Continue(info.prune_height().map(|height| height.0)))
    }

    async fn trusted_node(&self) -> Result<TrustedNode> {
        Ok(TrustedNode {
            attestor: Arc::new(JsonRpcAttestor::new(self.clone())),
            sampler: Arc::new(
                JsonRpcSampler::connect(self.clone())
                    .await
                    .wrap_err("read the prune height of the remote zakura node")?,
            ),
            id: NodeId { url: self.rpc().url().to_string(), version: String::new() },
        })
    }
}

impl ImportNode for ReadStateService {
    const NAME: &'static str = "the embedded zakura node";

    /// Reads the prune height of the state on a blocking thread. The embedded node runs the
    /// network of the index, thus there is no chain to check.
    async fn prune_height(
        &self,
        _network: &Network,
        _shutdown: &CancellationToken,
    ) -> Result<ControlFlow<(), Option<u32>>> {
        let state = self.clone();
        let prune_height = tokio::task::spawn_blocking(move || state.db().prune_height())
            .await
            .wrap_err("prune height task panicked")?;
        Ok(ControlFlow::Continue(prune_height.map(|height| height.0)))
    }

    async fn trusted_node(&self) -> Result<TrustedNode> {
        Ok(TrustedNode {
            attestor: Arc::new(ReadStateAttestor::new(self.clone())),
            sampler: Arc::new(
                ReadStateSampler::connect(self.clone())
                    .await
                    .wrap_err("read the prune height of the embedded zakura node")?,
            ),
            id: NodeId {
                url: "embedded".to_owned(),
                version: zakurad::application::build_version().to_string(),
            },
        })
    }
}

#[cfg(test)]
mod tests;
