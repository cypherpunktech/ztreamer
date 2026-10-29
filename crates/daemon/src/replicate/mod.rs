//! The `--replicate-from` startup path of the remote mode: a replica imports the sealed ranges of
//! snapshots or of peers, and the `--remote-rpc` node verifies them. Then the daemon indexes the
//! tail and follows the head from that node.
//!
//! Before it fetches a range, the import refuses:
//!
//! - a node that runs another genesis block than the network of its chain;
//! - a node that prunes the next block that the import needs;
//! - in [`Importer::run`], a manifest whose signature does not verify with [`SNAPSHOT_PUBKEY`],
//!   and a manifest for another network or genesis block than the index.

use std::{fmt, ops::ControlFlow, str::FromStr, sync::Arc};

use ed25519_dalek::VerifyingKey;
use eyre::{Result, WrapErr, ensure, eyre};
use http::Uri;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::info;
use zakura_chain::{block::Height, parameters::Network};
use zakura_rpc::client::GetBlockchainInfoResponse;
use ztreamer_import::{
    attest::JsonRpcAttestor,
    fetch::{HttpSource, PeerSource, RangeSource},
    importer::{ImportConfig, ImportError, Importer, TrustedNode},
    sample::{JsonRpcSampler, SamplePlan},
};
use ztreamer_index::index::{Index, range_meta::NodeId};
use ztreamer_remote::RemoteBackend;
use ztreamer_remote::ZakuraClient;
use ztreamer_snapshot::layout::SNAPSHOT_PUBKEY;

/// The source of an import: a `--replicate-from` value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaSource {
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
    /// The source of ranges at this address. `key` signs a snapshot. A peer gets a connection.
    async fn connect(&self, key: VerifyingKey) -> Result<Box<dyn RangeSource>> {
        match self {
            Self::Snapshot(base) => Ok(Box::new(HttpSource::new(base.clone(), key)?)),
            Self::Peer(endpoint) => {
                let peer = PeerSource::connect(endpoint.clone())
                    .await
                    .wrap_err_with(|| format!("connect to the peer {self}"))?;
                Ok(Box::new(peer))
            }
        }
    }
}

/// The import that `--replicate-from` asks for.
#[derive(Clone, Debug)]
pub(crate) struct Replica {
    source: ReplicaSource,
    /// The key that signs the snapshots: [`SNAPSHOT_PUBKEY`].
    key: VerifyingKey,
}

impl Replica {
    /// The import of `source`. The built-in key [`SNAPSHOT_PUBKEY`] checks its snapshots.
    pub(crate) fn new(source: ReplicaSource) -> Self {
        let key = VerifyingKey::from_bytes(&SNAPSHOT_PUBKEY).expect("the built-in key is valid");
        Self { source, key }
    }

    /// Imports the epochs of the manifest above the sealed tip of `index`, checked against the
    /// node of `client`. Each installed row names the node as `id`. `info` is the
    /// `getblockchaininfo` answer of the node, and `network` the network of its chain. Breaks on
    /// shutdown.
    ///
    /// The refusals of the module docs come first. An index that is sealed at or above the
    /// manifest needs no import. The import starts at the sealed tip, also inside an epoch of
    /// the manifest. Each other import error stops the daemon. On shutdown, the import completes
    /// the epoch that it installs, and stops.
    pub(crate) async fn import(
        &self,
        client: &ZakuraClient,
        id: NodeId,
        info: &GetBlockchainInfoResponse,
        network: &Network,
        index: &Arc<Index>,
        shutdown: &CancellationToken,
    ) -> Result<ControlFlow<()>> {
        // `RemoteNode::sources` checks the chain and the genesis block again, after the import. The
        // import can take hours, and in that time the node can restart on another chain, or
        // another node can answer at the URL. That second check refuses such a node before the
        // daemon serves from it.
        RemoteBackend::connect(client.clone(), network.clone())
            .await
            .wrap_err("check the chain of the remote zakura node")?;
        let sealed = {
            let index = Arc::clone(index);
            tokio::task::spawn_blocking(move || index.state())
                .await
                .wrap_err("index state task panicked")??
                .sealed_through()
        };
        // The samples and the tail read the block bodies from the next block on.
        let next = Height(sealed.map_or(0, |sealed| sealed + 1));
        let first = info.prune_height().unwrap_or(Height::MIN);
        ensure!(
            next >= first,
            "the import needs block {}, but the remote zakura node keeps block bodies only from \
             height {}: use a node that does not prune, or an index that is sealed through \
             height {} or above",
            next.0,
            first.0,
            first.0 - 1
        );

        let source = self.source.connect(self.key).await?;
        let node = TrustedNode {
            attestor: Arc::new(JsonRpcAttestor::new(client.clone())),
            sampler: Arc::new(JsonRpcSampler::new(client.clone())),
            id,
        };
        let seed = getrandom::u64().map_err(|error| eyre!("random sample seed: {error}"))?;
        let config = ImportConfig {
            network: network.clone(),
            verify_sample: SamplePlan { per_range: SamplePlan::PER_RANGE, seed },
            import_workers: ImportConfig::default_import_workers(),
        };
        info!(source = %self.source, from = ?sealed, import_state = "fetching");

        let (stop, stopped) = watch::channel(false);
        let mut import =
            tokio::spawn(Importer::new(source, node, Arc::clone(index), config).run(stopped));
        let result = tokio::select! {
            result = &mut import => result,
            _ = shutdown.cancelled() => {
                info!("stopping the import before shutdown");
                let _ = stop.send(true);
                import.await
            }
        }
        .wrap_err("import task panicked")?;
        let report = match result {
            Err(ImportError::Ahead { index, manifest }) => {
                info!(index, manifest, "the index needs no import");
                return Ok(ControlFlow::Continue(()));
            }
            // The blocking pool stopped: the runtime stops, as after `report.stopped`.
            Err(error @ ImportError::Cancelled(_)) => {
                info!(%error, "the import stops");
                return Ok(ControlFlow::Break(()));
            }
            result => result.wrap_err("import the sealed ranges")?,
        };
        info!(?report, "import complete");
        Ok(if report.stopped { ControlFlow::Break(()) } else { ControlFlow::Continue(()) })
    }
}

#[cfg(test)]
mod tests;
