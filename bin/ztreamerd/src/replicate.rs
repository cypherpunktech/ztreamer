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

use clap::Args;
use ed25519_dalek::VerifyingKey;
use eyre::{Result, WrapErr, ensure, eyre};
use http::Uri;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::info;
use zakura_chain::{block::Height, parameters::Network};
use zakura_rpc::client::GetBlockchainInfoResponse;
use ztreamer_indexer::index::{Index, range_meta::NodeId};
use ztreamer_remote::{FirstBlockWithBody as _, ZakuraClient};
use ztreamer_service::RemoteBackend;
use ztreamer_verify::{
    attest::JsonRpcAttestor,
    fetch::{HttpSource, PeerSource, RangeSource},
    importer::{ImportConfig, ImportError, Importer, TrustedNode},
    sample::{JsonRpcSampler, SamplePlan},
    snapshot::SNAPSHOT_PUBKEY,
};

/// The command-line flag of the import. It is off by default.
#[derive(Debug, Args)]
pub(crate) struct ReplicateArgs {
    /// Import the sealed ranges from this source before the tail: an http:// or https:// base
    /// URL of a snapshot, or the host:port of a peer that serves the Replicator gRPC service
    /// without TLS. Needs --remote-rpc: the node that attests the import, and that ztreamerd
    /// follows after it.
    #[arg(long, value_name = "URL|HOST:PORT", requires = "remote_rpc")]
    replicate_from: Option<ReplicaSource>,
}

impl ReplicateArgs {
    /// The import of `--replicate-from`. Call it only in the remote mode: there, clap makes the
    /// flag required.
    pub(crate) fn replica(&self) -> Replica {
        let key = VerifyingKey::from_bytes(&SNAPSHOT_PUBKEY).expect("the built-in key is valid");
        let source = self.replicate_from.clone().expect("--remote-rpc requires --replicate-from");
        Replica { source, key }
    }

    /// Logs the new import state of a replica at info level: `tail` or `following`. Does
    /// nothing in the embedded mode.
    pub(crate) fn log_import_state(&self, state: &'static str) {
        if self.replicate_from.is_some() {
            info!(import_state = state);
        }
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
    /// Imports the epochs of the manifest above the sealed tip of `index`, checked
    /// against the node of `client`. `info` is the `getblockchaininfo` answer of the node, and
    /// `network` the network of its chain. Breaks on shutdown.
    ///
    /// The refusals of the module docs come first. An index that is sealed at or above the
    /// manifest needs no import. Neither does an index whose sealed tip is not the end
    /// of an epoch of the manifest, or whose last sealed range has no `range_meta` row: the tail
    /// continues from the tip. On shutdown, the import completes the epoch that it installs, and
    /// stops.
    pub(crate) async fn import(
        &self,
        client: &ZakuraClient,
        info: &GetBlockchainInfoResponse,
        network: &Network,
        index: &Arc<Index>,
        shutdown: &CancellationToken,
    ) -> Result<ControlFlow<()>> {
        // `RemoteNode::sources` checks the genesis block again, after the import.
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
        // The tail, the samples and the tree states of zakura v1.5.1 (zakura-core/zakura#1725)
        // need the blocks from the next block on.
        let next = Height(sealed.map_or(0, |sealed| sealed + 1));
        let first = info.first_block_with_body();
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
            sampler: Arc::new(
                JsonRpcSampler::connect(client.clone())
                    .await
                    .wrap_err("read the prune height of the remote zakura node")?,
            ),
            id: NodeId { url: client.rpc().url().to_string(), version: String::new() },
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
            // The tail of an earlier start sealed ranges after the end of an older manifest. Thus
            // the tip is inside an epoch, or its range has no row and no frontiers.
            Err(error @ (ImportError::EpochBoundary { .. } | ImportError::MissingRow { .. }))
                if sealed.is_some() =>
            {
                info!(%error, "the import cannot start at the index tip; the tail continues");
                return Ok(ControlFlow::Continue(()));
            }
            result => result.wrap_err("import the sealed ranges")?,
        };
        info!(?report, "import complete");
        Ok(if report.stopped { ControlFlow::Break(()) } else { ControlFlow::Continue(()) })
    }
}

#[cfg(test)]
mod tests;
