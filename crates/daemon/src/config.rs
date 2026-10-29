//! The configuration of the daemon. `ztreamerd` makes it from the command line.
//!
//! [`DaemonConfig`] has the configuration of the index, of the sync, of the servers, and the
//! [`Mode`] of the node. The remote mode has its own type, [`RemoteConfig`].

use std::{net::SocketAddr, num::NonZeroUsize, path::PathBuf, sync::Arc};

use eyre::{Result, WrapErr};
use tonic::transport::{Identity, Server, ServerTlsConfig};
use zakura_chain::parameters::Network;
use ztreamer_index::index::Index;
use ztreamer_sync::pipeline::PipelineConfig;

use crate::remote::RemoteConfig;

/// The configuration of the daemon.
#[derive(Debug)]
pub struct DaemonConfig {
    /// The LMDB compact index.
    pub index: IndexConfig,
    /// The block pipeline passes and the head follower.
    pub sync: SyncConfig,
    /// The gRPC server and the metrics endpoint.
    pub servers: ServerConfig,
    /// The node that gives the chain.
    pub mode: Mode,
}

/// The node that gives the chain to the daemon. There are only two modes.
#[derive(Debug)]
pub enum Mode {
    /// A Zakura node in this process.
    Embedded {
        /// Zakura TOML configuration. Zakura defaults and `ZAKURA_*` variables apply when it is
        /// `None`.
        zakura_config: Option<PathBuf>,
    },
    /// An import of sealed ranges, then a remote Zakura node over JSON-RPC. The box keeps the
    /// two variants near the same size.
    Remote(Box<RemoteConfig>),
}

/// The LMDB compact index.
#[derive(Clone, Debug)]
pub struct IndexConfig {
    /// The directory of the index.
    pub dir: PathBuf,
    /// The maximum LMDB map size in bytes.
    pub map_size: NonZeroUsize,
}

impl IndexConfig {
    /// Opens the index on a blocking thread and refuses an index of another network.
    pub(crate) async fn open(&self, network: &Network) -> Result<Arc<Index>> {
        let (path, map_size) = (self.dir.clone(), self.map_size.get());
        let (name, genesis_hash) = (network.to_string(), network.genesis_hash());
        let index =
            tokio::task::spawn_blocking(move || Index::open(path, map_size, &name, genesis_hash))
                .await
                .wrap_err("index open task panicked")??;
        Ok(Arc::new(index))
    }

    /// Opens the index for the chain in its identity row. Refuses a directory with no index.
    pub fn open_existing(&self) -> Result<Index> {
        Ok(Index::open_existing(&self.dir, self.map_size.get())?)
    }
}

/// The block pipeline passes and the head follower.
#[derive(Clone, Copy, Debug)]
pub struct SyncConfig {
    /// The limits of the historical pipeline.
    pub pipeline: PipelineConfig,
    /// Stop after the pass of the catch-up stage, with no head reconcile, no servers and no head
    /// follower.
    pub index_only: bool,
}

/// The gRPC server and the metrics endpoint.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    /// The listener of the CompactTxStreamer and Replicator gRPC server.
    pub grpc_listen: SocketAddr,
    /// The TLS files of the gRPC server. `None` serves without TLS.
    pub tls: Option<TlsFiles>,
    /// The listener of the Prometheus metrics endpoint.
    pub metrics_listen: SocketAddr,
    /// Answer Ping. Testing only.
    pub ping_very_insecure: bool,
}

impl ServerConfig {
    /// The gRPC server, with the TLS identity of [`Self::tls`] if it is set. Reads the TLS files.
    pub(crate) fn grpc_server(&self) -> Result<Server> {
        let server = Server::builder();
        let Some(TlsFiles { cert: cert_path, key: key_path }) = &self.tls else {
            return Ok(server);
        };

        let cert = std::fs::read(cert_path)
            .wrap_err_with(|| format!("read TLS certificate {}", cert_path.display()))?;
        let key = std::fs::read(key_path)
            .wrap_err_with(|| format!("read TLS private key {}", key_path.display()))?;
        server
            .tls_config(ServerTlsConfig::new().identity(Identity::from_pem(cert, key)))
            .wrap_err("configure gRPC TLS")
    }
}

/// The PEM files of the TLS identity of the gRPC server.
#[derive(Clone, Debug)]
pub struct TlsFiles {
    /// The certificate chain.
    pub cert: PathBuf,
    /// The private key.
    pub key: PathBuf,
}
