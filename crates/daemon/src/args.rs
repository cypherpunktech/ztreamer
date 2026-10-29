//! The flags of the daemon, and the pipeline configuration of them.

use std::{
    net::SocketAddr,
    num::{NonZeroU32, NonZeroUsize},
    path::PathBuf,
    sync::Arc,
};

use clap::Args;
use eyre::{Result, WrapErr};
use zakura_chain::parameters::Network;
use ztreamer_index::index::Index;
use ztreamer_sync::pipeline::PipelineConfig;

use crate::{remote, replicate};

/// Must be larger than the full mainnet index. That index was 17.3 GiB at height 3,532,489
/// (2026-10-27).
const DEFAULT_MAP_SIZE: usize = 64 * 1024 * 1024 * 1024;

/// The daemon flags. They apply when no subcommand is given.
#[derive(Debug, Args)]
pub struct DaemonArgs {
    /// Zakura TOML configuration. Zakura defaults and ZAKURA_* variables apply when omitted.
    #[arg(long, value_name = "FILE")]
    pub(crate) zakura_config: Option<PathBuf>,

    /// Directory containing the LMDB compact index. A subcommand reads it too: give it after
    /// the subcommand name.
    #[arg(long, global = true, default_value = "ztreamer-index", value_name = "DIR")]
    pub index_dir: PathBuf,

    /// Maximum LMDB map size in bytes. A subcommand reads it too: give it after the subcommand
    /// name.
    #[arg(long, global = true, default_value_t = NonZeroUsize::new(DEFAULT_MAP_SIZE).unwrap())]
    pub index_map_size: NonZeroUsize,

    /// Historical fetch/parse workers. Defaults to the pipeline setting.
    #[arg(long = "fetch-workers")]
    pub(crate) workers: Option<NonZeroUsize>,

    /// Consecutive blocks scanned by each historical fetch worker.
    #[arg(long)]
    pub(crate) source_segment_blocks: Option<NonZeroU32>,

    /// Maximum bytes awaiting ordered historical ingestion.
    #[arg(long)]
    pub(crate) max_pending_bytes: Option<NonZeroUsize>,

    /// Maximum compact-index write batch size in bytes. It also limits how far the historical
    /// fetch workers read ahead: they can have at most `--max-pending-bytes` minus the larger of
    /// this value and 2 MB (the largest block record) in flight, plus the one block that the
    /// index needs next.
    #[arg(long)]
    pub(crate) max_batch_bytes: Option<NonZeroUsize>,

    /// CompactTxStreamer gRPC listener.
    #[arg(long, default_value = "127.0.0.1:9067")]
    pub(crate) grpc_listen: SocketAddr,

    /// Prometheus metrics listener.
    #[arg(long, default_value = "127.0.0.1:9999")]
    pub(crate) metrics_listen: SocketAddr,

    /// PEM-encoded TLS certificate chain for the gRPC server.
    #[arg(long, value_name = "FILE", requires = "tls_key")]
    pub(crate) tls_cert: Option<PathBuf>,

    /// PEM-encoded TLS private key for the gRPC server.
    #[arg(long, value_name = "FILE", requires = "tls_cert")]
    pub(crate) tls_key: Option<PathBuf>,

    /// Answer Ping. Testing only.
    #[arg(long)]
    pub(crate) ping_very_insecure: bool,

    /// Exit after historical indexing instead of starting servers and the head follower.
    #[arg(long)]
    pub(crate) index_only: bool,

    #[command(flatten)]
    pub(crate) remote: remote::RemoteArgs,

    #[command(flatten)]
    pub(crate) replicate: replicate::ReplicateArgs,
}

impl DaemonArgs {
    /// Opens the index of `--index-dir` on a blocking thread and refuses an index of another
    /// network.
    pub(crate) async fn open_index(&self, network: &Network) -> Result<Arc<Index>> {
        let (path, map_size) = (self.index_dir.clone(), self.index_map_size.get());
        let (name, genesis_hash) = (network.to_string(), network.genesis_hash());
        let index =
            tokio::task::spawn_blocking(move || Index::open(path, map_size, &name, genesis_hash))
                .await
                .wrap_err("index open task panicked")??;
        Ok(Arc::new(index))
    }

    /// Opens the index of `--index-dir` for the chain in its identity row. Refuses a directory
    /// with no index.
    pub fn open_existing_index(&self) -> Result<Index> {
        Ok(Index::open_existing(&self.index_dir, self.index_map_size.get())?)
    }
}

impl From<&DaemonArgs> for PipelineConfig {
    /// Starts from the pipeline defaults and applies the limits given on the command line.
    fn from(cli: &DaemonArgs) -> Self {
        let defaults = Self::default();
        Self {
            workers: cli.workers.map_or(defaults.workers, NonZeroUsize::get),
            source_segment_blocks: cli
                .source_segment_blocks
                .map_or(defaults.source_segment_blocks, NonZeroU32::get),
            max_pending_bytes: cli
                .max_pending_bytes
                .map_or(defaults.max_pending_bytes, NonZeroUsize::get),
            max_batch_bytes: cli
                .max_batch_bytes
                .map_or(defaults.max_batch_bytes, NonZeroUsize::get),
            ..defaults
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;
    use crate::test_utils::TestCli;

    #[test]
    fn pipeline_config_keeps_defaults_for_unset_flags() {
        let cli =
            TestCli::parse_from(["ztreamerd", "--fetch-workers", "3", "--max-batch-bytes", "4096"]);
        let defaults = PipelineConfig::default();
        let pipeline = PipelineConfig::from(&cli.daemon);
        assert_eq!(pipeline.workers, 3);
        assert_eq!(pipeline.max_batch_bytes, 4096);
        assert_eq!(pipeline.source_segment_blocks, defaults.source_segment_blocks);
        assert_eq!(pipeline.max_pending_bytes, defaults.max_pending_bytes);
        assert_eq!(pipeline.max_source_bytes, defaults.max_source_bytes);
    }
}
