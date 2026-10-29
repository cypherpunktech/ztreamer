//! The command line of `ztreamerd`. [`Cli::run`] runs the subcommand, or makes a [`DaemonConfig`]
//! from the daemon flags and runs the daemon.

use std::{
    net::SocketAddr,
    num::{NonZeroU32, NonZeroUsize},
    path::PathBuf,
};

use clap::{Args, Parser, Subcommand};
use eyre::{Result, eyre};
use ztreamer_daemon::{
    DaemonConfig, IndexConfig, Mode, RemoteConfig, ReplicaSource, ServerConfig, SyncConfig,
    TlsFiles, run_daemon,
};
use ztreamer_remote::NodeUrl;
use ztreamer_sync::pipeline::PipelineConfig;

use crate::export::ExportArgs;

/// Must be larger than the full mainnet index. That index was 17.3 GiB at height 3,532,489
/// (2026-10-27).
const DEFAULT_MAP_SIZE: usize = 64 * 1024 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(
    version,
    about = "CompactTxStreamer daemon. It runs an embedded Zakura node, or imports sealed ranges \
             and then follows a remote Zakura node",
    args_conflicts_with_subcommands = true
)]
pub(crate) struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    #[command(flatten)]
    daemon: DaemonArgs,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Write the sealed ranges of an index as a snapshot that a replica can import.
    ExportSnapshot(ExportArgs),
}

impl Cli {
    /// Runs the subcommand, or the daemon when no subcommand is given.
    pub(crate) fn run(self) -> Result<()> {
        let Self { command, daemon } = self;
        match command {
            Some(Command::ExportSnapshot(args)) => args.run(&daemon.index().open_existing()?),
            None => run_daemon(DaemonConfig::try_from(daemon)?),
        }
    }
}

/// The daemon flags. They apply when no subcommand is given.
#[derive(Debug, Args)]
struct DaemonArgs {
    /// Zakura TOML configuration. Zakura defaults and ZAKURA_* variables apply when omitted.
    #[arg(long, value_name = "FILE")]
    zakura_config: Option<PathBuf>,

    /// Directory containing the LMDB compact index. A subcommand reads it too: give it after
    /// the subcommand name.
    #[arg(long, global = true, default_value = "ztreamer-index", value_name = "DIR")]
    index_dir: PathBuf,

    /// Maximum LMDB map size in bytes. A subcommand reads it too: give it after the subcommand
    /// name.
    #[arg(long, global = true, default_value_t = NonZeroUsize::new(DEFAULT_MAP_SIZE).unwrap())]
    index_map_size: NonZeroUsize,

    /// Historical fetch/parse workers. Defaults to the pipeline setting.
    #[arg(long = "fetch-workers")]
    workers: Option<NonZeroUsize>,

    /// Consecutive blocks scanned by each historical fetch worker.
    #[arg(long)]
    source_segment_blocks: Option<NonZeroU32>,

    /// Maximum bytes awaiting ordered historical ingestion.
    #[arg(long)]
    max_pending_bytes: Option<NonZeroUsize>,

    /// Maximum compact-index write batch size in bytes. It also limits how far the historical
    /// fetch workers read ahead: they can have at most `--max-pending-bytes` minus the larger of
    /// this value and 2 MB (the largest block record) in flight, plus the one block that the
    /// index needs next.
    #[arg(long)]
    max_batch_bytes: Option<NonZeroUsize>,

    /// CompactTxStreamer gRPC listener.
    #[arg(long, default_value = "127.0.0.1:9067")]
    grpc_listen: SocketAddr,

    /// Prometheus metrics listener.
    #[arg(long, default_value = "127.0.0.1:9999")]
    metrics_listen: SocketAddr,

    /// PEM-encoded TLS certificate chain for the gRPC server.
    #[arg(long, value_name = "FILE", requires = "tls_key")]
    tls_cert: Option<PathBuf>,

    /// PEM-encoded TLS private key for the gRPC server.
    #[arg(long, value_name = "FILE", requires = "tls_cert")]
    tls_key: Option<PathBuf>,

    /// Answer Ping. Testing only.
    #[arg(long)]
    ping_very_insecure: bool,

    /// Exit after historical indexing instead of starting servers and the head follower.
    #[arg(long)]
    index_only: bool,

    /// JSON-RPC URL of a remote zakura node. Needs --replicate-from. ztreamerd starts no
    /// embedded node. It imports the sealed ranges of --replicate-from, and this node attests
    /// them. Then ztreamerd reads the tail and the head from this node. The node gives the
    /// network (mainnet or testnet), and the index must be of that network. For HTTP basic auth,
    /// put the credentials in the URL: https://user:password@host:8232. Without credentials and
    /// without --remote-rpc-cookie, a loopback URL uses the default cookie file of zakura if it
    /// exists.
    #[arg(long, value_name = "URL", conflicts_with = "zakura_config", requires = "replicate_from")]
    remote_rpc: Option<NodeUrl>,

    /// Cookie file of the --remote-rpc node.
    #[arg(long, value_name = "FILE", requires = "remote_rpc")]
    remote_rpc_cookie: Option<PathBuf>,

    /// Import the sealed ranges from this source before the tail: an http:// or https:// base
    /// URL of a snapshot, or the host:port of a peer that serves the Replicator gRPC service
    /// without TLS. Needs --remote-rpc: the node that attests the import, and that ztreamerd
    /// follows after it.
    #[arg(long, value_name = "URL|HOST:PORT", requires = "remote_rpc")]
    replicate_from: Option<ReplicaSource>,
}

impl DaemonArgs {
    /// The index of `--index-dir` and `--index-map-size`.
    fn index(&self) -> IndexConfig {
        IndexConfig { dir: self.index_dir.clone(), map_size: self.index_map_size }
    }
}

impl TryFrom<DaemonArgs> for DaemonConfig {
    type Error = eyre::Report;

    /// The configuration of the flags. Clap makes `--tls-cert` and `--tls-key` need each other,
    /// and `--remote-rpc` and `--replicate-from` need each other. Refuses a `--remote-rpc-cookie`
    /// together with credentials in the `--remote-rpc` URL.
    fn try_from(args: DaemonArgs) -> Result<Self> {
        let index = args.index();
        let sync =
            SyncConfig { pipeline: PipelineConfig::from(&args), index_only: args.index_only };
        let tls = match (args.tls_cert, args.tls_key) {
            (Some(cert), Some(key)) => Some(TlsFiles { cert, key }),
            (None, None) => None,
            _ => return Err(eyre!("--tls-cert and --tls-key must be provided together")),
        };
        let servers = ServerConfig {
            grpc_listen: args.grpc_listen,
            tls,
            metrics_listen: args.metrics_listen,
            ping_very_insecure: args.ping_very_insecure,
        };
        let mode = match (args.remote_rpc, args.replicate_from) {
            (Some(url), Some(source)) => {
                Mode::Remote(Box::new(RemoteConfig::new(url, args.remote_rpc_cookie, source)?))
            }
            (None, None) => Mode::Embedded { zakura_config: args.zakura_config },
            _ => return Err(eyre!("--remote-rpc and --replicate-from must be provided together")),
        };
        Ok(Self { index, sync, servers, mode })
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
    use std::path::PathBuf;

    use clap::error::ErrorKind;

    use super::*;

    #[test]
    fn pipeline_config_keeps_defaults_for_unset_flags() {
        let cli =
            Cli::parse_from(["ztreamerd", "--fetch-workers", "3", "--max-batch-bytes", "4096"]);
        assert!(cli.command.is_none());
        let defaults = PipelineConfig::default();
        let pipeline = PipelineConfig::from(&cli.daemon);
        assert_eq!(pipeline.workers, 3);
        assert_eq!(pipeline.max_batch_bytes, 4096);
        assert_eq!(pipeline.source_segment_blocks, defaults.source_segment_blocks);
        assert_eq!(pipeline.max_pending_bytes, defaults.max_pending_bytes);
        assert_eq!(pipeline.max_source_bytes, defaults.max_source_bytes);
    }

    #[test]
    fn export_snapshot_is_a_subcommand_beside_the_daemon_flags() {
        let export = ["export-snapshot", "--out", "snapshot", "--signing-key", "key"];
        let cli = Cli::parse_from(["ztreamerd"].into_iter().chain(export));
        assert!(matches!(cli.command, Some(Command::ExportSnapshot(_))));
        assert_eq!(cli.daemon.index_dir, PathBuf::from("ztreamer-index"));
        assert!(
            Cli::try_parse_from(["ztreamerd", "--index-only"].into_iter().chain(export)).is_err()
        );
        assert!(Cli::try_parse_from(["ztreamerd", "export-snapshot", "--out", "x"]).is_err());
    }

    #[test]
    fn export_snapshot_reads_the_global_index_flags() {
        let export = ["export-snapshot", "--out", "snapshot", "--signing-key", "key"];
        let index = ["--index-dir", "index", "--index-map-size", "4096"];
        let cli = Cli::parse_from(["ztreamerd"].into_iter().chain(export).chain(index));
        assert!(matches!(cli.command, Some(Command::ExportSnapshot(_))));
        assert_eq!(cli.daemon.index_dir, PathBuf::from("index"));
        assert_eq!(cli.daemon.index_map_size.get(), 4096);
    }

    #[test]
    fn export_snapshot_refuses_a_directory_with_no_index() {
        let dir = tempfile::tempdir().unwrap();
        let cli =
            Cli::parse_from(["ztreamerd".as_ref(), "--index-dir".as_ref(), dir.path().as_os_str()]);
        let error = cli.daemon.index().open_existing().unwrap_err();
        assert!(format!("{error:?}").contains("holds no index"), "{error:?}");
    }

    #[test]
    fn the_flags_keep_the_two_modes_apart() {
        let parse = |flags: &[&str]| {
            Cli::try_parse_from(["ztreamerd"].iter().chain(flags)).map(|cli| cli.daemon)
        };
        let replica = ["--replicate-from", "peer:9068"];
        let remote = |url| ["--remote-rpc", url].into_iter().chain(replica).collect::<Vec<_>>();
        let url = "http://127.0.0.1:8232";
        let remote_and_embedded = [remote(url), vec!["--zakura-config", "zakura.toml"]].concat();
        assert!(parse(&remote_and_embedded).is_err());
        assert!(parse(&["--remote-rpc-cookie", ".cookie"]).is_err());
        assert!(parse(&remote("http://user@127.0.0.1:8232")).is_err());
        let embedded = DaemonConfig::try_from(parse(&[]).unwrap()).unwrap();
        assert!(matches!(embedded.mode, Mode::Embedded { zakura_config: None }));
        let remote = DaemonConfig::try_from(parse(&remote(url)).unwrap()).unwrap();
        assert!(matches!(remote.mode, Mode::Remote(_)));
    }

    #[test]
    fn the_remote_mode_needs_both_remote_rpc_and_replicate_from() {
        let missing = |flags: &[&str]| {
            let error = Cli::try_parse_from(["ztreamerd"].iter().chain(flags)).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument, "{error}");
            error.to_string()
        };
        let error = missing(&["--remote-rpc", "http://127.0.0.1:8232"]);
        assert!(error.contains("--replicate-from <URL|HOST:PORT>"), "{error}");
        let error = missing(&["--replicate-from", "https://snapshots.example/mainnet"]);
        assert!(error.contains("--remote-rpc <URL>"), "{error}");
    }

    #[test]
    fn a_cookie_and_credentials_in_the_url_are_refused() {
        let flags = ["--remote-rpc", "http://user:hunter2@localhost:8232", "--replicate-from"];
        let flags =
            flags.into_iter().chain(["peer:9068", "--remote-rpc-cookie", "/zakura/.cookie"]);
        let args = Cli::parse_from(["ztreamerd"].into_iter().chain(flags)).daemon;

        let error = DaemonConfig::try_from(args).unwrap_err();

        assert_eq!(
            error.to_string(),
            "use --remote-rpc-cookie or credentials in the --remote-rpc URL, not both"
        );
    }

    #[test]
    fn replicate_from_takes_one_snapshot_or_one_peer() {
        let parse = |flags: &[&str]| {
            let remote = ["ztreamerd", "--remote-rpc", "http://127.0.0.1:8232"];
            Cli::try_parse_from(remote.iter().chain(flags)).map(|cli| cli.daemon)
        };
        assert!(parse(&["--replicate-from", "peer"]).is_err());
        assert!(parse(&["--replicate-from", "peer:9068/path"]).is_err());
        let two = [
            "--replicate-from",
            "https://snapshots.example/mainnet",
            "--replicate-from",
            "peer:9068",
        ];
        assert!(parse(&two).is_err());
        let snapshot = parse(&two[..2]).unwrap().replicate_from.unwrap();
        assert_eq!(snapshot.to_string(), "https://snapshots.example/mainnet");
        let peer = parse(&two[2..]).unwrap().replicate_from.unwrap();
        assert_eq!(peer.to_string(), "peer:9068");
        assert_eq!(peer, ReplicaSource::Peer("http://peer:9068".parse().unwrap()));
    }
}
