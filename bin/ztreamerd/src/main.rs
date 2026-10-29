//! `ztreamerd`: the command line of the daemon. [`ztreamer_daemon`] runs the daemon. The
//! `export-snapshot` subcommand is in [`export`].

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use clap::{Parser, Subcommand};
use eyre::Result;
use tracing_subscriber::EnvFilter;
use ztreamer_daemon::{DaemonArgs, run_daemon};

mod export;

#[derive(Debug, Parser)]
#[command(
    version,
    about = "CompactTxStreamer daemon. It runs an embedded Zakura node, or imports sealed ranges \
             and then follows a remote Zakura node",
    args_conflicts_with_subcommands = true
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    #[command(flatten)]
    daemon: DaemonArgs,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Write the sealed ranges of an index as a snapshot that a replica can import.
    ExportSnapshot(export::ExportArgs),
}

fn main() -> Result<()> {
    let Cli { command, daemon } = Cli::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    match command {
        Some(Command::ExportSnapshot(args)) => args.run(&daemon.open_existing_index()?),
        None => run_daemon(daemon),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

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
        let error = cli.daemon.open_existing_index().unwrap_err();
        assert!(format!("{error:?}").contains("holds no index"), "{error:?}");
    }
}
