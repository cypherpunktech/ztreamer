//! `ztreamerd`: the command line of the daemon. [`ztreamer_daemon`] runs the daemon. The
//! command line is in [`cli`], and the `export-snapshot` subcommand is in [`export`].

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use clap::Parser;
use eyre::Result;
use tracing_subscriber::EnvFilter;

use cli::Cli;

mod cli;
mod export;

fn main() -> Result<()> {
    let cli = Cli::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    cli.run()
}
