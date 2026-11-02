//! `ztreamerd`: the command line of the daemon. [`ztreamer_daemon`] runs the daemon. The
//! command line is in [`cli`], the `export-snapshot` subcommand is in [`export`], and the
//! telemetry flags are in [`telemetry`].

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

use clap::Parser;
use eyre::Result;
use ztreamer_daemon::init_telemetry;

use cli::Cli;

mod cli;
mod export;
mod telemetry;

fn main() -> Result<()> {
    let cli = Cli::parse();
    // The guard drops after the daemon stops, and sends the spans that wait.
    let _telemetry = init_telemetry(cli.telemetry())?;
    cli.run()
}
