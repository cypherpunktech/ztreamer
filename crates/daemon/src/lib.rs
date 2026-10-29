//! The ztreamer daemon: an embedded Zakura node, or a replica import and a remote Zakura node;
//! then historical indexing, head following, and the servers.
//!
//! `ztreamerd` parses the command line into [`DaemonArgs`] and calls [`run_daemon`].

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod application;
mod args;
mod daemon;
mod embedded;
mod lifecycle;
mod metrics_endpoint;
mod remote;
mod replicate;
#[cfg(test)]
mod test_utils;

pub use args::DaemonArgs;
pub use daemon::run_daemon;
