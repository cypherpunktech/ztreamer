//! The ztreamer daemon: an embedded Zakura node, or a replica import and a remote Zakura node;
//! then historical indexing, head following, and the servers.
//!
//! `ztreamerd` makes a [`DaemonConfig`] from the command line and calls [`run_daemon`].

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod application;
mod config;
mod daemon;
mod embedded;
mod lifecycle;
mod metrics_endpoint;
mod remote;
mod replicate;
#[cfg(test)]
mod test_utils;

pub use config::{DaemonConfig, IndexConfig, Mode, ServerConfig, SyncConfig, TlsFiles};
pub use daemon::run_daemon;
pub use remote::RemoteConfig;
pub use replicate::ReplicaSource;
