//! The Prometheus endpoint of `ztreamerd`. It serves the metrics of ztreamer in both modes, and
//! the metrics of the Zakura node in the embedded mode: both record to the one global recorder.

use std::net::SocketAddr;

use eyre::{Result, WrapErr};
use metrics::{counter, describe_counter};
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing::info;

const BUILD_INFO: &str = "ztreamerd.build.info";
const BUILD_INFO_HELP: &str = "1, with the ztreamerd version as the label `version`.";

/// The Prometheus endpoint at one address.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MetricsEndpoint(SocketAddr);

impl MetricsEndpoint {
    pub(crate) const fn new(address: SocketAddr) -> Self {
        Self(address)
    }

    /// Installs the global recorder and starts the HTTP listener on the Tokio runtime of the
    /// caller. Then it sets `ztreamerd.build.info`. It fails when the address is in use or a
    /// recorder is already installed.
    pub(crate) fn start(self) -> Result<()> {
        let Self(address) = self;
        PrometheusBuilder::new()
            .with_http_listener(address)
            .install()
            .wrap_err_with(|| format!("open the metrics endpoint at {address}"))?;
        describe_counter!(BUILD_INFO, BUILD_INFO_HELP);
        counter!(BUILD_INFO, "version" => env!("CARGO_PKG_VERSION")).increment(1);
        info!(%address, "opened the metrics endpoint");
        Ok(())
    }
}
