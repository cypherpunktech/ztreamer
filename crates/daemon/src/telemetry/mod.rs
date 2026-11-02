//! The telemetry of `ztreamerd`: the log on standard output and the trace export over
//! OTLP/gRPC. The metrics are in [`MetricsEndpoint`](crate::metrics_endpoint::MetricsEndpoint).
//!
//! [`init_telemetry`] installs one global subscriber with a filter for each output. The export
//! filter does not change the log. Without an export filter, the export uses the directives of
//! the log filter.
//!
//! The trace export needs the `otlp` feature of this crate. Without the feature, a
//! [`TelemetryConfig`] with an export fails in [`init_telemetry`].

mod config;
mod grpc;
#[cfg(feature = "otlp")]
mod otlp;

use eyre::{Result, WrapErr};
use tracing_subscriber::{EnvFilter, Layer, fmt, layer::SubscriberExt, util::SubscriberInitExt};

pub use self::config::{LogFormat, OtlpConfig, TelemetryConfig};
pub(crate) use self::grpc::{GrpcRouter, GrpcServer, GrpcSpan};
#[cfg(feature = "otlp")]
use self::otlp::OtlpExport;

/// The trace export of a build without the `otlp` feature. No value of it exists: the guard
/// of such a build holds `None`.
#[cfg(not(feature = "otlp"))]
#[derive(Debug)]
enum OtlpExport {}

/// Holds the trace export. On drop, it sends the spans that wait. Keep it in a local of
/// `main`, not in a `static`: a `static` never drops.
#[derive(Debug)]
#[must_use = "the trace export stops when the guard drops"]
pub struct TelemetryGuard {
    _export: Option<OtlpExport>,
}

/// Installs the global `tracing` subscriber of `config`. Call it once, at the start of `main`,
/// outside a Tokio runtime.
///
/// It fails when a filter is not valid, when the trace export does not start, when `config`
/// has a trace export but the crate was built without the `otlp` feature, or when the process
/// has a global subscriber.
///
/// ```no_run
/// use ztreamer_daemon::{LogFormat, TelemetryConfig, init_telemetry};
///
/// fn main() -> eyre::Result<()> {
///     let _telemetry = init_telemetry(TelemetryConfig {
///         version: env!("CARGO_PKG_VERSION"),
///         log_filter: "info".to_owned(),
///         log_format: LogFormat::Text,
///         otlp: None,
///     })?;
///     Ok(())
/// }
/// ```
pub fn init_telemetry(config: TelemetryConfig) -> Result<TelemetryGuard> {
    let log = match config.log_format {
        LogFormat::Text => fmt::layer().boxed(),
        LogFormat::Json => {
            fmt::layer().json().with_span_list(true).with_current_span(false).boxed()
        }
    };
    let log = log.with_filter(parse_filter(&config.log_filter).wrap_err("parse the log filter")?);
    let registry = tracing_subscriber::registry().with(log);
    #[cfg(feature = "otlp")]
    let guard = {
        let (export, export_layer) = match config.otlp {
            None => (None, None),
            Some(OtlpConfig { endpoint, filter }) => {
                let filter = parse_filter(filter.as_deref().unwrap_or(&config.log_filter))
                    .wrap_err("parse the trace export filter")?;
                let export = OtlpExport::start(&endpoint, config.version)?;
                let layer = export.layer().with_filter(filter);
                (Some(export), Some(layer))
            }
        };
        registry.with(export_layer).try_init().wrap_err("install the tracing subscriber")?;
        TelemetryGuard { _export: export }
    };
    #[cfg(not(feature = "otlp"))]
    let guard = {
        if let Some(otlp) = config.otlp {
            eyre::bail!(
                "the trace export to {} needs the otlp feature, but this binary was built \
                 without it: install with `cargo install --locked --features otlp --git \
                 https://github.com/cypherpunktech/ztreamer ztreamerd`",
                otlp.endpoint
            );
        }
        registry.try_init().wrap_err("install the tracing subscriber")?;
        TelemetryGuard { _export: None }
    };
    Ok(guard)
}

/// Parses `directives` in `RUST_LOG` syntax. It fails on a directive that is not valid. This is
/// different from `EnvFilter::from_default_env`.
fn parse_filter(directives: &str) -> Result<EnvFilter> {
    EnvFilter::try_new(directives).wrap_err_with(|| format!("directives {directives:?}"))
}

#[cfg(all(test, not(feature = "otlp")))]
mod tests {
    use super::*;

    /// Without the feature, a trace export fails before the subscriber installs, thus this test
    /// can run next to the other tests of the crate.
    #[test]
    fn export_without_feature_fails() {
        let error = init_telemetry(TelemetryConfig {
            version: "test",
            log_filter: "info".to_owned(),
            log_format: LogFormat::Text,
            otlp: Some(OtlpConfig { endpoint: "http://localhost:4317".to_owned(), filter: None }),
        })
        .unwrap_err();
        let message = error.to_string();
        assert!(message.contains("http://localhost:4317"), "{message}");
        assert!(message.contains("--features otlp"), "{message}");
    }
}
