//! The telemetry of `ztreamerd`: the log on standard output and the trace export over
//! OTLP/gRPC. The metrics are in [`MetricsEndpoint`](crate::metrics_endpoint::MetricsEndpoint).
//!
//! [`init_telemetry`] installs one global subscriber with a filter for each output. The export
//! filter does not change the log. Without an export filter, the export uses the directives of
//! the log filter.

mod config;
mod otlp;

use eyre::{Result, WrapErr};
use tracing_subscriber::{EnvFilter, Layer, fmt, layer::SubscriberExt, util::SubscriberInitExt};

pub use self::config::{LogFormat, OtlpConfig, TelemetryConfig};
use self::otlp::OtlpExport;

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
/// It fails when a filter is not valid, when the trace export does not start, or when the
/// process has a global subscriber.
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
    let TelemetryConfig { version, log_filter, log_format, otlp } = config;
    let log = match log_format {
        LogFormat::Text => fmt::layer().boxed(),
        LogFormat::Json => {
            fmt::layer().json().with_span_list(true).with_current_span(false).boxed()
        }
    };
    let log = log.with_filter(parse_filter(&log_filter).wrap_err("parse the log filter")?);
    let (export, export_layer) = match otlp {
        None => (None, None),
        Some(OtlpConfig { endpoint, filter }) => {
            let filter = parse_filter(filter.as_deref().unwrap_or(&log_filter))
                .wrap_err("parse the trace export filter")?;
            let export = OtlpExport::start(&endpoint, version)?;
            let layer = export.layer().with_filter(filter);
            (Some(export), Some(layer))
        }
    };
    tracing_subscriber::registry()
        .with(log)
        .with(export_layer)
        .try_init()
        .wrap_err("install the tracing subscriber")?;
    Ok(TelemetryGuard { _export: export })
}

/// Parses `directives` in `RUST_LOG` syntax. It fails on a directive that is not valid. This is
/// different from `EnvFilter::from_default_env`.
fn parse_filter(directives: &str) -> Result<EnvFilter> {
    EnvFilter::try_new(directives).wrap_err_with(|| format!("directives {directives:?}"))
}
