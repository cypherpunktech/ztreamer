//! The configuration of the telemetry. `ztreamerd` makes it from the command line.

/// The output format of the log on standard output.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LogFormat {
    /// One line of text for each event.
    #[default]
    Text,
    /// One JSON object for each event, with the list of its spans.
    Json,
}

/// The trace export over OTLP/gRPC. It needs the `otlp` feature of this crate.
#[derive(Clone, Debug)]
pub struct OtlpConfig {
    /// The OTLP/gRPC endpoint of the collector, for example `http://localhost:4317`.
    pub endpoint: String,
    /// The filter of the exported spans and events, in `RUST_LOG` syntax. `None` uses the log
    /// filter.
    pub filter: Option<String>,
}

/// The telemetry of `ztreamerd`: the log on standard output, and the trace export.
///
/// ```
/// use ztreamer_daemon::{LogFormat, OtlpConfig, TelemetryConfig};
///
/// let config = TelemetryConfig {
///     version: env!("CARGO_PKG_VERSION"),
///     log_filter: "info,ztreamer_sync=debug".to_owned(),
///     log_format: LogFormat::Json,
///     otlp: Some(OtlpConfig { endpoint: "http://localhost:4317".to_owned(), filter: None }),
/// };
/// ```
#[derive(Clone, Debug)]
pub struct TelemetryConfig {
    /// The version of the binary. The trace export sends it as `service.version`.
    pub version: &'static str,
    /// The filter of the log, in `RUST_LOG` syntax.
    pub log_filter: String,
    /// The output format of the log.
    pub log_format: LogFormat,
    /// The trace export. `None` exports no trace. `Some` needs the `otlp` feature:
    /// without it, [`init_telemetry`](crate::init_telemetry) fails.
    pub otlp: Option<OtlpConfig>,
}
