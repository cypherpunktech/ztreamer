//! The telemetry flags of `ztreamerd`. They apply to the daemon and to each subcommand.

use clap::{Args, ValueEnum};
use ztreamer_daemon::{LogFormat, OtlpConfig, TelemetryConfig};

/// The telemetry flags. With a subcommand, give them after the subcommand name.
#[derive(Debug, Args)]
pub(crate) struct TelemetryArgs {
    /// Log filter, in RUST_LOG syntax, for example `info,ztreamer_sync=debug`. A directive that
    /// is not valid stops ztreamerd.
    #[arg(
        long,
        global = true,
        env = "RUST_LOG",
        default_value = "info",
        value_name = "DIRECTIVES"
    )]
    log_filter: String,

    /// Log format on standard output.
    #[arg(long, global = true, value_enum, default_value_t = LogFormatArg::Text)]
    log_format: LogFormatArg,

    /// OTLP/gRPC endpoint of a trace collector, for example http://localhost:4317. ztreamerd
    /// sends its spans there. Without this flag, ztreamerd sends no trace. Needs a build with
    /// the `otlp` feature.
    #[arg(long, global = true, env = "OTEL_EXPORTER_OTLP_ENDPOINT", value_name = "URL")]
    otlp_endpoint: Option<String>,

    /// Filter of the spans that ztreamerd sends to --otlp-endpoint, in RUST_LOG syntax. The
    /// default is the log filter.
    #[arg(long, global = true, requires = "otlp_endpoint", value_name = "DIRECTIVES")]
    otlp_filter: Option<String>,
}

impl TelemetryArgs {
    /// The telemetry configuration of the flags, with the version of `ztreamerd`.
    pub(crate) fn config(&self) -> TelemetryConfig {
        TelemetryConfig {
            version: env!("CARGO_PKG_VERSION"),
            log_filter: self.log_filter.clone(),
            log_format: self.log_format.into(),
            otlp: self
                .otlp_endpoint
                .clone()
                .map(|endpoint| OtlpConfig { endpoint, filter: self.otlp_filter.clone() }),
        }
    }
}

/// The values of `--log-format`.
#[derive(Clone, Copy, Debug, ValueEnum)]
enum LogFormatArg {
    /// One line of text for each event.
    Text,
    /// One JSON object for each event, with the list of its spans.
    Json,
}

impl From<LogFormatArg> for LogFormat {
    fn from(format: LogFormatArg) -> Self {
        match format {
            LogFormatArg::Text => Self::Text,
            LogFormatArg::Json => Self::Json,
        }
    }
}
