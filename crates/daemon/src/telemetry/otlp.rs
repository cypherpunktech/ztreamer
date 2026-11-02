//! The trace export over OTLP/gRPC.

use eyre::{Result, WrapErr};
use opentelemetry::{KeyValue, trace::TracerProvider};
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::{Resource, trace::SdkTracerProvider};
use tokio::runtime::Runtime;
use tracing::{Subscriber, warn};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::registry::LookupSpan;

const SERVICE_NAME: &str = "ztreamerd";
const SERVICE_VERSION: &str = "service.version";

/// The tracer provider of the trace export, and the runtime of its gRPC client.
///
/// The tonic client needs a Tokio runtime. The runtime of the daemon starts after the telemetry
/// and stops before it, thus the export has its own runtime with one thread. On drop, the
/// provider sends the spans that wait, then the runtime stops.
pub(crate) struct OtlpExport {
    provider: SdkTracerProvider,
    _runtime: Runtime,
}

impl OtlpExport {
    /// Starts the export of spans to the OTLP/gRPC collector at `endpoint`.
    pub(crate) fn start(endpoint: &str, version: &'static str) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("otlp-export")
            .enable_all()
            .build()
            .wrap_err("start the runtime of the trace export")?;
        let exporter = {
            let _runtime = runtime.enter();
            SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .build()
                .wrap_err_with(|| format!("make the trace exporter for {endpoint}"))?
        };
        let resource = Resource::builder()
            .with_service_name(SERVICE_NAME)
            .with_attribute(KeyValue::new(SERVICE_VERSION, version))
            .build();
        let provider = SdkTracerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(exporter)
            .build();
        Ok(Self { provider, _runtime: runtime })
    }

    /// The layer that gives the spans of `tracing` to the provider.
    pub(crate) fn layer<S>(&self) -> OpenTelemetryLayer<S, opentelemetry_sdk::trace::Tracer>
    where
        S: Subscriber + for<'span> LookupSpan<'span>,
    {
        tracing_opentelemetry::layer().with_tracer(self.provider.tracer(SERVICE_NAME))
    }
}

impl std::fmt::Debug for OtlpExport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtlpExport").finish_non_exhaustive()
    }
}

impl Drop for OtlpExport {
    fn drop(&mut self) {
        if let Err(error) = self.provider.shutdown() {
            warn!(%error, "trace export did not send all spans");
        }
    }
}
