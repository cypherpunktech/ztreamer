//! The trace export sends the spans of `tracing` to an OTLP/gRPC collector. This test installs
//! the global subscriber, thus it is the only test in this binary.

use std::sync::{Arc, Mutex};

use opentelemetry_proto::tonic::{
    collector::trace::v1::{
        ExportTraceServiceRequest, ExportTraceServiceResponse,
        trace_service_server::{TraceService, TraceServiceServer},
    },
    common::v1::{KeyValue, any_value::Value},
};
use tonic::{
    Request, Response, Status,
    transport::{Server, server::TcpIncoming},
};
use ztreamer_daemon::{LogFormat, OtlpConfig, TelemetryConfig, init_telemetry};

/// A collector that keeps each request that it receives.
#[derive(Clone, Default)]
struct Collector(Arc<Mutex<Vec<ExportTraceServiceRequest>>>);

#[tonic::async_trait]
impl TraceService for Collector {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        self.0.lock().unwrap().push(request.into_inner());
        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

fn string_value(attributes: &[KeyValue], key: &str) -> Option<String> {
    attributes.iter().find(|attribute| attribute.key == key).and_then(|attribute| {
        match attribute.value.as_ref()?.value.as_ref()? {
            Value::StringValue(value) => Some(value.clone()),
            _ => None,
        }
    })
}

#[test]
fn guard_drop_sends_the_spans_to_the_collector() {
    let collector = Collector::default();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let incoming =
        runtime.block_on(async { TcpIncoming::bind(([127, 0, 0, 1], 0).into()) }).unwrap();
    let address = incoming.local_addr().unwrap();
    runtime.spawn(
        Server::builder()
            .add_service(TraceServiceServer::new(collector.clone()))
            .serve_with_incoming(incoming),
    );

    let guard = init_telemetry(TelemetryConfig {
        version: "0.0.0-test",
        log_filter: "off".to_owned(),
        log_format: LogFormat::Text,
        otlp: Some(OtlpConfig {
            endpoint: format!("http://{address}"),
            filter: Some("info".to_owned()),
        }),
    })
    .unwrap();
    // The log filter `off` does not change the export. The export filter `info` stops the debug
    // span.
    tracing::info_span!("test.work", item = 7).in_scope(|| tracing::info!("work done"));
    tracing::debug_span!("test.not_sent").in_scope(|| {});
    drop(guard);

    let requests = collector.0.lock().unwrap();
    let resource_spans: Vec<_> =
        requests.iter().flat_map(|request| &request.resource_spans).collect();
    let resource = &resource_spans[0].resource.as_ref().unwrap().attributes;
    assert_eq!(string_value(resource, "service.name").as_deref(), Some("ztreamerd"));
    assert_eq!(string_value(resource, "service.version").as_deref(), Some("0.0.0-test"));
    let names: Vec<_> = resource_spans
        .iter()
        .flat_map(|spans| &spans.scope_spans)
        .flat_map(|scope| &scope.spans)
        .map(|span| span.name.as_str())
        .collect();
    assert_eq!(names, ["test.work"]);
}
