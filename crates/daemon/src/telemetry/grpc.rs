//! The span of each gRPC request: `grpc.request`.

use std::time::Duration;

use http::{HeaderMap, Request, Response};
use percent_encoding::percent_decode_str;
use tonic::transport::{
    Server,
    server::{Router, TcpConnectInfo, TlsConnectInfo},
};
use tower_http::{
    classify::{GrpcErrorsAsFailures, GrpcFailureClass, SharedClassifier},
    trace::{MakeSpan, OnEos, OnFailure, OnResponse, TraceLayer},
};
use tower_layer::{Identity, Stack};
use tracing::{Span, field::Empty};

/// The layer that makes a `grpc.request` span for each request of the gRPC server. The span
/// stays open until the response body ends, thus it covers a server stream. A stream that the
/// client cancels has no `rpc.grpc.status_code`.
pub(crate) type GrpcTraceLayer = TraceLayer<
    SharedClassifier<GrpcErrorsAsFailures>,
    GrpcSpan,
    (),
    GrpcSpan,
    (),
    GrpcSpan,
    GrpcSpan,
>;

/// The gRPC server with [`GrpcTraceLayer`].
pub(crate) type GrpcServer = Server<Stack<GrpcTraceLayer, Identity>>;

/// The services of a [`GrpcServer`].
pub(crate) type GrpcRouter = Router<Stack<GrpcTraceLayer, Identity>>;

/// The callbacks of [`GrpcTraceLayer`]: they make the span and record the outcome.
#[derive(Clone, Copy, Debug)]
pub(crate) struct GrpcSpan;

impl GrpcSpan {
    /// The layer with these callbacks. The layer writes no event of its own.
    pub(crate) fn layer() -> GrpcTraceLayer {
        TraceLayer::new_for_grpc()
            .make_span_with(Self)
            .on_request(())
            .on_response(Self)
            .on_body_chunk(())
            .on_eos(Self)
            .on_failure(Self)
    }
}

impl<B> MakeSpan<B> for GrpcSpan {
    fn make_span(&mut self, request: &Request<B>) -> Span {
        let path = request.uri().path().trim_start_matches('/');
        let (service, method) = path.split_once('/').unwrap_or((path, ""));
        let extensions = request.extensions();
        let client = extensions
            .get::<TcpConnectInfo>()
            .or_else(|| {
                extensions.get::<TlsConnectInfo<TcpConnectInfo>>().map(TlsConnectInfo::get_ref)
            })
            .and_then(TcpConnectInfo::remote_addr);
        tracing::info_span!(
            parent: None,
            "grpc.request",
            otel.name = path,
            otel.kind = "server",
            rpc.system = "grpc",
            rpc.service = service,
            rpc.method = method,
            client.address = client.map(|address| tracing::field::display(address.ip())),
            rpc.grpc.status_code = Empty,
            otel.status_code = Empty,
            otel.status_description = Empty,
        )
    }
}

impl<B> OnResponse<B> for GrpcSpan {
    /// A response with only headers has its status in the headers.
    fn on_response(self, response: &Response<B>, _: Duration, span: &Span) {
        record_status(response.headers(), span);
    }
}

impl OnEos for GrpcSpan {
    /// Any other response has its status in the trailers.
    fn on_eos(self, trailers: Option<&HeaderMap>, _: Duration, span: &Span) {
        if let Some(trailers) = trailers {
            record_status(trailers, span);
        }
    }
}

impl OnFailure<GrpcFailureClass> for GrpcSpan {
    /// [`record_status`] already recorded a failed status. An error of the service or of the
    /// body has no status: record it here.
    fn on_failure(&mut self, failure: GrpcFailureClass, _: Duration, span: &Span) {
        if let GrpcFailureClass::Error(error) = failure {
            span.record("otel.status_code", "ERROR");
            span.record("otel.status_description", error);
        }
    }
}

/// Records the `grpc-status` of `headers`, and `grpc-message` when the status is not OK. gRPC
/// sends the message percent-encoded.
fn record_status(headers: &HeaderMap, span: &Span) {
    let header = |name| headers.get(name).and_then(|value| value.to_str().ok());
    let Some(code) = header("grpc-status").and_then(|code| code.parse::<i32>().ok()) else {
        return;
    };
    span.record("rpc.grpc.status_code", code);
    if code != 0 {
        span.record("otel.status_code", "ERROR");
        let message = header("grpc-message").unwrap_or_default();
        span.record("otel.status_description", &*percent_decode_str(message).decode_utf8_lossy());
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use serde_json::Value;
    use tokio::sync::watch;
    use tokio_stream::StreamExt as _;
    use tonic::transport::server::TcpIncoming;
    use zakura_chain::parameters::Network;
    use ztreamer_chain::test_utils::{capture_spans, closed_spans};
    use ztreamer_index::test_utils::open_index;
    use ztreamer_protocol::replicate::{
        Empty as EmptyRequest, GetRangeRequest, ListRangesRequest,
        replicator_client::ReplicatorClient, replicator_server::ReplicatorServer,
    };
    use ztreamer_replicator::ReplicatorService;
    use ztreamer_sync::head::ServingSnapshot;

    use super::*;

    /// The fields of the `grpc.request` span of `method`, when the span closed.
    fn request_span<'a>(lines: &'a [Value], method: &str) -> &'a Value {
        closed_spans(lines, "grpc.request")
            .find(|span| span["rpc.method"] == method)
            .unwrap_or_else(|| panic!("no closed grpc.request span for {method}"))
    }

    #[test]
    fn each_request_has_a_span_with_its_method_and_status() {
        let dir = tempfile::tempdir().unwrap();
        let index = Arc::new(open_index(dir.path()));
        let (_snapshots, receiver) = watch::channel(ServingSnapshot::from(index.state().unwrap()));
        let service = ReplicatorService::new(index, receiver, &Network::Mainnet);
        let runtime = tokio::runtime::Runtime::new().unwrap();

        let ((), lines) = capture_spans(|| {
            runtime.block_on(async {
                let incoming = TcpIncoming::bind(([127, 0, 0, 1], 0).into()).unwrap();
                let address = incoming.local_addr().unwrap();
                tokio::spawn(
                    Server::builder()
                        .layer(GrpcSpan::layer())
                        .add_service(ReplicatorServer::new(service))
                        .serve_with_incoming(incoming),
                );
                let mut client =
                    ReplicatorClient::connect(format!("http://{address}")).await.unwrap();
                client.get_export_info(EmptyRequest {}).await.unwrap();
                let request = ListRangesRequest { from_start_height: 0 };
                let mut ranges = client.list_ranges(request).await.unwrap().into_inner();
                while ranges.next().await.is_some() {}
                let missing = GetRangeRequest { start_height: 5, offset: 0 };
                let error = client.get_range(missing).await.unwrap_err();
                assert_eq!(error.code(), tonic::Code::NotFound);
                // The server closes a span after it sends the last frame.
                tokio::time::sleep(Duration::from_millis(100)).await;
            });
        });

        let unary = request_span(&lines, "GetExportInfo");
        assert_eq!(unary["otel.name"], "ztreamer.replicate.Replicator/GetExportInfo");
        assert_eq!(unary["rpc.service"], "ztreamer.replicate.Replicator");
        assert_eq!(unary["client.address"], "127.0.0.1");
        assert_eq!(unary["rpc.grpc.status_code"], 0);
        assert!(unary.get("otel.status_code").is_none());

        // A stream records its status from the trailers at its end.
        assert_eq!(request_span(&lines, "ListRanges")["rpc.grpc.status_code"], 0);

        // An error has its status in the headers.
        let failed = request_span(&lines, "GetRange");
        assert_eq!(failed["rpc.grpc.status_code"], 5);
        assert_eq!(failed["otel.status_code"], "ERROR");
        assert_eq!(failed["otel.status_description"], "no listed sealed range starts at 5");
    }
}
