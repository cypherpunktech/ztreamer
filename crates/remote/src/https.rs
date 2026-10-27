//! The HTTP/1 client over rustls of [`RpcClient`](crate::RpcClient) and of the snapshot source
//! of `ztreamer-verify`.

use std::{error::Error, sync::Arc, time::Duration};

use http::Request;
use hyper::body::Body;
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{
    client::legacy::{Client, ResponseFuture, connect::HttpConnector},
    rt::TokioExecutor,
};
use rustls::RootCertStore;

/// An HTTP/1 client for `http` and `https` URLs, with request bodies of type `B`. It uses
/// rustls with the `ring` provider and the safe default protocol versions. Clones share one
/// connection pool.
#[derive(Clone, Debug)]
pub struct HttpsClient<B> {
    client: Client<HttpsConnector<HttpConnector>, B>,
}

impl<B> HttpsClient<B>
where
    B: Body + Send + Unpin + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
{
    /// A client that trusts the web PKI roots. A TCP connect that takes longer than
    /// `connect_timeout` fails.
    pub fn new(connect_timeout: Option<Duration>) -> Result<Self, rustls::Error> {
        let roots = RootCertStore { roots: webpki_roots::TLS_SERVER_ROOTS.to_vec() };
        let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
        let mut tcp = HttpConnector::new();
        tcp.enforce_http(false);
        tcp.set_connect_timeout(connect_timeout);
        tcp.set_nodelay(true);
        let https = HttpsConnectorBuilder::new()
            .with_tls_config(tls)
            .https_or_http()
            .enable_http1()
            .wrap_connector(tcp);
        Ok(Self { client: Client::builder(TokioExecutor::new()).build(https) })
    }

    /// Sends `request`.
    pub fn request(&self, request: Request<B>) -> ResponseFuture {
        self.client.request(request)
    }
}
