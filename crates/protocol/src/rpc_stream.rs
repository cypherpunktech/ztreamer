//! [`RpcStream`]: the server stream type of the gRPC services.

/// A stream of gRPC answers: the type of each server stream of the services in this crate.
pub type RpcStream<T> = std::pin::Pin<
    Box<dyn tonic::codegen::tokio_stream::Stream<Item = Result<T, tonic::Status>> + Send>,
>;
