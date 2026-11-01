//! A response stream that ends after its first error.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use tokio_stream::Stream;
use tonic::Status;
use ztreamer_protocol::RpcStream;

/// Sends the items of a range stream, and ends after the first error. The transparent projection
/// can continue after an error, but a range response must stop at its first error.
pub(crate) struct EndAtError<T>(Option<RpcStream<T>>);

impl<T> EndAtError<T> {
    pub(crate) const fn new(inner: RpcStream<T>) -> Self {
        Self(Some(inner))
    }
}

impl<T> Stream for EndAtError<T> {
    type Item = Result<T, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(inner) = this.0.as_mut() else {
            return Poll::Ready(None);
        };
        let result = inner.as_mut().poll_next(cx);
        if matches!(result, Poll::Ready(None | Some(Err(_)))) {
            this.0 = None;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use tokio_stream::StreamExt;

    use super::*;

    #[tokio::test]
    async fn ends_after_the_first_error() {
        let inner = tokio_stream::iter([Ok(1), Err(Status::internal("injected")), Ok(2)]);
        let mut stream = EndAtError::new(Box::pin(inner));

        assert_eq!(stream.next().await.unwrap().unwrap(), 1);
        assert!(stream.next().await.unwrap().is_err());
        assert!(stream.next().await.is_none());
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn sends_each_item_of_a_stream_with_no_error() {
        let mut stream = EndAtError::new(Box::pin(tokio_stream::iter([Ok(1), Ok(2)])));

        assert_eq!(stream.next().await.unwrap().unwrap(), 1);
        assert_eq!(stream.next().await.unwrap().unwrap(), 2);
        assert!(stream.next().await.is_none());
    }
}
