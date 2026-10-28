//! Server streams whose items a spawned task produces.

use std::{future::Future, panic};

use futures_util::{StreamExt as _, future, stream};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use super::RpcStream;

/// The sender of the items of a [`ProducedStream`].
pub(crate) type StreamSender<T> = mpsc::Sender<Result<T, Status>>;

/// A server stream whose items a spawned task sends, one at a time. The producer stops when a
/// send fails: the client closed the stream.
///
/// After the last item, the stream joins the producer. A panic of the producer thus continues in
/// the stream, as a panic in a unary request does, and the client gets a reset of the stream.
/// Without the join, the sender drops with the task, and the client sees the normal end of the
/// stream after only a part of the items.
pub(crate) struct ProducedStream;

impl ProducedStream {
    /// Runs the future of `produce` in a task. The future sends the items with the sender.
    pub(crate) fn spawn<T: Send + 'static, F>(
        produce: impl FnOnce(StreamSender<T>) -> F,
    ) -> RpcStream<T>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let (sender, receiver) = mpsc::channel(1);
        Self::joined(receiver, tokio::spawn(produce(sender)))
    }

    /// Runs `produce` on the blocking thread pool. It sends the items with the sender, with
    /// `blocking_send`.
    pub(crate) fn spawn_blocking<T: Send + 'static>(
        produce: impl FnOnce(StreamSender<T>) + Send + 'static,
    ) -> RpcStream<T> {
        let (sender, receiver) = mpsc::channel(1);
        Self::joined(receiver, tokio::task::spawn_blocking(move || produce(sender)))
    }

    /// The items of `receiver`, then the join of `producer`. A producer that the runtime did not
    /// run ends the stream: the server stops.
    fn joined<T: Send + 'static>(
        receiver: mpsc::Receiver<Result<T, Status>>,
        producer: JoinHandle<()>,
    ) -> RpcStream<T> {
        let join = async move {
            let panic = producer.await.err()?.try_into_panic().ok()?;
            panic::resume_unwind(panic)
        };
        Box::pin(ReceiverStream::new(receiver).chain(stream::once(join).filter_map(future::ready)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_producer_that_returns_ends_the_stream_after_its_items() {
        let stream = ProducedStream::spawn(|sender| async move {
            for item in [1, 2] {
                sender.send(Ok(item)).await.unwrap();
            }
        });
        let items: Vec<_> = stream.map(Result::unwrap).collect().await;
        assert_eq!(items, [1, 2]);
    }

    /// The stream gives the items before the panic. Then the panic continues in the stream.
    async fn assert_the_panic_continues_after_one_item(mut stream: RpcStream<u32>) {
        assert_eq!(stream.next().await.unwrap().unwrap(), 1);
        let end = tokio::spawn(async move { stream.next().await.map(Result::unwrap) }).await;
        assert!(end.unwrap_err().is_panic());
    }

    #[tokio::test]
    async fn a_producer_panic_continues_in_the_stream() {
        let stream = ProducedStream::spawn(|sender| async move {
            sender.send(Ok(1)).await.unwrap();
            panic!("the producer panicked");
        });
        assert_the_panic_continues_after_one_item(stream).await;
    }

    #[tokio::test]
    async fn a_blocking_producer_panic_continues_in_the_stream() {
        let stream = ProducedStream::spawn_blocking(|sender| {
            sender.blocking_send(Ok(1)).unwrap();
            panic!("the producer panicked");
        });
        assert_the_panic_continues_after_one_item(stream).await;
    }
}
