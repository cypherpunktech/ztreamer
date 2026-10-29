//! The time of the last blob byte that a source sent.

use std::{
    sync::{Mutex, PoisonError},
    time::Duration,
};

use tokio::time::Instant;

/// The time of the last blob byte that a source sent, for all the requests of one
/// [`Fetcher`](super::Fetcher).
///
/// A request stalls when the source sends no byte for a limit, counted from the later of the
/// request start and the last byte. The clock is shared: a request that waits for a place in the
/// request limit of the source, or that a peer refuses, does not stall while the other requests
/// of the source receive bytes.
#[derive(Debug, Default)]
pub(crate) struct SourceProgress {
    last_byte: Mutex<Option<Instant>>,
}

impl SourceProgress {
    /// Records that the source sent a blob byte now.
    pub(crate) fn record_byte(&self) {
        *self.last_byte.lock().unwrap_or_else(PoisonError::into_inner) = Some(Instant::now());
    }

    /// The later of `request_start` and the last byte.
    fn last_progress_after(&self, request_start: Instant) -> Instant {
        let last_byte = *self.last_byte.lock().unwrap_or_else(PoisonError::into_inner);
        last_byte.map_or(request_start, |last_byte| last_byte.max(request_start))
    }

    /// Waits until the source sends no byte for `limit`, counted from the later of
    /// `request_start` and the last byte. Returns the time with no byte: `limit` or more.
    pub(crate) async fn wait_for_stall(&self, request_start: Instant, limit: Duration) -> Duration {
        loop {
            let last_progress = self.last_progress_after(request_start);
            let now = Instant::now();
            if now >= last_progress + limit {
                return now - last_progress;
            }
            tokio::time::sleep_until(last_progress + limit).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use futures_util::poll;

    use super::*;

    const LIMIT: Duration = Duration::from_secs(300);

    #[tokio::test(start_paused = true)]
    async fn a_source_with_no_byte_stalls_at_the_limit_after_the_request_start() {
        let progress = SourceProgress::default();
        let request_start = Instant::now();

        let waited = progress.wait_for_stall(request_start, LIMIT).await;

        assert_eq!((waited, request_start.elapsed()), (LIMIT, LIMIT));
    }

    #[tokio::test(start_paused = true)]
    async fn a_byte_during_the_wait_moves_the_stall() {
        let progress = SourceProgress::default();
        let request_start = Instant::now();
        let mut stall = pin!(progress.wait_for_stall(request_start, LIMIT));
        assert!(poll!(stall.as_mut()).is_pending());

        tokio::time::advance(LIMIT - Duration::from_secs(1)).await;
        progress.record_byte();
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(poll!(stall.as_mut()).is_pending());
        let waited = stall.await;

        assert_eq!(waited, LIMIT);
        assert_eq!(request_start.elapsed(), 2 * LIMIT - Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn a_byte_before_the_request_start_does_not_shorten_the_wait() {
        let progress = SourceProgress::default();
        progress.record_byte();
        tokio::time::advance(LIMIT).await;
        let request_start = Instant::now();

        let waited = progress.wait_for_stall(request_start, LIMIT).await;

        assert_eq!((waited, request_start.elapsed()), (LIMIT, LIMIT));
    }
}
