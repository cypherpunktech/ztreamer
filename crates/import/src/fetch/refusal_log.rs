//! The log of the `GetRange` refusals of a peer.

use std::{
    sync::{Mutex, PoisonError},
    time::Duration,
};

use http::Uri;
use tokio::time::Instant;
use tonic::Status;
use tracing::{debug, warn};

/// The `RESOURCE_EXHAUSTED` refusals of the `GetRange` streams of one peer, for the log.
///
/// A refusal is back-pressure, thus each refusal is a `debug` line. A `warn` line shows the
/// refusals at most once in [`Self::WARN_INTERVAL`], with the count of refusals since the last
/// `warn`. Thus an operator sees a peer that refuses for a long time, and the log does not fill.
#[derive(Debug, Default)]
pub(crate) struct RefusalLog {
    count: Mutex<RefusalCount>,
}

/// The refusals since the last `warn` line, and the time of that line.
#[derive(Debug, Default)]
struct RefusalCount {
    refusals: u64,
    last_warn: Option<Instant>,
}

impl RefusalLog {
    /// The shortest time between two `warn` lines.
    const WARN_INTERVAL: Duration = Duration::from_secs(30);

    /// Logs the refusal of the `GetRange` request for the range at `start_height` from byte
    /// `offset`, and the `wait` before the next request.
    pub(crate) fn log_refusal(
        &self,
        endpoint: &Uri,
        start_height: u32,
        offset: u64,
        status: &Status,
        wait: Duration,
    ) {
        match self.count_refusal() {
            Some(refusals) => warn!(
                %endpoint, refusals, start_height, offset, %status, ?wait,
                "peer refuses range streams; asking again"
            ),
            None => {
                debug!(%endpoint, start_height, offset, %status, ?wait, "peer is busy; asking again")
            }
        }
    }

    /// Counts one refusal. Returns the refusals since the last `warn` line when a `warn` line is
    /// necessary: at the first refusal, and when [`Self::WARN_INTERVAL`] passed since the last
    /// `warn` line.
    fn count_refusal(&self) -> Option<u64> {
        let mut count = self.count.lock().unwrap_or_else(PoisonError::into_inner);
        count.refusals += 1;
        let now = Instant::now();
        if count.last_warn.is_some_and(|last_warn| now - last_warn < Self::WARN_INTERVAL) {
            return None;
        }
        count.last_warn = Some(now);
        Some(std::mem::take(&mut count.refusals))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn a_refusal_warns_at_most_once_in_the_interval_with_the_count() {
        let log = RefusalLog::default();
        let second = Duration::from_secs(1);

        assert_eq!(log.count_refusal(), Some(1));
        let in_interval: Vec<_> = (0..3).map(|_| log.count_refusal()).collect();
        tokio::time::advance(RefusalLog::WARN_INTERVAL - second).await;
        let before_the_end = log.count_refusal();
        tokio::time::advance(second).await;
        let at_the_end = log.count_refusal();
        let after_the_warn = log.count_refusal();

        assert_eq!(in_interval, [None, None, None]);
        assert_eq!((before_the_end, at_the_end, after_the_warn), (None, Some(5), None));
    }
}
