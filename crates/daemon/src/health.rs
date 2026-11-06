//! Standard gRPC health for the whole server, from the same readiness as wallet tip reads.

use eyre::{Result, WrapErr as _};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tonic_health::{ServingStatus, server::HealthReporter};
use ztreamer_sync::head::ServingSnapshot;

/// Publishes aggregate health (`service = ""`) whenever the serving snapshot changes.
pub(crate) struct HealthTask {
    reporter: HealthReporter,
    snapshots: watch::Receiver<ServingSnapshot>,
}

impl HealthTask {
    /// Sets the initial status before the gRPC listener opens.
    pub(crate) async fn new(
        reporter: HealthReporter,
        snapshots: watch::Receiver<ServingSnapshot>,
    ) -> Self {
        let mut task = Self { reporter, snapshots };
        task.update().await;
        task
    }

    /// Follows readiness until shutdown, or fails if the head follower disappears.
    pub(crate) async fn run(mut self, shutdown: CancellationToken) -> Result<()> {
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    self.stop().await;
                    return Ok(());
                }
                changed = self.snapshots.changed() => {
                    if let Err(error) = changed {
                        self.stop().await;
                        return Err(error).wrap_err("health snapshot source closed");
                    }
                    self.update().await;
                }
            }
        }
    }

    async fn update(&mut self) {
        let status = if self.snapshots.borrow_and_update().readiness().tip {
            ServingStatus::Serving
        } else {
            ServingStatus::NotServing
        };
        self.reporter.set_service_status("", status).await;
    }

    async fn stop(&mut self) {
        self.reporter.set_service_status("", ServingStatus::NotServing).await;
        // Drop the sender so Watch streams finish and cannot hold up graceful shutdown.
        self.reporter.clear_service_status("").await;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;
    use tokio_stream::StreamExt as _;
    use tonic::Request;
    use tonic_health::{
        pb::{
            HealthCheckRequest, health_check_response::ServingStatus as Status,
            health_server::Health as _,
        },
        server::HealthService,
    };
    use tracing::Instrument as _;
    use ztreamer_index::index::IndexState;

    use super::*;

    #[tokio::test]
    async fn check_and_watch_follow_readiness_and_shutdown() {
        let (sender, snapshots) = watch::channel(ServingSnapshot::from(IndexState::default()));
        let reporter = HealthReporter::new();
        let service = HealthService::from_health_reporter(reporter.clone());
        let task = HealthTask::new(reporter, snapshots).await;
        let request = || Request::new(HealthCheckRequest { service: String::new() });
        assert_eq!(
            service.check(request()).await.unwrap().into_inner().status(),
            Status::NotServing
        );
        let mut statuses = service.watch(request()).await.unwrap().into_inner();
        assert_eq!(statuses.next().await.unwrap().unwrap().status(), Status::NotServing);

        let shutdown = CancellationToken::new();
        let running = tokio::spawn(task.run(shutdown.clone()).in_current_span());
        for (ready, tip_fresh, expected) in [
            (true, true, Status::Serving),
            (false, true, Status::NotServing),
            (true, true, Status::Serving),
            (true, false, Status::NotServing),
            (true, true, Status::Serving),
        ] {
            sender.send_modify(|snapshot| {
                snapshot.ready = ready;
                snapshot.tip_fresh = tip_fresh;
            });
            let status =
                timeout(Duration::from_secs(5), statuses.next()).await.unwrap().unwrap().unwrap();
            assert_eq!(status.status(), expected);
            assert_eq!(service.check(request()).await.unwrap().into_inner().status(), expected);
        }
        shutdown.cancel();
        timeout(Duration::from_secs(5), running).await.unwrap().unwrap().unwrap();
        assert_eq!(statuses.next().await.unwrap().unwrap().status(), Status::NotServing);
        assert!(timeout(Duration::from_secs(5), statuses.next()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_closed_snapshot_source_fails_closed() {
        let mut snapshot = ServingSnapshot::from(IndexState::default());
        snapshot.tip_fresh = true;
        let (sender, snapshots) = watch::channel(snapshot);
        let reporter = HealthReporter::new();
        let service = HealthService::from_health_reporter(reporter.clone());
        let task = HealthTask::new(reporter, snapshots).await;
        let request = Request::new(HealthCheckRequest { service: String::new() });
        let mut statuses = service.watch(request).await.unwrap().into_inner();
        assert_eq!(statuses.next().await.unwrap().unwrap().status(), Status::Serving);
        drop(sender);
        assert!(task.run(CancellationToken::new()).await.is_err());
        assert_eq!(statuses.next().await.unwrap().unwrap().status(), Status::NotServing);
        assert!(statuses.next().await.is_none());
    }
}
