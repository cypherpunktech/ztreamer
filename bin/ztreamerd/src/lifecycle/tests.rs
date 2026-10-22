use std::{cell::Cell, future, rc::Rc, time::Duration};

use eyre::eyre;
use tokio::{sync::oneshot, time::timeout};

use super::*;

#[tokio::test]
async fn application_completion_drains_node_without_send() {
    // Keeping an Rc across await proves the supervisor needs no Send bound.
    let cleaned = Rc::new(Cell::new(false));
    let shutdown = CancellationToken::new();
    let node = async {
        shutdown.cancelled().await;
        tokio::task::yield_now().await;
        cleaned.set(true);
        Ok(())
    };
    timeout(
        Duration::from_secs(5),
        supervise(node, async { Ok(()) }, shutdown.clone(), future::pending()),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(cleaned.get());
}

#[tokio::test]
async fn application_failure_still_drains_node() {
    let cleaned = Cell::new(false);
    let shutdown = CancellationToken::new();
    let node = async {
        shutdown.cancelled().await;
        cleaned.set(true);
        Ok(())
    };
    let error = timeout(
        Duration::from_secs(5),
        supervise(
            node,
            async { Err(eyre!("application failed")) },
            shutdown.clone(),
            future::pending(),
        ),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.to_string(), "application failed");
    assert!(cleaned.get());
}

#[tokio::test]
async fn node_failure_cancels_application_waiting_for_readiness() {
    let shutdown = CancellationToken::new();
    let cleaned = Cell::new(false);
    let application = async {
        shutdown.cancelled().await;
        tokio::task::yield_now().await;
        cleaned.set(true);
        Ok(())
    };
    let error = timeout(
        Duration::from_secs(5),
        supervise(
            async { Err(eyre!("node failed")) },
            application,
            shutdown.clone(),
            future::pending(),
        ),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.to_string(), "node failed");
    assert!(cleaned.get());
}

#[tokio::test]
async fn unexpected_successful_node_exit_is_an_error() {
    let shutdown = CancellationToken::new();
    let error = timeout(
        Duration::from_secs(5),
        supervise(
            async { Ok(()) },
            async {
                shutdown.cancelled().await;
                Ok(())
            },
            shutdown.clone(),
            future::pending(),
        ),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.to_string(), "Zakura stopped unexpectedly");
}

#[tokio::test]
async fn signal_drains_both_futures_concurrently_even_on_error() {
    let shutdown = CancellationToken::new();
    let (node_started_tx, node_started_rx) = oneshot::channel();
    let (app_started_tx, app_started_rx) = oneshot::channel();
    let (node_cleanup_tx, node_cleanup_rx) = oneshot::channel();
    let (app_cleanup_tx, app_cleanup_rx) = oneshot::channel();
    let node = async {
        node_started_tx.send(()).unwrap();
        shutdown.cancelled().await;
        node_cleanup_tx.send(()).unwrap();
        app_cleanup_rx.await.unwrap();
        Err(eyre!("node cleanup failed"))
    };
    let application = async {
        app_started_tx.send(()).unwrap();
        shutdown.cancelled().await;
        app_cleanup_tx.send(()).unwrap();
        node_cleanup_rx.await.unwrap();
        Ok(())
    };
    let signal = async {
        node_started_rx.await.unwrap();
        app_started_rx.await.unwrap();
        Ok(())
    };
    let error =
        timeout(Duration::from_secs(5), supervise(node, application, shutdown.clone(), signal))
            .await
            .unwrap()
            .unwrap_err();
    assert_eq!(error.to_string(), "node cleanup failed");
}

#[tokio::test]
async fn signal_failure_still_drains_both_futures() {
    let shutdown = CancellationToken::new();
    let node_cleaned = Cell::new(false);
    let app_cleaned = Cell::new(false);
    let error = timeout(
        Duration::from_secs(5),
        supervise(
            async {
                shutdown.cancelled().await;
                node_cleaned.set(true);
                Ok(())
            },
            async {
                shutdown.cancelled().await;
                app_cleaned.set(true);
                Ok(())
            },
            shutdown.clone(),
            async { Err(eyre!("signal failed")) },
        ),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.to_string(), "signal failed");
    assert!(node_cleaned.get() && app_cleaned.get());
}

#[tokio::test]
async fn drain_both_reuses_taken_results_and_awaits_the_rest() {
    let polled = Cell::new(false);
    let (a, b) = timeout(
        Duration::from_secs(5),
        drain_both(
            (
                async {
                    polled.set(true);
                    0
                },
                Some(1),
            ),
            (async { 2 }, None),
        ),
    )
    .await
    .unwrap();
    assert_eq!((a, b), (1, 2));
    assert!(!polled.get());
}

#[tokio::test]
async fn startup_failure_returns_original_error() {
    let mut config = zakurad::config::ZakuradConfig::default();
    config.zcashd_compat.enabled = false;
    config.zcashd_compat.block_gossip_peer_ips = vec!["127.0.0.1".parse().unwrap()];
    let shutdown = CancellationToken::new();
    let (ready_tx, ready_rx) = oneshot::channel();
    let node = async {
        Box::pin(zakurad::node::run_with_services_ready(config, vec![], shutdown.clone(), ready_tx))
            .await
            .map_err(|error| eyre!("{error}"))
    };
    let application = async {
        tokio::select! {
            _ = shutdown.cancelled() => Ok(()),
            result = ready_rx => {
                result.map_err(|_| eyre!("readiness closed"))?;
                Err(eyre!("invalid node unexpectedly started"))
            }
        }
    };
    let error = timeout(
        Duration::from_secs(5),
        supervise(node, application, shutdown.clone(), future::pending()),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(error.to_string().contains("block_gossip_peer_ips requires"), "{error}");
}

#[tokio::test]
async fn cancellation_before_startup_closes_readiness() {
    let shutdown = CancellationToken::new();
    let (ready_tx, ready_rx) = oneshot::channel();
    let node = async {
        Box::pin(zakurad::node::run_with_services_ready(
            zakurad::config::ZakuradConfig::default(),
            vec![],
            shutdown.clone(),
            ready_tx,
        ))
        .await
        .map_err(|error| eyre!("{error}"))
    };
    let application = async {
        shutdown.cancelled().await;
        assert!(ready_rx.await.is_err());
        Ok(())
    };
    timeout(
        Duration::from_secs(5),
        supervise(node, application, shutdown.clone(), async { Ok(()) }),
    )
    .await
    .unwrap()
    .unwrap();
}

use std::{
    net::UdpSocket,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use zakura_chain::block::Height as BlockHeight;
use zakura_network::zakura::{Peer, Service, Stream, ZakuraConnId, ZakuraPeerId};
use zakurad::{config::ZakuradConfig, node::CustomService};
use ztreamer_node::NodeClient;
#[derive(Debug)]
struct RegistrationProbe(Arc<AtomicBool>);

impl Service for RegistrationProbe {
    fn name(&self) -> &'static str {
        "registration-probe"
    }

    fn streams(&self) -> &[Stream] {
        self.0.store(true, Ordering::Relaxed);
        &[]
    }

    fn add_peer(&self, _peer: Peer) {}

    fn remove_peer(&self, _peer: &ZakuraPeerId, _conn_id: ZakuraConnId) {}
}

// A node sets a process-global shutdown flag, so each lifecycle needs its
// own process even when the suite is run with the standard Cargo harness.
fn run_in_subprocess(test: &str) -> bool {
    const CHILD: &str = "ZTREAMER_NODE_TEST_CHILD";
    if std::env::var(CHILD).as_deref() == Ok(test) {
        return false;
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--nocapture"])
        .env(CHILD, test)
        .status()
        .expect("lifecycle test subprocess starts");
    assert!(status.success(), "lifecycle test subprocess failed: {status}");
    true
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedded_node_stops_after_application_completion() {
    if run_in_subprocess("lifecycle::tests::embedded_node_stops_after_application_completion") {
        return;
    }
    embedded_node_lifecycle(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedded_node_stops_after_signal() {
    if run_in_subprocess("lifecycle::tests::embedded_node_stops_after_signal") {
        return;
    }
    embedded_node_lifecycle(true).await;
}

async fn embedded_node_lifecycle(stop_by_signal: bool) {
    let _guard = zakura_test::init();
    let native_socket = UdpSocket::bind("127.0.0.1:0").expect("test UDP port is available");
    let native_addr = native_socket.local_addr().expect("test UDP socket has an address");
    drop(native_socket);
    let identity_dir = tempfile::tempdir().expect("temporary identity directory is created");

    let mut config = ZakuradConfig::default();
    config.network.network = zakura_chain::parameters::Network::new_regtest(Default::default());
    config.network.listen_addr = "127.0.0.1:0".parse().expect("valid test address");
    config.network.p2p_stack = zakura_network::P2pStack::Dual;
    config.network.initial_mainnet_peers.clear();
    config.network.cache_dir = zakura_network::CacheDir::disabled();
    config.network.identity_dir = identity_dir.path().to_owned();
    config.network.zakura.listen_addr = Some(native_addr);
    config.network.zakura.bootstrap_peers.clear();
    config.state = zakura_state::Config::ephemeral();
    let service_registered = Arc::new(AtomicBool::new(false));

    let shutdown = CancellationToken::new();
    let signal_requested = CancellationToken::new();
    let (ready_tx, ready_rx) = oneshot::channel();
    let node = async {
        Box::pin(zakurad::node::run_with_services_ready(
            config,
            vec![CustomService {
                service: Arc::new(RegistrationProbe(service_registered.clone())),
                provides: vec![],
                seeks: vec![],
            }],
            shutdown.clone(),
            ready_tx,
        ))
        .await
        .map_err(|error| eyre!("{error}"))
    };
    let application = async {
        let client = NodeClient::from_services(ready_rx.await?);
        assert!(service_registered.load(Ordering::Relaxed));
        assert!(UdpSocket::bind(native_addr).is_err());
        let genesis = zakura_chain::block::genesis::regtest_genesis_block();
        let genesis_hash = genesis.hash();
        let mut tip_changes = client.subscribe_chain_tip();
        timeout(Duration::from_secs(30), tip_changes.wait_for_tip_change())
            .await
            .expect("tip changes within the test timeout")
            .expect("tip change listener remains open");
        assert_eq!(client.tip(), Some((BlockHeight(0), genesis_hash)));
        assert_eq!(client.database().tip(), Some((BlockHeight(0), genesis_hash)));
        assert!(client.mempool_transactions().await.expect("mempool query succeeds").is_empty());
        assert_eq!(
            client.block(BlockHeight(0)).await.expect("block query succeeds"),
            Some(genesis)
        );

        if stop_by_signal {
            signal_requested.cancel();
            shutdown.cancelled().await;
        }
        Ok(())
    };
    let signal = async {
        signal_requested.cancelled().await;
        Ok(())
    };
    timeout(Duration::from_secs(30), supervise(node, application, shutdown.clone(), signal))
        .await
        .expect("node lifecycle finishes within timeout")
        .expect("node lifecycle succeeds");
    // Endpoint shutdown can finish before the transport's socket task is dropped.
    let socket = timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(socket) = UdpSocket::bind(native_addr) {
                break socket;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("node shutdown releases the endpoint");
    drop(socket);
}

#[tokio::test]
async fn signal_waits_for_an_active_blocking_writer() {
    let shutdown = CancellationToken::new();
    let (started_tx, started_rx) = oneshot::channel();
    let (finish_tx, finish_rx) = std::sync::mpsc::channel();
    let writer_finished = Arc::new(AtomicBool::new(false));
    let writer_flag = writer_finished.clone();
    let application = async {
        let writer = tokio::task::spawn_blocking(move || {
            started_tx.send(()).unwrap();
            finish_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            writer_flag.store(true, Ordering::SeqCst);
        });
        shutdown.cancelled().await;
        writer.await?;
        Ok(())
    };
    let node = async {
        shutdown.cancelled().await;
        finish_tx.send(()).unwrap();
        Ok(())
    };
    let signal = async {
        started_rx.await.unwrap();
        Ok(())
    };
    timeout(Duration::from_secs(5), supervise(node, application, shutdown.clone(), signal))
        .await
        .unwrap()
        .unwrap();
    assert!(writer_finished.load(Ordering::SeqCst));
}
