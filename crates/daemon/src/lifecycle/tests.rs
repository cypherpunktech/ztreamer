use std::{
    cell::Cell,
    future,
    net::UdpSocket,
    rc::Rc,
    sync::{
        Arc,
        atomic::{
            AtomicBool, AtomicUsize,
            Ordering::{self, SeqCst},
        },
    },
    time::Duration,
};

use eyre::eyre;
use tokio::{sync::oneshot, time::timeout};
use zakura_chain::block::Height as BlockHeight;
use zakura_network::zakura::{Peer, Service, Stream, ZakuraConnId, ZakuraPeerId};
use zakurad::{config::ZakuradConfig, node::CustomService};
use ztreamer_embedded::NodeClient;

use super::*;

/// [`Supervisor::run`], with a panic when it does not finish within five seconds.
async fn run_within_five_seconds(
    supervisor: Supervisor,
    signal: impl Future<Output = Result<()>>,
    main: impl Future<Output = Result<()>>,
) -> Result<()> {
    timeout(Duration::from_secs(5), supervisor.run(signal, main)).await.unwrap()
}

/// A task that waits for the shutdown of `tasks`, then counts its cleanup in `cleaned`.
fn cleanup_task(
    tasks: &TaskSet,
    cleaned: &Arc<AtomicBool>,
) -> impl Future<Output = Result<()>> + Send + 'static {
    let (shutdown, cleaned) = (tasks.shutdown().clone(), Arc::clone(cleaned));
    async move {
        shutdown.cancelled().await;
        tokio::task::yield_now().await;
        cleaned.store(true, SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn main_completion_drains_tasks_and_main_needs_no_send() {
    let supervisor = Supervisor::new(CancellationToken::new());
    let cleaned = Arc::new(AtomicBool::new(false));
    supervisor.tasks().spawn("node", "node stopped", cleanup_task(supervisor.tasks(), &cleaned));
    // Keeping an Rc across await proves that the main work needs no Send bound.
    let main = async {
        let held = Rc::new(());
        tokio::task::yield_now().await;
        drop(held);
        Ok(())
    };
    run_within_five_seconds(supervisor, future::pending(), main).await.unwrap();
    assert!(cleaned.load(SeqCst));
}

#[tokio::test]
async fn main_failure_still_drains_tasks() {
    let supervisor = Supervisor::new(CancellationToken::new());
    let cleaned = Arc::new(AtomicBool::new(false));
    supervisor.tasks().spawn("node", "node stopped", cleanup_task(supervisor.tasks(), &cleaned));
    let main = async { Err(eyre!("main failed")) };
    let error = run_within_five_seconds(supervisor, future::pending(), main).await.unwrap_err();
    assert_eq!(error.to_string(), "main failed");
    assert!(cleaned.load(SeqCst));
}

#[tokio::test]
async fn task_failure_stops_main_waiting_for_readiness() {
    let supervisor = Supervisor::new(CancellationToken::new());
    let shutdown = supervisor.tasks().shutdown().clone();
    supervisor.tasks().spawn("node", "node stopped", async { Err(eyre!("node failed")) });
    let cleaned = Cell::new(false);
    let main = async {
        shutdown.cancelled().await;
        tokio::task::yield_now().await;
        cleaned.set(true);
        Ok(())
    };
    let error = run_within_five_seconds(supervisor, future::pending(), main).await.unwrap_err();
    assert_eq!(error.to_string(), "node failed");
    assert!(cleaned.get());
}

#[tokio::test]
async fn unexpected_successful_task_stop_is_an_error() {
    let supervisor = Supervisor::new(CancellationToken::new());
    let shutdown = supervisor.tasks().shutdown().clone();
    supervisor.tasks().spawn("node", "Zakura stopped unexpectedly", async { Ok(()) });
    let main = async {
        shutdown.cancelled().await;
        Ok(())
    };
    let error = run_within_five_seconds(supervisor, future::pending(), main).await.unwrap_err();
    assert_eq!(error.to_string(), "Zakura stopped unexpectedly");
}

#[tokio::test]
async fn signal_drains_main_and_tasks_concurrently_even_on_error() {
    let supervisor = Supervisor::new(CancellationToken::new());
    let shutdown = supervisor.tasks().shutdown().clone();
    let (node_started_tx, node_started_rx) = oneshot::channel();
    let (main_started_tx, main_started_rx) = oneshot::channel();
    let (node_cleanup_tx, node_cleanup_rx) = oneshot::channel();
    let (main_cleanup_tx, main_cleanup_rx) = oneshot::channel();
    let node_shutdown = shutdown.clone();
    supervisor.tasks().spawn("node", "node stopped", async move {
        node_started_tx.send(()).unwrap();
        node_shutdown.cancelled().await;
        node_cleanup_tx.send(()).unwrap();
        main_cleanup_rx.await.unwrap();
        Err(eyre!("node cleanup failed"))
    });
    let main = async {
        main_started_tx.send(()).unwrap();
        shutdown.cancelled().await;
        main_cleanup_tx.send(()).unwrap();
        node_cleanup_rx.await.unwrap();
        Ok(())
    };
    let signal = async {
        node_started_rx.await.unwrap();
        main_started_rx.await.unwrap();
        Ok(())
    };
    let error = run_within_five_seconds(supervisor, signal, main).await.unwrap_err();
    assert_eq!(error.to_string(), "node cleanup failed");
}

#[tokio::test]
async fn signal_failure_still_drains_main_and_tasks() {
    let supervisor = Supervisor::new(CancellationToken::new());
    let shutdown = supervisor.tasks().shutdown().clone();
    let task_cleaned = Arc::new(AtomicBool::new(false));
    supervisor.tasks().spawn(
        "node",
        "node stopped",
        cleanup_task(supervisor.tasks(), &task_cleaned),
    );
    let main_cleaned = Cell::new(false);
    let main = async {
        shutdown.cancelled().await;
        main_cleaned.set(true);
        Ok(())
    };
    let signal = async {
        tokio::task::yield_now().await;
        Err(eyre!("signal failed"))
    };
    let error = run_within_five_seconds(supervisor, signal, main).await.unwrap_err();
    assert_eq!(error.to_string(), "signal failed");
    assert!(task_cleaned.load(SeqCst) && main_cleaned.get());
}

#[tokio::test]
async fn task_errors_come_in_start_order() {
    let supervisor = Supervisor::new(CancellationToken::new());
    let (second_failed_tx, second_failed_rx) = oneshot::channel::<()>();
    supervisor.tasks().spawn("first", "first stopped", async {
        // Fail only after the second task failed.
        let _ = second_failed_rx.await;
        Err(eyre!("first failed"))
    });
    supervisor.tasks().spawn("second", "second stopped", async {
        drop(second_failed_tx);
        Err(eyre!("second failed"))
    });
    let shutdown = supervisor.tasks().shutdown().clone();
    let main = async move {
        shutdown.cancelled().await;
        Ok(())
    };
    let error = run_within_five_seconds(supervisor, future::pending(), main).await.unwrap_err();
    assert_eq!(error.to_string(), "first failed");
}

#[tokio::test]
async fn a_task_that_main_adds_later_is_supervised() {
    let supervisor = Supervisor::new(CancellationToken::new());
    let tasks = supervisor.tasks().clone();
    let main = async move {
        tokio::task::yield_now().await;
        tasks.spawn("server", "server stopped", async { Err(eyre!("server failed")) });
        tasks.shutdown().cancelled().await;
        Ok(())
    };
    let error = run_within_five_seconds(supervisor, future::pending(), main).await.unwrap_err();
    assert_eq!(error.to_string(), "server failed");
}

#[tokio::test]
async fn a_task_added_after_the_stop_never_starts() {
    let supervisor = Supervisor::new(CancellationToken::new());
    let tasks = supervisor.tasks().clone();
    let started = Arc::new(AtomicUsize::new(0));
    let main = {
        let started = Arc::clone(&started);
        async move {
            tasks.shutdown().cancelled().await;
            tasks.spawn("late", "late stopped", async move {
                started.fetch_add(1, SeqCst);
                Ok(())
            });
            Ok(())
        }
    };
    let signal = async { Ok(()) };
    run_within_five_seconds(supervisor, signal, main).await.unwrap();
    assert_eq!(started.load(SeqCst), 0);
}

#[tokio::test]
async fn a_panic_is_an_error_with_the_task_name() {
    let supervisor = Supervisor::new(CancellationToken::new());
    supervisor.tasks().spawn("filler", "filler stopped", async { panic!("filler bug") });
    let shutdown = supervisor.tasks().shutdown().clone();
    let main = async move {
        shutdown.cancelled().await;
        Ok(())
    };
    let error = run_within_five_seconds(supervisor, future::pending(), main).await.unwrap_err();
    assert_eq!(error.to_string(), "filler task panicked");
}

#[tokio::test]
async fn a_future_that_is_not_send_runs_on_a_blocking_thread() {
    let supervisor = Supervisor::new(CancellationToken::new());
    let shutdown = supervisor.tasks().shutdown().clone();
    let cleaned = Arc::new(AtomicBool::new(false));
    let task_cleaned = Arc::clone(&cleaned);
    supervisor.tasks().spawn_on_blocking_thread("node", "node stopped", move || async move {
        // The Rc makes the future not Send.
        let held = Rc::new(());
        shutdown.cancelled().await;
        drop(held);
        task_cleaned.store(true, SeqCst);
        Ok(())
    });
    run_within_five_seconds(supervisor, future::pending(), async { Ok(()) }).await.unwrap();
    assert!(cleaned.load(SeqCst));
}

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

    let supervisor = Supervisor::new(CancellationToken::new());
    let shutdown = supervisor.tasks().shutdown().clone();
    let signal_requested = CancellationToken::new();
    let (ready_tx, ready_rx) = oneshot::channel();
    let services = vec![CustomService {
        service: Arc::new(RegistrationProbe(service_registered.clone())),
        provides: vec![],
        seeks: vec![],
    }];
    let node_shutdown = shutdown.clone();
    supervisor.tasks().spawn_on_blocking_thread(
        "Zakura",
        "Zakura stopped unexpectedly",
        move || async move {
            Box::pin(zakurad::node::run_with_services_ready(
                config,
                services,
                node_shutdown,
                ready_tx,
            ))
            .await
            .map_err(|error| eyre!("{error}"))
        },
    );
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
    timeout(Duration::from_secs(30), supervisor.run(signal, application))
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
    let supervisor = Supervisor::new(CancellationToken::new());
    let shutdown = supervisor.tasks().shutdown().clone();
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
    let node_shutdown = shutdown.clone();
    supervisor.tasks().spawn("node", "node stopped", async move {
        node_shutdown.cancelled().await;
        finish_tx.send(()).unwrap();
        Ok(())
    });
    let signal = async {
        started_rx.await.unwrap();
        Ok(())
    };
    run_within_five_seconds(supervisor, signal, application).await.unwrap();
    assert!(writer_finished.load(Ordering::SeqCst));
}
