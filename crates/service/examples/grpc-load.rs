//! Closed-loop wallet load using Tonic and HdrHistogram; emits a JSON report.
use clap::Parser;
use hdrhistogram::Histogram;
use serde_json::json;
use std::{
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::Barrier, task::JoinSet};

use support::{Client, Error, LocalServer, consume, range};
use ztreamer_service::test_utils as support;

#[derive(Parser)]
struct Args {
    /// Use an existing server instead of the local synthetic fixture.
    #[arg(long)]
    endpoint: Option<String>,
    #[arg(long, default_value = "16")]
    clients: NonZeroUsize,
    #[arg(long, default_value = "10")]
    seconds: NonZeroUsize,
    #[arg(long, default_value = "0")]
    start: u32,
    #[arg(long, default_value = "1999")]
    end: u32,
    /// Consumer pause after each 64 decoded blocks.
    #[arg(long, default_value = "0")]
    delay_ms: u64,
    #[arg(long, default_value = "30")]
    timeout_seconds: NonZeroUsize,
}

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    let server = if args.endpoint.is_none() { Some(LocalServer::start().await) } else { None };
    let endpoint =
        args.endpoint.clone().unwrap_or_else(|| server.as_ref().unwrap().endpoint.clone());
    let timeout = Duration::from_secs(args.timeout_seconds.get() as u64);
    let delay = Duration::from_millis(args.delay_ms);
    let request = range(args.start, args.end);
    let mut clients = Vec::new();
    for _ in 0..args.clients.get() {
        let mut client = tokio::time::timeout(timeout, Client::connect(endpoint.clone())).await??;
        // Warm the connection and validate the request before collecting samples.
        tokio::time::timeout(timeout, consume(&mut client, request.clone(), Duration::ZERO))
            .await??;
        clients.push(client);
    }
    let barrier = Arc::new(Barrier::new(clients.len() + 1));
    let mut tasks = JoinSet::new();
    for mut client in clients {
        let barrier = barrier.clone();
        let request = request.clone();
        tasks.spawn(async move {
            let mut first = Histogram::<u64>::new(3).unwrap();
            let mut complete = Histogram::<u64>::new(3).unwrap();
            let (mut blocks, mut bytes, mut errors) = (0_u64, 0_u64, 0_u64);
            let mut last_error = None;
            barrier.wait().await;
            let stop = Instant::now() + Duration::from_secs(args.seconds.get() as u64);
            while Instant::now() < stop {
                match tokio::time::timeout(timeout, consume(&mut client, request.clone(), delay))
                    .await
                {
                    Ok(Ok(result)) => {
                        first.record(result.first.as_micros().max(1) as u64).unwrap();
                        complete.record(result.complete.as_micros().max(1) as u64).unwrap();
                        blocks += result.blocks;
                        bytes += result.bytes;
                    }
                    result => {
                        errors += 1;
                        last_error = Some(match result {
                            Err(error) => error.to_string(),
                            Ok(Err(error)) => error.to_string(),
                            Ok(Ok(_)) => unreachable!(),
                        });
                        // Avoid a tight failure loop against an unavailable server.
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
            }
            (first, complete, blocks, bytes, errors, last_error)
        });
    }
    let started_unix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let started = Instant::now();
    barrier.wait().await;
    let mut first = Histogram::<u64>::new(3)?;
    let mut complete = Histogram::<u64>::new(3)?;
    let (mut blocks, mut bytes, mut errors) = (0_u64, 0_u64, 0_u64);
    let mut last_error = None;
    while let Some(result) = tasks.join_next().await {
        let (f, c, n, b, e, error) = result?;
        first.add(&f)?;
        complete.add(&c)?;
        blocks += n;
        bytes += b;
        errors += e;
        if error.is_some() {
            last_error = error;
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    let percentiles = |h: &Histogram<u64>| {
        json!({
            "p50": h.value_at_quantile(0.50), "p95": h.value_at_quantile(0.95),
            "p99": h.value_at_quantile(0.99), "max": h.max(),
        })
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "schema_version": 1, "started_unix": started_unix,
            "workload": if server.is_some() { "synthetic-mixed-v1" } else { "external" },
            "model": "closed-loop; one outstanding request per client; warmed connections",
            "endpoint": endpoint, "clients": args.clients.get(),
            "start": args.start, "end": args.end, "delay_per_64_blocks_ms": args.delay_ms,
            "requested_seconds": args.seconds.get(), "elapsed_seconds": elapsed,
            "timeout_seconds": args.timeout_seconds.get(),
            "successful_requests": complete.len(), "errors": errors, "last_error": last_error,
            "blocks_per_second": blocks as f64 / elapsed,
            "protobuf_payload_bytes_per_second": bytes as f64 / elapsed,
            "first_block_us": percentiles(&first), "complete_range_us": percentiles(&complete),
        }))?
    );
    if let Some(server) = server {
        server.stop().await;
    }
    if errors > 0 {
        return Err("load run had failed requests; see JSON report".into());
    }
    Ok(())
}
