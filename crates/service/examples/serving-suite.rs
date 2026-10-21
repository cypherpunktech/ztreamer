//! Reproducible RPC measurements against a running server. See benchmarks/README.md.
use clap::{Parser, ValueEnum};
use prost::Message;
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    num::NonZeroUsize,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::Barrier, task::JoinSet};
use ztreamer_protocol::proto::{self, compact_tx_streamer_client::CompactTxStreamerClient};

type Client = CompactTxStreamerClient<tonic::transport::Channel>;
type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq, ValueEnum)]
enum Suite {
    Latency,
    Ranges,
    Concurrency,
    Sync,
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    endpoint: String,
    /// Caller-supplied identity of the SERVER, e.g. v0.0.1 or a full commit SHA.
    #[arg(long)]
    label: String,
    /// Must not already exist, to prevent mixing results from different runs.
    #[arg(long)]
    output: PathBuf,
    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        default_value = "latency,ranges,concurrency,sync"
    )]
    suites: Vec<Suite>,
    /// First eligible height; defaults to the server's Sapling activation height.
    #[arg(long)]
    start: Option<u32>,
    /// Fixed upper height; defaults to the tip observed during preflight.
    #[arg(long)]
    end: Option<u32>,
    #[arg(long, default_value_t = 20260829)]
    seed: u64,
    #[arg(long, default_value = "1")]
    repeats: NonZeroUsize,
    #[arg(long, default_value = "100")]
    requests: NonZeroUsize,
    /// Samples per range size per repeat; never silently reduced for large ranges.
    #[arg(long, default_value = "1000")]
    range_requests: NonZeroUsize,
    /// Override sample counts for specific sizes, e.g. 100000=100.
    #[arg(long, value_delimiter = ',', value_parser = parse_range_request_count)]
    range_request_counts: Vec<RangeRequestCount>,
    #[arg(long, value_delimiter = ',', default_value = "100,1000,10000,100000")]
    range_sizes: Vec<NonZeroUsize>,
    #[arg(long, value_delimiter = ',', default_value = "1,8,32,128")]
    clients: Vec<NonZeroUsize>,
    #[arg(long, default_value = "10000")]
    concurrency_blocks: NonZeroUsize,
    /// Sustained closed-loop duration PER concurrency level and repeat.
    #[arg(long, default_value = "10")]
    seconds: NonZeroUsize,
    #[arg(long, default_value = "10000")]
    sync_chunk: NonZeroUsize,
    #[arg(long, default_value = "120")]
    timeout_seconds: NonZeroUsize,
    /// Untimed requests per scenario/client; zero still uses connected channels.
    #[arg(long, default_value_t = 1)]
    warmup: usize,
    /// JSON object: {"addresses":{"quiet":"t..."},"txids":["display-order hex"]}.
    #[arg(long)]
    fixtures: Option<PathBuf>,
}

#[derive(Clone, Debug)]
struct RangeRequestCount {
    blocks: NonZeroUsize,
    requests: NonZeroUsize,
}

fn parse_range_request_count(value: &str) -> Result<RangeRequestCount, String> {
    let (blocks, requests) = value.split_once('=').ok_or("expected BLOCKS=REQUESTS")?;
    Ok(RangeRequestCount {
        blocks: blocks.parse().map_err(|_| "BLOCKS must be a positive integer")?,
        requests: requests.parse().map_err(|_| "REQUESTS must be a positive integer")?,
    })
}

#[derive(Clone)]
enum Rpc {
    Info,
    LatestBlock,
    Block,
    Tree,
    LatestTree,
    Subtrees(i32),
    Mempool,
    Transaction(Vec<u8>),
    AddressHistory(String, bool),
    Balance(String),
    Utxos(String),
    Range(u32),
}

#[derive(Clone)]
struct Case {
    name: String,
    rpc: Rpc,
}

#[derive(Default)]
struct Observation {
    first_us: Option<f64>,
    elapsed_us: f64,
    messages: u64,
    bytes: u64,
    blocks: u64,
    status: String,
    error: String,
}

impl Observation {
    fn message<M: Message>(&mut self, m: &M, started: Instant) {
        self.first_us.get_or_insert(started.elapsed().as_secs_f64() * 1e6);
        self.messages += 1;
        self.bytes += m.encoded_len() as u64;
    }
}

fn block(height: u32) -> proto::BlockId {
    proto::BlockId { height: height.into(), hash: vec![] }
}

fn range(start: u32, end: u32) -> proto::BlockRange {
    proto::BlockRange { start: Some(block(start)), end: Some(block(end)), pool_types: vec![] }
}

async fn drain<M: Message + Default>(
    mut stream: tonic::Streaming<M>,
    o: &mut Observation,
    t: Instant,
) -> Result<(), tonic::Status> {
    while let Some(m) = stream.message().await? {
        o.message(&m, t);
    }
    Ok(())
}

async fn measure(
    client: &mut Client,
    rpc: &Rpc,
    height: u32,
    end: u32,
    timeout: Duration,
) -> Observation {
    let mut o = Observation::default();
    let t = Instant::now();
    let result = tokio::time::timeout(timeout, async {
        match rpc {
            Rpc::Info => o.message(&client.get_lightd_info(proto::Empty {}).await?.into_inner(), t),
            Rpc::LatestBlock => {
                o.message(&client.get_latest_block(proto::ChainSpec {}).await?.into_inner(), t)
            }
            Rpc::Block => {
                let m = client.get_block(block(height)).await?.into_inner();
                if m.height != u64::from(height) {
                    return Err(tonic::Status::data_loss("unexpected block height"));
                }
                o.message(&m, t);
                o.blocks = 1;
            }
            Rpc::Tree => o.message(&client.get_tree_state(block(height)).await?.into_inner(), t),
            Rpc::LatestTree => {
                o.message(&client.get_latest_tree_state(proto::Empty {}).await?.into_inner(), t)
            }
            Rpc::Subtrees(pool) => {
                drain(
                    client
                        .get_subtree_roots(proto::GetSubtreeRootsArg {
                            start_index: 0,
                            shielded_protocol: *pool,
                            max_entries: 10,
                        })
                        .await?
                        .into_inner(),
                    &mut o,
                    t,
                )
                .await?
            }
            Rpc::Mempool => {
                drain(
                    client
                        .get_mempool_tx(proto::GetMempoolTxRequest::default())
                        .await?
                        .into_inner(),
                    &mut o,
                    t,
                )
                .await?
            }
            Rpc::Transaction(hash) => o.message(
                &client
                    .get_transaction(proto::TxFilter { hash: hash.clone(), ..Default::default() })
                    .await?
                    .into_inner(),
                t,
            ),
            Rpc::AddressHistory(address, legacy) => {
                let request = proto::TransparentAddressBlockFilter {
                    address: address.clone(),
                    range: Some(range(height, end)),
                };
                let response = if *legacy {
                    client.get_taddress_txids(request).await?
                } else {
                    client.get_taddress_transactions(request).await?
                };
                drain(response.into_inner(), &mut o, t).await?;
            }
            Rpc::Balance(address) => o.message(
                &client
                    .get_taddress_balance(proto::AddressList { addresses: vec![address.clone()] })
                    .await?
                    .into_inner(),
                t,
            ),
            Rpc::Utxos(address) => o.message(
                &client
                    .get_address_utxos(proto::GetAddressUtxosArg {
                        addresses: vec![address.clone()],
                        start_height: height.into(),
                        max_entries: 0,
                    })
                    .await?
                    .into_inner(),
                t,
            ),
            Rpc::Range(count) => {
                let mut stream =
                    client.get_block_range(range(height, height + (count - 1))).await?.into_inner();
                while let Some(m) = stream.message().await? {
                    if o.blocks >= u64::from(*count) || m.height != u64::from(height) + o.blocks {
                        return Err(tonic::Status::data_loss(
                            "unexpected block height or extra block",
                        ));
                    }
                    o.message(&m, t);
                    o.blocks += 1;
                }
                if o.blocks != u64::from(*count) {
                    return Err(tonic::Status::data_loss("incomplete block range"));
                }
            }
        }
        Ok::<(), tonic::Status>(())
    })
    .await;
    o.elapsed_us = t.elapsed().as_secs_f64() * 1e6;
    match result {
        Ok(Ok(())) => o.status = "ok".into(),
        Ok(Err(e)) => {
            o.status = format!("{:?}", e.code());
            o.error = e.message().into();
        }
        Err(_) => {
            o.status = "DeadlineExceeded".into();
            o.error = "client request deadline exceeded".into();
        }
    }
    o
}

// SplitMix64: explicitly fixed algorithm, independent of dependencies and platform.
fn height(seed: u64, sample: u64, start: u32, end: u32, count: u32) -> u32 {
    let mut z = seed.wrapping_add(sample.wrapping_mul(0x9e3779b97f4a7c15));
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^= z >> 31;
    start + (z % (u64::from(end) - u64::from(start) + 2 - u64::from(count))) as u32
}

async fn connect(endpoint: &str, timeout: Duration) -> Result<Client, Error> {
    Ok(Client::new(
        tonic::transport::Endpoint::from_shared(endpoint.to_owned())?
            .connect_timeout(timeout)
            .connect()
            .await?,
    )
    .max_decoding_message_size(512 * 1024 * 1024))
}

fn quantile(values: &mut [f64], p: f64) -> Option<f64> {
    values.sort_by(f64::total_cmp);
    (!values.is_empty()).then(|| {
        values[((values.len() as f64 * p).ceil() as usize).saturating_sub(1).min(values.len() - 1)]
    })
}

fn csv_row(w: &mut impl Write, values: &[Value]) -> std::io::Result<()> {
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            write!(w, ",")?;
        }
        let s = match v {
            Value::Null => String::new(),
            Value::String(s) => s.clone(),
            _ => v.to_string(),
        };
        write!(w, "\"{}\"", s.replace('"', "\"\""))?;
    }
    writeln!(w)
}

const SAMPLE_COLUMNS: &[&str] = &[
    "label",
    "suite",
    "case",
    "repeat",
    "clients",
    "client",
    "sample",
    "phase",
    "start_height",
    "end_height",
    "status",
    "error",
    "first_message_us",
    "complete_us",
    "messages",
    "blocks",
    "protobuf_bytes",
];
const SUMMARY_COLUMNS: &[&str] = &[
    "label",
    "suite",
    "case",
    "repeat",
    "clients",
    "status",
    "attempts",
    "successes",
    "errors",
    "first_message_samples",
    "warmup_attempts",
    "warmup_errors",
    "range_blocks",
    "start_height",
    "end_height",
    "wall_seconds",
    "successful_blocks",
    "successful_protobuf_bytes",
    "blocks_per_second",
    "protobuf_bytes_per_second",
    "complete_p50_us",
    "complete_p95_us",
    "complete_p99_us",
    "complete_max_us",
    "first_p50_us",
    "first_p95_us",
    "first_p99_us",
    "first_max_us",
];

struct Scenario<'a> {
    suite: &'a str,
    case: &'a str,
    repeat: usize,
    clients: usize,
}

struct Output {
    samples: BufWriter<File>,
    summaries: BufWriter<File>,
    rows: Vec<Value>,
    failed: bool,
}

impl Output {
    fn new(args: &Args) -> Result<Self, Error> {
        fs::create_dir(&args.output)?;
        let mut samples = BufWriter::new(File::create(args.output.join("samples.csv"))?);
        let mut summaries = BufWriter::new(File::create(args.output.join("summary.csv"))?);
        csv_row(&mut samples, &SAMPLE_COLUMNS.iter().map(|s| json!(s)).collect::<Vec<_>>())?;
        csv_row(&mut summaries, &SUMMARY_COLUMNS.iter().map(|s| json!(s)).collect::<Vec<_>>())?;
        Ok(Self { samples, summaries, rows: vec![], failed: false })
    }

    fn record(
        &mut self,
        args: &Args,
        scenario: Scenario<'_>,
        samples: &[Sample],
        wall: f64,
    ) -> Result<(), Error> {
        let Scenario { suite, case, repeat, clients } = scenario;
        for s in samples {
            let o = &s.o;
            csv_row(
                &mut self.samples,
                &[
                    json!(args.label),
                    json!(suite),
                    json!(case),
                    json!(repeat),
                    json!(clients),
                    json!(s.client),
                    json!(s.sample),
                    json!(s.phase),
                    json!(s.start),
                    json!(s.end),
                    json!(o.status),
                    json!(o.error),
                    json!(o.first_us),
                    json!(o.elapsed_us),
                    json!(o.messages),
                    json!(o.blocks),
                    json!(o.bytes),
                ],
            )?;
        }
        self.samples.flush()?;
        let measured: Vec<_> = samples.iter().filter(|s| s.phase == "measure").collect();
        let good: Vec<_> = measured.iter().filter(|s| s.o.status == "ok").collect();
        let errors = measured.len() - good.len();
        let warmup_failed = samples.iter().any(|s| s.phase == "warmup" && s.o.status != "ok");
        let status = if warmup_failed {
            "warmup_failed"
        } else if errors > 0 {
            "failed"
        } else if good.is_empty() {
            "no_samples"
        } else {
            "ok"
        };
        self.failed |= status != "ok";
        let blocks: u64 = good.iter().map(|s| s.o.blocks).sum();
        let bytes: u64 = good.iter().map(|s| s.o.bytes).sum();
        let mut complete: Vec<_> = good.iter().map(|s| s.o.elapsed_us).collect();
        let mut first: Vec<_> = good.iter().filter_map(|s| s.o.first_us).collect();
        let mut row = json!({"label": args.label, "suite": suite, "case": case, "repeat": repeat,
            "clients": clients, "status": status, "attempts": measured.len(), "successes": good.len(), "errors": errors,
            "first_message_samples": first.len(),
            "warmup_attempts": samples.iter().filter(|s| s.phase == "warmup").count(),
            "warmup_errors": samples.iter().filter(|s| s.phase == "warmup" && s.o.status != "ok").count(),
            "range_blocks": if suite == "ranges" || suite == "concurrency" {
                samples.first().map(|s| u64::from(s.end) - u64::from(s.start) + 1)
            } else if suite == "sync" { Some(args.sync_chunk.get() as u64) } else { None },
            "start_height": samples.iter().map(|s| s.start).min(),
            "end_height": samples.iter().map(|s| s.end).max(),
            "wall_seconds": wall, "successful_blocks": blocks, "successful_protobuf_bytes": bytes,
            "blocks_per_second": if status == "ok" { Some(blocks as f64 / wall) } else { None },
            "protobuf_bytes_per_second": if status == "ok" { Some(bytes as f64 / wall) } else { None },
        });
        for (name, p) in [("p50", 0.5), ("p95", 0.95), ("p99", 0.99), ("max", 1.0)] {
            row[format!("complete_{name}_us")] = json!(quantile(&mut complete, p));
            row[format!("first_{name}_us")] = json!(quantile(&mut first, p));
        }
        csv_row(
            &mut self.summaries,
            &SUMMARY_COLUMNS.iter().map(|k| row[*k].clone()).collect::<Vec<_>>(),
        )?;
        self.summaries.flush()?;
        self.rows.push(row);
        Ok(())
    }
}

struct Sample {
    client: usize,
    sample: usize,
    phase: &'static str,
    start: u32,
    end: u32,
    o: Observation,
}

fn count(rpc: &Rpc) -> u32 {
    if let Rpc::Range(n) = rpc { *n } else { 1 }
}
fn request_height(args: &Args, rpc: &Rpc, sample: u64, start: u32, end: u32) -> u32 {
    if matches!(rpc, Rpc::AddressHistory(..) | Rpc::Utxos(..)) {
        start
    } else {
        height(args.seed, sample, start, end, count(rpc))
    }
}
fn request_end(rpc: &Rpc, start: u32, end: u32) -> u32 {
    if matches!(rpc, Rpc::AddressHistory(..) | Rpc::Utxos(..)) {
        end
    } else {
        start + (count(rpc) - 1)
    }
}

async fn sequential(
    args: &Args,
    client: &mut Client,
    case: &Case,
    start: u32,
    end: u32,
    n: usize,
    timeout: Duration,
) -> (Vec<Sample>, f64) {
    let mut samples = vec![];
    for i in 0..args.warmup {
        let h = request_height(args, &case.rpc, i as u64, start, end);
        let o = measure(client, &case.rpc, h, end, timeout).await;
        let failed = o.status != "ok";
        samples.push(Sample {
            client: 0,
            sample: i,
            phase: "warmup",
            start: h,
            end: request_end(&case.rpc, h, end),
            o,
        });
        if failed {
            return (samples, 0.0);
        }
    }
    let t = Instant::now();
    for i in 0..n {
        let h = request_height(args, &case.rpc, i as u64, start, end);
        let o = measure(client, &case.rpc, h, end, timeout).await;
        samples.push(Sample {
            client: 0,
            sample: i,
            phase: "measure",
            start: h,
            end: request_end(&case.rpc, h, end),
            o,
        });
    }
    (samples, t.elapsed().as_secs_f64())
}

async fn concurrent(
    args: &Args,
    start: u32,
    end: u32,
    clients: usize,
    timeout: Duration,
) -> Result<(Vec<Sample>, f64), Error> {
    let mut connections = vec![];
    let mut samples = vec![];
    let blocks = u32::try_from(args.concurrency_blocks.get())?;
    for id in 0..clients {
        let mut client = connect(&args.endpoint, timeout).await?;
        for i in 0..args.warmup {
            let h = height(args.seed, id as u64, start, end, blocks);
            let o = measure(&mut client, &Rpc::Range(blocks), h, end, timeout).await;
            let failed = o.status != "ok";
            samples.push(Sample {
                client: id,
                sample: i,
                phase: "warmup",
                start: h,
                end: h + (blocks - 1),
                o,
            });
            if failed {
                return Ok((samples, 0.0));
            }
        }
        connections.push(client);
    }
    let barrier = Arc::new(Barrier::new(clients + 1));
    let mut tasks = JoinSet::new();
    for (id, mut client) in connections.into_iter().enumerate() {
        let barrier = barrier.clone();
        let seed = args.seed;
        let seconds = args.seconds.get() as u64;
        tasks.spawn(async move {
            let mut samples = vec![];
            barrier.wait().await;
            let stop = Instant::now() + Duration::from_secs(seconds);
            let mut i = 0;
            while Instant::now() < stop {
                let h = height(seed, (i * clients + id) as u64, start, end, blocks);
                let o = measure(&mut client, &Rpc::Range(blocks), h, end, timeout).await;
                let failed = o.status != "ok";
                samples.push(Sample {
                    client: id,
                    sample: i,
                    phase: "measure",
                    start: h,
                    end: h + (blocks - 1),
                    o,
                });
                i += 1;
                if failed {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
            samples
        });
    }
    let t = Instant::now();
    barrier.wait().await;
    while let Some(result) = tasks.join_next().await {
        samples.extend(result?);
    }
    Ok((samples, t.elapsed().as_secs_f64()))
}

async fn sync(
    args: &Args,
    client: &mut Client,
    start: u32,
    end: u32,
    timeout: Duration,
) -> (Vec<Sample>, f64) {
    let mut samples = vec![];
    let t = Instant::now();
    let mut h = u64::from(start);
    while h <= u64::from(end) {
        let n = (u64::from(end) - h + 1).min(args.sync_chunk.get() as u64) as u32;
        let o = measure(client, &Rpc::Range(n), h as u32, end, timeout).await;
        let failed = o.status != "ok";
        samples.push(Sample {
            client: 0,
            sample: samples.len(),
            phase: "measure",
            start: h as u32,
            end: (h + u64::from(n) - 1) as u32,
            o,
        });
        if failed {
            break;
        }
        h += u64::from(n);
    }
    (samples, t.elapsed().as_secs_f64())
}

fn cases(fixtures: &Value) -> Result<Vec<Case>, Error> {
    let mut cases: Vec<_> = [
        ("GetLightdInfo", Rpc::Info),
        ("GetLatestBlock", Rpc::LatestBlock),
        ("GetBlock", Rpc::Block),
        ("GetTreeState", Rpc::Tree),
        ("GetLatestTreeState", Rpc::LatestTree),
        ("GetSubtreeRoots/sapling", Rpc::Subtrees(0)),
        ("GetSubtreeRoots/orchard", Rpc::Subtrees(1)),
        ("GetMempoolTx", Rpc::Mempool),
    ]
    .into_iter()
    .map(|(name, rpc)| Case { name: name.into(), rpc })
    .collect();
    if let Some(addresses) = fixtures.get("addresses") {
        for (label, value) in addresses.as_object().ok_or("fixtures.addresses must be an object")? {
            let a = value.as_str().ok_or("fixture address must be a string")?;
            for (method, rpc) in [
                ("GetTaddressTxids", Rpc::AddressHistory(a.into(), true)),
                ("GetTaddressTransactions", Rpc::AddressHistory(a.into(), false)),
                ("GetTaddressBalance", Rpc::Balance(a.into())),
                ("GetAddressUtxos", Rpc::Utxos(a.into())),
            ] {
                cases.push(Case { name: format!("{method}/{label}"), rpc });
            }
        }
    }
    if let Some(txids) = fixtures.get("txids") {
        for (i, value) in
            txids.as_array().ok_or("fixtures.txids must be an array")?.iter().enumerate()
        {
            let mut hash = hex::decode(value.as_str().ok_or("txid must be a string")?)?;
            if hash.len() != 32 {
                return Err("txid must contain exactly 32 bytes".into());
            }
            hash.reverse();
            cases.push(Case { name: format!("GetTransaction/{i}"), rpc: Rpc::Transaction(hash) });
        }
    }
    Ok(cases)
}

async fn run(args: &Args, output: &mut Output) -> Result<(), Error> {
    let timeout = Duration::from_secs(args.timeout_seconds.get() as u64);
    let mut client = connect(&args.endpoint, timeout).await?;
    let info =
        tokio::time::timeout(timeout, client.get_lightd_info(proto::Empty {})).await??.into_inner();
    let tip = tokio::time::timeout(timeout, client.get_latest_block(proto::ChainSpec {}))
        .await??
        .into_inner();
    let start = args.start.unwrap_or(u32::try_from(info.sapling_activation_height)?);
    let end = args.end.unwrap_or(u32::try_from(tip.height)?);
    if start > end || u64::from(end) > tip.height {
        return Err("require start <= end <= observed server tip".into());
    }
    if args.sync_chunk.get() as u64 > u64::from(u32::MAX) {
        return Err("sync chunk must fit in u32".into());
    }
    let available = u64::from(end) - u64::from(start) + 1;
    let mut overridden = std::collections::HashSet::new();
    for entry in &args.range_request_counts {
        if !args.range_sizes.contains(&entry.blocks) || !overridden.insert(entry.blocks) {
            return Err("range-request-counts must name distinct configured range sizes".into());
        }
    }
    for size in args.range_sizes.iter().filter(|_| args.suites.contains(&Suite::Ranges)).chain(
        std::iter::once(&args.concurrency_blocks)
            .filter(|_| args.suites.contains(&Suite::Concurrency)),
    ) {
        if size.get() as u64 > available || size.get() as u64 > u64::from(u32::MAX) {
            return Err(format!(
                "range size {size} exceeds selected height interval or u32 capacity"
            )
            .into());
        }
    }
    let fixtures: Value = match &args.fixtures {
        Some(p) => serde_json::from_slice(&fs::read(p)?)?,
        None => json!({}),
    };
    if !fixtures.is_object() {
        return Err("fixtures must be a JSON object".into());
    }
    let cases = cases(&fixtures)?;
    let end_block =
        tokio::time::timeout(timeout, client.get_block(block(end))).await??.into_inner();
    if end_block.height != u64::from(end) {
        return Err("server returned the wrong block during preflight".into());
    }
    let metadata = json!({"schema_version": 1, "label": args.label, "endpoint": args.endpoint,
        "started_unix": SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        "start_height": start, "end_height": end, "observed_tip": tip.height, "observed_tip_hash": hex::encode(&tip.hash),
        "end_block_hash": hex::encode(&end_block.hash),
        "server": {"version": info.version, "git_commit": info.git_commit, "chain": info.chain_name, "backend_build": info.zcashd_build},
        "seed": args.seed, "height_generator": "splitmix64-v1", "arguments": std::env::args().collect::<Vec<_>>(),
        "fixtures": fixtures, "latency_cases": cases.iter().map(|c| &c.name).collect::<Vec<_>>(),
        "omitted_fixture_cases": {"transactions": fixtures.get("txids").is_none_or(|v| v.as_array().is_none_or(Vec::is_empty)),
            "addresses": fixtures.get("addresses").is_none_or(|v| v.as_object().is_none_or(serde_json::Map::is_empty))},
        "latency_model": "connected channel; configured warmups; complete response including protobuf decode; no cache reset",
        "concurrency_model": "one outstanding request per connected client; sustained duration plus in-flight completion",
        "sync_model": "sequential compact-block download; fixed bounds; no trial decryption; no cache reset",
        "percentiles": "nearest-rank, successful requests only; empty streams have no first-message latency",
        "throughput": "successful protobuf payload only, excluding transport framing; null if scenario failed",
    });
    fs::write(args.output.join("metadata.json"), serde_json::to_vec_pretty(&metadata)?)?;
    for repeat in 1..=args.repeats.get() {
        for suite in &args.suites {
            match suite {
                Suite::Latency | Suite::Ranges => {
                    let ranges: Vec<_> = args
                        .range_sizes
                        .iter()
                        .map(|n| Case {
                            name: format!("GetBlockRange/{n}"),
                            rpc: Rpc::Range(n.get() as u32),
                        })
                        .collect();
                    let (suite, cases, n) = if *suite == Suite::Latency {
                        ("latency", &cases, args.requests.get())
                    } else {
                        ("ranges", &ranges, args.range_requests.get())
                    };
                    for case in cases {
                        let n = match case.rpc {
                            Rpc::Range(blocks) => args
                                .range_request_counts
                                .iter()
                                .find(|entry| entry.blocks.get() as u64 == u64::from(blocks))
                                .map_or(n, |entry| entry.requests.get()),
                            _ => n,
                        };
                        eprintln!("repeat {repeat}: {suite} {}", case.name);
                        let (samples, wall) =
                            sequential(args, &mut client, case, start, end, n, timeout).await;
                        output.record(
                            args,
                            Scenario { suite, case: &case.name, repeat, clients: 1 },
                            &samples,
                            wall,
                        )?;
                    }
                }
                Suite::Concurrency => {
                    for clients in &args.clients {
                        eprintln!("repeat {repeat}: concurrency {clients}");
                        let (samples, wall) =
                            concurrent(args, start, end, clients.get(), timeout).await?;
                        let name = format!("GetBlockRange/{}", args.concurrency_blocks);
                        output.record(
                            args,
                            Scenario {
                                suite: "concurrency",
                                case: &name,
                                repeat,
                                clients: clients.get(),
                            },
                            &samples,
                            wall,
                        )?;
                    }
                }
                Suite::Sync => {
                    eprintln!("repeat {repeat}: wallet sync {start}..={end}");
                    let (samples, wall) = sync(args, &mut client, start, end, timeout).await;
                    let name = format!("GetBlockRange/chunk-{}", args.sync_chunk);
                    output.record(
                        args,
                        Scenario { suite: "sync", case: &name, repeat, clients: 1 },
                        &samples,
                        wall,
                    )?;
                }
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    let mut output = Output::new(&args)?;
    let result = run(&args, &mut output).await;
    let status = if result.is_err() {
        "aborted"
    } else if output.failed {
        "failed"
    } else {
        "ok"
    };
    fs::write(
        args.output.join("results.json"),
        serde_json::to_vec_pretty(&json!({
            "schema_version": 1, "label": args.label, "status": status,
            "fatal_error": result.as_ref().err().map(ToString::to_string), "scenarios": output.rows,
        }))?,
    )?;
    result?;
    if output.failed {
        return Err("one or more scenarios failed; see results.json and samples.csv".into());
    }
    Ok(())
}

#[cfg(test)]
#[path = "../benches/support/mod.rs"]
mod support;

#[cfg(test)]
mod tests {
    use super::*;

    fn args(endpoint: &str, output: &std::path::Path) -> Args {
        Args::parse_from([
            "serving-suite",
            "--endpoint",
            endpoint,
            "--label",
            "test,\"version\"",
            "--output",
            output.to_str().unwrap(),
            "--start",
            "0",
            "--end",
            "9",
            "--suites",
            "ranges,concurrency,sync",
            "--repeats",
            "1",
            "--range-sizes",
            "1,3",
            "--range-requests",
            "2",
            "--range-request-counts",
            "3=4",
            "--clients",
            "1,2",
            "--concurrency-blocks",
            "3",
            "--seconds",
            "1",
            "--sync-chunk",
            "4",
        ])
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn local_grpc_suite_exports_complete_ranges_concurrency_and_sync() {
        let server = support::LocalServer::start().await;
        let dir = tempfile::tempdir().unwrap();
        let args = args(&server.endpoint, &dir.path().join("run"));
        let mut output = Output::new(&args).unwrap();
        run(&args, &mut output).await.unwrap();
        assert!(!output.failed);
        assert_eq!(output.rows.len(), 5);
        assert_eq!(output.rows[0]["attempts"], 2);
        assert_eq!(output.rows[1]["attempts"], 4);
        let sync = output.rows.last().unwrap();
        assert_eq!(sync["attempts"], 3);
        assert_eq!(sync["successful_blocks"], 10);
        assert_eq!(sync["status"], "ok");
        for row in &output.rows {
            assert!(row["blocks_per_second"].as_f64().unwrap() > 0.0);
            assert!(row["complete_p99_us"].as_f64().unwrap() > 0.0);
            assert_eq!(row["errors"], 0);
        }
        for row in output.rows.iter().filter(|r| r["suite"] == "concurrency") {
            assert!(row["wall_seconds"].as_f64().unwrap() >= 1.0);
            assert_eq!(
                row["successful_blocks"].as_u64().unwrap(),
                row["successes"].as_u64().unwrap() * 3
            );
        }
        let samples = fs::read_to_string(args.output.join("samples.csv")).unwrap();
        assert!(samples.contains("\"test,\"\"version\"\"\""));
        // Last wallet-sync chunk is inclusive and shorter than the configured chunk.
        assert!(
            samples.lines().any(|l| l.contains("\"sync\"") && l.contains("\"8\",\"9\",\"ok\""))
        );
        let metadata: Value =
            serde_json::from_slice(&fs::read(args.output.join("metadata.json")).unwrap()).unwrap();
        assert_eq!(metadata["end_height"], 9);
        assert_eq!(metadata["omitted_fixture_cases"]["addresses"], true);
        assert!(Output::new(&args).is_err(), "must refuse to overwrite prior data");
        let mut client = connect(&server.endpoint, Duration::from_secs(10)).await.unwrap();
        let case = Case { name: "GetBlock".into(), rpc: Rpc::Block };
        let (samples, wall) =
            sequential(&args, &mut client, &case, 0, 9, 3, Duration::from_secs(10)).await;
        assert_eq!(samples.len(), 4); // One warmup and three measured requests.
        assert!(samples.iter().all(|s| s.o.status == "ok" && s.o.blocks == 1));
        output
            .record(
                &args,
                Scenario { suite: "latency", case: &case.name, repeat: 1, clients: 1 },
                &samples,
                wall,
            )
            .unwrap();
        assert_eq!(output.rows.last().unwrap()["successes"], 3);
        server.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn failed_range_is_saved_and_never_reported_as_successful_sync() {
        let server = support::LocalServer::start().await;
        let dir = tempfile::tempdir().unwrap();
        let args = args(&server.endpoint, &dir.path().join("run"));
        let mut client = connect(&server.endpoint, Duration::from_secs(10)).await.unwrap();
        // The synthetic index ends at 2209. Request beyond it and verify we stop.
        let (samples, wall) = sync(&args, &mut client, 2208, 2220, Duration::from_secs(10)).await;
        assert_eq!(samples.len(), 1);
        assert_ne!(samples[0].o.status, "ok");
        let mut output = Output::new(&args).unwrap();
        output
            .record(
                &args,
                Scenario { suite: "sync", case: "failure", repeat: 1, clients: 1 },
                &samples,
                wall,
            )
            .unwrap();
        assert!(output.failed);
        assert_eq!(output.rows[0]["errors"], 1);
        assert!(output.rows[0]["blocks_per_second"].is_null());
        assert!(output.rows[0]["complete_p50_us"].is_null());
        assert!(
            fs::read_to_string(args.output.join("samples.csv"))
                .unwrap()
                .contains(&samples[0].o.status)
        );

        let case = Case { name: "unavailable".into(), rpc: Rpc::Range(3) };
        let (warmup, wall) =
            sequential(&args, &mut client, &case, 2210, 2220, 5, Duration::from_secs(10)).await;
        output
            .record(
                &args,
                Scenario { suite: "ranges", case: "unavailable", repeat: 1, clients: 1 },
                &warmup,
                wall,
            )
            .unwrap();
        assert_eq!(output.rows[1]["status"], "warmup_failed");
        assert_eq!(output.rows[1]["attempts"], 0);
        assert_eq!(output.rows[1]["warmup_errors"], 1);
        server.stop().await;
    }
}
