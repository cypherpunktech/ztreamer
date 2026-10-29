//! Benchmark probe for a running ztreamerd. Prints one JSON line per event, with a Unix time in
//! milliseconds.
//!
//! - `grpc_probe <ENDPOINT> info <N>`: N `GetLightdInfo` calls, latency of each.
//! - `grpc_probe <ENDPOINT> tree <HEIGHT> <N>`: N `GetTreeState` calls at HEIGHT.
//! - `grpc_probe <ENDPOINT> watch <MILLIS>`: `GetLatestBlock` each MILLIS, a line per new height.

use std::{
    env,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use ztreamer_protocol::proto::{self, compact_tx_streamer_client::CompactTxStreamerClient};

type Error = Box<dyn std::error::Error + Send + Sync>;

fn now_ms() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = env::args().collect();
    let channel = tonic::transport::Endpoint::from_shared(args[1].clone())?.connect().await?;
    let mut client = CompactTxStreamerClient::new(channel).max_decoding_message_size(64 << 20);
    match args[2].as_str() {
        "info" => {
            for _ in 0..args[3].parse::<u32>()? {
                let started = Instant::now();
                let info = client.get_lightd_info(proto::Empty {}).await?.into_inner();
                let ms = started.elapsed().as_secs_f64() * 1e3;
                println!(
                    "{{\"t\":{},\"rpc\":\"GetLightdInfo\",\"ms\":{ms:.3},\"height\":{},\"version\":\"{}\",\"vendor\":\"{}\"}}",
                    now_ms(),
                    info.block_height,
                    info.version,
                    info.vendor
                );
            }
        }
        "tree" => {
            let height: u64 = args[3].parse()?;
            for _ in 0..args[4].parse::<u32>()? {
                let started = Instant::now();
                let state = client
                    .get_tree_state(proto::BlockId { height, hash: vec![] })
                    .await?
                    .into_inner();
                let ms = started.elapsed().as_secs_f64() * 1e3;
                println!(
                    "{{\"t\":{},\"rpc\":\"GetTreeState\",\"ms\":{ms:.3},\"height\":{},\"sapling_len\":{},\"orchard_len\":{}}}",
                    now_ms(),
                    state.height,
                    state.sapling_tree.len(),
                    state.orchard_tree.len()
                );
            }
        }
        "watch" => {
            let period = Duration::from_millis(args[3].parse()?);
            let mut last = 0;
            loop {
                let block = client.get_latest_block(proto::ChainSpec {}).await?.into_inner();
                if block.height != last {
                    last = block.height;
                    println!(
                        "{{\"t\":{},\"rpc\":\"GetLatestBlock\",\"height\":{}}}",
                        now_ms(),
                        block.height
                    );
                }
                tokio::time::sleep(period).await;
            }
        }
        other => return Err(format!("unknown mode {other}").into()),
    }
    Ok(())
}
