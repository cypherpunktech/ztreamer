# `--remote-rpc` live run, 2026-10-26

These are the first live measurements of `ztreamerd --remote-rpc` (#138) against a real zakura node.
They cover the three unmeasured items of the plan's section 9: the 2,000-block tail, `getblock`
throughput at 8 concurrent, and the head polling cost (assessment §4.1, risk D). The run was on
torvus on 2026-10-26 between 16:32 and 17:26 UTC. Raw outputs are in `out/`, node configs and logs
in `node/`, and the scripts and temporary programs in `tools/`. The machine is the one in
`../2026-10-22-replication/hardware.json`.

**Result.** The tail gate passes by a large margin. The head poll costs 207 RPCs per new block, as
risk D projected, but one poll takes about 0.1 s today and about 1 s at sandblast-era block sizes.
That is far below the 30 s `attempt_timeout`. **The run found a defect that stops the daemon on a
real node:** zakura sends error code `-1` for a `getblockhash` height above its tip, and
`RpcError::is_block_not_found` expects `-32602`. The unpatched daemon exits at its first head
reconcile. The head step ran on a build with a one-line benchmark patch (see deviation 5).

## Setup

| | |
| --- | --- |
| Host | torvus-ovh: AMD Ryzen 9 5900X, 12 cores, 24 threads, 62.7 GiB RAM, Linux 7.0.0-38. 2 × Micron NVMe in mdadm RAID1, ext4. |
| ztreamer | `dc684d77ab9e0e8d03c4388aed6c621976da3efd` (master, #141), release build, rustc 1.99.0. |
| zakura node | `zakurad 1.5.1+gaf944f5194ef` (tag v1.5.1, the version the workspace pins), archive mode, state format `29.1.0+indexer`. It ran on the state of the stopped ztreamer.service (`/var/lib/ztreamer/cache`) as user `ztreamer`. RPC on `127.0.0.1:18232` with a cookie. |
| Node tip | 3,531,461, frozen (no peers) for the tail, fan-out and gRPC steps. Moving (default peers) for the head step: 3,531,472 to 3,531,498. |
| Index | A copy of the stopped service index (18.6 GB `data.mdb`, durable tip 3,531,451, sealed through 3,530,999), moved back to durable tip **3,529,461** (node tip − 2,000) with `tools/rewind_index.rs`. Hash at 3,529,461 equals the node's `getblockhash 3529461`. `verify_continuity` passes. |
| Daemon | `ztreamerd --remote-rpc http://127.0.0.1:18232 --remote-rpc-cookie … --index-map-size 68719476736 --fetch-workers 8` (`tools/run-daemon.sh`). Requests in flight per worker: 8 (`DEFAULT_REQUESTS_IN_FLIGHT`). Segment: 256 blocks. |
| RPC accounting | `tcpdump -i lo` on port 18232 (snaplen 1,024; 256 for tail run 1), counted by `tools/pcap_rpc.py`. Master has no `node_rpc_seconds` metric. Byte counts are HTTP payload bytes from the original frame lengths. |
| CPU | `/proc/<pid>/stat` fields 14 + 15, sampled each second (`tools/cpu_sampler.sh`). |
| Load | Other sessions ran `cargo test` on the box. The 1-min load was 1.89 at tail run 1, **10.42** at tail run 2, 5.86 at the fan-out step, and 0.2 to 3.5 during the head step. No `sandblast-run*` session other than this run's existed. |

## Measurements against the gates

The ratio is measured / threshold or measured / projected.

| Quantity | Threshold or projection | Measured | Ratio | Note |
| --- | --- | --- | --- | --- |
| Tail, 2,000 blocks, 8 workers (§9 gate) | ≤ 2 min | **0.84 s** from process start to `historical compact index complete` (pass: 0.74 s, 1,900 blocks). **1.20 s** to gRPC serving, with the last 100 blocks through the head reconcile. | 0.007 | Run 2, patched build. Run 1 (unpatched) gave a 0.70 s pass, then exited (deviation 5). |
| Tail at sandblast-era block sizes | ≤ 2 min | 5.0 s for 2,000 blocks from 1,750,000 (supplementary client, c=8) | 0.04 | Projection for the daemon from the fan-out row below. |
| `getblock` throughput, c=8, daemon | ≥ 16.7 blocks/s (implied by the gate) | 2,560 req/s, 496 MB/s on the wire (hex), 248 MB/s of block bytes. Blocks average 97 KB. | 153 | Node CPU 1.80 CPU-s for 2,002 `getblock` (8.8 CPU-s per GB of block bytes). Daemon CPU 2.21 CPU-s. |
| `getblock` throughput, c=8, sandblast era | — | 402 req/s, 471 MB/s on the wire, 236 MB/s of block bytes, p50 13.7 ms, p95 47.1 ms. Node 9.9 CPU-s/GB. | — | Supplementary Python client (`tools/fanout.py`), heights 1,750,000 to 1,751,999, 585 KB per block. |
| Head poll, RPCs per new block (risk D) | about 200 | **207** for each of 26 blocks: 104 `getblockhash` + 103 `getblock`. Plus 1 `getblockchaininfo` each second. | 1.04 | Confirmed. |
| Head poll, bytes per new block (risk D) | about 80 MB of block bytes, sandblast era | Today: median 9.8 MB, max 26.3 MB on the wire (about half as block bytes). Sandblast era, projected from 103 re-read blocks: 60 to 81 MB of block bytes, 121 to 163 MB on the wire. | — | Confirmed. The wire carries hex, thus 2× the block bytes. |
| Head poll duration | < 30 s `attempt_timeout` | Median 0.103 s, max 0.251 s. Sandblast era, projected at the measured c=1 rate (81 MB/s of block bytes): 0.75 to 1.0 s. | 0.03 | The timeout part of risk D is refuted. |
| Node tip to `GetLatestBlock` | not set | Median 0.70 s, p90 1.05 s, max 1.14 s (26 blocks) | — | 1 s tip poll + 0.1 s poll burst + up to 0.1 s probe period. |
| Head CPU per new block | not set | Median per block interval: zakurad 0.44 CPU-s, ztreamerd 0.14 CPU-s. These include the 1 Hz `getblockchaininfo` poll and the node's own sync work. Sandblast era, projected at 7.65 CPU-s/GB (c=1): 0.46 to 0.62 node CPU-s per new block. | — | |

### Tail detail

| Run | Build | Pass | Start to complete | Start to serving | Requests (historical / head reconcile) | Wire bytes |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | master | 0.697 s | 0.840 s | exited | 2,118 in all (methods not visible at snaplen 256) | 408.8 MB |
| 2 | master + patch | 0.743 s | 0.839 s | 1.199 s | 1,900 `getblock`, 6 `getblockchaininfo`, 5 `getblockhash` / 102 `getblock`, 103 `getblockhash`, 1 `getblockchaininfo` | 368.2 MB / 40.7 MB |

The pass syncs to the node tip − 100 (3,531,361). The head reconcile reads the last 100 blocks and
the anchor sequentially, 2 RPCs per block. Both runs had `processed_blocks=1900` and
`processed_bytes=183917018`.

### Head detail

The daemon followed the head for 31.3 min (16:51:26 to 17:22:42 UTC) and published 26 new blocks.
The node was at the network tip before the daemon started, thus every block is a live block. No
`WARN` or `ERROR` line came from the daemon. Each new tip causes one poll that reads from
`old_visible_tip − 100`: the anchor, 101 blocks, one `getblockhash` above the tip that ends the read,
and a second read of the last block. Per-block rows are in `out/head-per-block.jsonl` and the poll
durations in `out/head-bursts.txt`. In the full window: 7,320 requests (1,888 `getblockchaininfo`,
2,731 `getblockhash`, 2,701 `getblock`) and 364.9 MB of responses.

### gRPC through the daemon

Latency in ms, `tools/grpc_probe.rs` on one connection, with the frozen node (tip 3,531,461):

| Call | Calls | First | Median | Max |
| --- | --- | --- | --- | --- |
| `GetLightdInfo` | 20 | 0.51 | 0.12 | 0.51 |
| `GetTreeState` 3,531,461 (tip) | 5 | 3.17 | 0.18 | 3.17 |
| `GetTreeState` 3,531,400 | 10 | 39.43 | 0.17 | 39.43 |
| `GetTreeState` 1,785,000 | 5 | 517.72 | 0.39 | 517.72 |
| `GetTreeState` 2,500,999 | 3 | 1,424.59 | 0.30 | 1,424.59 |

The first call at a height goes to `z_gettreestate` on the node. Its cost depends on the height, as
in the 2026-10-22 run. Later calls come from the `RemoteBackend` cache.

### Version note

The node was zakura v1.5.1, the pinned version, thus the `zakura_rpc` v1.5.1 response types parsed a
v1.5.1 node. The v1.6.0 system node was not used (deviation 1). All response bodies parsed. The
error codes differ from the client's model: zakura's `rpc_call_compatibility` middleware rewrites
`-32602` (InvalidParams) to `-1` (Misc) on the wire (`out/getblockhash-above-tip.txt`).

## Deviations, gaps and failures

1. **No node with JSON-RPC.** The embedded node of ztreamer.service starts no RPC server, and the
   pruned v1.6.0 state in `/var/lib/zakura` no longer exists. With the user's approval,
   ztreamer.service was stopped from **16:32:24 to 17:23:02 UTC** (gRPC serving again at 17:25:12,
   node at the network tip at 17:26:12, 3,531,501). A standalone zakurad ran on its state as user
   `ztreamer`. Nothing in the state was deleted or rewritten. During the head step zakurad added
   blocks 3,531,462 to 3,531,498, as the service would. Both zakurad stops were clean, with
   `database format is valid`. The service restarted with `NRestarts=0`.
2. **zakurad build.** zakurad came from ztreamer's own dependency on tag v1.5.1, built together
   with `ztreamerd` so that it has the same features (`indexer` included). It was not built from
   the zakura repository. A first build with `-p zakura` alone did not have the `indexer` feature.
   Before it opened the state, a run on an empty scratch state showed format `29.1.0+indexer`, the
   same as the service.
3. **Index.** The brief asks for a fresh `--index-only` build against an archive node. A full-chain
   build does not fit in the downtime window. The run used a copy of the service index, made while
   the service was stopped, and moved back 2,000 blocks with `Index::replace_deep_suffix`
   (`tools/rewind_index.rs`, a temporary example, not in `crates/`). The copy stays at
   `~/sandblast-bench/remote-rpc-run/index`.
4. **Tail definition.** The historical pass stops at the node tip − 100. The last 100 blocks go
   through the head reconcile. The table gives both times.
5. **Defect, and a patched build.** Tail run 1 used the unpatched master build. After the pass,
   `reconcile_head` failed with `getblockhash: node returned error -1: Provided index is greater than
   the current tip`, and the daemon exited. Every head poll ends with this call, thus an unpatched
   daemon cannot follow the head of a real zakura. `MockChain` sends `-32602`, thus the tests do not
   see it. Tail run 2, the gRPC step and the head step used a build with
   `tools/benchmark-only-not-found.patch`: `is_block_not_found` also accepts code `-1`. The patch
   changes only that classification. A fix is a separate PR.
6. **RPC accounting.** No `node_rpc_seconds` metric exists, thus the counts come from `tcpdump`. Run 1
   used snaplen 256, which cut the JSON body, so its methods are not known. The tail-2 capture also
   holds the gRPC probe and fan-out traffic; the tables use time windows. The pcaps (`tail.pcap`
   2.9 MB, `tail2.pcap` 72 MB, `head.pcap` 13 MB) stay in `~/sandblast-bench/remote-rpc-run/out`.
   `out/*.rpc-requests.jsonl.gz` hold the request rows.
7. **Load.** Other sessions ran `cargo test` during the tail-2 and fan-out steps (1-min load 10.42
   and 5.86). The wait for load < 2 was not met for tail run 2. Tail run 1 started at 1.89 and gave
   a similar pass time (0.70 s against 0.74 s).
8. **Supplementary fan-out.** The sandblast-era throughput is from a Python client with the same
   request shape (`getblock <h> 0`, 8 keep-alive connections), not from `ztreamerd`. At small
   blocks this client reached 1,710 req/s against 2,560 for the daemon, thus it is a lower bound.
9. **Peers.** The node ran with no peers for the tail, fan-out and gRPC steps, and with the default
   peers only for the head step. The switch needed a zakurad restart (16:48:15 to 16:51:12).
10. **Latency probe.** `GetLatestBlock` was polled each 100 ms, which adds up to 0.1 s to each
    latency. Node commit times come from zakurad's `sending committed block broadcast` log lines.

## Files

- `out/ztreamerd-tail.log`, `out/tail.pcap.summary.json`, `out/cpu-tail.txt`: tail run 1
- `out/ztreamerd-tail2.log`, `out/tail2.pcap.summary.json`, `out/tail2.rpc-requests.jsonl.gz`,
  `out/cpu-tail2.txt`, `out/metrics-tail2-*.txt`: tail run 2
- `out/rewind_index*.log`: index rollbacks before each tail run
- `out/fanout.jsonl`: supplementary `getblock` fan-out
- `out/grpc-*.jsonl`: `GetLightdInfo` and `GetTreeState`
- `out/ztreamerd-head.log`, `out/head-latest.jsonl`, `out/head.pcap.summary.json`,
  `out/head.rpc-requests.jsonl.gz`, `out/head-per-block.jsonl`, `out/head-bursts.txt`,
  `out/cpu-head.txt`, `out/metrics-head*.txt`: head step
- `out/getblockhash-above-tip.txt`: the node's answer that the daemon misreads
- `out/loadavg-*.txt`: load before each step
- `node/`: zakurad configs and logs, node info, and the service stop and restart records
- `tools/`: `run-daemon.sh`, `cpu_sampler.sh`, `scrape.sh`, `pcap_rpc.py`, `head_analysis.py`,
  `fanout.py`, `rewind_index.rs` and `grpc_probe.rs` (copy into `crates/indexer/examples/` and
  `crates/service/examples/` to build), and `benchmark-only-not-found.patch`
