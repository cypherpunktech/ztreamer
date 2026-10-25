# Replication acceptance measurements, 2026-10-22

These are the measurements from `benchmarks/replication-benchmark-prompt.md`, run on torvus on
2026-10-22 between 04:49 and 06:25 UTC. Raw outputs are in `out/`, node configs and logs in `node/`,
and the machine is described in `hardware.json`.

## Setup

| | |
| --- | --- |
| Host | torvus-ovh: AMD Ryzen 9 5900X, 12 physical cores, 24 threads, 64 MiB L3, 62.7 GiB RAM, Ubuntu 26.04.1, Linux 7.0.0-38 |
| Disk | 2 × Micron MTFDKCC960TGP NVMe in mdadm RAID1 (`/dev/md3`), ext4. The index and zakura's state share this disk. |
| ztreamer | `b1d408ff5ba1604961562162e8c5e18a538675ba`, branch `claude/ztreamer-rpc-mode-8st9s2` (fetched from the `Evalir/ztreamer` fork; it is not on `cypherpunktech/ztreamer`). rustc 1.99.0. |
| zakura node | `zakurad 1.5.1+gaf944f5194ef`, built from tag v1.5.1 with `--features indexer`. Archive mode. It is the same zakura revision ztreamerd embeds. |
| Index | `/home/evalir/sandblast-bench/replication/ztreamer-index`, `--index-map-size 68719476736` (64 GiB), 18.6 GB `data.mdb`. Built by ztreamerd in embedded `--index-only` mode in 5 min 01 s. The log line `historical compact index complete height=3525315` was seen, then the process exited. The embedded zakura tip was 3,526,314. The index has durable tip 3,525,315 and is sealed through 3,524,999. |
| Node tip | 3,526,315, frozen. The node runs with no peers (see the deviations section). |
| RPC | `listen_addr = 127.0.0.1:18232` with the default cookie (`~/.cache/zakura/.cookie`), and `indexer_listen_addr = 127.0.0.1:8230`. |
| Load | 1-min load was 9.58 at the first check, left over from this run's own builds. Every timed CPU step waited for load < 2 except the full-chain rebuild, which started at a 1-min load of 2.31. That run is single-threaded on a 24-thread box. |

## Projections against measurements

The ratio is measured / projected. ⚑ marks a ratio outside 0.75 to 1.33, which is more than 25 % away from the projection.

| Quantity | Projected | Measured | Ratio | Note |
| --- | --- | --- | --- | --- |
| Sapling append, sequential | 48.8 µs/leaf | 36.7 µs/leaf (synthetic `tree_cost`), 36.7 µs/leaf (real, full chain) | 0.75 | Just inside the band. The 5900X core is faster than the 4-core container's. |
| Orchard append, sequential | 16.6 µs/leaf | 12.1 µs/leaf (synthetic), 11.7 µs/leaf (real, full chain) | 0.73 / 0.70 ⚑ | Faster than projected. |
| Sapling batched, `append_batch(512)` | 14.7 µs/leaf on 4 cores | 4.4 µs/leaf on 24 threads (64: 10.6, 4096: 3.1) | 0.30 ⚑ | Different core count. Per core this is 4.4 × 12 = 53 core-µs, against 14.7 × 4 = 59 projected. Real per-block batches on the max range gave 3.6 µs/leaf. |
| Orchard batched, `append_batch(512)` | 4.8 µs/leaf on 4 cores | 1.6 µs/leaf on 24 threads (64: 4.0, 4096: 1.1) | 0.33 ⚑ | Same caveat. Per-block batches on the max range gave 3.1 µs/leaf (only 28,852 leaves in small batches). |
| Mainnet Sapling commitments | ~90 M | 73,988,057 | 0.82 | `tree_sizes` at durable tip 3,525,315 |
| Mainnet Orchard commitments | ~30 M | 50,477,534 | 1.68 ⚑ | The projected upper bound is wrong by 20 M. Orchard is the larger correction. |
| Mainnet Ironwood commitments | not projected | 821,593 | — | Ironwood activity starts in the range ending 3,428,999. |
| Full-chain frontier rebuild | ~80 core-min | 55.2 core-min of tree time (Sapling 2,718.0 s + Orchard 591.8 s), 55.5 min wall | 0.69 ⚑ | Fewer Sapling leaves and faster per-leaf cost outweigh the extra Orchard leaves. Ironwood is not included (see correctness). |
| Slowest single range, sequential | ~100 s | 38.4 s (range 1,785,000: 1,030,681 Sapling + 28,852 Orchard) | 0.38 ⚑ | The densest real range has about 1.06 M commitments, not the ~2 M the projection implies. |
| Slowest single range, batched | ~27 s | 3.84 s on 24 threads | 0.14 ⚑ | Core count differs, as above. |
| `z_gettreestate` latency | ms-class, cacheable | p50 329 ms, p95 1,056 ms (c=1, 50 heights). p50 236 ms, p95 1,147 ms (c=8, all 3,525 range ends). | ≈100× ⚑ | Refuted. Latency depends on height: < 1.5 ms at ≥ 3,428,999, 37 ms at 419,999, 770 ms at 1,687,999, 2.6 s at 2,500,999. Five repeat calls at one height cost the same each time, so the node does not cache. `getblockhash` and `z_getsubtreesbyindex` are sub-ms at c=1. |
| GetBlockRange throughput, 8 streams | ~1.1 GiB/s | 0.97 GiB/s (from genesis, 200k blocks). 1.76 GiB/s (from 1,750,000, 20k blocks). | 0.88 / 1.60 ⚑ | Faster than projected in the sandblast era. One stream reached 1.20 GiB/s. |
| GetBlockRange client CPU | ~1 CPU-s/GiB | 0.80 (genesis), 1.36 (sandblast), 0.39 (1 stream) | 0.80 / 1.36 ⚑ / 0.39 ⚑ | |
| GetBlockRange server CPU | 1.5 to 2 CPU-s/GiB | 3.42 (genesis), 2.96 (sandblast), 0.96 (1 stream) | 1.95 ⚑ / 1.69 ⚑ / 0.55 ⚑ (against the 1.75 midpoint) | With 8 streams, the real zakura costs 1.5× to 1.7× the top of the loopback model's range. One stream is much cheaper per GiB. |
| Root comparison | rebuilt roots equal `z_gettreestate` at every range end | 3,525 range ends checked, 0 mismatches | — | Confirmed for Sapling and Orchard. Ironwood is untested. |

## Correctness (step 4)

All 3,525 sealed range ends, through height 3,524,999, were checked against `z_gettreestate`, with 0 mismatching ranges:

- Sapling matched in all 3,106 ranges where the node has a Sapling root, but only with the hex byte-reversed (`match-reversed`). The node prints Sapling roots in reversed byte order.
- Orchard matched as printed (`match`) in all 1,838 ranges from 1,687,999 onward.
- Ironwood is present on this node, not absent. Every response has an `ironwood` object, and the node returns an Ironwood root for the 97 ranges from 3,428,999 onward. `verify_cost` does not rebuild Ironwood, so those roots were not checked.

`compare-tree-roots.py` prints `"sapling": "absent", "orchard": "absent", "ironwood": "absent"` for the full run (`out/compare.json`). That is because it reports the pool status of row 0 only, and row 0 (height 999) is before Sapling. The per-row tally is `out/pool_status.json`, made by `pool_status.py`. A second run of the script over the post-NU5 rows only (`out/compare-post-nu5.json`) shows `match-reversed` / `match` directly.

## Derived numbers

| | |
| --- | --- |
| Full-chain verification CPU, from counts × per-leaf | 73,988,057 × 36.7 µs + 50,477,534 × 11.7 µs = 3,306 s = **55.1 CPU-min**. The step-4 run measured 55.2. Ironwood is excluded; at Orchard's per-leaf cost it would add about 10 s. |
| Achieved parallelism, as the runbook specifies (step 5) | 24 ranges at stride 146 on 12 workers: wall 21.85 s, per-range tree time summed 24.3 s, ratio **1.11**. One range (1,751,000, 21.6 s) is the whole wall time, so this measures load imbalance in the sample, not CPU scaling. |
| Achieved parallelism, balanced (supplementary) | 24 copies of the p95 range (1,914,000, 247,816 commitments, 2.93 s alone) on 12 workers: wall 6.80 s, sum 76.7 s, ratio **11.28**. Per-range time rose 9 % under load. |
| Projected full-chain verification wall time on this box | Runbook formula: 3,309.8 s / 1.11 = **49.7 min**. With the balanced figure: 3,309.8 s / 11.28 = **4.9 min**, bounded below by the 38.4 s slowest range. |
| Peak RSS | `verify_cost --range 1785000`: 740 MB. `--all` and `index_stats`: about 18 GB, which is the LMDB map paged in (file-backed). |
| Slowest-range latency | **38.4 s** sequential, **3.84 s** batched (24 threads), range 1,785,000 |
| p50 range | 1,145,000 has 903 Sapling leaves and 0 Orchard. Tree time is 69 ms sequential and 80 ms batched, mostly fixed `root()` cost: about 19 ms for an empty Orchard tree. The µs/leaf in that log is meaningless for this reason. |
| Export ceiling (step 1) | 18,111,549,825 encoded bytes / 46.69 s summed `read_ms` = **388 MB/s (370 MiB/s)**, read with a cold page cache (`drop_caches` first). `read_ms` in `index_stats` also includes decoding and re-encoding each record, so this is a lower bound on raw LMDB throughput. The heaviest range (1,786,000, 121 MB) took 424 ms. |
| Range distribution (step 1) | 3,525 sealed ranges. Commitments per range: p50 903 (start 1,145,000), p95 247,816 (start 1,914,000), max 1,059,533 (start 1,785,000). |
| Remote backfill (step 7) | **0.97 GiB/s** early chain and **1.76 GiB/s** sandblast era with 8 streams on 2 connections. **1.20 GiB/s** on one stream. Every run returned all requested blocks; none hit the stall timeout. |

### Step 7 detail

The zakurad PID was 3427151. Server CPU is the change in `/proc/<pid>/stat` fields 14+15, divided by 100.

| Run | Blocks | Bytes | Wall | MiB/s | blocks/s | Client CPU/GiB | Server CPU | Server CPU/GiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 8 streams × 2 conns, start 0 | 200,000 | 6.21 GB (5.78 GiB) | 5.95 s | 995.9 | 33,628 | 0.80 | 19.78 s | 3.42 |
| 8 streams × 2 conns, start 1,750,000 | 20,000 | 15.81 GB (14.72 GiB) | 8.37 s | 1,800.3 | 2,389 | 1.36 | 43.56 s | 2.96 |
| 1 stream × 1 conn, start 1,750,000 | 5,000 | 3.64 GB (3.39 GiB) | 2.81 s | 1,232.6 | 1,777 | 0.39 | 3.25 s | 0.96 |

### Step 6 detail

RPC latency in ms, 50 calls per method at heights (or Sapling subtree indices 0 to 1,128) spread across the chain, with the cookie:

| Method | c=1 p50 | c=1 p95 | c=8 p50 | c=8 p95 |
| --- | --- | --- | --- | --- |
| `getblockhash` | 0.39 | 0.45 | 2.07 | 8.29 |
| `z_gettreestate` | 328.7 | 1,056.3 | 380.5 | 1,334.2 |
| `z_getsubtreesbyindex sapling i 1` | 0.36 | 0.81 | 1.95 | 2.72 |

`compare-tree-roots.py` at c=8 over all 3,525 range ends gave p50 235.5 ms, p95 1,146.8 ms, and 151.4 s total.

For the unauthenticated test, the node was restarted from a copy of its config with `enable_cookie_auth = false` (`node/zakura-node-nocookie.toml`). `getblockhash`, `z_gettreestate` and `z_getsubtreesbyindex` all answered without credentials, so none of the three is refused. The listener was in restricted mode: `stop` returned `-32601 Method not found`. `getpeerinfo` and `getinfo` were still served. The node was then restarted with the cookie for step 7.

## Deviations, gaps and failures

No step failed or exited non-zero. The deviations from the runbook:

1. **Branch location.** `claude/ztreamer-rpc-mode-8st9s2` exists only on `github.com/Evalir/ztreamer`. It was checked out into a worktree at `~/sandblast-bench/ztreamer-repl`.
2. **No ready node or index.** The box's system zakura (`/opt/zakura`, 1.6.0) runs in pruned mode (`tx_retention = 10000`), has no `indexer_listen_addr`, and owns `127.0.0.1:8232`. Instead:
   - A standalone zakurad v1.5.1 with `--features indexer` ran on the sandblast-bench archive snapshot (`~/sandblast-bench/zakura-cache`), with JSON-RPC on **18232** instead of 8232. `indexer_listen_addr` was 8230 as specified.
   - The index was built fresh, as the "built by embedded mode" precondition requires.
3. **Node not at the live tip.** The node ran with no peers (`initial_mainnet_peers = []`, peer cache off), so its tip stayed at 3,526,315 and the shared snapshot did not change during measurement. All measured quantities are historical and sit below the sealed height 3,524,999.
4. **The shared snapshot advanced.** The index build used the same `zakura-bench.toml` (with peers) as earlier sandblast runs. Its embedded zakura synced `~/sandblast-bench/zakura-cache` from 3,524,946 to 3,526,314 before indexing. Sandblast benchmark runs from before today started at 3,524,943 to 3,524,946.
5. **Binaries ran directly.** Every step ran `target/release/examples/<name>` and `target/release/rangebench` directly instead of through `cargo run --release`. These are the same binaries, built once in precondition 1. This keeps cargo's lock and startup out of the 12-way parallel timing.
6. **Step 5's prescribed sample.** The sample includes one sandblast range that takes the whole wall time. The prescribed ratio (1.11) is reported as is, and the balanced supplementary run (11.28) is reported next to it.
7. **`compare-tree-roots.py` summary.** It reports row 0's pool status, which is misleading for a CSV that starts at genesis. The per-row tally is `pool_status.py` / `out/pool_status.json`.
8. **Ironwood is not verified.** `verify_cost` builds only Sapling and Orchard trees. The node serves Ironwood roots from 3,428,999 onward, but those 97 ranges were not compared.
9. **Cold versus warm reads.** Step 1 ran after `vm.drop_caches=3`. Steps 3 to 7 ran with whatever page cache the earlier steps left.
10. **Overlap during step 4.** A 43 s, concurrency-1 sanity comparison of 480 early ranges (`out/compare-early-sanity.json`) ran during the full-chain rebuild.

## Files

- `out/ranges.csv`, `out/index_stats.log`: step 1
- `out/tree_cost.txt`: step 2
- `out/verify_range*.csv`, `out/verify_range.log`: step 3
- `out/roots.csv`, `out/verify_all.log`, `out/verify_all.time`, `out/compare*.json`, `out/pool_status.json`: step 4
- `out/parallel*`, `out/supp_*`, `out/single_rss.txt`: step 5
- `out/rpc_*.json`, `out/rpc_unauth_methods.txt`, `out/z_gettreestate_*`: step 6, with `rpc_timing.py`
- `out/rangebench.txt`, `out/rangebench_server_cpu.txt`: step 7
- `node/`: zakurad configs and logs, and the index-build log
