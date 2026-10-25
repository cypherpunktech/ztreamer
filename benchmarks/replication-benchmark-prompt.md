# Replication benchmark run

Paste everything below this line into Claude Code on a server that has the ztreamer repository, a
synced archive zakura node, and a ztreamer index. The goal is to replace the projected numbers in
the Index Replication Engineering Plan with measured ones before we commit to the design.

---

You are running the acceptance measurements for ztreamer's index replication plan. Measure, record,
and report. Do not change the plan, do not tune the examples beyond their documented flags, and do
not work around a failure silently: a step that fails is a result, write it down and continue.

## What the plan assumes, so you know what to confirm or refute

| Quantity | Projected | Source of projection |
| --- | --- | --- |
| Sapling tree append, sequential | 48.8 µs/leaf | `tree_cost` on a 4-core container |
| Orchard tree append, sequential | 16.6 µs/leaf | same |
| Sapling / Orchard batched (`append_batch`, 512 per batch, 4 cores) | 14.7 / 4.8 µs/leaf | same |
| Mainnet commitments | ~90 M Sapling, ~30 M Orchard | upper bound from a 17.26 GB index; unmeasured |
| Full-chain frontier rebuild | ~80 core-minutes | counts × per-leaf cost |
| Slowest single range (1,000 blocks) | ~100 s sequential, ~27 s batched | peak sandblasting density |
| Attestation RPC `z_gettreestate` | ms-class, cacheable | Zaino proxied-call latencies |
| GetBlockRange over loopback, 8 streams | ~1.1 GiB/s, ~1 CPU-s/GiB client, 1.5 to 2 CPU-s/GiB server | `rangebench` loopback model, not a real zakura |
| Root comparison method | rebuilt roots equal `z_gettreestate` final roots at every range end | untested on real data |

A measured value more than 25 % away from a projection is a finding. Say so explicitly in the report.

## Preconditions, check and record before measuring

1. Repository on branch `claude/ztreamer-rpc-mode-8st9s2` or a later branch that contains
   `crates/indexer/examples/{index_stats,verify_cost,tree_cost}.rs`, `benchmarks/rangebench`,
   and `scripts/compare-tree-roots.py`. Build once: `cargo build --release --examples -p ztreamer-indexer && cargo build --release -p rangebench`.
2. A zakura node, archive mode (not pruned), synced to the tip, with in its config:
   `[rpc] listen_addr = "127.0.0.1:8232"` and cookie auth left at its default, and
   `indexer_listen_addr = "127.0.0.1:8230"`. The node must be built with `--features indexer`;
   if it is not, say so, run everything except step 7, and note it as a gap.
3. A ztreamer index built by embedded mode, fully synced (the `historical compact index complete`
   log line) and then stopped so the LMDB is quiescent. Record its path, its `--index-map-size`,
   and the zakura tip height it was built against.
4. Record the machine: CPU model, physical cores, threads, RAM, the disk model and filesystem under
   the index and under zakura's state, and whether the index and the node share a disk. Write this
   to `benchmarks/<YYYY-MM-DD>-replication/hardware.json` in the same shape as
   `site/benchmarks/2026-09-11/hardware.json`.
5. Make sure nothing else heavy runs on the box. Note the load average before you start.

Put all outputs under `benchmarks/<YYYY-MM-DD>-replication/`. Keep raw CSVs. Commit at the end.

## Steps

### 1. Chain size and range distribution

```
cargo run --release -p ztreamer-indexer --example index_stats -- <index-dir> --map-size-gib <N> > out/ranges.csv 2> out/index_stats.log
```

Record from the log: `tree_sizes` at the durable tip (the real Sapling, Orchard, Ironwood counts),
the number of sealed ranges, total `encoded_bytes`, and the p50 / p95 / max commitments per range
with the start height of the max range. From the CSV, compute total `read_ms` and derive LMDB read
throughput as `encoded_bytes / read_ms` in MB/s; that is the ceiling for range export from this disk.

### 2. Synthetic tree cost on this CPU

```
cargo run --release -p ztreamer-indexer --example tree_cost > out/tree_cost.txt
```

Record all eight numbers (Sapling and Orchard, sequential and three batch sizes). Note the core
count the batched figures used.

### 3. Real per-range verification cost

Using the max range and a p50 range from step 1:

```
for r in <max_start> <p50_start>; do
  cargo run --release -p ztreamer-indexer --example verify_cost -- <index-dir> --range $r          2>> out/verify_range.log > /dev/null
  cargo run --release -p ztreamer-indexer --example verify_cost -- <index-dir> --range $r --batch  2>> out/verify_range.log > /dev/null
done
```

Record, per range and per mode: `sapling_s`, `orchard_s`, the derived µs/leaf, and `read_ms`.
The max-range sequential number is the plan's "slowest single range" tail latency.

### 4. Full-chain rebuild and root comparison, the correctness check

This is the most important step. It runs the trees from genesis, single-threaded, over the whole
index, and prints the Sapling and Orchard roots at every range end. Expect it to take on the order
of the projected 80 core-minutes. Run it under `nohup`.

```
nohup cargo run --release -p ztreamer-indexer --example verify_cost -- <index-dir> --all > out/roots.csv 2> out/verify_all.log &
```

When it finishes, record from the log: total `wall_s`, `sapling_s`, `orchard_s`, the per-leaf
figures, and the per-range p50 / p95 / max. Then compare every range end with the node:

```
scripts/compare-tree-roots.py out/roots.csv http://127.0.0.1:8232 --cookie <zakura cookie path> --concurrency 8 | tee out/compare.json
```

Record: `mismatching_ranges` (must be 0), whether Sapling and Orchard reported `match` or
`match-reversed` (that tells us the node's hex byte order), whether `ironwood` is `absent` on this
node, `rpc_p50_ms`, `rpc_p95_ms`, and `total_s` at concurrency 8. If any range mismatches, stop and
report the first mismatching height, the two roots, and the zakura version; do not continue to
step 5 until that is understood.

### 5. Parallel scaling of verification

Verification runs one range per worker in the plan. Confirm the CPU scales. Pick 2 × (physical cores)
range starts spread across the chain from `ranges.csv`, and run them with as many workers as cores:

```
cut -d, -f1 out/ranges.csv | tail -n +2 | awk 'NR % <stride> == 0' | head -n <2*cores> > out/parallel_ranges.txt
time xargs -P <cores> -I{} sh -c 'cargo run --release -p ztreamer-indexer --example verify_cost -- <index-dir> --range {} 2>> out/parallel.log > /dev/null' < out/parallel_ranges.txt
```

Record the wall time, the sum of per-range tree seconds from the log, and the ratio sum / wall as
the achieved parallelism. Note peak RSS of one process (`/usr/bin/time -v` on a single run) since
each opens the LMDB map. Derive the projected full-chain verification wall time for this box as
`step-4 (sapling_s + orchard_s) / achieved parallelism`.

### 6. Attestation RPC behavior

With the cookie, time 50 calls each of `getblockhash <h>`, `z_gettreestate <h>`, and
`z_getsubtreesbyindex sapling <i> 1` at heights spread across the chain, concurrency 1 and 8;
`compare-tree-roots.py` already covers `z_gettreestate` at concurrency 8, so reuse that and add
the other two with a short Python loop in the same style (urllib, Basic auth from the cookie).
Then repeat one call of each method without the cookie against `listen_addr` with
`enable_cookie_auth = false` set temporarily in a copy of the config, if you can restart the node;
if you cannot, skip and say so. Record p50 / p95 per method and which methods are refused
unauthenticated. Note whether `z_gettreestate` output contains an `ironwood` object.

### 7. GetBlockRange against the real zakura

Only if the node has the indexer feature and `indexer_listen_addr` on loopback. Record zakura's
PID, then:

```
target/release/rangebench client --url http://127.0.0.1:8230 --streams 8 --connections 2 --segment 256 --start 0       --blocks 200000 | tee -a out/rangebench.txt
target/release/rangebench client --url http://127.0.0.1:8230 --streams 8 --connections 2 --segment 256 --start 1750000 --blocks 20000  | tee -a out/rangebench.txt
target/release/rangebench client --url http://127.0.0.1:8230 --streams 1 --connections 1 --segment 256 --start 1750000 --blocks 5000   | tee -a out/rangebench.txt
```

Before and after each run read fields 14 and 15 of `/proc/<zakura pid>/stat` (utime, stime, in
clock ticks) and record the delta divided by 100 as server CPU seconds, then per GiB transferred.
Record MiB/s, blocks/s, client CPU per GiB, server CPU per GiB for each run. The 1,750,000 start
is the sandblasting era; expect large blocks and fewer blocks per second. If a run ends early with
fewer blocks than requested, record the count: that is the 60-second stall timeout or a missing body.

### 8. Report

Write `benchmarks/<date>-replication/README.md` with:

- The hardware block and the zakura and ztreamer versions and commits.
- One table with every quantity from the "What the plan assumes" table: projected, measured, ratio,
  and a one-line note. Flag every ratio outside 0.75 to 1.33.
- The correctness result of step 4 in one sentence: ranges checked, mismatches, byte order,
  Ironwood presence.
- The derived numbers the plan quotes: full-chain verification CPU-minutes from measured counts and
  per-leaf costs, projected verification wall time on this box from step 5, slowest-range latency
  from step 3, export ceiling from step 1, and remote backfill throughput from step 7 if run.
- Anything that failed, with the exact error.

Commit the directory with the raw CSVs and logs on the current branch and push. In the chat reply,
give the comparison table and the correctness sentence first, then the list of flagged deviations,
then anything that could not be run. Do not edit the engineering plan document; the numbers get
folded in after review.
