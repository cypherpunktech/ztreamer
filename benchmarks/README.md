# Serving benchmarks

## CI benchmark checks

The `Bench` check builds and executes the parser, codec, serving, gRPC, and storage
benchmarks with `--test`. It checks that the workloads and their assertions pass;
it does not collect performance samples. Known documentation/site-only changes
skip execution and leave an explanation in the job summary.

Add the `benchmark` label to a pull request to request the separate **Benchmarks**
workflow. It compares the PR merge revision against its base on the same runner.
New commits rerun the comparison while the label is present; remove and re-add
the label to request another comparison of the same commit. Unrelated label
changes do not start comparisons. New eligible runs cancel older comparisons
for the same PR. Removing the label prevents future comparisons, but does not
cancel a comparison already running.

Timing results are advisory. Empty/unrecognized output and report-generation
failures are reported as unavailable or unclassified, rather than unchanged.
A failed report or comment does not fail the comparison job; benchmark execution
errors still do. Raw results remain in the `benchmark-results` artifact for 30
days. Same-repository PRs also get a sticky comment; fork PRs use the job summary
and artifact and remain subject to the repository's normal Actions approval.

Branch protection should require `Bench`; `Benchmark comparison` is optional.

`scripts/benchmark-serving.sh` runs a native Rust/Tonic client against a running
gRPC server. It measures RPC latency, range scaling, sustained concurrent
serving, and sequential compact-block wallet sync. It writes data, not plots.
It does not build or start the server, modify its index, or switch revisions.
The existing `benchmark-grpc.sh` remains available for a quick single-range load
test, including its local synthetic fixture.

## Compare two versions

Build and run each server version separately against equivalent indexed data.
Use **the same checkout of this benchmark client** for both runs. The release
does not need to contain the benchmark code. Choose a fixed upper height both
servers can serve and use the same fixtures, seed, settings, and machine:

```bash
bash scripts/benchmark-serving.sh \
  --endpoint http://127.0.0.1:9067 --label v0.0.1 \
  --start 419200 --end 3450000 --fixtures /path/to/fixtures.json

# After starting the HEAD server:
bash scripts/benchmark-serving.sh \
  --endpoint http://127.0.0.1:9067 --label 86bf4b0 \
  --start 419200 --end 3450000 --fixtures /path/to/fixtures.json
```

The labels are supplied by the caller, not verified commit identities. Server
version/build fields returned by `GetLightdInfo` are recorded separately from
the benchmark **client's** commit, lockfile, toolchain, and hardware provenance.
Keep server build/configuration records with each run. Run versions sequentially
to avoid competing load; alternate their order for repeated comparisons.

Without explicit bounds, the suite uses the server's Sapling activation height
and the tip observed once during preflight. Explicit bounds are needed for
matching workloads across runs as the chain advances. Live-tip, mempool, balance,
and UTXO responses can still change; fixing historical heights does not freeze
the server. This is a warm/connected benchmark: preflight and warmups issue RPCs,
and no OS or server caches are cleared.

## Workloads and defaults

Each suite runs once by default (`--repeats 1`). Additional repeats are opt-in
and retain separate rows.
All RPCs have a whole-response client timeout (`--timeout-seconds 120`), including
stream draining. Setup and warmups are excluded from scenario timings.

| Suite | Default workload | Controls |
| --- | --- | --- |
| `latency` | 100 samples per RPC, one client | `--requests`, `--fixtures` |
| `ranges` | 1,000 samples each at 100, 1,000, 10,000, 100,000 blocks | `--range-requests`, `--range-sizes` |
| `concurrency` | 1, 8, 32, 128 clients; 10 seconds each; 10,000 blocks/request | `--clients`, `--seconds`, `--concurrency-blocks` |
| `sync` | Entire selected interval, in 10,000-block chunks | `--start`, `--end`, `--sync-chunk` |

Select subsets with `--suites latency,ranges`. Lists use commas. All selected
range sizes must fit the chosen interval; invalid settings fail preflight rather
than silently changing the workload. The range-sweep sample count is never
reduced automatically for larger ranges. Large ranges and address histories can
make full runs take substantial time; sample counts are explicit in the output.
The default range sweep collects 1,000 samples per size. Optional repeats reuse
the same deterministic height sequence; they are separate runs of the same
workload, not additional distinct random ranges.
Use `--range-request-counts 100000=100` to collect fewer samples for the largest
size while retaining 1,000 for the smaller sizes. Overrides must name distinct
configured sizes; actual sample counts remain explicit in every summary.

Latency covers `GetLightdInfo`, `GetLatestBlock`, `GetBlock`, `GetTreeState`,
`GetLatestTreeState`, `GetSubtreeRoots` (Sapling and Orchard, first ten roots),
and `GetMempoolTx`. Fixtures additionally enable `GetTransaction`,
`GetTaddressTxids`, `GetTaddressTransactions`, `GetTaddressBalance`, and
`GetAddressUtxos`. Address history covers the entire selected interval, and UTXO
queries use its starting height. Fixture-dependent omissions are recorded in
metadata. This is not an exhaustive protocol-conformance suite.

Fixtures are a JSON object with `addresses` (labels mapped to real addresses)
and `txids` (ordinary explorer/display-order 64-character hex transaction IDs).
For example, the following shape needs real values substituted before running:

```json
{
  "addresses": {"quiet": "REAL_TRANSPARENT_ADDRESS", "busy": "REAL_TRANSPARENT_ADDRESS"},
  "txids": ["REAL_64_CHARACTER_TRANSACTION_ID"]
}
```

Omit either collection when it is not needed. Fixture contents are archived in
metadata. Transaction IDs are converted to wire byte order by the client.

Heights use a fixed SplitMix64 algorithm and seed (`--seed 20260829`). Identical
bounds and settings produce identical request sequences across versions and
repeats. Concurrent clients each have a deterministic sequence; faster servers
complete more requests during the duration. Connections are established and
warmed before a barrier starts all clients. Each client has one outstanding
request, and an in-flight request may finish after the requested duration.
Reported throughput uses actual elapsed time including that completion.

There is one untimed warmup per latency/range scenario and per concurrent client
(`--warmup 1`). Warmup failures are saved and mark the scenario failed without
collecting misleading measured samples. `--warmup 0` disables these requests but
still uses established connections. Sync has no separate warmup; it records each
chunk and stops on the first failed/incomplete/out-of-order range. It measures
compact-block download, **not** wallet trial decryption, scanning, or storage.

## Output

Every wrapper run creates a fresh directory under `benchmark-runs/serving/`
(`RUN_ROOT` overrides the parent). Its `data/` directory contains:

| File | Contents |
| --- | --- |
| `metadata.json` | Fixed bounds and upper-block hash, observed tip/hash, server identity, fixtures, seed, exact arguments, measurement definitions |
| `samples.csv` | One request/chunk per row, including warmups and errors: labels, repeat/client/sample IDs, heights, status, first-message and completion microseconds, message/block counts, protobuf bytes |
| `summary.csv` | One scenario/repeat/concurrency level per row: numeric range sizes, attempts/successes/errors, warmup counts, elapsed seconds, successful bytes/blocks, throughput, p50/p95/p99/max first-message and completion latency |
| `results.json` | The same summary rows, overall status, and any fatal error |

Logs, exit status, command, and `client-provenance/` sit alongside `data/`.
CSV headers contain units and every row carries the version label. Files from
two runs can be concatenated by header for plotting; no plotting dependencies
are installed and no charts or version-difference calculations are generated.
`samples.csv` retains the observations needed for other percentiles or analyses.
Memory use scales with samples in the current scenario; completed scenarios are
flushed to disk. Keep durations bounded for very high-throughput workloads.

Percentiles use nearest-rank successful samples only. An empty successful stream
has a completion latency but no first-message latency. Failure rows retain any
partial message/byte counts; summary throughput is **blank/null for failed
scenarios**, including incomplete syncs. Do not interpret that as zero speed.
Latency sample counts, especially for ranges, matter when interpreting tails.
Payload byte rates exclude HTTP/2, TLS, and other transport overhead.

The process exits nonzero if any scenario fails, including unsupported RPCs, and
continues through independent scenarios when possible. A failed connection or
preflight aborts the run. Completed scenario CSVs survive a later fatal error;
hard interruption may lose the currently buffered scenario. No server resource
sampling or cold-cache claims are included. Compare client CPU utilization
separately if results suggest the load generator is the bottleneck.

For a short run against a small indexed interval:

```bash
bash scripts/benchmark-serving.sh \
  --endpoint http://127.0.0.1:9067 --label smoke \
  --start 419200 --end 420199 --suites ranges,concurrency,sync \
  --repeats 1 --range-sizes 10,100 --range-requests 2 \
  --clients 1,2 --concurrency-blocks 100 --seconds 1 --sync-chunk 100
```

## Snapshot stage times

`just snapshot-stages RUN_DIR [--band BLOCKS]` (`scripts/benchmark-stages.py`) shows, for each band of heights of a `just snapshot` run, the wall time, the blocks per second, the I/O, the CPU time and the time of each pipeline stage.
