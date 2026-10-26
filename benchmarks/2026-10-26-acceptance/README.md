# Replication acceptance run (plan 7.4), 2026-10-26

These are the plan's section 9 gates, measured on torvus on 2026-10-26 between 19:42 and 21:10 UTC
on master `1c6c90b` (#153). Raw outputs are in `out/`, node configs and records in `node/`, and the
scripts and benchmark-only patches in `tools/`. The machine is the one in
`../2026-10-22-replication/hardware.json`.

**Result.** Eight gates are met. The serving gate is met at 10,000-block ranges and not at the other
range sizes. The import gate is met over HTTP and not met over a peer. An empty replica serves a
verified index 7.2 min after start; Zaino needed 3 h 43 min to serve from empty (see "Time to a
serving instance"). Two defects came up; both stop a real import:

1. The first mainnet epoch is rejected (`AttestedRoots` at height 9,999): the filler writes an
   inactive pool as `000000`, the verifier expects empty bytes, and zakura gives no root.
2. An import from one peer fails: the source allows 4 range streams, the replica opens 2 per
   worker (44 here, 8 with 4 workers), and a refused stream ends the import. #175 fixes this; with
   it, a live peer import completed in 412 s.

The HTTP import ran with a benchmark-only patch for defect 1 and a throwaway signing key.

## Setup

| | |
| --- | --- |
| Host | torvus-ovh: AMD Ryzen 9 5900X, 12 cores, 24 threads, 62.7 GiB RAM, Linux 7.0.0-38. 2 × Micron NVMe in mdadm RAID1, ext4. |
| ztreamer | master `1c6c90b` with two benchmark-only patches (`tools/`): `SNAPSHOT_PUBKEY` set to a throwaway test key, and `Trees::roots` gives no root for an empty tree (defect 1). Release build, rustc 1.99.0. |
| Node state | A hard-linked snapshot of the ztreamer.service state (zakura v1.5.1, archive, format `29.1.0+indexer`), taken with the service stopped from 19:42:37 to 19:43:21 UTC (`tools/snapshot-window.sh`). SST files are hard links; every other RocksDB file is a real copy. Tip 3,531,614, frozen: no peers. |
| Source S | master `ztreamerd` in embedded mode on the snapshot, as user `ztreamer`, with zakura JSON-RPC on 127.0.0.1:28232 (cookie) and CompactTxStreamer + Replicator gRPC on 127.0.0.1:29067. Index: a copy of the service index (durable tip 3,531,604, sealed through 3,530,999). The filler wrote all 3,531 `range_meta` rows in about 9 min (19:49:49 to 19:58:37). |
| Snapshot | `export-snapshot` of S's index with the test key: 3,531 ranges, 18,161,010,220 bytes of range blobs, served by `python3 -m http.server` on 127.0.0.1:28080. |
| Replicas | master `ztreamerd --remote-rpc http://127.0.0.1:28232 --remote-rpc-cookie … --replicate-from <source>` on an empty index. Import parallelism is the built-in cores − 2 = 22. |

## Measurements against the gates

The ratio is measured / threshold.

| Gate (§9) | Threshold | Measured | Ratio | Result |
| --- | --- | --- | --- | --- |
| `verify_epoch`, densest epoch, one core | ≤ 45 s | 38.27 s (criterion, 10 samples, `RAYON_NUM_THREADS=1`) | 0.85 | met |
| `verify_epoch`, densest epoch, batched, 24 threads | ≤ 5 s | 4.25 s | 0.85 | met |
| Attestation, all epochs, c=8, JSON-RPC | ≤ 2 min | 34.7 s: 3,531 `getblockhash` (p50 0.88 ms) and 405 `z_gettreestate` (p50 396 ms, p95 2.28 s, max 7.68 s), 0 errors | 0.29 | met |
| Tail, 2,000 blocks, 8 concurrent `getblock` | ≤ 2 min | 0.83 s to `historical compact index complete`, 0.89 s to serving | 0.007 | met |
| Import end to end, 12 cores, loopback node | ≤ 8 min to fully verified | **HTTP:** 7.23 min (433.6 s) from start to `import complete`, `verified_through` 3,530,999; serving 0.2 s later. **Peer:** fails on master (defect 2); 412 s with #175 (see defect 2). | 0.90 | met over HTTP; over a peer only with #175 |
| Export, raw blob reads, cold cache | ≥ 350 MB/s | 494 MB/s: 18.16 GB in 36.74 s after `drop_caches`, including BLAKE3 and the writes | 1.41 | met |
| Embedded pipeline with `range_meta` filler | within 2 % of 119 s | 107.3 s and 111.1 s (mean 109.2 s): the `historical compact index complete` pass of a full-chain `--index-only` build from an empty index, two runs | 0.92 | met: faster than the baseline |
| Serving after import, `GetBlockRange` | ≥ 140,000 blocks/s | 147,259 at 10,000-block ranges and 144,594 in the 1-client concurrency case (warm). 126,603 at 1,000, 60,801 at 100,000, 119,542 in sync. | 1.05 / 0.90 / 0.43 / 0.85 | met at 10,000-block ranges only |
| Ironwood roots | 97 of 97 ranges match | 103 of 103 ranges (3,428,999 to 3,530,999): node `finalState` equals the manifest frontier byte for byte; the import rebuilt every range to that frontier and matched the root at all 11 Ironwood epoch ends | 1.0 | met |

### HTTP import detail

| Phase | Epochs | Time |
| --- | --- | --- |
| 0 to 1,699,999 | 170 | 30.4 s |
| 1,700,000 to 2,199,999 (sandblast era) | 101 | 325.7 s |
| 2,200,000 to 3,530,999 | 134 | 32.0 s |

The replica used 14.8 cores on average (6,392 CPU-s), S 1.3 cores (attestation and samples), and
the HTTP server 0.02 cores. The average transfer rate was 41.9 MB/s, so the import is bound by the
replica's verification CPU, not by HTTP. The sandblast era is 75 % of the time.

### Serving detail

`serving-suite` with `--suites ranges,concurrency,sync --range-sizes 1000,10000,100000
--range-requests 200 --range-request-counts 100000=20 --clients 1,8 --seconds 10`. Blocks/s:

| Case | Replica, first run | Replica, warm | S (embedded index) |
| --- | --- | --- | --- |
| ranges, 1,000 blocks, 1 client | 69,902 | 126,603 | 126,392 |
| ranges, 10,000 blocks, 1 client | 87,260 | 147,259 | 149,464 |
| ranges, 100,000 blocks, 1 client | 46,165 | 60,801 | 60,850 |
| concurrency, 10,000 blocks, 1 client | 141,497 | 144,594 | 143,965 |
| concurrency, 10,000 blocks, 8 clients | 603,523 | 620,411 | 613,010 |
| sync, 10,000-block chunks, 1 client | 116,504 | 119,542 | 117,871 |

The replica serves as fast as the index that embedded mode built: the import adds no serving cost.
The 140,000 figure matches the 1-client rows of the 2026-09-11 run (139,043 to 149,959), which
used a lower tip. The 100,000-block case is bound by bytes (690 MB/s): those ranges include
sandblast-era blocks.

## Time to a serving instance

From an empty index to serving the full chain. The node's own sync is excluded in each row.

| Instance | Run | Node needed on the same host | Time to serving | CPU-s |
| --- | --- | --- | --- | --- |
| Zaino `d27ec930`, persistent index | 2026-09-11, Ryzen 9 9950X3D, tip 3,478,842 (`site/benchmarks/2026-09-11/indexing-summary.csv`) | synced Zebra | 13,380 s = 3 h 43 min | 24,506 |
| ztreamer v0.1.0, embedded | same box and day | synced archive zakura | 167 s = 2.8 min | 374 |
| ztreamer master, embedded (this run) | torvus, tip 3,531,614 | synced archive zakura | 170 s wall, of which the pass is 109 s | (not sampled) |
| **ztreamer master, replica over HTTP (this run)** | torvus, tip 3,531,614 | **none**: JSON-RPC to a node for attestation | **434 s = 7.2 min**, fully verified | 6,392 |

The replica is 31× faster than Zaino to a serving instance, on a slower CPU and with 52,772 more
blocks. Zaino and embedded ztreamer also need a synced node on the host first; the replica needs
none. The rows come from different machines and days, and Zaino's "ready" (persistent index
synced) is weaker than the replica's (every epoch verified against the node). Over a real bucket,
the replica also downloads 18.16 GB: about 2.5 min at 1 Gbit/s, about 25 min at 100 Mbit/s,
overlapped with the verification.

## Defects

1. **Inactive pool roots (stops every import from genesis).** The replica rejected the first epoch:
   `range check AttestedRoots failed at height 9999` (`out/replica-http-attempt1.log`). The filler
   writes the frontier of a pool that is not active yet as `000000`
   (`out/manifest-early-entries.txt`). `Trees::roots` gives no root only for empty frontier bytes,
   thus it gives the empty-tree root. zakura answers `"commitments": {}` before a pool activates
   (`out/z_gettreestate-samples.txt`), which the attestor reads as no root. The unit tests use empty
   frontiers, so they do not see it. The frontier also cannot tell "not active" (no root) from
   "active, no commitments yet" (empty-tree root), so the fix is a per-pool rule: no root from the
   node is valid only for an empty rebuilt tree. The benchmark patch gives no root for an empty
   tree; on mainnet this matches the node at every epoch end, because each pool has commitments
   before its first epoch end after activation.
2. **Peer stream limit (stops a peer import).** The source's Replicator allows
   `MAX_RANGE_STREAMS = 4` streams (`crates/service/src/replicate.rs:36`). The importer has
   available cores − 2 workers (`crates/verify/src/importer.rs:104`), 22 here, and fetches 2
   epochs per worker, one stream each: 44 streams. A refused stream gets
   `ResourceExhausted: "4 range streams are running"`; the fetcher took it as a transfer error,
   and with one source the import ended (`out/replica-peer-attempt1-22workers.log`). With the
   replica pinned to 6 CPUs (4 workers, 8 streams) the import ran to height 1,742,000 and ended the
   same way (`out/replica-peer.log`). An earlier version of this README called that a 59 s
   transfer stop: it was not. Every failed attempt has `offset=0`, and the 3 resumes and the
   failure came within 0.3 ms; 20:27:10 was an earlier log line. #175 makes a refusal back-pressure
   and keeps at most 4 streams per peer; with it, a live import from S with 22 workers completed in
   412 s (404 epochs, 3,524 ranges: the peer manifest ends at the last closed epoch).

## Deviations

1. **Benchmark-only patches.** The test key and the defect-1 patch (`tools/*.patch`) are not in
   this PR. The HTTP import therefore measures the import path with a key that no real replica
   trusts. Plan PR 8.1 (production key, publishing) is not decided; its draft is #172.
2. **Node.** The node is the embedded zakura of S on a hard-linked snapshot, not a standalone
   zakurad. Its tip is frozen at 3,531,614. ztreamer.service was down for 44 s for the snapshot,
   and served gRPC again 4 min after the stop (19:46:34), at the chain tip at 19:47:33.
3. **Index.** S's index is a copy of the service index (built by an older ztreamer, `cb6bdc7`),
   not a fresh embedded build. Its `range_meta` rows come from master's filler.
4. **Snapshot file server.** `python3 -m http.server` (threaded) on loopback, not a CDN or bucket.
   It used 0.02 cores, so it did not limit the import, but the network path of a real bucket is
   not measured.
5. **Attestation.** Measured as its JSON-RPC calls (`tools/attest_rpc.py`) at c=8, without the
   subtree-root calls and the sample blocks. The import does the full attestation inside its
   7.23 min.
6. **Tail.** On the HTTP replica's index, moved back to 3,529,614 (S's tip − 2,000) with
   `tools/rewind_index.rs`. The pass covers 1,900 blocks; the head reconcile reads the last 100.
7. **Serving.** Fewer samples than the 2026-09-11 defaults for 100,000-block ranges (20, not
   1,000). The first replica run had a partly cold page cache; the table gives both runs.
8. **Ironwood.** Master has no `verify/tests/ironwood.rs`. The gate is checked per range against
   the node's `finalState` (`out/ironwood-ranges.txt`) and through the import's epoch-end roots.
9. **Load.** Every timed step waited for a 1-min load < 2 and records the load in
   `out/loadavg-*.txt`. Other sessions ran builds and tests on the box during the run.
10. **Embedded gate.** The 119 s baseline is the 2026-10-22 full-chain pass (115.2 s there). On
    master the filler starts only after the historical pass, so the gate checks that the pass
    did not slow down. The pass ran on the frozen snapshot with the 2026-10-22 pipeline flags.
11. **tmux.** The session is `sandblast-run-7_4`: tmux replaces the dot of `sandblast-run-7.4`.

## Files

- `out/source.log`: S, including the filler
- `out/export.*`: export gate
- `out/replica-http*.log`, `out/import-http-summary.txt`, `out/cpu-import-http.txt`: HTTP import
- `out/replica-peer*.log`: peer import attempts
- `out/attest-rpc-c8.json`: attestation gate
- `out/tail.log`, `out/tail-summary.txt`, `out/rewind-tail.log`: tail gate
- `out/epoch-*.txt`: `verify_epoch` gates
- `out/embedded-*`: embedded pipeline gate
- `out/serving-*`: serving gate (`summary.csv`, `samples.csv`, `metadata.json` per run)
- `out/ironwood-ranges.txt`, `out/z_gettreestate-samples.txt`, `out/manifest-early-entries.txt`
- `node/`: node config, service stop and restart records, snapshot log
- `tools/`: run scripts, `snapshot-window.sh`, `attest_rpc.py`, `rewind_index.rs`, the two
  benchmark-only patches
