#!/usr/bin/env python3
"""Per-block cost of head polling in --remote-rpc mode.

Inputs (in OUT): head.requests.jsonl (pcap_rpc.py --requests), head-latest.jsonl (grpc_probe
watch), cpu-head.txt (cpu_sampler.sh), and the zakurad log (commit times).

For each new block that the daemon publishes, the interval is from the node's commit of that
block to the node's commit of the next block. In that interval it counts the RPCs by method and
the bytes on the wire, and the CPU of both processes. The latency is node commit -> first
GetLatestBlock answer with that height.

Usage: head_analysis.py <OUT> <ZAKURAD_LOG> <WINDOW_START_UNIX> <WINDOW_END_UNIX>
"""
import bisect
import collections
import datetime
import json
import re
import statistics
import sys

out, zlog, w0, w1 = sys.argv[1], sys.argv[2], float(sys.argv[3]), float(sys.argv[4])

commits = {}
for line in open(zlog):
    if "sending committed block broadcast" in line:
        t = datetime.datetime.fromisoformat(line.split()[0].replace("Z", "+00:00")).timestamp()
        h = int(re.search(r"Height\((\d+)\)", line).group(1))
        commits.setdefault(h, t)

latest = {}
for line in open(f"{out}/head-latest.jsonl"):
    r = json.loads(line)
    latest.setdefault(r["height"], r["t"] / 1e3)

reqs, resps = [], []
for line in open(f"{out}/head.requests.jsonl"):
    r = json.loads(line)
    if "method" in r:
        reqs.append((r["t"], r["method"]))
    elif "response_bytes" in r:
        resps.append((r["t"], r["response_bytes"]))
req_t = [t for t, _ in reqs]
resp_t = [t for t, _ in resps]
resp_cum = [0]
for _, b in resps:
    resp_cum.append(resp_cum[-1] + b)

cpu = collections.defaultdict(list)
for line in open(f"{out}/cpu-head.txt"):
    t, name, ticks = line.split()
    cpu[name].append((float(t), int(ticks)))


def cpu_between(name, a, b):
    samples = cpu[name]
    ts = [t for t, _ in samples]
    i, j = bisect.bisect_left(ts, a), bisect.bisect_left(ts, b)
    j = min(j, len(samples) - 1)
    i = min(i, j)
    return (samples[j][1] - samples[i][1]) / 100


heights = sorted(h for h in commits if w0 <= commits[h] <= w1 and h in latest)
rows = []
for h, nxt in zip(heights, heights[1:] + [None]):
    a = commits[h]
    b = commits[nxt] if nxt else w1
    i, j = bisect.bisect_left(req_t, a), bisect.bisect_left(req_t, b)
    methods = collections.Counter(m for _, m in reqs[i:j])
    k, l = bisect.bisect_left(resp_t, a), bisect.bisect_left(resp_t, b)
    rows.append({
        "height": h,
        "node_commit": round(a, 3),
        "interval_s": round(b - a, 1),
        "latency_s": round(latest[h] - a, 3),
        "rpcs": sum(methods.values()),
        "methods": dict(methods),
        "response_bytes": resp_cum[l] - resp_cum[k],
        "zakurad_cpu_s": round(cpu_between("zakurad", a, b), 2),
        "ztreamerd_cpu_s": round(cpu_between("ztreamerd", a, b), 2),
    })

for r in rows:
    print(json.dumps(r))
# Per-block cost: the RPCs that one tip change causes, minus the 1 Hz getblockchaininfo poll.
burst = [r["rpcs"] - r["methods"].get("getblockchaininfo", 0) for r in rows]
lat = sorted(r["latency_s"] for r in rows)
summary = {
    "blocks": len(rows),
    "window_s": round(w1 - w0, 1),
    "rpcs_per_block_excluding_tip_poll": {"median": statistics.median(burst), "min": min(burst), "max": max(burst)},
    "getblock_per_block_median": statistics.median(r["methods"].get("getblock", 0) for r in rows),
    "getblockhash_per_block_median": statistics.median(r["methods"].get("getblockhash", 0) for r in rows),
    "getblockchaininfo_per_s": round(sum(r["methods"].get("getblockchaininfo", 0) for r in rows) / sum(r["interval_s"] for r in rows), 2),
    "response_MB_per_block": {"median": round(statistics.median(r["response_bytes"] for r in rows) / 1e6, 1),
                              "max": round(max(r["response_bytes"] for r in rows) / 1e6, 1)},
    "latency_s": {"median": statistics.median(lat), "p90": lat[int(len(lat) * 0.9)], "max": lat[-1]},
    "zakurad_cpu_s_per_block_median": statistics.median(r["zakurad_cpu_s"] for r in rows),
    "ztreamerd_cpu_s_per_block_median": statistics.median(r["ztreamerd_cpu_s"] for r in rows),
}
print(json.dumps({"summary": summary}))
