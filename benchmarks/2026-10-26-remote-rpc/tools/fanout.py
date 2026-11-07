#!/usr/bin/env python3
"""`getblock <height> 0` fan-out against a zakura node, as RemoteTailSource sends it.

Each of C threads keeps one HTTP/1.1 connection and takes the next height from a shared counter.
Prints one JSON object: wall time, requests/s, response bytes (hex JSON on the wire) and
block bytes (decoded), and the node's CPU seconds from /proc/<pid>/stat.

Usage: fanout.py <URL host:port> <COOKIE> <START> <COUNT> <C> <NODE_PID>
"""
import base64
import http.client
import itertools
import json
import os
import sys
import threading
import time


def cpu(pid):
    with open(f"/proc/{pid}/stat") as f:
        fields = f.read().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def main():
    hostport, cookie, start, count, c, pid = sys.argv[1:7]
    start, count, c, pid = int(start), int(count), int(c), int(pid)
    auth = "Basic " + base64.b64encode(open(cookie).read().strip().encode()).decode()
    heights = iter(range(start, start + count))
    lock = threading.Lock()
    totals = {"requests": 0, "wire_bytes": 0, "block_bytes": 0, "errors": 0, "latency": []}

    def worker():
        host, port = hostport.split(":")
        conn = http.client.HTTPConnection(host, int(port), timeout=60)
        while True:
            with lock:
                h = next(heights, None)
            if h is None:
                return
            body = json.dumps({"jsonrpc": "2.0", "id": h, "method": "getblock", "params": [str(h), 0]})
            t0 = time.perf_counter()
            conn.request("POST", "/", body, {"content-type": "application/json", "authorization": auth})
            raw = conn.getresponse().read()
            dt = time.perf_counter() - t0
            reply = json.loads(raw)
            with lock:
                totals["requests"] += 1
                totals["wire_bytes"] += len(raw)
                totals["latency"].append(dt)
                if reply.get("error"):
                    totals["errors"] += 1
                else:
                    totals["block_bytes"] += len(reply["result"]) // 2

    cpu0, t0 = cpu(pid), time.perf_counter()
    threads = [threading.Thread(target=worker) for _ in range(c)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall, node_cpu = time.perf_counter() - t0, cpu(pid) - cpu0
    lat = sorted(totals.pop("latency"))
    print(json.dumps({
        "start": start, "count": count, "concurrency": c, "wall_s": round(wall, 3),
        **totals,
        "requests_per_s": round(totals["requests"] / wall, 1),
        "wire_MB_per_s": round(totals["wire_bytes"] / wall / 1e6, 1),
        "block_MB_per_s": round(totals["block_bytes"] / wall / 1e6, 1),
        "latency_ms_p50": round(lat[len(lat) // 2] * 1e3, 2),
        "latency_ms_p95": round(lat[int(len(lat) * 0.95)] * 1e3, 2),
        "node_cpu_s": round(node_cpu, 2),
        "node_cpu_s_per_GB_blocks": round(node_cpu / (totals["block_bytes"] / 1e9), 2) if totals["block_bytes"] else None,
    }))


if __name__ == "__main__":
    main()
