#!/usr/bin/env python3
"""The JSON-RPC part of attestation for all epochs, at concurrency C.

For each sealed range end: `getblockhash`. For each epoch end: `z_gettreestate`. This is what
`Epoch::attest` asks the node, without the subtree-root calls (few) and the sample blocks.
Prints one JSON object with the wall time and per-method latency.

Usage: attest_rpc.py <host:port> <cookie file> <manifest.json> <C>
"""
import base64
import http.client
import json
import sys
import threading
import time


def main():
    hostport, cookie, manifest, c = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
    auth = "Basic " + base64.b64encode(open(cookie).read().strip().encode()).decode()
    entries = json.load(open(manifest))["entries"]
    calls = [("getblockhash", [e["end_height"]]) for e in entries]
    calls += [("z_gettreestate", [str(e["end_height"])]) for e in entries if e.get("epoch_end")]
    work = iter(calls)
    lock = threading.Lock()
    lat = {"getblockhash": [], "z_gettreestate": []}
    errors = []

    def worker():
        host, port = hostport.split(":")
        conn = http.client.HTTPConnection(host, int(port), timeout=120)
        while True:
            with lock:
                call = next(work, None)
            if call is None:
                return
            method, params = call
            body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
            t0 = time.perf_counter()
            conn.request("POST", "/", body, {"content-type": "application/json", "authorization": auth})
            reply = json.loads(conn.getresponse().read())
            dt = time.perf_counter() - t0
            with lock:
                lat[method].append(dt)
                if reply.get("error"):
                    errors.append((method, params, reply["error"]))

    t0 = time.perf_counter()
    threads = [threading.Thread(target=worker) for _ in range(c)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall = time.perf_counter() - t0

    def stats(v):
        v = sorted(v)
        return {"n": len(v), "sum_s": round(sum(v), 2), "p50_ms": round(v[len(v) // 2] * 1e3, 2),
                "p95_ms": round(v[int(len(v) * 0.95)] * 1e3, 2), "max_ms": round(v[-1] * 1e3, 1)}

    print(json.dumps({"concurrency": c, "wall_s": round(wall, 2), "errors": len(errors),
                      **{m: stats(v) for m, v in lat.items()}}))


if __name__ == "__main__":
    main()
