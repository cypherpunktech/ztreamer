#!/usr/bin/env python3
"""Time the attestation RPCs the replication plan relies on, in the style of scripts/compare-tree-roots.py.

Calls getblockhash, z_gettreestate and z_getsubtreesbyindex at points spread across the chain,
N calls per method, at the given concurrency, and prints p50 / p95 per method as JSON.

Usage:
  rpc_timing.py http://127.0.0.1:18232 --tip 3524943 --sapling-subtrees N [--cookie PATH]
      [--calls 50] [--concurrency 1] [--methods getblockhash,z_gettreestate,z_getsubtreesbyindex]
"""
import argparse
import base64
import json
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor


def rpc(url, auth, method, params, timeout=60):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    if auth:
        req.add_header("Authorization", "Basic " + base64.b64encode(auth.encode()).decode())
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = json.load(resp)
    if payload.get("error"):
        raise RuntimeError(payload["error"])
    return payload["result"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("url")
    ap.add_argument("--tip", type=int, required=True)
    ap.add_argument("--sapling-subtrees", type=int, required=True)
    ap.add_argument("--cookie")
    ap.add_argument("--calls", type=int, default=50)
    ap.add_argument("--concurrency", type=int, default=1)
    ap.add_argument("--methods", default="getblockhash,z_gettreestate,z_getsubtreesbyindex")
    args = ap.parse_args()
    auth = open(args.cookie).read().strip() if args.cookie else None

    def params(method, i):
        height = args.tip * i // args.calls
        if method == "getblockhash":
            return [height]
        if method == "z_gettreestate":
            return [str(height)]
        return ["sapling", args.sapling_subtrees * i // args.calls, 1]

    summary = {"concurrency": args.concurrency, "authenticated": bool(auth)}
    for method in args.methods.split(","):
        def call(i):
            started = time.monotonic()
            try:
                result = rpc(args.url, auth, method, params(method, i))
                error = None
            except urllib.error.HTTPError as err:
                result, error = None, f"HTTP {err.code}"
            except Exception as err:  # noqa: BLE001 - a refused method is a result
                result, error = None, str(err)
            return time.monotonic() - started, result, error

        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            results = list(pool.map(call, range(args.calls)))
        total = time.monotonic() - started
        times = sorted(r[0] for r in results)
        errors = [r[2] for r in results if r[2]]
        entry = {
            "calls": len(results),
            "errors": len(errors),
            "p50_ms": round(times[len(times) // 2] * 1000, 2),
            "p95_ms": round(times[int(len(times) * 0.95)] * 1000, 2),
            "total_s": round(total, 2),
        }
        if errors:
            entry["first_error"] = errors[0]
        if method == "z_gettreestate":
            ok = [r[1] for r in results if r[1]]
            entry["ironwood_object_present"] = any("ironwood" in r for r in ok) if ok else None
            if ok:
                entry["sample_keys"] = sorted(ok[-1].keys())
        summary[method] = entry
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
