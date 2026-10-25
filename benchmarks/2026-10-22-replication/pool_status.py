#!/usr/bin/env python3
"""Per-row pool status tally for roots.csv against z_gettreestate (compare-tree-roots.py reports row 0's status only)."""
import base64, collections, csv, json, sys, urllib.request
from concurrent.futures import ThreadPoolExecutor

csv_path, url, cookie = sys.argv[1:4]
auth = "Basic " + base64.b64encode(open(cookie).read().strip().encode()).decode()


def state(height):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "z_gettreestate", "params": [str(height)]}).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json", "Authorization": auth})
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.load(resp)["result"]


def status(row):
    s = state(int(row["end_height"]))
    out = {"keys": tuple(sorted(s.keys()))}
    for pool in ("sapling", "orchard", "ironwood"):
        theirs = (s.get(pool) or {}).get("commitments", {}).get("finalRoot")
        ours = row.get(f"{pool}_root", "")
        if not theirs:
            out[pool] = "absent"
        elif not ours:
            out[pool] = "not-computed"
        elif ours == theirs:
            out[pool] = "match"
        elif bytes.fromhex(ours)[::-1].hex() == theirs:
            out[pool] = "match-reversed"
        else:
            out[pool] = "MISMATCH"
    return int(row["end_height"]), out


rows = list(csv.DictReader(open(csv_path)))
with ThreadPoolExecutor(8) as pool:
    results = list(pool.map(status, rows))
summary = {}
for p in ("sapling", "orchard", "ironwood"):
    c = collections.Counter(r[p] for _, r in results)
    first = {k: min(h for h, r in results if r[p] == k) for k in c}
    summary[p] = {"counts": dict(c), "first_end_height": first}
summary["response_keys"] = {",".join(k): n for k, n in collections.Counter(r["keys"] for _, r in results).items()}
print(json.dumps(summary, indent=2))
