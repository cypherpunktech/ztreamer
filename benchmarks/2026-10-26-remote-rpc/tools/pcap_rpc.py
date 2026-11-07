#!/usr/bin/env python3
"""Reads a `tcpdump -i lo -s 256 -w` capture of JSON-RPC traffic on one port.

Prints JSON lines: one per request ({"t", "method", "params"}) and, at the end, totals.
Byte counts use the original frame length, thus a short snaplen does not change them.

Usage: pcap_rpc.py <PCAP> <PORT> [--requests]
"""
import json
import re
import struct
import sys

METHOD = re.compile(rb'"method"\s*:\s*"([A-Za-z_]+)"')
PARAMS = re.compile(rb'"params"\s*:\s*\[([^\]]{0,80})')


def packets(path):
    with open(path, "rb") as f:
        head = f.read(24)
        magic, = struct.unpack("<I", head[:4])
        endian = "<" if magic in (0xA1B2C3D4, 0xA1B23C4D) else ">"
        nano = magic in (0xA1B23C4D, 0x4D3CB2A1)
        linktype, = struct.unpack(endian + "I", head[20:24])
        while rec := f.read(16):
            sec, frac, incl, orig = struct.unpack(endian + "IIII", rec)
            data = f.read(incl)
            yield sec + frac / (1e9 if nano else 1e6), orig, linktype, data


def main():
    path, port = sys.argv[1], int(sys.argv[2])
    show = "--requests" in sys.argv
    totals = {"requests": 0, "methods": {}, "request_bytes": 0, "response_bytes": 0, "first_t": None, "last_t": None}
    for t, orig, linktype, data in packets(path):
        link = {1: 14, 113: 16, 276: 20}.get(linktype, 14)
        ip = data[link:]
        if len(ip) < 20 or ip[0] >> 4 != 4:
            continue
        ihl = (ip[0] & 15) * 4
        tcp = ip[ihl:]
        sport, dport = struct.unpack(">HH", tcp[:4])
        thl = (tcp[12] >> 4) * 4
        payload_len = orig - link - ihl - thl
        if payload_len <= 0:
            continue
        payload = tcp[thl:]
        totals["first_t"] = totals["first_t"] or t
        totals["last_t"] = t
        if dport == port:
            totals["request_bytes"] += payload_len
            posts = payload.count(b"POST /")
            m = METHOD.search(payload)
            method = m.group(1).decode() if m else ("unknown" if posts else None)
            if method:
                totals["requests"] += max(posts, 1) if m is None else 1
                totals["methods"][method] = totals["methods"].get(method, 0) + 1
                if show:
                    p = PARAMS.search(payload)
                    print(json.dumps({"t": round(t, 6), "method": method,
                                      "params": p.group(1).decode(errors="replace") if p else None}))
        elif sport == port:
            totals["response_bytes"] += payload_len
            if show:
                print(json.dumps({"t": round(t, 6), "response_bytes": payload_len}))
    print(json.dumps({"totals": totals}))


if __name__ == "__main__":
    main()
