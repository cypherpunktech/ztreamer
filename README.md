![ztreamer](./banner.png)

# ztreamer [![GitHub Actions][gha-badge]][gha] [![X][x-badge]][x]

[gha]: https://github.com/cypherpunktech/ztreamer/actions/workflows/ci.yml
[gha-badge]: https://github.com/cypherpunktech/ztreamer/actions/workflows/ci.yml/badge.svg?branch=master
[x]: https://x.com/cypherpunk
[x-badge]: https://img.shields.io/twitter/follow/cypherpunk

A high-performance [Zcash](https://z.cash) indexer and light-wallet server. ztreamerd reads the chain from a [zakura](https://github.com/zakura-core/zakura) node, writes a compact index, and serves `lightwallet-protocol` from that index.

## Install

Install [Protocol Buffers](https://protobuf.dev/installation/), then:

```console
cargo install --locked --git https://github.com/cypherpunktech/ztreamer ztreamerd
```

## Modes

ztreamerd has two modes. Both modes write the same index and serve the same gRPC API.

| Mode | Command | Chain source |
| --- | --- | --- |
| Embedded | `ztreamerd --zakura-config zakura.toml` | A zakura node in the ztreamerd process |
| Remote | `ztreamerd --remote-rpc URL --replicate-from SOURCE` | Sealed ranges from snapshots or peers, then a remote zakura node over JSON-RPC |

### Embedded

This is the default mode. The zakura configuration file is optional. The embedded node also serves:

- `CompactTxStreamer` over v2 P2P, for peer-to-peer light wallets;
- the zakura JSON-RPC requests, when the zakura configuration sets `[rpc] listen_addr`.

### Remote

ztreamerd starts no node. `--remote-rpc` and `--replicate-from` need each other: ztreamerd refuses one without the other. This mode does not serve P2P or JSON-RPC.

First, ztreamerd imports sealed ranges from the one `--replicate-from` source. The remote node verifies the import. Then ztreamerd reads the blocks above the import from the remote node, and follows its head. A source is:

- the `http://` or `https://` base URL of a snapshot; or
- the `host:port` of a ztreamerd peer. The peer serves the Replicator service on its gRPC listener, without TLS.

The import refuses a manifest that the built-in snapshot key did not sign. It also refuses a manifest of a different network or genesis block.

The remote node sets the network (mainnet or testnet). The index must be of the same network. To give the node credentials, use one of these:

- HTTP basic auth in the URL: `https://user:password@host:8232`;
- a cookie file: `--remote-rpc-cookie FILE`.

With no credentials and a loopback URL, ztreamerd uses the default zakura cookie file, if it exists. It does not send this cookie to other hosts.

## Listeners

| Listener | Flag | Default | Serves |
| --- | --- | --- | --- |
| gRPC | `--grpc-listen` | `127.0.0.1:9067` | `CompactTxStreamer` and Replicator |
| Prometheus | `--metrics-listen` | `127.0.0.1:9999` | Metrics |

The gRPC listener uses plaintext. For TLS, give a PEM certificate chain and its PEM private key:

```console
ztreamerd --zakura-config zakura.toml \
  --grpc-listen 0.0.0.0:9067 \
  --tls-cert /etc/letsencrypt/live/ztreamer.example/fullchain.pem \
  --tls-key /etc/letsencrypt/live/ztreamer.example/privkey.pem
```

Restart ztreamerd after a certificate change. The Prometheus listener has no TLS. Keep it private, or put it behind a reverse proxy.

## Snapshot export

```console
ztreamerd export-snapshot --index-dir ztreamer-index --out snapshot --signing-key key.seed
```

This command writes the sealed ranges of an index as a snapshot. The remote mode can import this snapshot. The signing key is a raw 32-byte Ed25519 seed. An export into the same directory writes only the new ranges. Give `--index-dir` and `--index-map-size` after the subcommand name.

## Index size

`--index-map-size` sets the maximum LMDB map size. The default is 64 GiB. On 2026-10-27, the mainnet index at height 3,532,489 was 17.3 GiB.

## Protocol compatibility

ztreamerd implements all `lightwallet-protocol` methods, with these differences:

- `GetBlock` does not send transparent data. Current wallets do not use it.
- `GetBlockRange` accepts transparent filters, but these requests do not use the index. Do not use ztreamerd for transparent scans.
- `GetTaddressBalanceStream` accepts a maximum of 10,000 distinct addresses in one request. It counts a repeated address one time. One more distinct address gives `ResourceExhausted`.

In the embedded mode, ztreamerd serves 24 of the 27 JSON-RPC requests of Zaino direct mode. It does not serve `getblockdeltas`, `getspentinfo` and `gettxoutsetinfo`.

## Benchmarks

### Historical indexing

| Metric                  |          Result |
| ----------------------- | --------------: |
| Genesis to serving      |            92 s |
| Index rate              | 37,848 blocks/s |
| Index size              |          16 GiB |
| Peak Physical Footprint |        3.66 GiB |
| Total CPU seconds       |             493 |

This benchmark indexed mainnet from genesis to height 3,459,912. It used an M3 Ultra with 512 GiB RAM and a warm cache.

### Serving

`scripts/benchmark-serving.sh` measures a running server: RPC latency, range-size scaling, concurrent requests and compact-block wallet sync. It writes the request samples and the summaries as CSV and JSON. [The benchmark guide](benchmarks/README.md) gives the workloads, the fixtures, and how to compare HEAD with a release.
