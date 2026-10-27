![ztreamer](./banner.png)

# ztreamer [![GitHub Actions][gha-badge]][gha] [![X][x-badge]][x]

[gha]: https://github.com/cypherpunktech/ztreamer/actions/workflows/ci.yml
[gha-badge]: https://github.com/cypherpunktech/ztreamer/actions/workflows/ci.yml/badge.svg?branch=master
[x]: https://x.com/cypherpunk
[x-badge]: https://img.shields.io/twitter/follow/cypherpunk

A high-performance [Zcash](https://z.cash) indexer and light-wallet server, powered by an embedded [zakura](https://github.com/zakura-core/zakura) node.

Implements `lightwallet-protocol` over gRPC and serves `CompactTxStreamer` over v2 P2P for peer-to-peer light wallets.

## Install

Install [Protocol Buffers](https://protobuf.dev/installation/), then:

```console
cargo install ztreamerd
```

## Run

```console
ztreamerd --zakura-config zakura.toml
```

The gRPC listener is plaintext by default. To serve it over TLS, provide a PEM certificate chain and matching PEM private key together:

```console
ztreamerd --zakura-config zakura.toml \
  --grpc-listen 0.0.0.0:9067 \
  --tls-cert /etc/letsencrypt/live/ztreamer.example/fullchain.pem \
  --tls-key /etc/letsencrypt/live/ztreamer.example/privkey.pem
```

Certificate changes require a restart. The Prometheus listener is not covered by these options and should remain private or be secured by a reverse proxy.

## Protocol compatibility

All `lightwallet-protocol` methods are implemented. `GetBlock` intentionally differs from the protocol by excluding transparent data, which current wallets do not use. `GetBlockRange` accepts transparent filters, but these requests bypass the index; transparent scanning is not a recommended use case.

`GetTaddressBalanceStream` counts each address once and accepts up to 10,000 distinct addresses per request. An additional distinct address returns `ResourceExhausted`; repeated addresses do not count toward the limit.

Ztreamer supports direct, in-process Zakura mode. It serves 24 of the 27 JSON-RPC requests provided by Zaino direct mode.

`getblockdeltas`, `getspentinfo`, and `gettxoutsetinfo` are not yet implemented.

## Historical indexing benchmark

| Metric                  |          Result |
| ----------------------- | --------------: |
| Genesis to serving      |            92 s |
| Index rate              | 37,848 blocks/s |
| Index size              |          16 GiB |
| Peak Physical Footprint |        3.66 GiB |
| Total CPU seconds       |             493 |

The benchmark indexed mainnet from genesis (to height 3,459,912) on an M3 Ultra with 512 GiB RAM and a warm cache.

## Serving benchmarks

`scripts/benchmark-serving.sh` measures RPC latencies, range-size scaling,
concurrent serving, and compact-block wallet sync against a running server. It
exports request samples and summaries as CSV and JSON for comparisons and later
plotting. See [the benchmark guide](benchmarks/README.md) for workloads, fixtures,
and HEAD-versus-release instructions.
