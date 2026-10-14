# ztreamer

`ztreamer` is a heavily optimized zcash indexer and `lightwallet-protocol` implementation backed by an embedded [zakura](https://github.com/zakura-core/zakura) node.

`ztreamer` additionally adds a `CompactTxStreamer` server over v2 p2p, which enables p2p light wallets.

## Install

Install [Protocol Buffers](https://protobuf.dev/installation/), then:

```console
cargo install --git https://github.com/distractedm1nd/ztreamer ztreamerd
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

All `lightwallet-protocol` methods are implemented. We intentionally deviate `lightwallet-protocol` for `GetBlock`, to exclude transparent data which current wallets do not use. `GetBlockRange` will accept transparent filters, but this will not hit the index as transparent scanning is not a recommended use case.

`GetTaddressBalanceStream` counts each address once and accepts up to 10,000 distinct addresses per request. An additional distinct address returns `ResourceExhausted`; repeated addresses do not count toward the limit.

Transparent address balance, UTXO, and transaction-history queries require Zakura's transparent indexes. Archive storage maintains these indexes. Pruned storage with checkpoint sync enabled stops maintaining them, and the affected RPCs return `Unavailable`. To retain these indexes with pruned storage, sync a fresh Zakura cache with `consensus.checkpoint_sync = false`. Transaction-history queries still require the requested raw transactions to be retained. Changing that setting after index updates have been skipped does not restore the missing history.

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
