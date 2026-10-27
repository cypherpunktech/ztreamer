![ztreamer](./banner.png)

# ztreamer [![GitHub Actions][gha-badge]][gha] [![X][x-badge]][x]

[gha]: https://github.com/cypherpunktech/ztreamer/actions/workflows/ci.yml
[gha-badge]: https://github.com/cypherpunktech/ztreamer/actions/workflows/ci.yml/badge.svg?branch=master
[x]: https://x.com/cypherpunk
[x-badge]: https://img.shields.io/twitter/follow/cypherpunk

A high-performance [Zcash](https://z.cash) indexer and light-wallet server. It gets the chain from an embedded [zakura](https://github.com/zakura-core/zakura) node, or from a remote zakura node over JSON-RPC.

Implements `lightwallet-protocol` over gRPC. With the embedded node, it also serves `CompactTxStreamer` over v2 P2P for peer-to-peer light wallets.

## Install

Install [Protocol Buffers](https://protobuf.dev/installation/), then:

```console
cargo install --locked --git https://github.com/cypherpunktech/ztreamer ztreamerd
```

ztreamerd is not on crates.io: it has git dependencies. `--locked` uses the `Cargo.lock` of the repository.

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

The gRPC listener also serves the Replicator service. Replicas use it to copy sealed ranges (see `--replicate-from`).

## Remote node

```console
ztreamerd --remote-rpc http://127.0.0.1:8232
```

`--remote-rpc` reads the chain from a remote zakura node over JSON-RPC. ztreamerd then starts no embedded node, and `--zakura-config` is not permitted. The node gives the network (mainnet or testnet). The index must be of that network.

Credentials:

- For HTTP basic auth, put them in the URL: `https://user:password@host:8232`.
- `--remote-rpc-cookie FILE` gives the cookie file of the node.
- Without these, a loopback URL uses the default zakura cookie file, if it exists. ztreamerd does not send that cookie to another host.

This mode does not serve P2P.

## Replica import

```console
ztreamerd --remote-rpc http://127.0.0.1:8232 \
  --replicate-from https://snapshots.example/mainnet \
  --replicate-from peer.example:9067
```

`--replicate-from` imports the sealed ranges before the tail index starts. It needs `--remote-rpc`: that node verifies the import. A source is one of these:

- the `http://` or `https://` base URL of a snapshot;
- the `host:port` of a peer that serves the Replicator gRPC service without TLS.

Give the flag again for more sources. The first source is the reference. The import refuses a manifest whose signature does not verify with the built-in snapshot key, and a manifest for another network or genesis block than the index. After the import, ztreamerd indexes the tail and follows the head, as in the remote mode.

## Snapshot export

```console
ztreamerd export-snapshot --index-dir ztreamer-index --out snapshot --signing-key key.seed
```

`export-snapshot` writes the sealed ranges of an index as a snapshot that a replica can import. It starts at genesis and stops at the first sealed range without a `range_meta` row. `--signing-key` is an Ed25519 key as a raw 32-byte seed. It signs the manifest and `latest.json`. An export into the same directory writes only the new ranges, and never replaces a manifest. Give `--index-dir` and `--index-map-size` after the subcommand name.

## Protocol compatibility

All `lightwallet-protocol` methods are implemented. `GetBlock` intentionally differs from the protocol by excluding transparent data, which current wallets do not use. `GetBlockRange` accepts transparent filters, but these requests bypass the index; transparent scanning is not a recommended use case.

`GetTaddressBalanceStream` counts each address once and accepts up to 10,000 distinct addresses per request. An additional distinct address returns `ResourceExhausted`; repeated addresses do not count toward the limit.

ztreamerd has no JSON-RPC server. It serves gRPC and P2P only. In the `--remote-rpc` mode, it is a JSON-RPC client of a zakura node.

## Historical indexing benchmark

| Metric                  |          Result |
| ----------------------- | --------------: |
| Genesis to serving      |            92 s |
| Index rate              | 37,848 blocks/s |
| Index size              |          16 GiB |
| Peak Physical Footprint |        3.66 GiB |
| Total CPU seconds       |             493 |

The benchmark indexed mainnet from genesis (to height 3,459,912) on an M3 Ultra with 512 GiB RAM and a warm cache.

The index grows with the chain. On 2026-10-27, a mainnet index at height 3,532,489 was 17.3 GiB. `--index-map-size` sets the maximum LMDB map size. The default is 64 GiB.

## Serving benchmarks

`scripts/benchmark-serving.sh` measures RPC latencies, range-size scaling,
concurrent serving, and compact-block wallet sync against a running server. It
exports request samples and summaries as CSV and JSON for comparisons and later
plotting. See [the benchmark guide](benchmarks/README.md) for workloads, fixtures,
and HEAD-versus-release instructions.
