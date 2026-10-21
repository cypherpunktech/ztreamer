default:
    @just --list

build:
    cargo build --release -p ztreamerd

test:
    cargo test --workspace --locked

clippy:
    cargo clippy --workspace --all-targets --locked -- -D warnings

fmt:
    cargo +nightly fmt --all

fmt-check:
    cargo +nightly fmt --all -- --check

doc:
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked

# Everything CI checks before tests: fmt, clippy, docs.
check: fmt-check clippy doc

# Criterion: `just bench`, `just bench parse_block`, `just bench -- --save-baseline base`
bench *args:
    cargo bench --locked --bench parser --bench codec --bench serve {{ args }}

# Real HTTP/2 transport and concurrent clients.
bench-grpc *args:
    cargo bench --locked --bench grpc {{ args }}

# Durable writes, ordering, reorgs, and warm restart (uses temporary storage).
bench-index *args:
    cargo bench --locked --bench index {{ args }}

# JSON latency/throughput report; accepts --endpoint for an existing server.
load *args:
    bash scripts/benchmark-grpc.sh {{ args }}

# RPC latency, range scaling, concurrency, and wallet download; CSV + JSON.
bench-serving *args:
    bash scripts/benchmark-serving.sh {{ args }}

# Historical genesis→tip. Needs a writable Zakura cache and config.
snapshot cache config *args:
    scripts/benchmark-snapshot.sh {{ cache }} {{ config }} {{ args }}
