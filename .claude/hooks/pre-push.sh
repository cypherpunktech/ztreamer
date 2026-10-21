#!/usr/bin/env bash
# Blocks `git push` until fmt, clippy, and rustdoc are clean.
set -euo pipefail

# Consume stdin (hook protocol).
cat > /dev/null

cd "$(dirname "$0")/../.."

echo "Running pre-push checks..." >&2

if ! cargo +nightly fmt --all -- --check 2>&1; then
    echo "Format check failed. Run 'cargo +nightly fmt --all' to fix." >&2
    exit 2
fi

if ! cargo clippy --workspace --all-targets --locked -- -D warnings 2>&1; then
    echo "Clippy failed." >&2
    exit 2
fi

if ! RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked 2>&1; then
    echo "Doc check failed." >&2
    exit 2
fi

echo "All pre-push checks passed." >&2
exit 0
