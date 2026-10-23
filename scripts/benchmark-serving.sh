#!/usr/bin/env bash
# All arguments are forwarded to the native serving suite; --output is supplied here.
set -Eeuo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
if [[ ${1:-} == --help ]]; then
    cargo run --locked --manifest-path "$repo/Cargo.toml" --release \
        -p serving-suite -- --help
    exit
fi
run_root=${RUN_ROOT:-"$repo/benchmark-runs/serving"}
mkdir -p "$run_root"
run=$(mktemp -d "$run_root/$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX")
python3 -c 'import tomllib'
echo "Building serving benchmark; artifacts: $run"
cargo build --locked --manifest-path "$repo/Cargo.toml" --release \
    -p serving-suite > "$run/build.log" 2>&1 || {
    cat "$run/build.log" >&2
    exit 1
}
python3 "$repo/scripts/benchmark-metadata.py" "$repo" "$run/client-provenance"
command=(cargo run --locked --manifest-path "$repo/Cargo.toml" --release \
    -p serving-suite -- --output "$run/data" "$@")
printf '%q ' "${command[@]}" > "$run/command.txt"
printf '\n' >> "$run/command.txt"
set +e
"${command[@]}" > "$run/stdout.log" 2> >(tee "$run/run.log" >&2)
status=$?
set -e
echo "$status" > "$run/exit-status.txt"
echo "Serving benchmark artifacts: $run (exit $status)"
exit "$status"
