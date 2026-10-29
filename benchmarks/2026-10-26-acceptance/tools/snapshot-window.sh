#!/usr/bin/env bash
# Hard-linked snapshot of the stopped ztreamer.service state, plus an index copy. Run as root.
# SST files are immutable in RocksDB, thus they are hard links. Every other file is a real copy,
# because RocksDB appends to it or replaces it. chown touches only directories and real copies:
# a chown of a hard link would change the production file.
set -euo pipefail
SRC=/var/lib/ztreamer/cache
DST=$1/cache
IDX=$1/index-src
OWNER=$2
test ! -e "$DST" && test ! -e "$IDX"
mkdir -p "$DST/state/v29" "$IDX"
cp -al "$SRC/state/v29/mainnet" "$DST/state/v29/mainnet"
find "$DST/state/v29/mainnet" -type f -name 'LOG.old.*' -delete
find "$DST/state/v29/mainnet" -type f ! -name '*.sst' | while read -r f; do
  cp -p "$f" "$f.copy" && mv -f "$f.copy" "$f" && chown "$OWNER:$OWNER" "$f"
done
cp -a "$SRC/non_finalized_state" "$DST/non_finalized_state"
chown -R "$OWNER:$OWNER" "$DST/non_finalized_state"
find "$DST" -type d -exec chown "$OWNER:$OWNER" {} +
cp --sparse=always /var/lib/ztreamer/index/data.mdb "$IDX/data.mdb"
chown -R "$OWNER:$OWNER" "$IDX"
echo "sst_links=$(find "$DST/state/v29/mainnet" -name '*.sst' -links +1 | wc -l)"
echo "real_files=$(find "$DST/state/v29/mainnet" -type f ! -name '*.sst' -links 1 | wc -l)"
