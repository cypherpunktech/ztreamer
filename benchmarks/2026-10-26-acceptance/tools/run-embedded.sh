#!/usr/bin/env bash
# Embedded pipeline gate: master ztreamerd --index-only from an empty index on the frozen snapshot,
# with the 2026-10-22 pipeline flags, as user ztreamer. $1 = run name.
sudo -n install -d -o ztreamer -g ztreamer /srv/accept-7.4/$1
date +%s.%N > /home/evalir/sandblast-bench/accept-7.4/out/$1.log.start
/usr/bin/time -v sudo -n -u ztreamer /srv/accept-7.4/bin/ztreamerd --zakura-config /srv/accept-7.4/source-zakura.toml \
  --index-dir /srv/accept-7.4/$1 --index-map-size 68719476736 --fetch-workers 8 --source-segment-blocks 256 \
  --max-pending-bytes 268435456 --max-batch-bytes 16777216 --metrics-listen 127.0.0.1:29999 --index-only \
  > /home/evalir/sandblast-bench/accept-7.4/out/$1.log 2> /home/evalir/sandblast-bench/accept-7.4/out/$1.time
echo "exit=$?" >> /home/evalir/sandblast-bench/accept-7.4/out/$1.time
