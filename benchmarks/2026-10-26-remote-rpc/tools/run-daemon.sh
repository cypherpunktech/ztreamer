#!/usr/bin/env bash
# Starts ztreamerd in --remote-rpc mode on the index copy. $1 = log file.
date +%s.%N > $1.start
exec /home/evalir/sandblast-bench/ztreamer-ci-target/release/ztreamerd --remote-rpc http://127.0.0.1:18232 --remote-rpc-cookie /home/evalir/sandblast-bench/remote-rpc-run/cookie \
  --index-dir /home/evalir/sandblast-bench/remote-rpc-run/index --index-map-size 68719476736 --fetch-workers 8 \
  --grpc-listen 127.0.0.1:19067 --metrics-listen 127.0.0.1:19999 > $1 2>&1
