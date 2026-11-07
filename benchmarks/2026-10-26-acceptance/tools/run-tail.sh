#!/usr/bin/env bash
# Tail gate: master ztreamerd in --remote-rpc mode on the rolled-back replica index. $1 = log file.
date +%s.%N > $1.start
exec /home/evalir/sandblast-bench/accept-7.4/target/release/ztreamerd --remote-rpc http://127.0.0.1:28232 --remote-rpc-cookie /home/evalir/sandblast-bench/accept-7.4/source.cookie \
  --index-dir /home/evalir/sandblast-bench/accept-7.4/replica-http --index-map-size 68719476736 --fetch-workers 8 \
  --grpc-listen 127.0.0.1:37067 --metrics-listen 127.0.0.1:37967 > $1 2>&1
