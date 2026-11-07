#!/usr/bin/env bash
# Replica: master ztreamerd (test-key build) on an empty index, importing from $2 and verifying
# against S's JSON-RPC. $1 = name (index dir and log), $2 = --replicate-from value, $3 = gRPC port.
mkdir -p /home/evalir/sandblast-bench/accept-7.4/$1
date +%s.%N > /home/evalir/sandblast-bench/accept-7.4/out/$1.log.start
exec /home/evalir/sandblast-bench/accept-7.4/target/release/ztreamerd --remote-rpc http://127.0.0.1:28232 --remote-rpc-cookie /home/evalir/sandblast-bench/accept-7.4/source.cookie \
  --replicate-from "$2" --index-dir /home/evalir/sandblast-bench/accept-7.4/$1 --index-map-size 68719476736 \
  --grpc-listen 127.0.0.1:$3 --metrics-listen 127.0.0.1:$(($3 + 900)) > /home/evalir/sandblast-bench/accept-7.4/out/$1.log 2>&1
