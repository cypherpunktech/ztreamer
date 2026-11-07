#!/usr/bin/env bash
# Source S: master ztreamerd (test-key build) in embedded mode on the snapshot, as user ztreamer.
# $1 = log file.
date +%s.%N > $1.start
sudo -n -u ztreamer /srv/accept-7.4/bin/ztreamerd --zakura-config /srv/accept-7.4/source-zakura.toml \
  --index-dir /srv/accept-7.4/index-src --index-map-size 68719476736 \
  --grpc-listen 127.0.0.1:29067 --metrics-listen 127.0.0.1:29999 2>&1 | tee $1 > /dev/null
