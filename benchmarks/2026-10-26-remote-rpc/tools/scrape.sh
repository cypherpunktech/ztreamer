#!/usr/bin/env bash
# Appends a timestamped Prometheus scrape of the daemon every 30 s. $1 = output file.
while true; do echo "# scrape $(date +%s.%N)" >> $1; curl -s http://127.0.0.1:19999/metrics | grep -E '^ztreamer_(head|index_durable|pipeline_committed)' >> $1; sleep 30; done
