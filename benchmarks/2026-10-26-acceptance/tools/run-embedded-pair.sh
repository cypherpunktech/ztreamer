#!/usr/bin/env bash
# Runs the embedded pipeline gate twice, waiting for load < 2 before each run.
for run in embedded-1 embedded-2; do
  for i in $(seq 1 60); do l=$(cut -d' ' -f1 /proc/loadavg); awk "BEGIN{exit !($l<2)}" && break; sleep 5; done
  uptime > /home/evalir/sandblast-bench/accept-7.4/out/loadavg-$run.txt
  /home/evalir/sandblast-bench/accept-7.4/tools/run-embedded.sh $run
done
echo all-done > /home/evalir/sandblast-bench/accept-7.4/out/embedded.done
