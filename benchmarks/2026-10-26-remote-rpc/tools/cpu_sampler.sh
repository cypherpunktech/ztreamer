#!/usr/bin/env bash
# Prints "<unix time> <name> <utime+stime ticks>" each second for each PID argument (name=pid pairs).
while true; do
  t=$(date +%s.%N)
  for pair in "$@"; do
    name=${pair%%=*}; pid=${pair#*=}
    [ -r /proc/$pid/stat ] && awk -v t=$t -v n=$name '{print t, n, $14+$15}' /proc/$pid/stat
  done
  sleep 1
done
