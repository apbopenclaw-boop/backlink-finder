#!/bin/bash
# Check if conversion is done
if pgrep -f convert_parquet_v2 > /dev/null; then
    RSS=$(ps aux | grep convert_parquet_v2 | grep -v grep | awk '{printf "%.0f", $6/1024}')
    CPU=$(ps aux | grep convert_parquet_v2 | grep -v grep | awk '{print $3}')
    echo "RUNNING - CPU: ${CPU}%, RSS: ${RSS}MB"
    exit 1
else
    VSIZE=$(stat -f%z /tmp/backlink-parquet/vertices.parquet 2>/dev/null || echo 0)
    ECOUNT=$(ls /tmp/backlink-parquet/edges/ 2>/dev/null | wc -l | tr -d ' ')
    echo "DONE - vertices: $((VSIZE/1024/1024))MB, edge partitions: ${ECOUNT}"
    exit 0
fi
