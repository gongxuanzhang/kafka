#!/bin/bash

TOPIC=$1
NUM_RECORDS=1200
THROUGHPUT=-1
BOOTSTRAP_SERVERS=localhost:9092
OFFSET_FILE=".${TOPIC}_offset"
INTERVAL=5

# 初始化 offset 文件
if [ ! -f "$OFFSET_FILE" ]; then
  echo 0 > "$OFFSET_FILE"
fi

while true; do
  OFFSET=$(cat "$OFFSET_FILE")

  bin/kafka-producer-perf-test.sh \
    --topic "$TOPIC" \
    --num-records "$NUM_RECORDS" \
    --payload-monotonic \
    --offset "$OFFSET" \
    --throughput "$THROUGHPUT" \
    --producer-props bootstrap.servers="$BOOTSTRAP_SERVERS"

  OFFSET=$((OFFSET + NUM_RECORDS))
  echo "$OFFSET" > "$OFFSET_FILE"

  sleep "$INTERVAL"
done
