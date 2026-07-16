#!/usr/bin/env bash
# Provision all TeleStream topics (idempotent — safe to re-run).
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"

TOPICS=(
  subscriber-created
  airtime-purchases
  bundle-purchases
  voice-calls
  sms
  data-usage
  tower-events
  failed-transactions
)

for topic in "${TOPICS[@]}"; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists --topic "$topic" \
    --partitions 3 --replication-factor 1
done

echo "All topics provisioned:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
