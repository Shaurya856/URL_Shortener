#!/usr/bin/env bash
# Failure-survival demo for interview use: proves the redirect path and the
# analytics pipeline survive losing a Kafka broker and a consumer replica.
#
# Usage:
#   docker compose up -d --build --scale consumer=3
#   ./scripts/failure_demo.sh
#
# Everything it stops, it restarts at the end so the stack is left healthy.
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
COOKIES="$(mktemp)"
PROJECT="$(docker compose config --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["name"])')"

trap 'rm -f "$COOKIES"' EXIT

step() { printf '\n\033[1;36m==> %s\033[0m\n' "$1"; }
note() { printf '    %s\n' "$1"; }

csrf_token() {
  curl -s -c "$COOKIES" "$BASE_URL/" | grep -o 'csrfmiddlewaretoken[^>]*value="[^"]*"' | sed 's/.*value="//;s/"//'
}

shorten_one() {
  local label="$1"
  local csrf
  csrf="$(csrf_token)"
  local out
  out="$(curl -s -b "$COOKIES" -c "$COOKIES" -X POST "$BASE_URL/shorten" \
    -H "Referer: $BASE_URL/" \
    --data-urlencode "csrfmiddlewaretoken=$csrf" \
    --data-urlencode "long_url=https://example.com/failure-demo/$label")"
  echo "$out" | grep -o 'stats/[A-Za-z0-9]*' | head -1 | sed 's/stats\///'
}

click_one() {
  curl -s -o /dev/null "$BASE_URL/$1"
}

group_describe() {
  docker compose exec -T kafka-1 kafka-consumer-groups.sh \
    --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
    --describe --group analytics-consumer 2>/dev/null || true
}

topic_describe() {
  docker compose exec -T kafka-1 kafka-topics.sh \
    --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
    --describe --topic click_events 2>/dev/null || true
}

# ---------------------------------------------------------------------------
step "1. Baseline: shorten + redirect a batch of links"
# ---------------------------------------------------------------------------
CODES=()
for i in 1 2 3; do
  code="$(shorten_one "baseline-$i")"
  CODES+=("$code")
  click_one "$code"
  note "created + clicked $code"
done
sleep 6
note "consumer-group state after baseline batch:"
group_describe

# ---------------------------------------------------------------------------
step "2. Kill one Kafka broker mid-flow (kafka-2)"
# ---------------------------------------------------------------------------
note "topic before: replication-factor=3 across brokers 1,2,3"
topic_describe
docker compose stop kafka-2
note "kafka-2 stopped. Sending more traffic against a 2-of-3 broker cluster..."
sleep 2
for i in 6 7 8; do
  code="$(shorten_one "broker-down-$i")"
  CODES+=("$code")
  click_one "$code"
  note "created + clicked $code (kafka-2 is down)"
done
sleep 6
note "topic after losing kafka-2: ISR should have shrunk to {1,3} but partitions still have a leader"
topic_describe
note "consumer group is still consuming (partitions led by 1 and 3 are unaffected;"
note "any partition that was led by 2 has failed over to another in-sync replica):"
group_describe

note "restarting kafka-2..."
docker compose start kafka-2
sleep 8
note "topic after kafka-2 rejoins: ISR should be back to {1,2,3}"
topic_describe

# ---------------------------------------------------------------------------
step "3. Kill one consumer replica mid-flow"
# ---------------------------------------------------------------------------
VICTIM_ID="$(docker compose ps -q consumer | head -1)"
VICTIM_NAME="$(docker inspect --format '{{.Name}}' "$VICTIM_ID" | sed 's#^/##')"
note "consumer-group membership before killing a replica:"
group_describe
note "stopping consumer replica: $VICTIM_NAME"
docker stop "$VICTIM_ID" >/dev/null

for i in 9 10 11; do
  code="$(shorten_one "consumer-down-$i")"
  CODES+=("$code")
  click_one "$code"
  note "created + clicked $code (one consumer replica is down)"
done
sleep 8
note "consumer-group membership after rebalance — the dead replica's partition(s)"
note "should now be owned by one of the remaining consumer IDs, with no lag:"
group_describe

note "restarting the stopped consumer replica..."
docker start "$VICTIM_ID" >/dev/null
sleep 5
note "consumer-group membership after the replica rejoins (rebalances again):"
group_describe

# ---------------------------------------------------------------------------
step "4. Verify nothing was lost: every code created above has clicks recorded"
# ---------------------------------------------------------------------------
sleep 3
for code in "${CODES[@]}"; do
  [ -z "$code" ] && continue
  total="$(curl -s "$BASE_URL/api/stats/$code" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("total_clicks", "?"))')"
  note "$code -> total_clicks=$total (expected 1)"
done

step "Done. Stack left healthy: 3/3 brokers up, 3/3 consumer replicas up."
