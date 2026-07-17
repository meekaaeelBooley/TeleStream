#!/usr/bin/env bash
# Capture PNGs of every provisioned dashboard into docs/img/.
# Requires the stack running with the render overlay:
#   docker compose -f docker-compose.yml -f docker-compose.render.yml up -d
set -euo pipefail

GRAFANA="${GRAFANA_URL:-http://localhost:3000}"
AUTH="${GRAFANA_AUTH:-admin:admin}"
OUT="$(dirname "$0")/../docs/img"
mkdir -p "$OUT"

DASHBOARDS=(executive network sales customer pipeline)

for dash in "${DASHBOARDS[@]}"; do
  echo "rendering $dash..."
  curl -fsS -u "$AUTH" -o "$OUT/dashboard-$dash.png" \
    "$GRAFANA/render/d/telestream-$dash/x?kiosk&width=1600&height=1000&from=now-30m&to=now&timeout=60"
done

echo "wrote ${#DASHBOARDS[@]} screenshots to $OUT"
