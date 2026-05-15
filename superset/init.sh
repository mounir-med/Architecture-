#!/usr/bin/env bash
# 2026-05-09: Updated dashboard import command to `superset import_dashboards` (underscore) and added `--username admin`.
set -euo pipefail

POSTGRES_URI="postgresql+psycopg2://media:media@postgres:5432/media"
DASH_DIR="/app/docker-init/dashboards"

# Import datasets (Legacy YAML)
if [ -f "/app/docker-init/datasources.yaml" ]; then
  echo "Importing legacy datasources..."
  superset legacy-import-datasources -p /app/docker-init/datasources.yaml || true
fi

# Import dashboards (Legacy JSON)
if [ -d "${DASH_DIR}" ]; then
  for f in "${DASH_DIR}"/*.json; do
    if [ -f "$f" ]; then
      echo "Importing legacy dashboard: $f"
      superset legacy-import-dashboards -p "$f" -u admin || true
    fi
  done
fi

echo "OK - superset init.sh done"
