#!/usr/bin/env bash
# 2026-05-09: Updated dashboard import command to `superset import_dashboards` (underscore) and added `--username admin`.
set -euo pipefail

POSTGRES_URI="postgresql+psycopg2://media:media@postgres:5432/media"
DASH_DIR="/app/superset/dashboards"

superset shell -c "
from superset import db
from superset.models.core import Database

uri = '${POSTGRES_URI}'
name = 'media-postgres'

db_obj = db.session.query(Database).filter(Database.database_name == name).one_or_none()
if db_obj is None:
    db_obj = Database(database_name=name)
    db.session.add(db_obj)

db_obj.sqlalchemy_uri = uri
# allow DDL/queries
try:
    db_obj.allow_run_async = True
except Exception:
    pass

db.session.commit()
print('OK - database upserted:', name)
" || true

if [ -d "${DASH_DIR}" ]; then
  for f in "${DASH_DIR}"/*.json; do
    if [ -f "$f" ]; then
      superset import_dashboards -p "$f" --username admin || true
    fi
  done
fi

echo "OK - superset init.sh done"
