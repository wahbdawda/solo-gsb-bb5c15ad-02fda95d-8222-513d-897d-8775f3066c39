#!/bin/sh
# Container entrypoint:
#   1. ensure Python dependencies are installed (already baked into the image;
#      also supports mounting source over /app for development),
#   2. create the SQLite database file and schema before serving traffic.
set -e

PYTHON="${PYTHON:-python}"

echo "[entrypoint] verifying Python dependencies..."
"$PYTHON" -c "import fastapi, uvicorn, pydantic" 2>/dev/null || {
    echo "[entrypoint] dependencies missing, installing..."
    "$PYTHON" -m pip install --no-cache-dir -r /app/requirements.txt
}

echo "[entrypoint] initialising SQLite database at ${DB_PATH:-/data/measurements.db}..."
"$PYTHON" -c "from app.database import init_db; init_db(); print('[entrypoint] database ready')"

echo "[entrypoint] starting: $*"
exec "$@"
