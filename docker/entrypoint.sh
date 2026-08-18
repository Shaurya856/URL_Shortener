#!/bin/sh
set -e

echo "Waiting for Postgres at ${POSTGRES_HOST:-postgres}:${POSTGRES_PORT:-5432}..."
until python - <<'PYEOF'
import os
import socket
import sys

host = os.environ.get("POSTGRES_HOST", "postgres")
port = int(os.environ.get("POSTGRES_PORT", "5432"))
try:
    with socket.create_connection((host, port), timeout=2):
        sys.exit(0)
except OSError:
    sys.exit(1)
PYEOF
do
  sleep 1
done
echo "Postgres is up."

if [ "$1" = "gunicorn" ]; then
  python manage.py migrate --noinput
  python manage.py collectstatic --noinput --clear
elif [ "$2" = "consume_clicks" ]; then
  # Consumer container: wait for the web container to run migrations
  # rather than racing it, since both start from the same image.
  echo "Waiting for schema (migrations applied by web service)..."
  until python manage.py migrate --check >/dev/null 2>&1; do
    sleep 2
  done
fi

exec "$@"
