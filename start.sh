#!/bin/sh
echo "Starting FastAPI Backtesting API..."
echo "DATABASE_URL is set: ${DATABASE_URL:+Yes}"
echo "Running database migrations..."
alembic upgrade head || { echo "Migration failed!"; exit 1; }
echo "Migrations complete. Starting server..."
exec uvicorn app.main:app --host 0.0.0.0 --port 80 --forwarded-allow-ips "192.168.1.1,10.0.0.0/8" --proxy-headers
# exec uvicorn app.main:app --host 0.0.0.0 --port 80 