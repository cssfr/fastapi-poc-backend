#!/bin/sh
echo "Starting FastAPI Backtesting API..."
echo "DATABASE_URL is set: ${DATABASE_URL:+Yes}"
echo "Running database migrations..."
alembic upgrade head || { echo "Migration failed!"; exit 1; }
echo "Migrations complete. Starting server..."
exec uvicorn app.main:app --host 0.0.0.0 --port 80
# exec uvicorn app.main:app --host 0.0.0.0 --port 80 