# FastAPI Backtesting PoC

A REST API for financial market backtesting, built with FastAPI, Supabase, MinIO, and DuckDB. This API will power a frontend that allows users to create, manage, and analyze trading strategy backtests with real market data.

## Table of Contents
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Running the Application](#running-the-application)
- [API Documentation](#api-documentation)
- [Project Structure](#project-structure)
- [Development Guide](#development-guide)
- [Deployment](#deployment)
- [Troubleshooting](#troubleshooting)

## Features

- **Authentication**: JWT-based authentication using Supabase
- **Backtest Management**: Create, update, and track trading strategy backtests
- **Trade Recording**: Log individual trades within backtests
- **Strategy Templates**: Save and reuse trading strategies
- **Market Data**: Access OHLCV (Open, High, Low, Close, Volume) data for multiple symbols
- **High Performance**: DuckDB integration for fast time-series queries
- **Storage**: MinIO integration for efficient market data storage
- **API Documentation**: Auto-generated Swagger/OpenAPI documentation

## Market Data Architecture

The application uses a high-performance architecture for market data:

1. **Storage**: Market data is stored as Parquet files in MinIO (S3-compatible)
2. **Query Engine**: DuckDB queries Parquet files directly from S3 without loading into memory
3. **Data Format**: 
   - Primary structure: `ohlcv/1Y/symbol={SYMBOL}/year={YYYY}/{SYMBOL}_{YYYY}.parquet`
   - Each yearly file contains 1-minute resolution data for that entire year
   - Legacy structure (being decommissioned): `ohlcv/1m/symbol={SYMBOL}/date={YYYY-MM-DD}/...`
4. **Caching**: Historical data is cached; current day data has short TTL

This architecture enables:
- Sub-second queries on years of 1-minute data
- Efficient storage (one file per year vs 365 files)
- Minimal memory usage through columnar format
- Easy horizontal scaling
- Cost-effective storage

## Prerequisites

Before you begin, ensure you have the following installed:

- **Python 3.11+**
- **Git** (for cloning the repository)

You'll also need accounts for:
- **Supabase** (provides authentication and PostgreSQL database)
- **MinIO** or S3-compatible storage (required for market data features)

Optional but recommended:
- **Docker** (for containerized deployment)
- **UV** (fast Python package installer)

## Quick Start

### 1. Clone the Repository

```bash
git clone <your-repository-url>
cd fastapi-poc-backend
```

### 2. Set Up Supabase

Since the application uses Supabase for both authentication and database:

1. **Create a Supabase Project**
   - Go to [supabase.com](https://supabase.com) and create a new project
   - Wait for the project to finish setting up

2. **Get Your Connection Details**
   - In your Supabase dashboard, go to **Settings → API**
   - Copy the **URL** (this is your `SUPABASE_URL`)
   - Copy the **JWT Secret** (this is your `SUPABASE_JWT_SECRET`)
   
3. **Get Your Database URL**
   - Go to **Settings → Database**
   - Copy the **Connection String** (URI) - this is your `DATABASE_URL`
   - Make sure to use the connection string with the password included

### 3. Set Up Python Environment

#### Option A: Using UV (Recommended - Faster)
```bash
# Install UV
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment
uv venv

# Activate virtual environment
# On Linux/Mac:
source .venv/bin/activate
# On Windows:
.venv\Scripts\activate

# Install dependencies
uv pip install -r requirements.txt
```

#### Option B: Using pip (Traditional)
```bash
# Create virtual environment
python -m venv .venv

# Activate virtual environment
# On Linux/Mac:
source .venv/bin/activate
# On Windows:
.venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Copy the example environment file and update with your values:

```bash
cp .env.example .env
```

Edit `.env` with your actual values:

```env
# Supabase Configuration (all from your Supabase dashboard)
DATABASE_URL="postgresql://postgres:[YOUR-PASSWORD]@db.[YOUR-PROJECT-REF].supabase.co:5432/postgres"
SUPABASE_URL="https://[YOUR-PROJECT-REF].supabase.co"
SUPABASE_JWT_SECRET="your-supabase-jwt-secret"

# Market Data Storage (required for OHLCV features)
MINIO_ENDPOINT="localhost:9000"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"
MINIO_BUCKET="dukascopy-node"

# Application Settings
LOG_LEVEL="INFO"
ENVIRONMENT="development"
```

### 5. Run Database Migrations

The application uses Alembic to manage database schema. Run migrations on your Supabase database:

```bash
alembic upgrade head
```

This will create all necessary tables in your Supabase PostgreSQL database.

### 6. Set Up Market Data Storage (Required for OHLCV Features)

The API uses MinIO (S3-compatible storage) for market data. You have two options:

#### Option A: Use Existing MinIO with Data
If you have access to a MinIO instance with market data already loaded in the correct format.

#### Option B: Set Up Local MinIO
```bash
# Using Docker
docker run -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  minio/minio server /data --console-address ":9001"
```

**Note**: You'll need to populate MinIO with market data in Parquet format following the structure:
- `ohlcv/1Y/symbol={SYMBOL}/year={YYYY}/{SYMBOL}_{YYYY}.parquet` (contains 1-minute resolution data for the entire year)

The system can aggregate this 1-minute data to any larger timeframe (5m, 15m, 1h, 1d, etc.) on the fly using DuckDB.

### 7. Start the Application

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

The API will be available at `http://localhost:8000`

### 8. Verify Installation

1. Check health endpoint: `http://localhost:8000/api/v1/health/`
2. View API documentation: `http://localhost:8000/docs`
3. Test with a simple curl (requires valid JWT token):
   ```bash
   curl -H "Authorization: Bearer <your-token>" http://localhost:8000/api/v1/users/
   ```

## Configuration

### Required Environment Variables

| Variable | Description | Where to Find |
|----------|-------------|---------------|
| `DATABASE_URL` | Supabase PostgreSQL connection string | Supabase Dashboard → Settings → Database → Connection String |
| `SUPABASE_URL` | Your Supabase project URL | Supabase Dashboard → Settings → API → URL |
| `SUPABASE_JWT_SECRET` | Supabase JWT secret for token verification | Supabase Dashboard → Settings → API → JWT Secret |

### Market Data Storage (Required for OHLCV endpoints)

| Variable | Description | Default |
|----------|-------------|---------|
| `MINIO_ENDPOINT` | MinIO server endpoint | `localhost:9000` |
| `MINIO_ACCESS_KEY` | MinIO access key | None |
| `MINIO_SECRET_KEY` | MinIO secret key | None |
| `MINIO_SECURE` | Use HTTPS for MinIO | `false` |
| `MINIO_BUCKET` | Bucket containing market data | `dukascopy-node` |

### Optional Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `LOG_LEVEL` | Logging level | `INFO` |
| `LOG_FORMAT` | Log format (json/text) | `json` |
| `DB_POOL_MIN` | Min database connections | `1` |
| `DB_POOL_MAX` | Max database connections | `5` |

## Running the Application

### Development Mode

With auto-reload enabled (recommended for development):

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### Production Mode

Without auto-reload:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 80
```

### Using Docker

Docker provides several benefits for running the API:
- **Consistency**: Same environment across all deployments
- **No Python conflicts**: Container includes the exact Python version needed
- **Easy deployment**: One command to run anywhere
- **Isolated dependencies**: No conflicts with your system packages

**Important**: Docker containers still need to connect to external services (Supabase, MinIO), so you'll still need those set up.

Build and run with Docker:

```bash
# Build the image
docker build -t backtesting-api .

# Run the container
docker run -p 8000:80 \
  -e DATABASE_URL="postgresql://postgres:[YOUR-PASSWORD]@db.[YOUR-PROJECT].supabase.co:5432/postgres" \
  -e SUPABASE_URL="https://[YOUR-PROJECT].supabase.co" \
  -e SUPABASE_JWT_SECRET="your-secret" \
  -e MINIO_ENDPOINT="host.docker.internal:9000" \
  -e MINIO_ACCESS_KEY="minioadmin" \
  -e MINIO_SECRET_KEY="minioadmin" \
  backtesting-api
```

**Note**: 
- Use `host.docker.internal` for MinIO on Mac/Windows
- On Linux, use your host's actual IP address or configure Docker networking

## API Documentation

Once the application is running, you can access:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI Schema**: http://localhost:8000/openapi.json

### Main API Endpoints

#### Authentication
All endpoints (except health checks) require a Bearer token in the Authorization header:

```
Authorization: Bearer <your-jwt-token>
```

**Getting a JWT Token**: You'll need to authenticate through Supabase Auth first. The token you receive from Supabase login can be used with this API. See [Supabase Auth documentation](https://supabase.com/docs/guides/auth) for details on obtaining tokens.

#### Health Checks
- `GET /api/v1/health/` - Basic health check
- `GET /api/v1/health/detailed` - Detailed health with dependencies
- `GET /api/v1/health/ready` - Readiness check for deployments

#### Backtests
- `POST /api/v1/backtests` - Create a new backtest
- `GET /api/v1/backtests` - List all backtests for the user
- `GET /api/v1/backtests/{backtest_id}` - Get specific backtest
- `PUT /api/v1/backtests/{backtest_id}` - Update backtest
- `DELETE /api/v1/backtests/{backtest_id}` - Delete backtest

#### Trades
- `POST /api/v1/trades` - Create a new trade
- `GET /api/v1/trades/backtest/{backtest_id}` - Get trades for a backtest

#### Strategies
- `POST /api/v1/strategies` - Create a strategy
- `GET /api/v1/strategies` - List strategies
- `GET /api/v1/strategies/{strategy_id}` - Get specific strategy
- `PUT /api/v1/strategies/{strategy_id}` - Update strategy
- `DELETE /api/v1/strategies/{strategy_id}` - Delete strategy

#### Market Data (OHLCV)
- `GET /api/v1/ohlcv/symbols` - List available symbols
- `GET /api/v1/ohlcv/data` - Get OHLCV data (supports any timeframe: 1m, 5m, 15m, 1h, 1d, etc.)
- `POST /api/v1/ohlcv/data` - Get OHLCV data (with body)
- `GET /api/v1/ohlcv/timeframes` - List available timeframes

**Note**: All data is stored at 1-minute resolution in yearly files. The API automatically aggregates to the requested timeframe.

### Example API Calls

#### Create a Backtest
```bash
curl -X POST http://localhost:8000/api/v1/backtests \
  -H "Authorization: Bearer <your-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "My First Backtest",
    "strategy": "Moving Average Crossover",
    "symbol": "BTC",
    "start_date": "2023-01-01",
    "end_date": "2023-12-31",
    "initial_capital": 10000
  }'
```

#### Get Market Data
```bash
curl -X GET "http://localhost:8000/api/v1/ohlcv/data?symbol=BTC&start_date=2023-01-01&end_date=2023-01-31&timeframe=1d&source_resolution=1Y" \
  -H "Authorization: Bearer <your-token>"
```

The `source_resolution=1Y` parameter tells the API to use the yearly files (recommended). The `timeframe` parameter controls the output aggregation (1m, 5m, 15m, 1h, 1d, etc.).

## Architecture Overview

The application follows a clean, layered architecture:

### Core Layers

1. **API Layer** (`app/api/v1/`)
   - FastAPI endpoints that handle HTTP requests
   - Input validation using Pydantic models
   - Authentication via JWT tokens
   - Returns standardized responses

2. **Service Layer** (`app/services/`)
   - Business logic and orchestration
   - Coordinates between repositories
   - Handles complex operations and validations
   - No direct database access

3. **Repository Layer** (`app/repositories/`)
   - Data access abstraction
   - All SQL queries live here
   - Returns simple data structures
   - Handles database transactions

4. **Infrastructure Layer** (`app/infrastructure/`)
   - External service adapters (DuckDB, Cache)
   - Performance monitoring
   - Low-level technical concerns

### Key Design Principles

- **Separation of Concerns**: Each layer has a specific responsibility
- **Dependency Injection**: Services receive repositories as dependencies
- **Domain Exceptions**: Custom exceptions for better error handling
- **Transaction Support**: Database operations use proper transaction boundaries

To see the current folder structure, run:
```bash
tree -d app/
```

## Development Guide

### Adding a New Endpoint

1. **Create the Pydantic models** in `app/models.py`:
```python
class MyNewModel(BaseModel):
    field1: str
    field2: int
```

2. **Create the repository** in `app/repositories/`:
```python
class MyRepository(BaseRepository):
    async def create(self, data: dict) -> dict:
        # Database operations
        pass
```

3. **Create the service** in `app/services/`:
```python
class MyService:
    def __init__(self, repository: MyRepository = None):
        self.repository = repository or MyRepository()
    
    async def process_data(self, data: MyNewModel):
        # Business logic
        return await self.repository.create(data.dict())
```

4. **Create the endpoint** in `app/api/v1/`:
```python
@router.post("/my-endpoint")
async def create_something(
    data: MyNewModel,
    user_id: str = Depends(verify_token)
):
    service = MyService()
    return await service.process_data(data)
```

### Running Tests

```bash
# Install test dependencies
pip install pytest pytest-asyncio

# Run tests
pytest

# Run with coverage
pytest --cov=app
```

### Database Migrations

Create a new migration:
```bash
alembic revision -m "Description of changes"
```

Apply migrations:
```bash
alembic upgrade head
```

Rollback migrations:
```bash
alembic downgrade -1
```

## Deployment

### Using Docker

See the [Using Docker](#using-docker) section above for running with Docker. Remember that the container still needs to connect to your Supabase project and MinIO instance.

### Deploying to Coolify

1. **Connect your repository** to Coolify
2. **Set environment variables** in Coolify's interface:
   - All Supabase connection details
   - MinIO configuration if using market data features
3. **Configure build settings**:
   - Build command: `pip install -r requirements.txt`
   - Start command: `./start.sh`
4. **Deploy** and monitor logs

### Deploying to Other Platforms

The application can be deployed to any platform that supports Python/Docker:
- **Railway/Render**: Use the Dockerfile
- **Heroku**: Add a `Procfile` with `web: uvicorn app.main:app --host 0.0.0.0 --port $PORT`
- **AWS/GCP/Azure**: Use container services or VMs

Remember: All deployments need access to:
- Supabase (for auth and database)
- MinIO or S3 (for market data features)



### Getting Help

1. Check the API documentation at `/docs`
2. Review logs with structured JSON output
3. Enable debug logging: `LOG_LEVEL=DEBUG`
4. Test Supabase connection: 
   ```bash
   psql "$DATABASE_URL" -c "SELECT 1"
   ```
5. Verify MinIO access:
   ```bash
   curl http://localhost:9000/minio/health/live
   ```

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make your changes and add tests
4. Commit with clear messages: `git commit -m "Add new feature"`
5. Push to your fork: `git push origin feature/my-feature`
6. Create a Pull Request
