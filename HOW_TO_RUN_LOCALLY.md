# How to Run CMS Pricing API Locally

This guide walks you through setting up and running the CMS Pricing API on your local machine.

## 🚀 **Quick Start (Docker - Recommended)**

The easiest way to get started is using Docker Compose, which sets up all services automatically.

### **Prerequisites**
- Docker Desktop installed and running
- Git (to clone the repository)

### **Step 1: Clone and Navigate**
```bash
git clone https://github.com/alex-bea/cms_api.git
cd cms_api
```

### **Step 2: Start All Services**
```bash
# Start PostgreSQL, Redis, API, and Worker
docker-compose up -d

# Check that all services are running
docker-compose ps
```

### **Step 3: Run Database Migrations**
```bash
# Apply database migrations
docker-compose exec api alembic upgrade head
```

### **Step 4: Verify Everything Works**
```bash
# Test health check
curl -H "X-API-Key: dev-key-123" http://localhost:8000/healthz

# Test readiness check
curl -H "X-API-Key: dev-key-123" http://localhost:8000/readyz
```

### **Step 5: Explore the API**
- **Interactive API Documentation**: http://localhost:8000/docs
- **Alternative Documentation**: http://localhost:8000/redoc
- **OpenAPI JSON**: http://localhost:8000/openapi.json

🎉 **You're done!** The API is running at `http://localhost:8000`

---

## 🛠️ **Manual Setup (Advanced)**

If you prefer to run services manually or need to customize the setup:

### **Prerequisites**
- Python 3.11+
- PostgreSQL 15+
- Redis (optional, for caching)
- Poetry (for dependency management)

### **Step 1: Install Dependencies**
```bash
# Install Poetry if you don't have it
curl -sSL https://install.python-poetry.org | python3 -

# Install project dependencies
poetry install

# Activate the virtual environment
poetry shell
```

### **Step 2: Set Up Environment**
```bash
# Copy environment template
cp env.example .env

# Edit .env with your settings
# At minimum, update DATABASE_URL if needed
```

### **Step 3: Start Database Services**

#### **Option A: Using Docker (Just Databases)**
```bash
# Start PostgreSQL
docker run -d --name postgres \
  -e POSTGRES_DB=cms_pricing \
  -e POSTGRES_USER=cms_user \
  -e POSTGRES_PASSWORD=cms_password \
  -p 5432:5432 \
  postgres:15-alpine

# Start Redis
docker run -d --name redis \
  -p 6379:6379 \
  redis:7-alpine
```

#### **Option B: Local Installation**
```bash
# Install PostgreSQL locally (macOS)
brew install postgresql@15
brew services start postgresql@15

# Create database
createdb cms_pricing

# Install Redis (macOS)
brew install redis
brew services start redis
```

### **Step 4: Run Migrations**
```bash
# Apply database migrations
poetry run alembic upgrade head
```

### **Step 5: Start the API**
```bash
# Start the development server
poetry run uvicorn cms_pricing.main:app --host 0.0.0.0 --port 8000 --reload
```

### **Step 6: Start Background Worker (Optional)**
```bash
# In a separate terminal
poetry run python -m cms_pricing.worker
```

---

## 🧪 **Testing the API**

### **Basic Health Checks**
```bash
# Health check
curl -H "X-API-Key: dev-key-123" http://localhost:8000/healthz

# Readiness check
curl -H "X-API-Key: dev-key-123" http://localhost:8000/readyz

# Metrics (requires API key)
curl -H "X-API-Key: dev-key-123" http://localhost:8000/metrics
```

### **Create a Treatment Plan**
```bash
curl -X POST -H "X-API-Key: dev-key-123" -H "Content-Type: application/json" \
  http://localhost:8000/plans \
  -d '{
    "name": "Outpatient Knee Surgery",
    "description": "Total knee arthroscopy outpatient procedure",
    "components": [
      {
        "code": "27447",
        "setting": "OPPS",
        "units": 1,
        "professional_component": true,
        "facility_component": true,
        "sequence": 1
      },
      {
        "code": "99213",
        "setting": "MPFS",
        "units": 1,
        "professional_component": true,
        "facility_component": false,
        "sequence": 2
      }
    ]
  }'
```

### **Price a Single Code**
```bash
curl -H "X-API-Key: dev-key-123" \
  "http://localhost:8000/pricing/codes/price?zip=94110&code=99213&setting=MPFS&year=2025"
```

### **Resolve Geography**
```bash
curl -H "X-API-Key: dev-key-123" \
  "http://localhost:8000/geography/resolve?zip=94110"
```

### **Compare Locations**
```bash
curl -X POST -H "X-API-Key: dev-key-123" -H "Content-Type: application/json" \
  http://localhost:8000/pricing/compare \
  -d '{
    "zip_a": "94110",
    "zip_b": "73301",
    "plan_id": "your-plan-id-from-above",
    "year": 2025,
    "quarter": "1"
  }'
```

---

## 🔧 **Development Commands**

### **Using the Makefile**
```bash
# Run tests
make test

# Run golden tests (pricing parity validation)
make test-golden

# Format code
make format

# Run linting
make lint

# Start development server
make dev

# View Docker logs
make docker-logs
```

### **Using Poetry Directly**
```bash
# Run tests
poetry run pytest

# Run specific test file
poetry run pytest tests/test_plans.py -v

# Run with coverage
poetry run pytest --cov=cms_pricing --cov-report=html

# Start development server
poetry run uvicorn cms_pricing.main:app --reload

# Run CLI tool
poetry run python -m cms_pricing.cli status
```

---

## 📊 **Data Ingestion**

### **Test Data Ingestion**
```bash
# Test MPFS ingestion
poetry run python -m cms_pricing.cli ingestion ingest --dataset MPFS --year 2025

# Test OPPS ingestion
poetry run python -m cms_pricing.cli ingestion ingest --dataset OPPS --year 2025 --quarter 1

# List ingestion tasks
poetry run python -m cms_pricing.cli ingestion list-tasks

# Show system status
poetry run python -m cms_pricing.cli status
```

### **Comprehensive Ingestion**
```bash
# Ingest all datasets for 2025
poetry run python scripts/ingest_all.py --all --year 2025

# Ingest specific dataset
poetry run python scripts/ingest_all.py --dataset MPFS --year 2025

# Set up automated ingestion
./scripts/setup_cron.sh
```

---

## 🐛 **Troubleshooting**

### **Common Issues**

#### **Port Already in Use**
```bash
# Kill process on port 8000
lsof -ti:8000 | xargs kill -9

# Or use a different port
poetry run uvicorn cms_pricing.main:app --port 8001 --reload
```

#### **Database Connection Issues**
```bash
# Check if PostgreSQL is running
docker ps | grep postgres

# Or restart the database
docker-compose restart db

# Check database connection
poetry run python -c "from cms_pricing.database import engine; print(engine.execute('SELECT 1').scalar())"
```

#### **Permission Issues**
```bash
# Make scripts executable
chmod +x scripts/*.sh
chmod +x scripts/*.py

# Fix Poetry permissions
poetry config virtualenvs.in-project true
poetry install
```

#### **Missing Dependencies**
```bash
# Reinstall everything
poetry install --sync

# Update dependencies
poetry update
```

### **Check Service Status**
```bash
# Docker services
docker-compose ps

# Check logs
docker-compose logs api
docker-compose logs db
docker-compose logs redis

# Check API health
curl -H "X-API-Key: dev-key-123" http://localhost:8000/healthz
```

### **Reset Everything**
```bash
# Stop all services
docker-compose down

# Remove volumes (WARNING: deletes all data)
docker-compose down -v

# Start fresh
docker-compose up -d
docker-compose exec api alembic upgrade head
```

---

## 📁 **Project Structure**

```
cms_api/
├── cms_pricing/           # Main application code
│   ├── engines/          # Pricing engines (MPFS, OPPS, etc.)
│   ├── ingestion/        # Data ingestion system
│   ├── models/           # SQLAlchemy database models
│   ├── routers/          # FastAPI route handlers
│   ├── schemas/          # Pydantic request/response schemas
│   ├── services/         # Business logic services
│   └── main.py           # FastAPI application
├── tests/                # Test suite
├── scripts/              # Utility scripts
├── examples/             # Usage examples
├── docker-compose.yml    # Docker services configuration
├── Dockerfile            # Container image definition
├── pyproject.toml        # Python dependencies and metadata
├── README.md             # Project overview
├── INGESTION_GUIDE.md    # Data ingestion documentation
└── HOW_TO_RUN_LOCALLY.md # This file
```

---

## 🎯 **Next Steps**

Once you have the API running locally:

1. **Explore the API Documentation** at http://localhost:8000/docs
2. **Create treatment plans** and price them
3. **Test location comparisons** (San Francisco vs Dallas)
4. **Set up data ingestion** for real CMS datasets
5. **Run the test suite** to verify everything works
6. **Deploy to production** when ready

## 📞 **Getting Help**

- **API Documentation**: http://localhost:8000/docs (when running)
- **Project README**: See README.md for comprehensive overview
- **Ingestion Guide**: See INGESTION_GUIDE.md for data ingestion details
- **Test Examples**: See tests/ directory for usage examples
- **CLI Help**: `poetry run python -m cms_pricing.cli --help`

---

**Happy coding!** 🚀
