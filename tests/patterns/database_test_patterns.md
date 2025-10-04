# Database Test Patterns

This document defines patterns for database-backed tests to prevent conflicts and ensure reliable test execution.

## 1. Database URL Patterns

### Standard Test Database URLs
```bash
# Development
TEST_DATABASE_URL_DEV=postgresql://cms_user:cms_password@localhost:5432/cms_pricing_test

# CI/CD (unique per build)
TEST_DATABASE_URL_CI=postgresql://cms_user:cms_password@localhost:5432/cms_pricing_ci_${BUILD_ID}

# Local development
TEST_DATABASE_URL_LOCAL=postgresql://cms_user:cms_password@localhost:5432/cms_pricing_local

# Integration tests
TEST_DATABASE_URL_INTEGRATION=postgresql://cms_user:cms_password@localhost:5432/cms_pricing_integration
```

### Why Dedicated Databases?
- **Isolation:** Prevents conflicts between test runs
- **Clean state:** Each test starts with a known baseline
- **Parallel execution:** Multiple test suites can run simultaneously
- **Schema conflicts:** Avoids app startup table creation vs Alembic migration conflicts

## 2. Test Lifecycle Order of Operations

### Required Sequence
```bash
# 1. Environment Setup
export TEST_DATABASE_URL=postgresql://cms_user:cms_password@localhost:5432/cms_pricing_test

# 2. Infrastructure Provisioning
docker compose up -d db
# OR: provision managed Postgres instance

# 3. Schema Bootstrap (Alembic only)
python tests/scripts/bootstrap_test_db.py --database-url "$TEST_DATABASE_URL"

# 4. Test Execution
python -m pytest tests/api/ tests/integration/ --database-url "$TEST_DATABASE_URL"

# 5. Cleanup
docker compose down db
```

### Critical Rules
- **Never mix app startup table creation with test fixtures**
- **Always use Alembic for schema management in tests**
- **Each test run gets a dedicated database**
- **Clean up after each test run**

## 3. Test Types and Database Requirements

### Requires PostgreSQL
- **API tests** (JSONB, ARRAY, UUID types)
- **Integration tests** (real database connections)
- **Model tests** (type validation, constraints)
- **End-to-end tests** (full stack validation)

### Can Use SQLite/Mocks
- **Unit tests** (business logic only)
- **Scraper tests** (HTTP mocking)
- **Calculator tests** (pure functions)
- **Parser tests** (data transformation)

## 4. Test Fixture Patterns

### Database Fixtures
```python
@pytest.fixture(scope="session")
def test_engine():
    """Create a test database engine with PostgreSQL compatibility"""
    database_url = settings.test_database_url
    engine = create_engine(
        database_url,
        echo=False,
        pool_size=5,
        max_overflow=10
    )
    
    # Tables are already created by Alembic migrations
    # No need to create them again
    
    return engine

@pytest.fixture(scope="function")
def test_db_session(test_engine):
    """Create a test database session with automatic cleanup"""
    Session = sessionmaker(bind=test_engine)
    session = Session()
    
    try:
        yield session
    finally:
        session.close()
```

### Data Fixtures
```python
@pytest.fixture
def sample_plan_data():
    """Sample plan data for testing"""
    return {
        "name": "Test Plan",
        "description": "A test plan",
        "metadata": {"test": True},
        "created_by": "test_user"
    }

@pytest.fixture
def sample_plan(test_db_session, sample_plan_data):
    """Create a sample plan in the database"""
    plan = Plan(**sample_plan_data)
    test_db_session.add(plan)
    test_db_session.commit()
    test_db_session.refresh(plan)
    return plan
```

## 5. Common Anti-Patterns to Avoid

### ❌ Don't Do This
```python
# Don't create tables in test fixtures
@pytest.fixture
def test_db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)  # Conflicts with Alembic
    return engine

# Don't use shared database URLs
TEST_DATABASE_URL=postgresql://cms_user:cms_password@localhost:5432/cms_pricing

# Don't mix app startup with test setup
def test_something():
    # App creates tables on startup
    app = create_app()
    # Test tries to create same tables
    Base.metadata.create_all(engine)  # Conflict!
```

### ✅ Do This Instead
```python
# Use dedicated test database
TEST_DATABASE_URL=postgresql://cms_user:cms_password@localhost:5432/cms_pricing_test

# Let Alembic handle schema
@pytest.fixture
def test_db():
    # Tables already exist from bootstrap_test_db.py
    engine = create_engine(settings.test_database_url)
    return engine

# Use proper test lifecycle
def test_something():
    # Database is already set up by test harness
    # Just use the fixtures
    pass
```

## 6. Troubleshooting

### Common Issues
1. **"Table already exists"** → App startup table creation conflicts with Alembic
2. **"Column already exists"** → Duplicate migrations or app table creation
3. **"Connection refused"** → Database not ready, increase wait time
4. **"JSONB not supported"** → Using SQLite instead of PostgreSQL

### Solutions
1. **Use dedicated test databases** for each environment
2. **Follow strict test lifecycle** order of operations
3. **Never mix app startup with test setup**
4. **Use Alembic for all schema management in tests**

## 7. CI/CD Integration

### GitHub Actions Example
```yaml
- name: Setup Test Database
  run: |
    export TEST_DATABASE_URL=postgresql://cms_user:cms_password@localhost:5432/cms_pricing_ci_${{ github.run_id }}
    docker compose up -d db
    python tests/scripts/bootstrap_test_db.py --database-url "$TEST_DATABASE_URL"

- name: Run Tests
  run: |
    export TEST_DATABASE_URL=postgresql://cms_user:cms_password@localhost:5432/cms_pricing_ci_${{ github.run_id }}
    python -m pytest tests/api/ tests/integration/

- name: Cleanup
  run: docker compose down db
```

## 8. References

- **STD-qa-testing-prd-v1.0.md:** QA Testing Standard
- **RUN-global-operations-prd-v1.0.md:** Test Harness Dependency
- **scripts/test_with_postgres.sh:** Test script wrapper
- **tests/scripts/bootstrap_test_db.py:** Database bootstrap script
