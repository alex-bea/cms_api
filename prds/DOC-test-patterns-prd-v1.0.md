# Test Patterns & Best Practices Guide (v1.0)

## 0. Overview
This document provides technical patterns and best practices for testing within the CMS API system, with a focus on database-backed tests, test isolation, and reliable test execution. It complements the QA Testing Standard (`STD-qa-testing-prd-v1.0.md`) and operational runbooks (`RUN-global-operations-prd-v1.0.md`) by providing concrete implementation guidance.

**Status:** Draft v1.0 (proposed)  
**Owners:** QA Guild & Platform Engineering  
**Consumers:** Engineering, QA, DevOps  
**Change control:** PR + QA Guild review  

**Cross-References:**
- **STD-qa-testing-prd-v1.0.md:** QA Testing Standard and requirements
- **RUN-global-operations-prd-v1.0.md:** Test harness dependency and operational procedures
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog and dependency map

---

## 1. Database Test Patterns

### 1.1 Database URL Patterns

#### Standard Test Database URLs
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

#### Why Dedicated Databases?
- **Isolation:** Prevents conflicts between test runs
- **Clean state:** Each test starts with a known baseline
- **Parallel execution:** Multiple test suites can run simultaneously
- **Schema conflicts:** Avoids app startup table creation vs Alembic migration conflicts

### 1.2 Test Lifecycle Order of Operations

#### Required Sequence
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

#### Critical Rules
- **Never mix app startup table creation with test fixtures**
- **Always use Alembic for schema management in tests**
- **Each test run gets a dedicated database**
- **Clean up after each test run**

### 1.3 Test Types and Database Requirements

#### Requires PostgreSQL
- **API tests** (JSONB, ARRAY, UUID types)
- **Integration tests** (real database connections)
- **Model tests** (type validation, constraints)
- **End-to-end tests** (full stack validation)

#### Can Use SQLite/Mocks
- **Unit tests** (business logic only)
- **Scraper tests** (HTTP mocking)
- **Calculator tests** (pure functions)
- **Parser tests** (data transformation)

---

## 2. Test Fixture Patterns

### 2.1 Database Fixtures
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

### 2.2 Data Fixtures
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

---

## 3. Common Anti-Patterns to Avoid

### 3.1 ❌ Don't Do This
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

### 3.2 ✅ Do This Instead
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

---

## 4. Troubleshooting

### 4.1 Common Issues
1. **"Table already exists"** → App startup table creation conflicts with Alembic
2. **"Column already exists"** → Duplicate migrations or app table creation
3. **"Connection refused"** → Database not ready, increase wait time
4. **"JSONB not supported"** → Using SQLite instead of PostgreSQL

### 4.2 Solutions
1. **Use dedicated test databases** for each environment
2. **Follow strict test lifecycle** order of operations
3. **Never mix app startup with test setup**
4. **Use Alembic for all schema management in tests**

---

## 5. CI/CD Integration

### 5.1 GitHub Actions Example
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

### 5.2 Environment-Specific Configuration
```yaml
# .github/workflows/test.yml
env:
  TEST_DATABASE_URL: postgresql://cms_user:cms_password@localhost:5432/cms_pricing_ci_${{ github.run_id }}
  
jobs:
  test:
    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_USER: cms_user
          POSTGRES_PASSWORD: cms_password
          POSTGRES_DB: cms_pricing_ci_${{ github.run_id }}
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
```

---

## 6. Performance Considerations

### 6.1 Test Database Optimization
- Use `scope="session"` for expensive fixtures
- Use `scope="function"` for data that needs isolation
- Consider database connection pooling for integration tests
- Use transactions with rollback for faster cleanup

### 6.2 Parallel Test Execution
- Each test run must use a unique database name
- Use pytest-xdist for parallel execution within a single run
- Ensure test isolation to prevent conflicts

---

## 7. Security Considerations

### 7.1 Test Data Security
- Never use production data in tests
- Use synthetic or anonymized test data
- Ensure test databases are properly isolated
- Clean up test data after execution

### 7.2 Database Credentials
- Use dedicated test database credentials
- Never commit database credentials to version control
- Use environment variables for configuration
- Rotate test credentials regularly

---

## 8. Monitoring and Observability

### 8.1 Test Metrics
- Track test execution time
- Monitor database connection usage
- Measure test reliability and flakiness
- Alert on test failures and performance degradation

### 8.2 Debugging
- Enable SQL query logging for failed tests
- Use database transaction logs for debugging
- Capture test execution traces
- Store test artifacts for post-mortem analysis

---

## 9. References

### 9.1 Related Documents
- **STD-qa-testing-prd-v1.0.md:** QA Testing Standard
- **RUN-global-operations-prd-v1.0.md:** Test Harness Dependency
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog

### 9.2 Implementation Files
- **scripts/test_with_postgres.sh:** Test script wrapper
- **tests/scripts/bootstrap_test_db.py:** Database bootstrap script
- **tests/conftest.py:** Test configuration and fixtures
- **tests/patterns/database_test_patterns.md:** Detailed technical patterns

### 9.3 External Resources
- [pytest documentation](https://docs.pytest.org/)
- [SQLAlchemy testing guide](https://docs.sqlalchemy.org/en/20/orm/session_transaction.html#joining-a-session-into-an-external-transaction-such-as-for-test-suites)
- [PostgreSQL testing best practices](https://www.postgresql.org/docs/current/runtime-config-logging.html)

---

## 10. Change Log

| Version | Date | Changes |
|---------|------|---------|
| v1.0 | 2025-01-27 | Initial version with database test patterns and best practices |

---

## 11. Appendix: Quick Reference

### 11.1 Database Test Checklist
- [ ] Use dedicated test database URL
- [ ] Follow strict test lifecycle order
- [ ] Use Alembic for schema management
- [ ] Avoid app startup table creation conflicts
- [ ] Clean up after test execution
- [ ] Use appropriate fixture scopes
- [ ] Ensure test isolation
- [ ] Monitor test performance

### 11.2 Common Commands
```bash
# Start test database
docker compose up -d db

# Bootstrap test database
python tests/scripts/bootstrap_test_db.py --database-url "$TEST_DATABASE_URL"

# Run tests
python -m pytest tests/api/ --database-url "$TEST_DATABASE_URL"

# Cleanup
docker compose down db
```

### 11.3 Environment Variables
```bash
# Required
TEST_DATABASE_URL=postgresql://cms_user:cms_password@localhost:5432/cms_pricing_test

# Optional
PG_WAIT_TIMEOUT=120
PG_WAIT_INTERVAL=3
DOCKER_COMPOSE_BIN=docker compose
ALEMBIC_INI=alembic.ini
```
