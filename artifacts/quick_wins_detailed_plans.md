# Quick Wins: Detailed Implementation Plans

**Date:** 2025-10-31  
**Status:** Planning  
**Estimated Total Effort:** 16-22 hours across 3 tasks

---

## Quick Win #1: Dataset Snapshots Table (4-6 hours)

### Overview
Create a `dataset_snapshots` table to serve as a registry of available dataset versions. This enables deterministic snapshot selection and completes the provenance tracking story.

### Current State
- `snapshots` table exists (`cms_pricing/models/snapshots.py`) but uses different schema
- `GeographySnapshotService` has TODO to implement snapshots table
- `SnapshotRegistry` class has placeholder implementations

### Target Schema (Per Readiness Plan Section 5.1)
```sql
CREATE TABLE dataset_snapshots (
    dataset_id VARCHAR(50) NOT NULL,
    release_id VARCHAR(50) NOT NULL,
    digest VARCHAR(64) NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE,
    manifest_url VARCHAR(500),
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (dataset_id, release_id),
    INDEX idx_snapshots_dataset_effective (dataset_id, effective_from, effective_to),
    INDEX idx_snapshots_digest (digest)
);
```

### Implementation Steps

#### Step 1.1: Create Alembic Migration (1 hour)
**File:** `alembic/versions/XXX_add_dataset_snapshots_table.py`

**Tasks:**
- Create migration to add `dataset_snapshots` table
- Include uniqueness constraint on `(dataset_id, release_id)`
- Add indexes for efficient queries
- Include rollback logic

**DDL:**
```python
def upgrade() -> None:
    op.create_table(
        'dataset_snapshots',
        sa.Column('dataset_id', sa.String(50), nullable=False),
        sa.Column('release_id', sa.String(50), nullable=False),
        sa.Column('digest', sa.String(64), nullable=False),
        sa.Column('effective_from', sa.Date(), nullable=False),
        sa.Column('effective_to', sa.Date(), nullable=True),
        sa.Column('manifest_url', sa.String(500), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.PrimaryKeyConstraint('dataset_id', 'release_id', name='pk_dataset_snapshots'),
    )
    op.create_index('idx_snapshots_dataset_effective', 'dataset_snapshots', 
                    ['dataset_id', 'effective_from', 'effective_to'])
    op.create_index('idx_snapshots_digest', 'dataset_snapshots', ['digest'])
```

**Acceptance Criteria:**
- [ ] Migration creates table with correct schema
- [ ] Uniqueness constraint on `(dataset_id, release_id)`
- [ ] Indexes created successfully
- [ ] Rollback works correctly

#### Step 1.2: Create SQLAlchemy Model (30 minutes)
**File:** `cms_pricing/models/dataset_snapshots.py` (new file)

**Tasks:**
- Define `DatasetSnapshot` model matching migration
- Add helper methods for querying active snapshots
- Include relationship to fee schedule tables (optional)

**Model Structure:**
```python
class DatasetSnapshot(Base):
    __tablename__ = "dataset_snapshots"
    
    dataset_id = Column(String(50), primary_key=True)
    release_id = Column(String(50), primary_key=True)
    digest = Column(String(64), nullable=False)
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date, nullable=True)
    manifest_url = Column(String(500), nullable=True)
    created_at = Column(DateTime, nullable=False, server_default=text('NOW()'))
    
    __table_args__ = (
        Index("idx_snapshots_dataset_effective", "dataset_id", "effective_from", "effective_to"),
        Index("idx_snapshots_digest", "digest"),
    )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses"""
        return {
            "dataset_id": self.dataset_id,
            "release_id": self.release_id,
            "digest": self.digest,
            "effective_from": self.effective_from.isoformat() if self.effective_from else None,
            "effective_to": self.effective_to.isoformat() if self.effective_to else None,
            "manifest_url": self.manifest_url,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }
```

**Acceptance Criteria:**
- [ ] Model matches migration schema exactly
- [ ] Primary key uses composite `(dataset_id, release_id)`
- [ ] Helper methods work correctly
- [ ] No linting errors

#### Step 1.3: Update Snapshot Selection Service (2 hours)
**File:** `cms_pricing/services/pricing.py`

**Tasks:**
- Implement `_select_dataset_snapshot()` method
- Deterministic selection logic:
  1. Query `dataset_snapshots` for dataset_id matching valuation date
  2. Select snapshot where `effective_from <= valuation_date AND (effective_to IS NULL OR effective_to >= valuation_date)`
  3. If multiple, prefer latest `effective_from`, then latest `created_at`
  4. If none found, log warning and return None
- Update `_collect_datasets_used()` to query snapshot table
- Fallback to extracting from line items if snapshot not found

**Implementation Pattern:**
```python
def _select_dataset_snapshot(
    self,
    dataset_id: str,
    valuation_year: int,
    valuation_quarter: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Select active dataset snapshot for given valuation period.
    
    Returns snapshot with release_id, digest, effective_from, effective_to.
    Returns None if no snapshot found (logs warning).
    """
    from cms_pricing.models.dataset_snapshots import DatasetSnapshot
    from datetime import date
    
    # Determine valuation date
    valuation_date = date(valuation_year, 1, 1)
    if valuation_quarter:
        quarter_month = {'1': 1, '2': 4, '3': 7, '4': 10}.get(valuation_quarter, 1)
        valuation_date = date(valuation_year, quarter_month, 1)
    
    # Query for active snapshot
    snapshot = self.db.query(DatasetSnapshot).filter(
        and_(
            DatasetSnapshot.dataset_id == dataset_id,
            DatasetSnapshot.effective_from <= valuation_date,
            or_(
                DatasetSnapshot.effective_to.is_(None),
                DatasetSnapshot.effective_to >= valuation_date
            )
        )
    ).order_by(
        DatasetSnapshot.effective_from.desc(),
        DatasetSnapshot.created_at.desc()
    ).first()
    
    if snapshot:
        return snapshot.to_dict()
    else:
        logger.warning(
            "No dataset snapshot found",
            dataset_id=dataset_id,
            valuation_date=valuation_date.isoformat()
        )
        return None
```

**Acceptance Criteria:**
- [ ] Snapshot selection queries table correctly
- [ ] Deterministic ordering (latest effective_from, then created_at)
- [ ] Warning logged when snapshot not found
- [ ] Returns None gracefully (doesn't break pricing)

#### Step 1.4: Create Snapshot Registration Utility (1 hour)
**File:** `scripts/register_dataset_snapshot.py` (new file)

**Tasks:**
- CLI script to register snapshots in the table
- Accepts: dataset_id, release_id, digest, effective_from, effective_to, manifest_url
- Handles uniqueness conflicts (updates existing if same dataset_id+release_id)

**Usage:**
```bash
python scripts/register_dataset_snapshot.py \
  --dataset-id MPFS \
  --release-id mpfs_2025_annual_20250115 \
  --digest abc123... \
  --effective-from 2025-01-01 \
  --manifest-url https://...
```

**Acceptance Criteria:**
- [ ] Script registers snapshots correctly
- [ ] Handles duplicate key conflicts (update vs insert)
- [ ] Validates date formats
- [ ] Logs actions for audit

#### Step 1.5: Add Snapshot Queries to Health Endpoint (30 minutes)
**File:** `cms_pricing/services/geography_health.py` or new health endpoint

**Tasks:**
- Add `/health/snapshots` endpoint (if not exists)
- Query `dataset_snapshots` table
- Return list of active snapshots by dataset

**Acceptance Criteria:**
- [ ] Endpoint returns all active snapshots
- [ ] Grouped by dataset_id
- [ ] Includes release_id, digest, effective dates
- [ ] Documented in OpenAPI

#### Step 1.6: Tests (1 hour)
**File:** `tests/models/test_dataset_snapshots.py` (new)

**Tasks:**
- Test model creation and uniqueness constraint
- Test snapshot selection logic (multiple snapshots, date filtering)
- Test registration script
- Test health endpoint

**Acceptance Criteria:**
- [ ] All tests pass
- [ ] Uniqueness constraint enforced
- [ ] Selection logic tested with edge cases

### Deliverables
1. ✅ Alembic migration file
2. ✅ `DatasetSnapshot` model
3. ✅ Snapshot selection in `PricingService`
4. ✅ Registration script
5. ✅ Health endpoint enhancement
6. ✅ Unit tests

### Estimated Time: 4-6 hours

---

## Quick Win #2: Unified CodePricingItem Schema (6-8 hours)

### Overview
Define a unified `CodePricingItem` Pydantic model that all pricing endpoints return, replacing the various response formats currently used.

### Current State
- Multiple response models: `LineItemResponse`, MPFS responses, OPPS responses, etc.
- Each endpoint has slightly different response structure
- ClearBill needs consistent schema for consumption

### Target Schema
```python
class CodePricingItem(BaseModel):
    """Unified pricing item response across all datasets"""
    
    # Identity
    code: str  # HCPCS/CPT code
    setting: str  # MPFS, OPPS, ASC, etc.
    modifier: Optional[str] = None
    
    # Pricing
    allowed_cents: int
    beneficiary_deductible_cents: int
    beneficiary_coinsurance_cents: int
    beneficiary_total_cents: int
    program_payment_cents: int
    
    # Components
    professional_allowed_cents: Optional[int] = None
    facility_allowed_cents: Optional[int] = None
    
    # Provenance (Phase 2)
    dataset_id: str
    release_id: Optional[str] = None
    batch_id: Optional[str] = None
    trace_refs: List[str] = Field(default_factory=list)
    
    # Metadata
    source: str  # benchmark, mrf, tic
    facility_specific: bool = False
    packaged: bool = False
    units: float = 1.0
```

### Implementation Steps

#### Step 2.1: Define Unified Schema (1 hour)
**File:** `cms_pricing/schemas/pricing.py` (add new model)

**Tasks:**
- Create `CodePricingItem` Pydantic model
- Include all common fields from existing response models
- Add provenance fields (Phase 2)
- Add comprehensive Field descriptions

**Acceptance Criteria:**
- [ ] Schema includes all necessary fields
- [ ] Compatible with existing `LineItemResponse`
- [ ] Provenance fields documented
- [ ] OpenAPI schema generates correctly

#### Step 2.2: Update Engines to Return CodePricingItem (2 hours)
**Files:** All 6 engine files
- `cms_pricing/engines/mpfs.py`
- `cms_pricing/engines/opps.py`
- `cms_pricing/engines/asc.py`
- `cms_pricing/engines/clfs.py`
- `cms_pricing/engines/dmepos.py`
- `cms_pricing/engines/ipps.py`

**Tasks:**
- Update `price_code()` return type hint to `CodePricingItem`
- Convert return dict to `CodePricingItem` instance
- Ensure all fields map correctly
- Test each engine still works

**Pattern:**
```python
from cms_pricing.schemas.pricing import CodePricingItem

async def price_code(...) -> CodePricingItem:
    # ... existing logic ...
    
    return CodePricingItem(
        code=code,
        setting="MPFS",
        modifier=modifier,
        allowed_cents=allowed_cents,
        beneficiary_deductible_cents=deductible_cents,
        beneficiary_coinsurance_cents=coinsurance_cents,
        beneficiary_total_cents=beneficiary_total,
        program_payment_cents=program_payment,
        dataset_id=DATASET_ID,
        release_id=mpfs_data.release_id,
        batch_id=mpfs_data.batch_id,
        trace_refs=trace_refs,
        source="benchmark",
        # ... other fields
    )
```

**Acceptance Criteria:**
- [ ] All engines return `CodePricingItem`
- [ ] All fields populated correctly
- [ ] Existing tests still pass (may need minor updates)
- [ ] No breaking changes to response structure

#### Step 2.3: Update Service Layer (1 hour)
**File:** `cms_pricing/services/pricing.py`

**Tasks:**
- Update `price_single_code()` to return `CodePricingItem`
- Update `_price_line_item()` to convert engine results to `CodePricingItem`
- Ensure `LineItemResponse` can be created from `CodePricingItem` (for plan pricing)

**Pattern:**
```python
async def price_single_code(...) -> CodePricingItem:
    # Get result from engine (already CodePricingItem)
    result = await engine.price_code(...)
    return result  # Already CodePricingItem
```

**Acceptance Criteria:**
- [ ] `price_single_code()` returns `CodePricingItem`
- [ ] Plan pricing still works (converts `CodePricingItem` → `LineItemResponse`)
- [ ] No data loss in conversion

#### Step 2.4: Update Router Endpoints (1.5 hours)
**Files:**
- `cms_pricing/routers/pricing.py` (`/pricing/codes/price`)
- `cms_pricing/routers/mpfs.py` (if has single-code endpoint)
- `cms_pricing/routers/opps.py` (if has single-code endpoint)

**Tasks:**
- Update response_model to `CodePricingItem` where appropriate
- Keep `PricingResponse` for plan pricing (uses `LineItemResponse[]`)
- Update docstrings to reference unified schema

**Pattern:**
```python
@router.get("/codes/price", response_model=CodePricingItem)
async def price_single_code(...):
    """Price a single code - returns unified CodePricingItem"""
    result = await pricing_service.price_single_code(...)
    return result
```

**Acceptance Criteria:**
- [ ] Single-code endpoints use `CodePricingItem`
- [ ] Plan endpoints still use `PricingResponse`
- [ ] OpenAPI docs show unified schema
- [ ] Backward compatible (same fields, just structured)

#### Step 2.5: Create Compatibility Adapter (Optional, 1 hour)
**File:** `cms_pricing/schemas/pricing.py`

**Tasks:**
- Add `LineItemResponse.from_code_pricing_item()` class method
- Ensures plan pricing can use `CodePricingItem` internally
- Maintains backward compatibility

**Acceptance Criteria:**
- [ ] Conversion preserves all fields
- [ ] Handles None values gracefully
- [ ] No data loss

#### Step 2.6: Update Tests (1.5 hours)
**Files:**
- `tests/services/test_pricing_provenance.py`
- `tests/engines/test_*.py` (update as needed)
- `tests/api/test_*.py` (update response assertions)

**Tasks:**
- Update test assertions to expect `CodePricingItem`
- Verify all fields populated
- Test compatibility adapter if created

**Acceptance Criteria:**
- [ ] All tests pass
- [ ] Response structure validated
- [ ] No regressions

### Deliverables
1. ✅ `CodePricingItem` Pydantic model
2. ✅ All engines return `CodePricingItem`
3. ✅ Service layer updated
4. ✅ Router endpoints updated
5. ✅ Compatibility adapter (if needed)
6. ✅ Tests updated

### Estimated Time: 6-8 hours

---

## Quick Win #3: Basic API Keys Table (6-8 hours)

### Overview
Replace environment variable-based API key storage with a database-backed system supporting scopes, tenant attribution, and per-key metrics.

### Current State
- API keys stored in `settings.api_keys_str` (comma-separated)
- Simple validation in `auth.py` and `SecurityMiddleware`
- No scopes, no tenant attribution, no per-key metrics

### Target Schema
```sql
CREATE TABLE api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key_hash VARCHAR(255) NOT NULL UNIQUE,  -- bcrypt hash of actual key
    name VARCHAR(100) NOT NULL,  -- Human-readable name
    tenant_id VARCHAR(50),  -- Optional tenant identifier
    scopes TEXT[],  -- Array of scope strings: ['pricing.read', 'plans.write', etc.]
    rate_limit_per_minute INTEGER DEFAULT 120,
    created_at TIMESTAMP DEFAULT NOW(),
    last_used_at TIMESTAMP,
    expires_at TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    metadata JSONB,  -- Additional key-value store
    INDEX idx_api_keys_hash (key_hash),
    INDEX idx_api_keys_tenant (tenant_id),
    INDEX idx_api_keys_active (is_active, expires_at)
);
```

### Implementation Steps

#### Step 3.1: Create Alembic Migration (1 hour)
**File:** `alembic/versions/XXX_add_api_keys_table.py`

**Tasks:**
- Create `api_keys` table with all columns
- Use PostgreSQL array type for scopes
- Add indexes for performance
- Include rollback logic

**DDL:**
```python
def upgrade() -> None:
    op.create_table(
        'api_keys',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text('gen_random_uuid()')),
        sa.Column('key_hash', sa.String(255), nullable=False, unique=True),
        sa.Column('name', sa.String(100), nullable=False),
        sa.Column('tenant_id', sa.String(50), nullable=True),
        sa.Column('scopes', postgresql.ARRAY(sa.String()), nullable=True),
        sa.Column('rate_limit_per_minute', sa.Integer(), nullable=False, server_default='120'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('last_used_at', sa.DateTime(), nullable=True),
        sa.Column('expires_at', sa.DateTime(), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('metadata', postgresql.JSONB(), nullable=True),
    )
    op.create_index('idx_api_keys_hash', 'api_keys', ['key_hash'])
    op.create_index('idx_api_keys_tenant', 'api_keys', ['tenant_id'])
    op.create_index('idx_api_keys_active', 'api_keys', ['is_active', 'expires_at'])
```

**Acceptance Criteria:**
- [ ] Table created with correct schema
- [ ] Unique constraint on `key_hash`
- [ ] Array type for scopes
- [ ] Indexes created
- [ ] Rollback works

#### Step 3.2: Create SQLAlchemy Model (1 hour)
**File:** `cms_pricing/models/api_keys.py` (new file)

**Tasks:**
- Define `APIKey` model
- Add helper methods for key validation
- Include scope checking methods

**Model Structure:**
```python
from sqlalchemy.dialects.postgresql import UUID, ARRAY, JSONB
import bcrypt
from cms_pricing.database import Base

class APIKey(Base):
    __tablename__ = "api_keys"
    
    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text('gen_random_uuid()'))
    key_hash = Column(String(255), nullable=False, unique=True, index=True)
    name = Column(String(100), nullable=False)
    tenant_id = Column(String(50), nullable=True, index=True)
    scopes = Column(ARRAY(String), nullable=True)
    rate_limit_per_minute = Column(Integer, nullable=False, server_default='120')
    created_at = Column(DateTime, nullable=False, server_default=text('NOW()'))
    last_used_at = Column(DateTime, nullable=True)
    expires_at = Column(DateTime, nullable=True)
    is_active = Column(Boolean, nullable=False, server_default='true')
    metadata = Column(JSONB, nullable=True)
    
    __table_args__ = (
        Index('idx_api_keys_active', 'is_active', 'expires_at'),
    )
    
    @staticmethod
    def hash_key(key: str) -> str:
        """Hash an API key using bcrypt"""
        return bcrypt.hashpw(key.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    
    def verify_key(self, key: str) -> bool:
        """Verify a key against the stored hash"""
        return bcrypt.checkpw(key.encode('utf-8'), self.key_hash.encode('utf-8'))
    
    def has_scope(self, scope: str) -> bool:
        """Check if key has a specific scope"""
        if not self.scopes:
            return False
        return scope in self.scopes
    
    def is_valid(self) -> bool:
        """Check if key is active and not expired"""
        if not self.is_active:
            return False
        if self.expires_at and self.expires_at < datetime.now(timezone.utc):
            return False
        return True
```

**Acceptance Criteria:**
- [ ] Model matches migration
- [ ] Helper methods work correctly
- [ ] bcrypt hashing/verification works
- [ ] Scope checking works

#### Step 3.3: Update Auth Module (1.5 hours)
**File:** `cms_pricing/auth.py`

**Tasks:**
- Replace `verify_api_key()` to query database
- Update `is_admin_key()` to check scopes
- Add `require_scope()` dependency for route-level scope checks
- Handle bcrypt key verification
- Update `last_used_at` on successful auth

**New Implementation:**
```python
from sqlalchemy.orm import Session
from cms_pricing.database import get_db
from cms_pricing.models.api_keys import APIKey
from datetime import datetime, timezone

async def verify_api_key(
    x_api_key: Optional[str] = Header(None),
    db: Session = Depends(get_db)
) -> APIKey:
    """Verify API key from database"""
    if not x_api_key:
        raise HTTPException(status_code=401, detail="API key required")
    
    # Find key by attempting to verify against all active keys
    active_keys = db.query(APIKey).filter(
        APIKey.is_active == True,
        or_(
            APIKey.expires_at.is_(None),
            APIKey.expires_at > datetime.now(timezone.utc)
        )
    ).all()
    
    for key_record in active_keys:
        if key_record.verify_key(x_api_key):
            # Update last_used_at
            key_record.last_used_at = datetime.now(timezone.utc)
            db.commit()
            return key_record
    
    raise HTTPException(status_code=401, detail="Invalid API key")

def require_scope(scope: str):
    """Dependency factory for scope-based access control"""
    async def scope_checker(api_key: APIKey = Depends(verify_api_key)) -> APIKey:
        if not api_key.has_scope(scope):
            raise HTTPException(
                status_code=403,
                detail=f"Missing required scope: {scope}"
            )
        return api_key
    return scope_checker
```

**Performance Note:** For production, consider:
- Caching active keys in Redis
- Key lookup by prefix (store first 8 chars separately)
- Index on key_hash for faster lookups

**Acceptance Criteria:**
- [ ] Keys verified from database
- [ ] bcrypt hashing works correctly
- [ ] Scope checking works
- [ ] `last_used_at` updated on use
- [ ] Expired keys rejected

#### Step 3.4: Update Middleware (1 hour)
**File:** `cms_pricing/middleware.py`

**Tasks:**
- Update `SecurityMiddleware` to use database-backed auth
- Maintain backward compatibility with env vars during migration
- Add per-key metrics emission

**Pattern:**
```python
# Try database first, fallback to env vars
try:
    api_key_record = await verify_api_key_from_db(api_key)
except:
    # Fallback to env var validation for migration period
    if api_key not in settings.get_api_keys():
        return JSONResponse(status_code=401, ...)
```

**Acceptance Criteria:**
- [ ] Database lookup works
- [ ] Fallback to env vars works
- [ ] Metrics emitted per key (optional)
- [ ] No performance regression

#### Step 3.5: Create Key Management CLI (1.5 hours)
**File:** `scripts/manage_api_keys.py` (new file)

**Tasks:**
- CLI to create, list, update, delete API keys
- Generate secure random keys
- Set scopes and metadata
- Deactivate/expire keys

**Commands:**
```bash
# Create key
python scripts/manage_api_keys.py create \
  --name "ClearBill Production" \
  --tenant clearbill \
  --scopes pricing.read plans.read \
  --rate-limit 1000

# List keys
python scripts/manage_api_keys.py list --tenant clearbill

# Update key
python scripts/manage_api_keys.py update <key-id> \
  --scopes pricing.read plans.read plans.write

# Deactivate key
python scripts/manage_api_keys.py deactivate <key-id>
```

**Acceptance Criteria:**
- [ ] All CRUD operations work
- [ ] Keys generated securely
- [ ] Scopes validated
- [ ] Audit trail logged

#### Step 3.6: Migration Script (30 minutes)
**File:** `scripts/migrate_api_keys_to_db.py` (new file)

**Tasks:**
- Read keys from `API_KEYS` env var
- Create database records for each
- Assign default scopes
- Log migration results

**Usage:**
```bash
python scripts/migrate_api_keys_to_db.py \
  --default-scopes pricing.read plans.read \
  --tenant production
```

**Acceptance Criteria:**
- [ ] All env var keys migrated
- [ ] Keys hash correctly
- [ ] Can verify migrated keys
- [ ] Rollback option available

#### Step 3.7: Add Per-Key Metrics (Optional, 1 hour)
**File:** `cms_pricing/auth.py` or `cms_pricing/middleware.py`

**Tasks:**
- Emit Prometheus metrics per key:
  - `requests_total{api_key_id, scope}`
  - `requests_4xx_total{api_key_id}`
  - `requests_rate_limited_total{api_key_id}`
- Update existing metrics to include key_id label

**Acceptance Criteria:**
- [ ] Metrics emitted correctly
- [ ] Key ID included (not full key)
- [ ] Scope labels populated

#### Step 3.8: Tests (1 hour)
**Files:**
- `tests/models/test_api_keys.py` (new)
- `tests/auth/test_api_keys.py` (new)

**Tasks:**
- Test key creation and hashing
- Test key verification
- Test scope checking
- Test expired/inactive keys
- Test middleware integration

**Acceptance Criteria:**
- [ ] All tests pass
- [ ] Security tested (bcrypt, scope enforcement)
- [ ] Edge cases covered

### Deliverables
1. ✅ Alembic migration
2. ✅ `APIKey` model
3. ✅ Updated `auth.py` with database lookup
4. ✅ Updated middleware
5. ✅ Key management CLI
6. ✅ Migration script
7. ✅ Per-key metrics (optional)
8. ✅ Tests

### Estimated Time: 6-8 hours

---

## Implementation Order Recommendation

### Option A: Sequential (Recommended)
1. **Day 1:** Quick Win #1 (Dataset Snapshots) - 4-6 hours
2. **Day 2:** Quick Win #2 (Unified Schema) - 6-8 hours
3. **Day 3:** Quick Win #3 (API Keys) - 6-8 hours

**Total:** 2-3 days

### Option B: Parallel (If Multiple Developers)
- **Developer 1:** Quick Win #1 (Dataset Snapshots)
- **Developer 2:** Quick Win #2 (Unified Schema)
- **Developer 3:** Quick Win #3 (API Keys)

**Total:** 1-2 days (depending on coordination)

---

## Dependencies & Prerequisites

### Quick Win #1 (Dataset Snapshots)
- ✅ Phase 2 provenance columns exist
- ✅ Migration infrastructure ready
- Database access for testing

### Quick Win #2 (Unified Schema)
- ✅ All engines updated (Phase 2.5)
- ✅ Service layer stable
- ClearBill team alignment on schema (if needed)

### Quick Win #3 (API Keys)
- Database access
- bcrypt library (`pip install bcrypt`)
- Decision on scope names (e.g., `pricing.read`, `plans.write`)

---

## Success Criteria

### Quick Win #1
- [ ] `dataset_snapshots` table exists and populated
- [ ] Snapshot selection works deterministically
- [ ] Health endpoint shows active snapshots
- [ ] Registration script works

### Quick Win #2
- [ ] All single-code endpoints return `CodePricingItem`
- [ ] OpenAPI docs show unified schema
- [ ] Backward compatible (no breaking changes)
- [ ] Tests pass

### Quick Win #3
- [ ] API keys stored in database
- [ ] Key verification works
- [ ] Scope checking works
- [ ] Management CLI operational
- [ ] Per-key metrics emitted (optional)

---

## Risk Mitigation

### Quick Win #1
- **Risk:** Snapshot selection returns wrong version
- **Mitigation:** Comprehensive tests, deterministic ordering, logging

### Quick Win #2
- **Risk:** Breaking changes for existing clients
- **Mitigation:** Compatibility adapter, feature flag, dual-write period

### Quick Win #3
- **Risk:** Performance impact of database lookups
- **Mitigation:** Redis cache, key prefix indexing, connection pooling

---

## Next Steps After Quick Wins

1. **End-to-End Validation:** Test all three together
2. **Production Deployment:** Gradual rollout with monitoring
3. **Documentation:** Update API docs, client guides
4. **Monitoring:** Set up alerts for key metrics

