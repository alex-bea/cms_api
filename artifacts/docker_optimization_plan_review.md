# Docker Optimization Plan Review

## Executive Summary
✅ **The plan is well-structured and addresses real issues.** Estimated impact: **40-60% image size reduction** if executed correctly.

## Status
✅ Completed – Dockerfile and .dockerignore updates landed per this plan; ready for rebuild/measurement.

## Current State Analysis

### Issues Confirmed
1. ✅ `build-essential` and `python3-dev` in base stage (affects both dev and prod)
2. ✅ `COPY . .` includes unnecessary files
3. ✅ Dev dependencies (pytest, black, etc.) in `requirements.txt` lines 51-68
4. ✅ `.dockerignore` excludes some items but could be tighter

### Package Analysis

**Runtime packages with C extensions (need build tools during install):**
- `numpy`, `pandas`, `scipy` - Usually have wheels, but build tools ensure compatibility
- `lxml` - May need build tools
- `psycopg2-binary` - Already uses binary wheels ✅
- `pypdfium2` - May need system libraries (libpdfium)
- `pytesseract` - Python wrapper; needs tesseract-ocr system package

**Potential runtime system dependencies:**
- `tesseract-ocr` (for pytesseract) - Only if PDF OCR is runtime-critical
- `libffi-dev`, `libssl-dev` (for cryptography)

## Plan Strengths

1. ✅ Multi-stage build approach is correct
2. ✅ Baseline measurement before changes
3. ✅ Validation step is critical
4. ✅ Already have `requirements-dev.txt` structure

## Recommendations & Improvements

### 1. Requirements Strategy (HIGH PRIORITY)

**Current Problem:**
- `requirements.txt` includes dev deps (lines 51-68)
- `requirements-dev.txt` exists but duplicates some entries

**Recommended Approach:**
```
# Step 1: Clean requirements.txt (remove lines 51-68)
# Step 2: Create requirements.prod.txt (copy of cleaned requirements.txt)
# Step 3: Update requirements-dev.txt to use -r requirements.txt
# Step 4: Verify no duplicates
```

**Action Items:**
- [ ] Extract dev deps from `requirements.txt` to `requirements-dev.txt`
- [ ] Create `requirements.prod.txt` = cleaned `requirements.txt`
- [ ] Update Dockerfile to use `requirements.prod.txt` for production

### 2. Multi-Stage Build (CRITICAL)

**Recommended Structure:**
```dockerfile
# Stage 1: Builder (has build tools)
FROM python:3.11-slim AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.prod.txt .
RUN pip install --user --no-cache-dir -r requirements.prod.txt

# Stage 2: Runtime (slim, no build tools)
FROM python:3.11-slim AS production
# Install only runtime system deps (curl for healthcheck, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
# Copy installed packages from builder
COPY --from=builder /root/.local /root/.local
# Copy only application code
COPY cms_pricing/ ./cms_pricing/
COPY alembic/ ./alembic/
COPY alembic.ini .
# ... rest of production setup
```

### 3. Enhanced .dockerignore

**Add these exclusions:**
```
# Documentation and planning
artifacts/
planning/
prds/
docs/
*.md
!README.md

# Test data and fixtures
test_data/
sample_data/
tests/
test_*.py
*_test.py

# Debug and temporary files
debug_output*/
*.log
*.json
!package.json
!pyproject.toml

# Render deployment docs (not needed in image)
RENDER_*.md
TRIGGER_*.md
VERIFY_*.md

# Development tools
.github/
.git/
.vscode/
.cursor/

# Large data directories
data/raw/
data/scraped/
data/quarantine/
```

### 4. PDF Library Considerations

**Question:** Are PDF libraries (pypdfium2, pytesseract) needed at runtime or only during ingestion?

**If ingestion-only:**
- Move to separate `requirements.ingestion.txt`
- Don't install in production image
- Create separate `ingestion` build target if needed

**If runtime-critical:**
- Add system dependencies to production stage:
  ```dockerfile
  RUN apt-get install -y --no-install-recommends \
      tesseract-ocr \
      libpdfium-dev \
      && rm -rf /var/lib/apt/lists/*
  ```

### 5. BuildKit Cache Mounts (OPTIONAL BUT RECOMMENDED)

Add to builder stage:
```dockerfile
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --user --no-cache-dir -r requirements.prod.txt
```

### 6. Validation Checklist

**Before/After Comparison:**
```bash
# Baseline
docker build -t cms-api:baseline -f Dockerfile .
docker images cms-api:baseline

# Optimized
docker build -t cms-api:optimized -f Dockerfile --target production .
docker images cms-api:optimized

# Compare
docker history cms-api:baseline
docker history cms-api:optimized
```

**Functional Tests:**
- [ ] Production image starts successfully
- [ ] Healthcheck endpoint responds
- [ ] Database migrations run
- [ ] API endpoints respond correctly
- [ ] No missing dependencies at runtime

### 7. Specific File Copy Strategy

Instead of `COPY . .`, use explicit copies:
```dockerfile
COPY cms_pricing/ ./cms_pricing/
COPY alembic/ ./alembic/
COPY alembic.ini .
COPY pyproject.toml .
# Only copy what's needed for runtime
```

## Implementation Order

1. **Baseline measurement** (5 min)
   - Build current image
   - Record size and layer history

2. **Clean requirements** (15 min)
   - Extract dev deps
   - Create requirements.prod.txt
   - Test that runtime still works

3. **Implement multi-stage build** (30 min)
   - Create builder stage
   - Update production stage
   - Test build

4. **Tighten .dockerignore** (10 min)
   - Add exclusions
   - Rebuild and verify size reduction

5. **Optimize apt usage** (10 min)
   - Add --no-install-recommends
   - Remove unnecessary packages

6. **Validation** (20 min)
   - Compare sizes
   - Run smoke tests
   - Verify functionality

**Total estimated time: 1.5-2 hours**

## Expected Results

**Image Size Reduction:**
- Current: ~800MB-1.2GB (estimate)
- Target: ~400-600MB (40-60% reduction)

### Observed Results (2025-11-06)

- Baseline (`cms-api:baseline` from legacy Dockerfile): **4.65 GB**
- Optimized (`cms-api:optimized` from multi-stage Dockerfile): **1.18 GB**
- Net reduction: **−3.47 GB (~75%)**
- Healthcheck: `/health` served successfully from optimized image
- Build command: `docker build --target production -t cms-api:optimized .`
- Runtime smoke test: `docker run --rm -e PORT=8000 -p 8001:8000 cms-api:optimized uvicorn cms_pricing.main:app --host 0.0.0.0 --port 8000 --workers 1`

**Layer Optimization:**
- Fewer layers
- Smaller individual layers
- Better cache utilization

**Build Time:**
- First build: Similar or slightly longer (multi-stage overhead)
- Subsequent builds: Faster (better caching, smaller context)

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Missing runtime dependency | High | Thorough testing, keep baseline image |
| PDF libraries not available | Medium | Confirm if needed at runtime |
| Build time increases | Low | Use BuildKit cache mounts |
| Breaking dev workflow | Medium | Test dev target separately |

## Additional Suggestions

1. **Consider distroless image** for even smaller production image
2. **Add .dockerignore patterns** for future-proofing
3. **Document build targets** in README
4. **Set up CI to build both targets** automatically
5. **Consider using pip-tools** for dependency pinning

## Next Steps Execution Plan

1. **Roll Out the New Images**
   - [ ] Merge Dockerfile, `.dockerignore`, and requirements changes.
   - [ ] Build both targets locally to confirm caching and dev workflow:
     - `docker build --target production -t cms-api:prod .`
     - `docker build --target development -t cms-api:dev .`
   - [ ] Tag/push the production image once smoke tests pass.
   - [ ] Update Render/Compose deployment configs to reference `cms-api:prod`.

2. **Document & Communicate**
   - [x] Add Docker usage instructions to `README.md` (`development` vs `production` targets, `requirements.prod.txt` flow).
   - [ ] Capture before/after image sizes in the release notes or PR summary.
   - [ ] Note the healthcheck endpoint and non-root runtime change for SRE/ops.

3. **CI/CD Integration**
   - [ ] Update CI pipeline to:
    - [x] Build both targets using BuildKit.
    - [x] Publish the production image to the registry.
    - [x] Cache pip/apt directories (`--mount=type=cache`) for faster rebuilds.
   - [x] Add an automated `docker run` smoke test (health endpoint + basic command).
   - [x] Wire in image vulnerability scanning (Trivy/Grype) for the prod target.

4. **Dependency Review**
   - [ ] Confirm whether OCR/PDF packages (`pytesseract`, `pypdfium2`) need system deps at runtime; if so, document and install the minimal set.
   - [ ] Separate ingestion-only Python dependencies into `requirements.ingestion.txt` if they can be excluded from the API runtime.
   - [ ] Consider trimming Selenium/web scraping deps from the base runtime if unused in production.

5. **Operational Hardening**
   - [ ] Add OCI metadata to CI pipeline (dynamic version, commit SHA).
   - [ ] Set resource limits in Compose/Render manifests (memory/CPU).
   - [ ] Track healthcheck status post-deployment to ensure `healthy` state.
   - [ ] Schedule periodic rebuilds to pick up security patches (weekly).

6. **Monitoring & KPIs**
   - [ ] Record baseline vs optimized image sizes and rebuild times.
   - [ ] Notify the team with a short Loom/Slack update summarizing the impact.
   - [ ] Track Docker cache hit rates in CI after cache mounts go live.

## Conclusion

The plan is **solid and implementable**. Follow the recommendations above for maximum impact while minimizing risk.

######### optional optimization
# syntax=docker/dockerfile:1

# Pin to a specific family (and consider pinning by digest in CI/CD)
ARG PYTHON_IMAGE=python:3.11-slim-bookworm

# ---------- Builder ----------
FROM ${PYTHON_IMAGE} AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Use a dedicated virtualenv we can copy to the runtime image
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

WORKDIR /app

# Install build tools only in the builder; keep update+install in one layer and clean
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install only production deps (copy requirements first to leverage cache)
COPY requirements.prod.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir -r requirements.prod.txt

# ---------- Runtime ----------
FROM ${PYTHON_IMAGE} AS production

# Labels (OCI)
LABEL org.opencontainers.image.title="cms-api" \
      org.opencontainers.image.description="CMS API production image" \
      org.opencontainers.image.source="https://example.com/your-repo" \
      org.opencontainers.image.version="0.0.0"

# Install runtime-only system deps (if needed)
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user (see docs on USER)
RUN groupadd -r app && useradd --no-log-init -r -g app app

WORKDIR /app

# Bring in the prebuilt venv
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Copy only what's needed for runtime; set ownership at copy time
COPY --chown=app:app cms_pricing/ ./cms_pricing/
COPY --chown=app:app alembic/ ./alembic/
COPY --chown=app:app alembic.ini .
COPY --chown=app:app pyproject.toml .

USER app

# Optional: define a healthcheck here (or in Compose)
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

EXPOSE 8080
CMD ["gunicorn", "--config", "gunicorn.conf.py", "cms_pricing.app:app"]
