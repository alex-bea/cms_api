# RVU Pipeline - Next Steps Plan

**Date:** 2025-10-28  
**Status:** Deployment Complete, Ready for Data Loading  
**Version:** v1.0.1

## Current Status ✅

- ✅ Code deployed to Render (https://cms-pricing-api.onrender.com)
- ✅ API is responding correctly
- ✅ Authentication working (API key: `dev-key-123`)
- ✅ Database connectivity verified (Render Postgres)
- ✅ Database tables created (6 RVU tables ready)
- ✅ Tests passing (10/13 RVU tests)

## Next Steps: Data Loading & Validation

### Phase 1: Local Testing (Optional but Recommended)

**Goal:** Test the pipeline locally before loading into production database

**Steps:**
1. **Prepare test data**
   ```bash
   # Verify test data exists
   ls -lh sample_data/rvu25a/
   
   # Should have:
   # - PPRRVU25_JAN.txt (3.1MB)
   # - GPCI2025.txt (17KB)
   # - ANES2025.txt (8.4KB)
   # - OPPSCAP_JAN.txt (629KB)
   # - 25LOCCO.txt (18KB)
   ```

2. **Run local test**
   ```bash
   # Connect to local test database
   # Run RVU pipeline with test data
   python -m pytest tests/ingestors/test_rvu_ingestor_e2e.py::test_full_dis_pipeline -v
   ```

3. **Verify local database**
   - Check data appears in tables
   - Validate data integrity
   - Ensure no errors

**Estimated Time:** 15-30 minutes  
**Risk:** Low (testing locally)

---

### Phase 2: Production Data Loading

**Goal:** Load real RVU data into Render production database

**Option A: Via Render Web Interface** (Recommended)

1. **Access Render Dashboard**
   - Go to https://dashboard.render.com
   - Navigate to cms-pricing-api service
   - Click "Shell" or "Console"

2. **Run ingestion command**
   ```bash
   # In Render shell, run:
   python -c "
   from cms_pricing.ingestion.ingestors.rvu_ingestor import RVUIngestor
   import asyncio
   
   async def load_data():
       ingestor = RVUIngestor('./data/rvu_scraper')
       result = await ingestor.ingest_from_manifest(
           release_id='rvu-2025-01',
           batch_id='batch-001',
           manifest_path='sample_data/rvu25a/manifest.json'
       )
       print(result)
   
   asyncio.run(load_data())
   "
   ```

3. **Or use provided scripts**
   ```bash
   # Check for existing CLI
   cms_pricing/cli/ingestion.py rvu --help
   ```

**Option B: Via API Endpoint** (If Available)

1. **Check if ingest endpoint exists**
   ```bash
   curl -X POST -H "X-API-Key: dev-key-123" \
     "https://cms-pricing-api.onrender.com/api/v1/rvu/scraper/download-historical?start_year=2025&end_year=2025"
   ```

2. **Monitor deployment**
   - Watch Render logs
   - Check for errors
   - Verify completion

**Option C: Local Connection to Render DB**

1. **Get Render database URL**
   - From Render dashboard → Database → Internal Database URL
   - Or use external connection string

2. **Set environment variable**
   ```bash
   export DATABASE_URL="<render-db-url>"
   ```

3. **Run pipeline locally pointing to Render DB**
   ```bash
   python -m cms_pricing.cli.ingestion rvu --input sample_data/rvu25a/
   ```

**Estimated Time:** 30-60 minutes  
**Risk:** Medium (loading into production)

---

### Phase 3: Data Verification

**Goal:** Verify data loaded correctly

**Steps:**
1. **Check via API**
   ```bash
   # Get releases
   curl -H "X-API-Key: dev-key-123" \
     "https://cms-pricing-api.onrender.com/api/v1/rvu/releases"
   
   # Should show recent release with data
   ```

2. **Query database directly**
   ```sql
   -- Connect to Render Postgres
   SELECT COUNT(*) FROM rvu_items;
   SELECT COUNT(*) FROM gpci_indices;
   SELECT COUNT(*) FROM anes_conversion_factors;
   SELECT COUNT(*) FROM oppscap_data;
   SELECT COUNT(*) FROM locality_county;
   SELECT COUNT(*) FROM releases;
   
   -- Should show non-zero counts
   ```

3. **Test data retrieval**
   ```bash
   # Test RVU search (if endpoint exists)
   curl -H "X-API-Key: dev-key-123" \
     "https://cms-pricing-api.onrender.com/api/v1/rvu/items?limit=10"
   
   # Test GPCI data
   curl -H "X-API-Key: dev-key-123" \
     "https://cms-pricing-api.onrender.com/api/v1/rvu/gpci?state=CA&locality=18"
   ```

**Expected Results:**
- All 6 tables have data
- Counts match expected values from files
- API endpoints return data
- No errors in logs

**Estimated Time:** 15-30 minutes  
**Risk:** Low (verification only)

---

### Phase 4: Integration Testing

**Goal:** Test complete end-to-end flow

**Steps:**
1. **Test complete pricing flow**
   - Use geography → RVU → pricing pipeline
   - Verify all components work together

2. **Performance testing**
   - Test response times
   - Check for timeouts
   - Verify caching works

3. **Error handling**
   - Test invalid inputs
   - Verify error messages
   - Check error logging

**Estimated Time:** 30-60 minutes  
**Risk:** Low

---

## Recommended Path Forward

### Immediate Next Steps (Today):

1. **Option 1: Quick Local Test** (15 min)
   - Run local pipeline test
   - Verify logic works
   - Then load to production

2. **Option 2: Direct Production Load** (60 min)
   - Use Render Shell to run ingestion
   - Monitor logs
   - Verify data loaded

3. **Option 3: Wait for CLI Tool** (time TBD)
   - Fix any CLI issues
   - Then use CLI for loading

### Decision Matrix:

| Scenario | Risk | Time | Recommendation |
|----------|------|------|----------------|
| Have working CLI | Low | 30 min | Use CLI |
| CLI has issues | Medium | 60 min | Use Render Shell |
| Want to be safe | Low | 45 min | Test local, then production |

---

## Questions to Resolve

1. **CLI Status:** Is `cms_pricing/cli/ingestion.py` working?
   - Need to fix missing dependencies
   - Or use alternative loading method

2. **Data Source:** Load from:
   - sample_data/rvu25a/ (local files)
   - Render's file system
   - Direct web scraping

3. **Rollback Plan:** What if data load fails?
   - Database restore from backup
   - Truncate tables and retry

---

## Success Criteria

- ✅ Database contains RVU data (all 6 tables populated)
- ✅ API endpoints return real data
- ✅ No errors in production logs
- ✅ Response times < 500ms p95
- ✅ Data integrity validated

---

## Documentation Updates Needed

After completion:
- [ ] Update deployment runbook
- [ ] Document data loading procedure
- [ ] Create troubleshooting guide
- [ ] Update API documentation with examples
- [ ] Update CHANGELOG.md

---

## Risk Mitigation

- **Database Issues:** Render has daily backups
- **Data Quality:** Run validation tests
- **Performance:** Monitor metrics during load
- **Rollback:** Keep backup plan ready

---

**Next Action:** Choose one of the 3 options above and proceed!
