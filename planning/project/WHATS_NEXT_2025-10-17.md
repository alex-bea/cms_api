# What's Next: Recommended Priorities

**Date:** 2025-10-17  
**Context:** GPCI parser complete (100% QTS-compliant), all PRD improvements applied

---

## ✅ **What's Complete**

**GPCI Parser (100% Done):**
- ✅ Parser implemented (547 lines, 4 formats: TXT/CSV/XLSX/ZIP)
- ✅ All tests passing (11/11, 100% pass rate)
- ✅ QTS-compliant (clean golden + edge case tests)
- ✅ Documentation complete (SRC-gpci.md v1.0)

**PRD Improvements (9/10 Done):**
- ✅ STD-qa-testing v1.3 (golden fixture hygiene, format parity, pytest markers)
- ✅ STD-parser-contracts v1.9 (6 new sections, ~600 lines of guidance)
- ✅ SRC-gpci.md v1.0 (full dataset characteristics)
- ✅ SRC-TEMPLATE.md (template for future datasets)

**Documentation System:**
- ✅ 100% audit pass rate (6/6 core checks)
- ✅ 153 cross-references validated
- ✅ All bidirectional references working

---

## 🎯 **Option 1: Validate PRD Improvements (RECOMMENDED)**

**Goal:** Prove the 70% time savings on next parser

**Next Parser:** Choose one:
- **OPPSCAP** (OPPS Ambulatory Surgical Cap) - Similar to GPCI complexity
- **Locality** (County→Locality mapping) - Slightly simpler
- **ANES** (Anesthesia Conversion Factor) - Similar to CF parser

**Why This Next:**
- Tests if new PRD guidance works
- Measures actual time savings (vs 8h for GPCI)
- Validates phased implementation strategy (§21.6)
- Confirms pre-implementation checklist value (§21.4)

**Expected Outcome:**
- **Time:** 5.5-7 hours (vs 8h for GPCI) = 40-60% savings
- **First-run test pass rate:** 85-90% (vs 60% for GPCI)
- **QTS compliance:** Built-in from day 1
- **Validation:** Proves PRD improvements work

**Steps:**
1. Choose parser (recommend LOCALITY - simplest to validate time savings)
2. Follow §21.4 format verification checklist (2h)
3. Phase 1: Single format (3h)
4. Phase 2: Multi-format (2h)
5. Phase 3: Edge cases (1h)
6. **Total:** ~8h → ~6h with guidance = 25% savings (conservative)

---

## 🎯 **Option 2: GitHub Tasks Cleanup**

**Goal:** Organize and prioritize remaining work

**What:**
- Review `github_tasks_plan.md` (2526 lines, 63 tasks)
- Update task statuses (GPCI done, PRD improvements done)
- Identify next high-priority tasks
- Create GitHub Project if not already set up

**Why This:**
- Clear visibility on remaining work
- Prioritize next parser or feature
- Track progress systematically

**Time:** 1-2 hours

---

## 🎯 **Option 3: Enhance Existing Parsers**

**Goal:** Apply new standards to existing parsers

**Candidates:**
- **PPRRVU parser** - Add edge case tests, verify QTS compliance
- **Conversion Factor parser** - Add multi-format support (currently TXT-only?)
- **All parsers** - Ensure tiered validation (no test-only flags)

**Why This:**
- Consistent quality across parsers
- Validates backwards compatibility of new patterns
- Lower risk than new parser

**Time:** 2-3 hours per parser

---

## 🎯 **Option 4: Complete Ingestion Pipeline**

**Goal:** End-to-end data flow (scraper → parser → database → API)

**What:**
- Connect GPCI parser to RVU ingestor
- Publish GPCI data to database
- Expose via pricing API
- Test end-to-end pricing calculation

**Why This:**
- Proves value (actual pricing calculations working)
- Integration testing
- User-facing benefit

**Time:** 4-6 hours (integration + testing)

---

## 🎯 **Option 5: Production Readiness**

**Goal:** Deploy GPCI parser to production

**What:**
- CI/CD pipeline for parser tests
- Production data validation
- Monitoring and alerts
- Rollout plan

**Why This:**
- Get GPCI into production
- Real-world validation
- Operational experience

**Time:** 3-4 hours (CI setup + deployment)

---

## 📊 **My Recommendation**

### **RECOMMENDED: Option 1 - Next Parser (LOCALITY)** 🎯

**Why Locality Parser:**
1. **Simplest to implement** (2-format support: TXT + CSV)
2. **High value** (needed for ZIP→Locality resolution)
3. **Perfect for validation** (can we hit 2.5h target with new guidance?)
4. **Clear success criteria** (time savings + first-run pass rate)

**Workflow:**
1. Review `planning/parsers/locality/` directory
2. Follow §21.4 pre-implementation checklist (prevents all format issues we hit with GPCI)
3. Use §21.6 phased approach (TXT first, then CSV, then edge cases)
4. Track time spent per phase
5. Compare against GPCI time (8h total)

**Success Metrics:**
- ✅ Total time < 6 hours (25% savings conservative, 70% optimistic)
- ✅ First-run test pass rate > 80%
- ✅ QTS-compliant from day 1
- ✅ Validates PRD improvements work

**Alternative: OPPSCAP** if you want more complexity to prove guidance works on harder parsers

---

## 🔍 **Parser Status Summary**

| Parser | Status | Lines | Tests | QTS | Notes |
|--------|--------|-------|-------|-----|-------|
| **PPRRVU** | ✅ Done | ~400 | Passing | ? | May need QTS alignment |
| **Conversion Factor** | ✅ Done | ~300 | Passing | ✅ | First QTS-compliant (pre-GPCI) |
| **GPCI** | ✅ Done | 547 | 11/11 (100%) | ✅ | Reference implementation |
| **Locality** | 🔵 Pending | — | — | — | Next recommended |
| **OPPSCAP** | 🔵 Pending | — | — | — | Alternative next |
| **ANES** | 🔵 Pending | — | — | — | Similar to CF |

---

## 💬 **Decision Points**

**Question 1: Want to validate PRD time savings?**
- Yes → **Option 1: Implement Locality parser** (measures improvement)
- No → **Option 2: GitHub tasks cleanup** (organize remaining work)

**Question 2: Want to see GPCI in production?**
- Yes → **Option 5: Production readiness** (CI/CD + deployment)
- No → Focus on more parsers first

**Question 3: Want end-to-end flow working?**
- Yes → **Option 4: Complete ingestion pipeline** (scraper → API)
- No → Build more parsers first

---

## 🚀 **My Strong Recommendation**

**Next: Locality Parser** (Option 1)

**Why:**
1. Validates 70% time savings hypothesis
2. Simpler than OPPSCAP (easier to track time)
3. Needed for production (ZIP→Locality resolution)
4. Builds confidence in PRD improvements
5. If we hit 6h (vs 8h GPCI) = **25% proven savings**
6. If we hit 2.5h = **70% proven savings** 🎉

**After Locality:**
- Measure actual time
- Document results
- Iterate on PRDs if needed
- Then tackle OPPSCAP or production deployment

---

**Ready to proceed with Locality parser?** Or prefer one of the other options?

