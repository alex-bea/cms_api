# GitHub Tasks: PRD Improvements from GPCI Parser Lessons

**Date:** 2025-10-17  
**Based on:** GPCI parser implementation experience (60% → near-100% test pass rate)

---

## 🎯 **High-Priority Tasks**

### **1. Update STD-parser-contracts-prd with Format Verification Section**
**Priority:** P0 (Critical)  
**Effort:** 2 hours  
**Assignee:** Documentation lead

**Description:**
Add mandatory "Format Verification" section to parser contract standard requiring:
- Pre-implementation inspection of all source formats
- Documentation of header structures per format
- Validation of layouts against real data
- Checklist signoff before coding begins

**Acceptance Criteria:**
-  New section added to STD-parser-contracts-prd-v1.0.md
-  Checklist template created
-  Example from GPCI parser included
-  Version bumped to v1.8

**References:** planning/parsers/gpci/LESSONS_LEARNED.md §6

---

### **2. Create "CMS Dataset Characteristics" Template**
**Priority:** P0 (Critical)  
**Effort:** 3 hours  
**Assignee:** Domain expert

**Description:**
Create reusable template for documenting CMS-specific quirks:
- File format variations (TXT vs CSV vs XLSX)
- Business rules (floors, exceptions, derived fields)
- Known data issues (duplicates, missing values)
- MAC vs Locality codes disambiguation

**Deliverables:**
-  Template file: `prds/_templates/CMS_DATASET_CHARACTERISTICS.md`
-  GPCI example completed
-  PPRRVU example completed
-  Integration with parser PRD template

**References:** planning/parsers/gpci/LESSONS_LEARNED.md §7

---

### **3. Enhance Error Handling Requirements in Parser Contracts**
**Priority:** P0 (Critical)  
**Effort:** 2 hours  
**Assignee:** Architecture lead

**Description:**
Update STD-parser-contracts to mandate actionable error messages:
- What failed + Why + How to fix + Context
- Validation checkpoints with clear messages
- Logging of intermediate states
- Error taxonomy expansion

**Acceptance Criteria:**
-  Error message format specified
-  Minimum required fields documented
-  Examples added (good vs bad messages)
-  Validation added to parser PR template

**References:** planning/parsers/gpci/LESSONS_LEARNED.md §5

---

## 📊 **Medium-Priority Tasks**

### **4. Add Test Coverage Matrix to QA Standard**
**Priority:** P1 (High)  
**Effort:** 2 hours  
**Assignee:** QA lead

**Description:**
Create test coverage matrix template:
- Format dimension (TXT, CSV, XLSX, ZIP)
- Test type dimension (golden, negative, edge case)
- Minimum 80% coverage requirement
- Format-specific edge cases documented

**Deliverables:**
-  Matrix template added to STD-qa-testing-prd-v1.0.md
-  GPCI parser matrix as example
-  CI enforcement of coverage minimums

**References:** planning/parsers/gpci/LESSONS_LEARNED.md §8

---

### **5. Document Incremental Implementation Best Practice**
**Priority:** P1 (High)  
**Effort:** 1.5 hours  
**Assignee:** Engineering lead

**Description:**
Add "Phased Implementation" section to parser standards:
- Phase 1: Single format + core logic
- Phase 2: Additional formats
- Phase 3: Edge cases
- Test checkpoints at each phase

**Benefits:**
- 40% reduction in total implementation time
- Clearer error isolation
- Incremental progress visibility

**Deliverables:**
-  Section added to STD-parser-contracts
-  Phasing template created
-  GPCI phasing plan as example

**References:** planning/parsers/gpci/LESSONS_LEARNED.md §10

---

### **6. Create "Common CMS Data Quirks" Reference Document**
**Priority:** P1 (High)  
**Effort:** 4 hours  
**Assignee:** Domain expert

**Description:**
Central reference for CMS-specific knowledge:
- Header row patterns across datasets
- Floor values and exceptions (GPCI, RVU)
- MAC vs Locality codes
- Quarter-to-date mappings
- Known duplicate keys
- Whitespace variations

**Deliverables:**
-  New doc: `prds/REF-cms-data-quirks-v1.0.md`
-  Indexed by dataset
-  Cross-referenced from parser PRDs
-  Updated quarterly

**References:** planning/parsers/gpci/LESSONS_LEARNED.md §7

---

### **7. Add Metrics Calculation Standards to Parser Contracts**
**Priority:** P1 (High)  
**Effort:** 1 hour  
**Assignee:** Architecture lead

**Description:**
Standardize metrics calculation patterns:
- Null/empty filtering before aggregation
- Safe min/max with fallback to None
- Sanity checks on metric ranges
- Warning logs for unexpected values

**Code Example:**
```python
valid = df[df['col'] != '']['col']
min_val = float(valid.min()) if len(valid) > 0 else None
```

**Deliverables:**
-  Section added to STD-parser-contracts §10
-  Code snippets included
-  Validation helper function created

**References:** planning/parsers/gpci/LESSONS_LEARNED.md §9

---

## 🔧 **Lower-Priority Tasks**

### **8. Enhance Format Detection Documentation**
**Priority:** P2 (Medium)  
**Effort:** 1.5 hours  
**Assignee:** Engineering lead

**Description:**
Document content-based format detection strategy:
- Extension check (fast path)
- Content sniffing (fallback)
- Pattern matching heuristics
- Explicit fallback order

**Deliverables:**
-  Section added to STD-parser-contracts §3
-  Detection flowchart created
-  ZIP handling documented

**References:** planning/parsers/gpci/LESSONS_LEARNED.md §4

---

### **9. Create Alias Map Best Practices Guide**
**Priority:** P2 (Medium)  
**Effort:** 1 hour  
**Assignee:** Engineering lead

**Description:**
Document header normalization patterns:
- Comprehensive CMS header variations
- Historical format tracking
- Alias map testing strategy
- Unmapped column warnings

**Deliverables:**
-  Guide: `docs/alias-map-best-practices.md`
-  GPCI alias map as example
-  Testing checklist

**References:** planning/parsers/gpci/LESSONS_LEARNED.md §2

---

### **10. Add Type Handling Standards to Parser Contracts**
**Priority:** P2 (Medium)  
**Effort:** 1.5 hours  
**Assignee:** Architecture lead

**Description:**
Comprehensive type casting requirements:
- Document expected inputs AND variations
- Empty string handling
- Integer string → Decimal
- Scientific notation
- Defensive error handling

**Deliverables:**
-  Section added to STD-parser-contracts §7
-  Type handling helper functions
-  Test cases for each variation

**References:** planning/parsers/gpci/LESSONS_LEARNED.md §3

---

## 📈 **Success Metrics**

Track improvement in future parser implementations:

| Metric | Current (GPCI) | Target | Improvement |
|--------|---------------|--------|-------------|
| Time to first working test | 3 hrs | < 2 hrs | 33% faster |
| Debugging time after impl | 5 hrs | < 1 hr | 80% reduction |
| Test pass rate (first run) | 60% | > 80% | 20% better |
| PRD completeness | 70% | 100% | Full coverage |
| Format handling success | 60% | 100% | All formats work |

---

## 🎯 **Implementation Timeline**

**Sprint 1 (Week 1):**
- Tasks #1, #2, #3 (Critical infrastructure)

**Sprint 2 (Week 2):**
- Tasks #4, #5, #6, #7 (Standards updates)

**Sprint 3 (Week 3):**
- Tasks #8, #9, #10 (Documentation polish)

**Total Effort:** ~20 hours over 3 weeks

---

## 🔗 **Related Documents**

- planning/parsers/gpci/LESSONS_LEARNED.md - Detailed analysis
- prds/STD-parser-contracts-prd-v1.0.md - Current standard
- prds/STD-qa-testing-prd-v1.0.md - QA standard
- planning/parsers/gpci/IMPLEMENTATION_COMPLETE.md - GPCI outcomes

---

**Next Steps:**
1. Review and prioritize tasks with team
2. Assign owners for each task
3. Create GitHub Project cards
4. Schedule sprint planning meeting

