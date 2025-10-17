# Planning Files Audit & Organization

**Date:** 2025-10-16  
**Purpose:** Organize planning documents for better maintainability

---

## 📊 **Current State (Workspace Root)**

### GPCI Parser Planning (8 files - 110KB total)
- `GPCI_PARSER_PLAN_FINAL.md` (26K) ✅ **ACTIVE - Use this one**
- `GPCI_PARSER_PLAN.md` (35K) ⚠️ Superseded (v1.0/v2.0 mixed)
- `GPCI_PARSER_PLAN_V2.md` (18K) ⚠️ Superseded by FINAL
- `GPCI_IMPROVEMENTS_SUMMARY.md` (7.6K) 📋 Reference
- `GPCI_PLAN_COMPLETENESS.md` (7.2K) 📋 Reference
- `GPCI_PLAN_REVIEW.md` (13K) 📋 Reference
- `GPCI_QUICK_START.md` (4.2K) 📋 Quick reference
- `GPCI_SCHEMA_V1.2_RATIONALE.md` (6.6K) 📋 ADR reference

### PPRRVU Parser Planning (3 files - 29KB total)
- `PPRRVU_HANDOFF.md` (9.1K) 📋 Reference
- `PPRRVU_FIX_PLAN.md` (6.7K) 📋 Historical
- `NEXT_SESSION_PPRRVU.md` (13K) ⚠️ Obsolete?
- `README_PPRRVU.md` (5.5K) ✅ Parser documentation

### Project Management (7 files)
- `STATUS_REPORT.md` ✅ Current status
- `NEXT_TODOS.md` ✅ Active TODO list
- `github_tasks_plan.md` ✅ Task tracker
- `INGESTOR_DEVELOPMENT_TASKS.md` ✅ Ingestor tasks
- `TESTING_SUMMARY.md` 📋 Test status
- `INGESTOR_INVENTORY.md` 📋 Ingestor catalog
- `DIS_COMPLIANCE_SUMMARY.md` 📋 Compliance status

### Architecture & Standards (3 files)
- `STD_PARSER_CONTRACTS_V1_3_UPDATES.md` 📋 Historical
- `cross_reference_report.md` 📋 Audit artifact

### Guides (5 files)
- `README.md` ✅ Main README
- `README.docker.md` ✅ Docker guide
- `HOW_TO_RUN_LOCALLY.md` ✅ Setup guide
- `INGESTION_GUIDE.md` ✅ Ingestion guide
- `CHANGELOG.md` ✅ Project changelog

**TOTAL:** 26 markdown files in workspace root (many planning-related)

---

## 💡 **Recommended Organization**

### Option 1: Create `planning/` Folder ⭐⭐⭐ **RECOMMENDED**

**Structure:**
```
/Users/alexanderbea/Cursor/cms-api/
├── planning/
│   ├── parsers/
│   │   ├── gpci/
│   │   │   ├── GPCI_PARSER_PLAN_FINAL.md (ACTIVE)
│   │   │   ├── GPCI_SCHEMA_V1.2_RATIONALE.md (ADR)
│   │   │   ├── GPCI_QUICK_START.md (Quick reference)
│   │   │   └── archive/
│   │   │       ├── GPCI_PARSER_PLAN.md (v1.0/v2.0)
│   │   │       ├── GPCI_PARSER_PLAN_V2.md
│   │   │       ├── GPCI_IMPROVEMENTS_SUMMARY.md
│   │   │       ├── GPCI_PLAN_COMPLETENESS.md
│   │   │       └── GPCI_PLAN_REVIEW.md
│   │   ├── pprrvu/
│   │   │   ├── README_PPRRVU.md (Documentation)
│   │   │   ├── PPRRVU_HANDOFF.md (Reference)
│   │   │   └── archive/
│   │   │       ├── PPRRVU_FIX_PLAN.md
│   │   │       └── NEXT_SESSION_PPRRVU.md
│   │   └── conversion_factor/
│   │       └── (future CF planning docs)
│   ├── tasks/
│   │   ├── github_tasks_plan.md
│   │   ├── NEXT_TODOS.md
│   │   ├── INGESTOR_DEVELOPMENT_TASKS.md
│   │   └── INGESTOR_INVENTORY.md
│   └── status/
│       ├── STATUS_REPORT.md
│       ├── TESTING_SUMMARY.md
│       └── DIS_COMPLIANCE_SUMMARY.md
├── docs/
│   └── (keep as-is for general docs)
├── prds/
│   └── (keep as-is for PRDs)
└── (root - keep only essential files)
    ├── README.md
    ├── CHANGELOG.md
    ├── HOW_TO_RUN_LOCALLY.md
    ├── INGESTION_GUIDE.md
    └── README.docker.md
```

**Benefits:**
- ✅ Clear separation: planning vs docs vs PRDs
- ✅ Parser-specific organization
- ✅ Archive pattern for superseded plans
- ✅ Clean workspace root (5 essential files only)
- ✅ Scales to future parsers (ANES, OPPSCAP, Locality)

---

### Option 2: Create `docs/planning/` Subfolder ⭐⭐

**Structure:**
```
docs/
├── planning/
│   ├── parsers/
│   │   ├── gpci/...
│   │   └── pprrvu/...
│   ├── tasks/...
│   └── status/...
└── prd_learning_inbox.md
```

**Benefits:**
- ✅ All docs in one place
- ❌ Mixing planning with other docs
- ❌ Less clear separation

---

### Option 3: Keep in Root with Prefix ⭐

**Pattern:**
- `PLAN-GPCI-v2.1-FINAL.md`
- `PLAN-PPRRVU-HANDOFF.md`
- `TASK-github-tasks.md`
- `STATUS-project.md`

**Benefits:**
- ✅ No directory changes
- ❌ Still clutters root
- ❌ Doesn't scale well

---

## 🎯 **My Recommendation: Option 1 (planning/ folder)**

### **Action Plan:**

1. **Create structure** (2 min)
2. **Move files** (5 min)
3. **Update references** (3 min)
4. **Clean up** (2 min)

**Total:** 12 minutes

Want me to execute this organization now?

