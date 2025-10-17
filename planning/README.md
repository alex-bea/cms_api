# Planning Documents

This directory contains implementation plans, task trackers, and status reports for the CMS Pricing API project.

**Last Updated:** 2025-10-16

---

## 📂 Directory Structure

### `parsers/` - Parser Implementation Plans
Parser-specific planning documents organized by dataset.

**Active Parsers:**
- **GPCI:** `parsers/gpci/IMPLEMENTATION.md` (v2.1 - CMS-native schema)
- **PPRRVU:** `parsers/pprrvu/PARSER_DOCUMENTATION.md`
- **Conversion Factor:** (in code, no separate planning doc)

**Archive Pattern:** Superseded plans are in `parsers/{dataset}/archive/` subdirectories.

### `project/` - Project Management
- **STATUS_REPORT.md** - Overall project status (parsers, infrastructure, progress)
- **NEXT_TODOS.md** - Active TODO list with priorities
- **github_tasks_plan.md** - Comprehensive task tracker with 63+ tasks
- **INGESTOR_DEVELOPMENT_TASKS.md** - Ingestor-specific tasks
- **TESTING_SUMMARY.md** - Test status and coverage
- **INGESTOR_INVENTORY.md** - Ingestor catalog
- **DIS_COMPLIANCE_SUMMARY.md** - DIS compliance status

### `architecture/` - Architecture Decisions
- **STD_PARSER_CONTRACTS_V1_3_UPDATES.md** - Historical parser contract updates
- **cross_reference_report.md** - PRD cross-reference audit

---

## 🎯 **Quick Navigation**

### Current Work
- **GPCI Parser Implementation:** `parsers/gpci/IMPLEMENTATION.md`
- **Project Status:** `project/STATUS_REPORT.md`
- **Active TODOs:** `project/NEXT_TODOS.md`

### Task Management
- **GitHub Tasks:** `project/github_tasks_plan.md`
- **Ingestor Tasks:** `project/INGESTOR_DEVELOPMENT_TASKS.md`

### Reference
- **GPCI Quick Start:** `parsers/gpci/QUICK_START.md`
- **GPCI Schema Rationale:** `parsers/gpci/SCHEMA_V1.2_RATIONALE.md`
- **PPRRVU Handoff:** `parsers/pprrvu/PPRRVU_HANDOFF.md`

---

## 📋 **Active Parser Status**

| Parser | Status | Plan Location | Schema | Tests |
|--------|--------|---------------|--------|-------|
| **PPRRVU** | ✅ Complete | `parsers/pprrvu/` | v1.1 | 7/7 passing |
| **Conversion Factor** | ✅ Complete | (in code) | v2.0 | Golden + 11 negatives |
| **GPCI** | 🚧 Ready to implement | `parsers/gpci/IMPLEMENTATION.md` | v1.2 | Planned: 13 tests |
| ANES | ⏸️ Pending | - | v1.0 | - |
| OPPSCAP | ⏸️ Pending | - | v1.0 | - |
| Locality | ⏸️ Pending | - | v1.0 | - |

**Progress:** 2/6 parsers complete (33%)

---

## 🗂️ **Archive Policy**

**When to archive:**
- Plan is superseded by newer version
- Implementation is complete and plan is historical reference
- Document was work-in-progress and final version exists

**How to archive:**
- Move to `{category}/{topic}/archive/` subdirectory
- Rename with descriptive name (e.g., `plan_v1.0.md`)
- Keep for historical reference and learning

**What to keep active:**
- Final/current implementation plans
- Active TODOs and status reports
- Architecture decision records (ADRs)

---

## 📚 **Document Types**

| Type | Example | Location |
|------|---------|----------|
| **Implementation Plan** | GPCI_IMPLEMENTATION.md | `parsers/{dataset}/` |
| **Schema Rationale** | SCHEMA_V1.2_RATIONALE.md | `parsers/{dataset}/` |
| **Quick Start** | QUICK_START.md | `parsers/{dataset}/` |
| **Handoff Doc** | HANDOFF.md | `parsers/{dataset}/` |
| **Status Report** | STATUS_REPORT.md | `project/` |
| **Task Tracker** | github_tasks_plan.md | `project/` |
| **Architecture Decision** | STD_PARSER_CONTRACTS_V1_3_UPDATES.md | `architecture/` |

---

## 🔄 **Update Frequency**

- **Daily:** STATUS_REPORT.md, NEXT_TODOS.md
- **Per PR:** Parser implementation plans, test summaries
- **Weekly:** github_tasks_plan.md
- **As needed:** Architecture docs, handoffs

---

## 📞 **Questions?**

See main project README: `/Users/alexanderbea/Cursor/cms-api/README.md`

For PRDs and standards: `/Users/alexanderbea/Cursor/cms-api/prds/`

