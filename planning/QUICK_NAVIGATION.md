# Quick Navigation - Planning Documents

**Updated:** 2025-10-16

---

## 🎯 **START HERE**

### Current Work (GPCI Parser)
- **Implementation Plan:** `parsers/gpci/IMPLEMENTATION.md` ⭐
- **Quick Start:** `parsers/gpci/QUICK_START.md`
- **Schema Rationale:** `parsers/gpci/GPCI_SCHEMA_V1.2_RATIONALE.md`

### Project Status
- **Overall Status:** `project/STATUS_REPORT.md`
- **Next TODOs:** `project/NEXT_TODOS.md`
- **Task Tracker:** `project/github_tasks_plan.md`

---

## 📂 **Directory Guide**

```
planning/
├── parsers/           Parser implementation plans
│   ├── gpci/         GPCI parser (ACTIVE - implementing now)
│   ├── pprrvu/       PPRRVU parser (COMPLETE)
│   └── conversion_factor/  CF parser (COMPLETE)
│
├── project/          Project management
│   ├── STATUS_REPORT.md
│   ├── NEXT_TODOS.md
│   └── github_tasks_plan.md
│
└── architecture/     Architecture decisions
    └── STD_PARSER_CONTRACTS_V1_3_UPDATES.md
```

---

## 📋 **File Naming Convention**

| File Name | Purpose |
|-----------|---------|
| `IMPLEMENTATION.md` | Authoritative implementation plan (active) |
| `QUICK_START.md` | Fast reference for implementers |
| `PARSER_DOCUMENTATION.md` | Parser-specific documentation |
| `*_HANDOFF.md` | Handoff documentation |
| `*_RATIONALE.md` | Architectural decision records (ADRs) |
| `archive/*.md` | Superseded documents (preserved for history) |

---

## 🚀 **Quick Commands**

```bash
# Navigate to GPCI implementation
cd /Users/alexanderbea/Cursor/cms-api/planning/parsers/gpci
open IMPLEMENTATION.md

# Check project status
cd /Users/alexanderbea/Cursor/cms-api/planning/project
open STATUS_REPORT.md

# View all planning docs
cd /Users/alexanderbea/Cursor/cms-api/planning
find . -name "*.md" -not -path "*/archive/*"
```

---

**For full inventory:** See `INDEX.md`

