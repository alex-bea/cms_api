# Parser Planning - Quick Reference Card

**For:** Adding new parsers  
**Time:** 10-15 minutes (planning setup)

---

## 🚀 **One-Command Setup**

```bash
./tools/create_parser_plan.sh {parser_name} {schema_id} [{prd_name}]
```

**Example:**
```bash
./tools/create_parser_plan.sh mac_locality cms_mac_locality_v1.0 PRD-mac-locality-prd-v1.0.md
```

**Creates:**
- ✅ `planning/parsers/{name}/` directory
- ✅ `planning/parsers/{name}/README.md` (from template)
- ✅ `planning/parsers/{name}/archive/` directory

---

## 📋 **Manual 3-Step Process**

```bash
# 1. Create structure (30 sec)
mkdir -p planning/parsers/{name}/archive

# 2. Copy template (5 sec)
cp planning/parsers/_template/README.md planning/parsers/{name}/

# 3. Customize README (5-10 min)
#    Replace: {PARSER_NAME}, {schema_id}, TBD values
vim planning/parsers/{name}/README.md
```

---

## ✅ **Required Customizations**

In `README.md`, replace:

| Find | Replace With |
|------|--------------|
| `{PARSER_NAME}` | Display name (e.g., "MAC Locality") |
| `{schema_id}` | Schema contract ID |
| `YYYY-MM-DD` | Today's date |
| `{relevant-prd}` | Related PRD filename |
| `{parser_name}` | Snake_case name (e.g., mac_locality) |
| `{LAYOUT_NAME}` | Layout constant (if fixed-width) |
| `TBD` | Actual values (natural keys, row counts, etc.) |

---

## 📂 **Standard Structure**

```
planning/parsers/{name}/
├── README.md                      # Index (required)
├── IMPLEMENTATION.md              # Full guide (create before coding)
├── PRE_IMPLEMENTATION_PLAN.md     # Optional (if complex setup)
├── QUICK_START.md                 # Optional (fast reference)
├── SCHEMA_RATIONALE.md            # Optional (if custom schema)
└── archive/                       # Required (for historical docs)
```

---

## 🔗 **Required Updates**

### 1. Master Index
**File:** `planning/parsers/README.md`  
**Section:** §3 CMS Parsers  
**Add:**
```markdown
### X. {Parser} Parser
**Status:** ⏳ Planned
**Location:** `{name}/`
**Priority:** PX
```

### 2. PRD Backwards Reference
**File:** `prds/{prd-name}.md`  
**Section:** Implementation Resources:  
**Add:**
```markdown
- **{Parser} Planning:** `planning/parsers/{name}/README.md`
- **Implementation Plan:** `planning/parsers/{name}/IMPLEMENTATION.md`
```

---

## 📖 **Reference Examples**

| Parser | Use As Reference For |
|--------|---------------------|
| **GPCI** | Complete structure, all optional docs |
| **Conversion Factor** | Simple CSV/XLSX parser |
| **PPRRVU** | Fixed-width parser |

**Browse:**
```bash
ls -1 planning/parsers/gpci/    # Full example
ls -1 planning/parsers/conversion_factor/  # Minimal example
```

---

## ⏱️ **Time Estimates**

| Task | Time |
|------|------|
| Planning setup (structure + README) | 10-15 min |
| Pre-implementation plan | 25 min (optional) |
| Implementation plan | 30-60 min |
| Code parser + tests | 2-3 hours |
| **Total (full)** | **3-4 hours** |

---

## ✅ **Verification Checklist**

- [ ] Directory: `planning/parsers/{name}/` exists
- [ ] Archive: `planning/parsers/{name}/archive/` exists
- [ ] README exists and has no `{placeholders}`
- [ ] README has no `TBD` values
- [ ] Added to master index (`planning/parsers/README.md`)
- [ ] PRD has backwards reference
- [ ] Sample data location verified
- [ ] Schema contract exists (or planned)

---

## 🆘 **Quick Links**

- **How-To Guide:** `planning/parsers/HOW_TO_ADD_PARSER.md`
- **Master Index:** `planning/parsers/README.md`
- **Template:** `planning/parsers/_template/README.md`
- **Script:** `tools/create_parser_plan.sh`
- **Standards:** `prds/STD-parser-contracts-prd-v1.0.md`

---

## 🎯 **TL;DR**

```bash
# 1. Run script
./tools/create_parser_plan.sh new_parser cms_new_v1.0

# 2. Edit README (fill TBD values)
vim planning/parsers/new_parser/README.md

# 3. Update master index
vim planning/parsers/README.md

# 4. Add PRD backwards reference
vim prds/{relevant-prd}.md

# Done! (10-15 min total)
```

---

**That's it!** With the standardized structure, adding a new parser planning directory takes only **10-15 minutes**. 🚀

