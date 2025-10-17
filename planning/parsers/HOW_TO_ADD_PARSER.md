# How to Add a New Parser - Step-by-Step Guide

**Time to Set Up:** 10-15 minutes for planning structure  
**Time to Full Implementation:** 2-3 hours (following the template)

---

## 🚀 **Quick Start (5 Minutes)**

### Option 1: Using the Script (Fastest)

```bash
cd /Users/alexanderbea/Cursor/cms-api

# Run the setup script (creates directory + README from template)
./tools/create_parser_plan.sh {parser_name} {schema_id} {prd_name}

# Example:
./tools/create_parser_plan.sh mac_locality cms_mac_locality_v1.0 PRD-rvu-gpci-prd-v0.1.md
```

**Output:**
- ✅ `planning/parsers/{parser_name}/` directory created
- ✅ `planning/parsers/{parser_name}/README.md` populated from template
- ✅ `planning/parsers/{parser_name}/archive/` created
- ✅ Entry added to `planning/parsers/README.md`

---

### Option 2: Manual Setup (10 Minutes)

```bash
cd /Users/alexanderbea/Cursor/cms-api

# 1. Create directory structure (30 seconds)
PARSER_NAME="mac_locality"  # Replace with your parser name
mkdir -p "planning/parsers/${PARSER_NAME}/archive"

# 2. Copy template (15 seconds)
cp planning/parsers/_template/README.md "planning/parsers/${PARSER_NAME}/"

# 3. Edit README with parser details (5-10 minutes)
# Replace placeholders:
#   {PARSER_NAME} → Your parser name (e.g., "MAC Locality")
#   {schema_id} → Schema ID (e.g., cms_mac_locality_v1.0)
#   {LAYOUT_NAME} → Layout constant (e.g., MAC_LOCALITY_2025_LAYOUT)
#   {relevant-prd} → PRD filename (e.g., PRD-rvu-gpci-prd-v0.1.md)
#   {path}/{filename} → Sample data path

# 4. Update master index (2 minutes)
# Add entry to planning/parsers/README.md (§3 CMS Parsers section)
```

---

## 📋 **Template Placeholders to Fill**

When editing `README.md` from template, replace these:

| Placeholder | Replace With | Example |
|------------|--------------|---------|
| `{PARSER_NAME}` | Display name | `MAC Locality` |
| `{schema_id}` | Schema contract ID | `cms_mac_locality_v1.0` |
| `{LAYOUT_NAME}` | Layout registry constant | `MAC_LOCALITY_2025_LAYOUT` |
| `{relevant-prd}` | Related PRD filename | `PRD-mac-locality-prd-v1.0.md` |
| `{path}/{filename}` | Sample data location | `sample_data/rvu25d_0/MAC_LOC.txt` |
| `YYYY-MM-DD` | Today's date | `2025-10-17` |

---

## 🎯 **Complete Workflow (New Parser from Scratch)**

### Phase 0: Planning Setup (10-15 min)

#### Step 1: Create Directory Structure
```bash
mkdir -p planning/parsers/new_parser/archive
```

#### Step 2: Copy and Customize README
```bash
cp planning/parsers/_template/README.md planning/parsers/new_parser/
# Edit: Fill in parser-specific details (use table above)
```

#### Step 3: Update Master Index
```bash
# Edit: planning/parsers/README.md
# Add your parser to the "CMS Parsers" section
```

✅ **Checkpoint:** You now have discoverable planning structure!

---

### Phase 1: Pre-Implementation (25 min)

**Use GPCI as reference:** `planning/parsers/gpci/`

#### Step 4: Create Pre-Implementation Plan (if needed)
```bash
# Copy GPCI's plan as template
cp planning/parsers/gpci/PRE_IMPLEMENTATION_PLAN.md planning/parsers/new_parser/

# Customize for your parser:
# - Verify sample data exists
# - Measure line lengths (if fixed-width)
# - Document column positions
# - Update layout registry
```

**When to skip:** Simple CSV-only parsers don't need this.

---

### Phase 2: Implementation Planning (30-60 min)

#### Step 5: Create Implementation Plan
```bash
# Copy GPCI's implementation as template
cp planning/parsers/gpci/IMPLEMENTATION.md planning/parsers/new_parser/

# Customize sections:
# 1. Schema overview (Core, Enrichment, Provenance columns)
# 2. Natural keys & expected row counts
# 3. Format support (TXT/CSV/XLSX/ZIP)
# 4. 11-step parser template (from STD-parser-contracts §21.1)
# 5. Helper functions
# 6. Validation rules
# 7. Testing strategy
# 8. Time estimates
```

**Key customizations:**
- Natural keys
- Row count expectations
- Validation rules
- GPCI bounds → your domain-specific bounds
- Helper functions specific to your format

✅ **Checkpoint:** You have a complete implementation guide!

---

### Phase 3: Implementation (2-3 hours)

#### Step 6: Code the Parser
Follow your `IMPLEMENTATION.md` step-by-step:

1. **Create parser file** (10 min)
   ```python
   # File: cms_pricing/ingestion/parsers/new_parser.py
   from cms_pricing.ingestion.parsers._parser_kit import *
   
   PARSER_VERSION = "v1.0.0"
   SCHEMA_ID = "cms_new_schema_v1.0"
   NATURAL_KEYS = ["key1", "key2", "effective_from"]
   ```

2. **Implement 11-step template** (90 min)
   - Copy structure from `conversion_factor_parser.py` or `pprrvu_parser.py`
   - Customize for your schema

3. **Add tests** (45 min)
   - Golden tests (valid files)
   - Negative tests (invalid files)
   - Determinism test

4. **Document** (15 min)
   - Update CHANGELOG.md
   - Create fixture README
   - Update status in planning README

✅ **Checkpoint:** Parser complete!

---

### Phase 4: Add Backwards Reference to PRD (5 min)

#### Step 7: Link PRD to Implementation
```bash
# Edit: prds/{relevant-prd}.md
# Add to "Implementation Resources:" section:

**Implementation Resources:**
- **{Parser} Planning:** `planning/parsers/{parser_name}/README.md`
- **Implementation Plan:** `planning/parsers/{parser_name}/IMPLEMENTATION.md`
- **Schema Contract:** `cms_pricing/ingestion/contracts/{schema_id}.json`
- **Sample Data:** `sample_data/{path}/{filename}`
```

✅ **Checkpoint:** Bidirectional links complete!

---

## ⏱️ **Time Breakdown**

| Phase | Activity | Time | Can Skip? |
|-------|----------|------|-----------|
| **0. Planning Setup** | Create structure, README, index | 10-15 min | ❌ No |
| **1. Pre-Implementation** | Layout verification, measurements | 25 min | ✅ If CSV-only |
| **2. Implementation Planning** | Write IMPLEMENTATION.md | 30-60 min | ⚠️ Not recommended |
| **3. Implementation** | Code parser + tests | 2-3 hours | ❌ No |
| **4. PRD Link** | Add backwards reference | 5 min | ❌ No |
| **Total (Full)** | With all phases | **3-4 hours** | - |
| **Total (Minimal)** | Skip pre-impl + quick planning | **2.5 hours** | - |

---

## 🎨 **Example: Adding MAC Locality Parser**

### Step-by-Step Example

```bash
# 1. Create structure (30 seconds)
mkdir -p planning/parsers/mac_locality/archive

# 2. Copy template (5 seconds)
cp planning/parsers/_template/README.md planning/parsers/mac_locality/

# 3. Edit README (5 minutes)
vim planning/parsers/mac_locality/README.md
# Replace:
#   {PARSER_NAME} → MAC Locality
#   {schema_id} → cms_mac_locality_v1.0
#   Status: ⏳ Planned
#   Natural Keys: TBD → ['mac', 'locality_code', 'effective_from']
#   Expected Rows: TBD → 100-120 rows
#   Related PRD → PRD-mac-locality-prd-v1.0.md

# 4. Update master index (2 minutes)
vim planning/parsers/README.md
# Add to §3 (after Locality):
# ### 6. MAC Locality Parser
# **Status:** ⏳ Planned
# **Location:** `mac_locality/`
# **Priority:** P2 (Medium)

# 5. Create implementation plan (copy GPCI as reference)
cp planning/parsers/gpci/IMPLEMENTATION.md planning/parsers/mac_locality/
# Edit: Customize for MAC Locality specifics (30 minutes)

# 6. Add PRD backwards reference
vim prds/PRD-mac-locality-prd-v1.0.md
# Add "Implementation Resources:" section
```

**Total time:** ~45 minutes for complete planning structure!

---

## ✅ **What Makes It Easy?**

### 1. **Template-Driven** ✅
- Copy `_template/README.md` → instant structure
- All placeholders clearly marked
- Consistent across all parsers

### 2. **Reference Examples** ✅
- GPCI: Complete, well-documented (reference for complex parsers)
- Conversion Factor: Simple CSV/XLSX (reference for simple parsers)
- PPRRVU: Fixed-width example

### 3. **Clear Standards** ✅
- STD-parser-contracts §21.1 (11-step template)
- Standard file names (README, IMPLEMENTATION, etc.)
- Standard sections in every doc

### 4. **Minimal Boilerplate** ✅
- Only 3 required files: directory, README, archive/
- Optional files only when needed
- Master index auto-discovery

### 5. **Bidirectional Links** ✅
- PRD → Planning docs
- Planning docs → PRD
- Planning docs → Code/Schema/Data

---

## 🚨 **Common Mistakes to Avoid**

### ❌ Don't Do This:
1. **Skip the README** - Makes parser undiscoverable
2. **Skip master index update** - Parser won't show in catalog
3. **Skip PRD backwards reference** - Breaks discoverability from PRD
4. **Copy-paste without customization** - Placeholder text left in docs
5. **Create without archive/ directory** - No place for historical docs

### ✅ Do This Instead:
1. **Always create README first** - Use template
2. **Update master index immediately** - Add to planning/parsers/README.md
3. **Add PRD link** - Bidirectional references
4. **Customize all placeholders** - Search for `{` and `TBD`
5. **Create archive/ upfront** - Standard structure

---

## 🎯 **Verification Checklist**

After creating new parser planning:

-  Directory exists: `planning/parsers/{name}/`
-  Archive exists: `planning/parsers/{name}/archive/`
-  README exists and customized (no `{placeholders}` or `TBD`)
-  Entry in master index: `planning/parsers/README.md`
-  PRD has backwards reference (if PRD exists)
-  Sample data location verified
-  Schema contract exists (or planned)
-  Layout defined (if fixed-width)

---

## 📚 **Quick Reference Commands**

```bash
# List all parser planning directories
ls -d planning/parsers/*/

# Check structure of a parser
ls -1 planning/parsers/{name}/

# Find all parser READMEs
find planning/parsers -name "README.md" -maxdepth 2

# Validate no placeholders left
grep -r "{" planning/parsers/{name}/README.md

# Check master index includes your parser
grep -i "{name}" planning/parsers/README.md
```

---

## 🆘 **Need Help?**

**Questions about:**
- **Structure:** See `planning/parsers/README.md`
- **Template:** See `planning/parsers/_template/README.md`
- **Complex parser:** See `planning/parsers/gpci/` (7 docs, complete structure)
- **Simple parser:** See `planning/parsers/conversion_factor/` (minimal structure)
- **Standards:** See `prds/STD-parser-contracts-prd-v1.0.md` (§21.1)

---

## 🎉 **Summary**

**With the standardized structure:**
- ✅ **10-15 minutes** to set up planning structure
- ✅ **Copy-paste template** → instant README
- ✅ **Reference examples** for every parser type
- ✅ **Clear standards** (11-step template)
- ✅ **Discoverable** (master index + bidirectional links)

**Before standardization:**
- ❌ 30-60 minutes figuring out structure
- ❌ Inconsistent file names
- ❌ No template
- ❌ Hard to find related docs

**Improvement:** **~75% time savings** on planning setup! 🚀

