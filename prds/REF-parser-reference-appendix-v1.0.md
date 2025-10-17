# Parser Reference Appendix

**Purpose:** Static reference tables, format characteristics, and compatibility notes  
**Audience:** Parser developers, QA engineers  
**Status:** Draft v1.0  
**Owners:** Data Platform Engineering  
**Consumers:** All parser developers  
**Change control:** PR review  

**Cross-References:**
- **STD-parser-contracts-prd-v2.0.md:** Core contracts
- **STD-parser-contracts-impl-v2.0.md:** Implementation guidance (§1.3 alias maps)
- **REF-parser-routing-detection-v1.0.md:** Router architecture
- **REF-parser-quality-guardrails-v1.0.md:** Validation patterns

---

## 1. Overview

This appendix provides static reference information for parser development:
- **Appendix A:** CMS file format characteristics
- **Appendix B:** Column normalization examples
- **Appendix C:** Reference validation details
- **Appendix D:** Backward compatibility notes

**Use this for:**
- Quick reference during parser development
- Understanding CMS file format variations
- Common column name aliases
- Reference validation patterns

---

## Appendix A: CMS File Format Reference

### A.1 Fixed-Width TXT Files

**Datasets:** PPRRVU, GPCI, ANES, LOCCO, OPPSCAP

**Characteristics:**
- No delimiters (comma, tab, pipe)
- Column positions defined in layout specification
- Usually include header row (skip line 1)
- Line length varies by dataset (165-300 chars typical)

**Authoritative Source:** CMS RVU cycle PDFs (e.g., RVU25D.pdf)

**Common Patterns:**
- HCPCS codes at start (position 0-5)
- Modifier immediately after HCPCS (position 5-7)
- RVU values right-aligned with decimal precision
- Indicator flags as single characters (Y/N, 0-9)

**Encoding:** Typically UTF-8, occasionally CP1252

### A.2 CSV Files

**Datasets:** All datasets have CSV variants

**Characteristics:**
- Header row (case varies: Title Case, lowercase, UPPERCASE)
- Comma-delimited (sometimes tab)
- Quoted fields for descriptions containing commas
- UTF-8 or Latin-1 encoding
- May have UTF-8 BOM (0xEF 0xBB 0xBF)

**Common Quirks:**
- Multiple header rows (title + column names)
- Trailing spaces in headers
- Typos in headers (e.g., "Adminstrative" not "Administrative")
- Year prefixes in column names (2025_pw_gpci, 2024_pw_gpci)

**Encoding:** UTF-8 (with or without BOM), CP1252, Latin-1

### A.3 Excel Workbooks

**Datasets:** Conversion factors, some quarterly files

**Characteristics:**
- .xlsx format (Office Open XML)
- Multiple sheets possible (Title, Data, Notes)
- Header row typically on row 2-3 (not row 1)
- Numeric formatting can vary
- May contain formula cells (evaluate before parse)

**Common Quirks:**
- Numeric coercion (0 → 0.0, 1 → 1.0)
- Date autoformatting
- Scientific notation for large numbers
- Hidden columns/rows
- Multiple quarterly snapshots in one file

**Encoding:** Binary (Excel format), not text

### A.4 ZIP Archives

**Datasets:** All CMS releases distributed as ZIP

**Contents:**
- Multiple files (TXT, CSV, XLSX, PDF)
- Directory structure varies
- Readme/documentation files included
- May contain subdirectories

**Common Patterns:**
- PPRRVU in TXT format
- GPCI in TXT format
- Conversion factors in XLSX
- PDFs with release notes

**Handling:**
- Extract inner files in memory (don't write to disk)
- Skip non-data files (PDFs, docs)
- Recursive format detection for inner files

---

## Appendix B: Column Normalization Examples

### B.1 HCPCS Code Variations

**Aliases found in CMS files:**
- `HCPCS`, `HCPCS_CODE`, `HCPCS_CD`, `CPT`, `CODE`

**Canonical:** `hcpcs`

**Normalization:**
```python
ALIAS_MAP = {
    'HCPCS': 'hcpcs',
    'HCPCS_CODE': 'hcpcs',
    'HCPCS_CD': 'hcpcs',
    'CPT': 'hcpcs',
    'CODE': 'hcpcs',
}
```

### B.2 RVU Component Variations

**Work RVU aliases:**
- `WORK_RVU`, `RVU_WORK`, `WORK`, `WORK_RVUS`

**PE Non-Facility aliases:**
- `PE_NONFAC_RVU`, `PE_RVU_NONFAC`, `NON_FAC_PE_RVU`, `PE_NON_FAC`

**Canonical (Schema Format):**
- `rvu_work`, `rvu_pe_nonfac`, `rvu_pe_fac`, `rvu_malp`

**Note:** API uses different format (`work_rvu`, `pe_rvu_nonfac`). Parsers output schema format only.

### B.3 GPCI Component Variations

**Aliases:**
- `2025 PW GPCI (with 1.0 floor)`, `2025_pw_gpci_(with_1.0_floor)`, `2025 PW GPCI`, `pw_gpci_2025`
- `2025 PE GPCI`, `2025_pe_gpci`, `pe_gpci_2025`
- `2025 MP GPCI`, `2025_mp_gpci`, `mp_gpci_2025`

**Canonical:**
- `gpci_work`, `gpci_pe`, `gpci_mp`

### B.4 Locality/Geography Variations

**Locality Code:**
- `LOCALITY`, `LOCALITY_CODE`, `LOCALITY_NUMBER`, `LOC_CD`

**State:**
- `STATE`, `STATE_NAME`, `STATE_CD`, `ST`

**County:**
- `COUNTY`, `COUNTY_NAME`, `COUNTIES`, `CNTY`

**Canonical:**
- `locality_code`, `state_name`, `county_names`

---

## Appendix C: Reference Validation Details

### C.1 HCPCS/CPT Validation

**Phase 1 (Minimal):**
- Format: 5 alphanumeric characters `^[A-Z0-9]{5}$`
- Action: WARN on format violation, quarantine

**Phase 2 (Comprehensive):**
- Lookup in CMS HCPCS reference file
- Check effective date range
- Validate status (active, deleted)
- Action: BLOCK on unknown codes

**Implementation:**
```python
# Phase 1: Format check
invalid_format = ~df['hcpcs'].str.match(r'^[A-Z0-9]{5}$')
if invalid_format.any():
    logger.warning(f"{invalid_format.sum()} invalid HCPCS formats")
    # Quarantine
    
# Phase 2: Reference lookup
from reference import load_hcpcs_codes
valid_codes = load_hcpcs_codes(vintage=metadata['vintage_date'])
unknown = ~df['hcpcs'].isin(valid_codes)
if unknown.any():
    logger.warning(f"{unknown.sum()} unknown HCPCS codes")
    # Quarantine or BLOCK per policy
```

### C.2 Locality Code Validation

**Phase 1:**
- Format: 2-digit string `^\d{2}$`
- Action: WARN on format violation

**Phase 2:**
- Lookup in locality crosswalk
- Validate MAC + locality combination
- Check effective date
- Action: BLOCK on unknown locality

### C.3 FIPS Code Validation

**Format:** 5-digit string (2-digit state + 3-digit county)

**Validation:**
- State FIPS exists (01-56)
- County FIPS exists for that state
- Combined FIPS in authoritative list

**Action:** BLOCK on invalid FIPS

---

## Appendix D: Backward Compatibility

### D.1 Deprecation Policy

**When replacing private methods with public parsers:**

1. Keep original method as wrapper (1-2 releases)
2. Add `DeprecationWarning`
3. Update docstring with migration path
4. Remove in next major version

**Example:**
```python
def _parse_pprrvu_file(self, file_obj, filename):
    """
    DEPRECATED: Use cms_pricing.ingestion.parsers.pprrvu_parser.parse_pprrvu()
    
    This wrapper maintained for backwards compatibility.
    Will be removed in v2.0.
    """
    warnings.warn(
        "RVUIngestor._parse_pprrvu_file is deprecated. "
        "Use pprrvu_parser.parse_pprrvu() instead.",
        DeprecationWarning
    )
    
    from cms_pricing.ingestion.parsers.pprrvu_parser import parse_pprrvu
    
    metadata = self._build_parser_metadata()
    return parse_pprrvu(file_obj, filename, metadata)
```

### D.2 Migration Timeline

- **v1.0**: Introduce shared parsers, keep wrappers
- **v1.5**: Deprecation warnings active, migration guide published
- **v2.0**: Remove wrappers, shared parsers only

### D.3 Schema Evolution

**Adding Optional Columns:**
- Minor version bump (v1.0 → v1.1)
- Old parsers still work
- New parsers populate new column

**Removing Columns:**
- Major version bump (v1.0 → v2.0)
- Requires parser updates
- Deprecation period required

**Changing Types:**
- Major version bump
- Test all downstream consumers
- Document migration path

---

## Appendix E: Quick Reference Tables

### E.1 File Extensions by Dataset

| Dataset | TXT | CSV | XLSX | ZIP |
|---------|-----|-----|------|-----|
| PPRRVU | ✅ | ✅ | ❌ | ✅ |
| GPCI | ✅ | ✅ | ✅ | ✅ |
| ANES | ✅ | ✅ | ❌ | ✅ |
| LOCCO | ✅ | ✅ | ✅ | ✅ |
| Conversion Factor | ❌ | ❌ | ✅ | ✅ |

### E.2 Typical Line Lengths (Fixed-Width)

| Dataset | Min | Max | Notes |
|---------|-----|-----|-------|
| PPRRVU | 165 | 173 | Use min_line_length=160 |
| GPCI | 160 | 173 | Use min_line_length=155 |
| ANES | 140 | 150 | Use min_line_length=135 |
| LOCCO | 120 | 200+ | Variable (county lists) |

### E.3 Natural Keys by Dataset

| Dataset | Natural Keys | Uniqueness Policy |
|---------|--------------|-------------------|
| PPRRVU | `['hcpcs', 'modifier']` | BLOCK |
| GPCI | `['locality_code']` | WARN |
| ANES | `['cf_type', 'effective_from']` | BLOCK |
| LOCCO (raw) | `['mac', 'locality_code']` | WARN |
| Conversion Factor | `['cf_type', 'effective_from']` | BLOCK |

---

## Cross-References

**Main Documents:**
- STD-parser-contracts-prd-v2.0.md
- STD-parser-contracts-impl-v2.0.md
- REF-parser-routing-detection-v1.0.md
- REF-parser-quality-guardrails-v1.0.md
- RUN-parser-qa-runbook-prd-v1.0.md

**Related:**
- STD-qa-testing-prd-v1.0.md
- STD-data-architecture-prd-v1.0.md

---

## Source Section Mapping (v1.11 → appendix v1.0)

**For reference during transition:**

This appendix contains content from the following sections of `STD-parser-contracts-prd-v1.11-ARCHIVED.md`:

| appendix v1.0 Section | Original v1.11 Section | Lines in v1.11 |
|-----------------------|------------------------|----------------|
| Appendix A: File Formats | Appendix A: CMS File Format Reference | 4308-4350 |
| Appendix B: Column Normalization | Appendix B: Column Normalization Examples | 4353-4372 |
| Appendix C: Reference Validation | Appendix C: Reference Validation Details | 4375-4400 |
| Appendix D: Backward Compatibility | Appendix D: Backward Compatibility | 4403-4442 |
| Appendix E: Quick Reference Tables | (New - extracted from various sections) | Various |

**Sections NOT in this document (see other companions):**
- Core policy → STD-parser-contracts-prd-v2.0.md
- Implementation → STD-parser-contracts-impl-v2.0.md
- Routing → REF-parser-routing-detection-v1.0.md
- Quality → REF-parser-quality-guardrails-v1.0.md
- QA → RUN-parser-qa-runbook-prd-v1.0.md

**Archived source:** `/Users/alexanderbea/Cursor/cms-api/prds/STD-parser-contracts-prd-v1.11-ARCHIVED.md`

**Cross-Reference:**
- **DOC-master-catalog-prd-v1.0.md:** Master system catalog registration

---

## Change Log

| Date | Version | Author | Summary |
|------|---------|--------|---------|
| **2025-10-17** | **v1.0** | **Team** | **Initial reference appendix.** Split from STD-parser-contracts-prd-v1.11 Appendices A-D. Contains: CMS file format reference (Appendix A: TXT/CSV/XLSX/ZIP characteristics), column normalization examples (Appendix B: HCPCS/RVU/GPCI aliases), reference validation details (Appendix C: validation patterns), backward compatibility notes (Appendix D: deprecation policy), quick reference tables (Appendix E: extensions/line lengths/natural keys by dataset). Total: ~300 lines. **Cross-References:** All parser standards documents. |

---

*End of Reference Appendix*

*For core contracts, see STD-parser-contracts-prd-v2.0.md*  
*For implementation guidance, see STD-parser-contracts-impl-v2.0.md*

