# Locality Parser: Format Authority Matrix

**Purpose:** Document which format is authoritative per vintage for real-source parity testing (QTS §5.1.3).

---

## 2025 Quarter D (2025D)

**Authority Format:** TXT  
**Date Declared:** 2025-10-17  
**Rationale:**
- TXT format (`25LOCCO.txt`) is the canonical CMS fixed-width format
- Most stable over time (fewest CMS layout changes)
- Used as source for CSV/XLSX generation by CMS
- Contains 109 unique localities after parsing and dedup

**Format Priority:** `TXT > CSV > XLSX`

**Parity Thresholds:**
- Natural-key overlap vs TXT: ≥ 98%
- Row-count variance vs TXT: ≤ 1% OR ≤ 2 rows (whichever is stricter)

**Known Variance (as of 2025-10-17):**
- **CSV:** Matches TXT exactly (109 localities)
  - NK overlap: 100%
  - Row variance: 0 rows (0%)
  - Status: ✅ PASS
  
- **XLSX:** Contains 93 localities (~15% fewer than TXT)
  - NK overlap: ~85% (estimated)
  - Row variance: 16 rows (15%)
  - Missing: 16 localities present in TXT
  - Extra: 8 localities NOT in TXT
  - Status: ⚠️ EXPECTED FAIL (documented variance)
  - Investigation: TBD (may be different vintage or manual edits)

**Test Implementation:**
- `tests/parsers/test_locality_parser.py::test_locality_parity_real_source`
- Uses `@pytest.mark.real_source` marker
- Generates diff artifacts in `tests/artifacts/variance/`

**Artifacts Generated:**
- `locality_parity_missing_in_csv.csv` (if any)
- `locality_parity_extra_in_csv.csv` (if any)
- `locality_parity_summary_csv.json`
- `locality_parity_missing_in_xlsx.csv` (if any)
- `locality_parity_extra_in_xlsx.csv` (if any)
- `locality_parity_summary_xlsx.json`

**XLSX Known Mismatch Policy:**

Given XLSX has 15% row variance (exceeds threshold), we:
1. ✅ Document variance in this file
2. ✅ Generate diff artifacts for investigation
3. ⚠️ Expect test to FAIL until variance is resolved
4. 📋 Create GitHub issue to investigate XLSX provenance
5. 📋 Consider using `@pytest.mark.xfail(strict=True)` with issue reference

**Next Actions:**
- [ ] Create GitHub issue for XLSX variance investigation
- [ ] Determine if XLSX is from different vintage
- [ ] Decide: Fix XLSX file OR document as permanent known variance
- [ ] If permanent: Add `xfail` marker with issue ID and expiry date

---

## Future Vintages

When processing new vintages (2025A, 2025B, 2025C, 2026A, etc.):

1. Review row counts across all formats
2. Confirm TXT remains most stable format
3. Update this document if authority changes
4. Document any known variance patterns
5. Re-run parity tests and update metrics

