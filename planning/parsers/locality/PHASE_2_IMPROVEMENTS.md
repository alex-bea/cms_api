# Phase 2 Plan Improvements (User Feedback)

**Date:** 2025-10-17  
**Applied:** 12 enhancements from user feedback

## Improvements Applied

1. ✅ **Dynamic Header Detection** - Don't hardcode skiprows
   - CSV: Scan first 15 rows for 'Locality' + 'Counties'
   - XLSX: Auto-select sheet with matching headers
   
2. ✅ **Robust Header Normalization** - Lowercase, condense spaces, strip
   - Handles typos ("Adminstrative")
   - Handles trailing spaces
   - Case-insensitive matching

3. ✅ **Key Harmonization** - Zero-pad locality_code to width 2
   - TXT: "00", CSV: "0" → both become "00"
   - Ensures format consistency

4. ✅ **Excel Dtype Control** - Prevent float coercion
   - Converters for Locality Number/Code
   - Strip '.0' from Excel integers

5. ✅ **Deterministic Column Order** - Enforced in all parsers
   - ['mac', 'locality_code', 'state_name', 'fee_area', 'county_names']

6. ✅ **Duplicate Policy** - Drop duplicates in canonicalization helper
   - Consistent dedup across formats

7. ✅ **Encoding/BOM Handling** - detect_encoding + BOM strip
   - CSV BOM edge case test added

8. ✅ **TXT Parity Guardrails** - Comparison after harmonization
   - Forward-fill in TXT, but comparison uses same normalization

9. ✅ **Robust Consistency Test** - Full DataFrame equality
   - _canonicalize_for_comparison() helper
   - pd.testing.assert_frame_equal() for deep comparison
   - Natural key sets also verified (catches different drift)

10. ✅ **Metrics & Logging** - Per-format details
    - header_row_detected, rows_read, rows_after_dedup
    - encoding, sheet_name (XLSX)

11. ✅ **Additional Edge Case Tests**
    - CSV with BOM
    - CSV with trailing spaces/typo
    - XLSX with multiple sheets
    - Locality codes: 0, 7, 18, 99 (zero-padding)
    - County names with slashes/commas

12. ✅ **Documentation Improvements**
    - fee_area included in all expected sets
    - Format normalization vs business transformations clarified
    - Normalized alias keys documented

## Impact

**Time Saved:** Prevents 1-2 hours of format-specific debugging
**Quality:** Bulletproof format consistency
**CI:** No flakiness from format variations

## References

- planning/parsers/locality/PHASE_2_CSV_XLSX_PLAN.md (updated plan)
- STD-parser-contracts §21.6 (Incremental Implementation)
- STD-qa-testing §5.1.2 (Multi-Format Fixture Parity)
