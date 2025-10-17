# GPCI Edge Case Test Fixtures

**Purpose:** Real CMS data quirks and edge cases for validation testing  
**Source:** CMS RVU25D bundle (authentic data patterns)

---

## 📂 **Edge Case Fixtures**

### **1. `GPCI2025_duplicate_locality_00.txt`**

**Purpose:** Test duplicate natural key handling (real CMS behavior)

**Content:**
- Alabama (AL) locality 00
- Alaska (AK) locality 01  
- Arizona (AZ) locality 00 ← **DUPLICATE**

**CMS Reality:**
In real CMS GPCI files, both Alabama and Arizona use locality code 00. This is **authentic CMS data** but creates duplicate natural keys when parsed with `(locality_code, effective_from)`.

**Expected Behavior:**
- Input: 3 rows
- Valid: 1-2 rows (depending on duplicate handling strategy)
- Rejects: 1-2 rows (duplicate locality 00)
- Severity: WARN (duplicates quarantined, not failed)

**Test Pattern:**
```python
@pytest.mark.edge_case
@pytest.mark.gpci
def test_gpci_real_cms_duplicate_locality_00():
    """
    Real CMS quirk: AL and AZ both use locality 00.
    
    Tests that parser correctly handles this authentic CMS pattern.
    """
    fixture = 'tests/fixtures/gpci/edge_cases/GPCI2025_duplicate_locality_00.txt'
    
    with open(fixture, 'rb') as f:
        result = parse_gpci(f, 'GPCI2025_duplicate_locality_00.txt', metadata)
    
    # Should quarantine duplicate locality 00
    assert len(result.rejects) > 0, "Should detect duplicate locality 00"
    assert 'duplicate' in str(result.rejects.iloc[0]).lower()
    
    # Should keep one locality 00 (first occurrence)
    assert len(result.data[result.data['locality_code'] == '00']) <= 1
    
    # Should preserve Alaska (unique)
    assert len(result.data[result.data['locality_code'] == '01']) == 1
```

**Why Separate from Golden Tests:**
- Golden tests validate clean happy path (`rejects == 0`)
- Edge case tests validate real CMS quirks (`rejects > 0`)
- Both are critical, different purposes

**QTS Compliance:**
- ✅ Golden tests: Clean data per §5.1
- ✅ Edge tests: Real-world validation per §2.2 (negative testing)

---

## 🎯 **Purpose of Hybrid Approach**

### **Golden Tests (`tests/fixtures/gpci/golden/`)**
- **Data:** Clean, idealized (no duplicates)
- **Assertions:** `rejects == 0`, exact row counts
- **Purpose:** Validate happy path, deterministic output
- **Aligns with:** STD-qa-testing-prd §5.1 (clean golden datasets)

### **Edge Case Tests (`tests/fixtures/gpci/edge_cases/`)**
- **Data:** Real CMS quirks (duplicate locality 00)
- **Assertions:** `rejects > 0`, duplicate handling
- **Purpose:** Validate production error handling
- **Aligns with:** STD-qa-testing-prd §2.2 (negative testing)

---

**Result:** Best of both worlds - standards compliance + real-world validation! ✅

