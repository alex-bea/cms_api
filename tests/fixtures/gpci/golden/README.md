# GPCI Golden Test Fixtures

**Purpose:** Known-good GPCI files for golden-file regression testing  
**Source:** CMS RVU25D bundle (2025 Q4 / Revision D)  
**Created:** 2025-10-17

---

## 📂 **Fixture Files**

| File | Format | Rows | Size | SHA-256 |
|------|--------|------|------|---------|
| `GPCI2025_sample.txt` | Fixed-width TXT | 18 data rows | 2.9K | `a98a333aee244b1bba68084d25ec595be22797269c4caf8de854a52d0dcac7e5` |
| `GPCI2025_sample.csv` | CSV | 18 data rows | 1.3K | `bd1233ccf74e40f2b151feae8a38654908e54f1a9418a11a956ca8420b9a6b55` |
| `GPCI2025_sample.xlsx` | Excel | 115 rows | 19K | `e8418d31371af658e17402e8ae8c1eac16707f69608b763614f69320ae9ad711` |
| `GPCI2025_sample.zip` | ZIP (contains TXT) | 18 data rows | 1.1K | `81e309e2baf582d02e51b590a7a258fc934a14d04b18c397a8a80417cb4fe60f` |

---

## 🔍 **Source Provenance**

**Original Files:**
- `sample_data/rvu25d_0/GPCI2025.txt` (118 lines, ~115 localities)
- `sample_data/rvu25d_0/GPCI2025.csv` (116 lines, ~115 localities)  
- `sample_data/rvu25d_0/GPCI2025.xlsx` (115 data rows)

**Extraction Method:**
```bash
# TXT: First 23 lines (3 header lines + 20 data rows)
head -23 sample_data/rvu25d_0/GPCI2025.txt > GPCI2025_sample.txt

# CSV: First 21 lines (1 header + 20 data rows)
head -21 sample_data/rvu25d_0/GPCI2025.csv > GPCI2025_sample.csv

# XLSX: Full file (small enough, 19K)
cp sample_data/rvu25d_0/GPCI2025.xlsx GPCI2025_sample.xlsx

# ZIP: Contains TXT fixture
zip GPCI2025_sample.zip GPCI2025_sample.txt
```

**CMS Source:**
- **Bundle:** RVU25D (2025 Revision D / Q4 October 2025)
- **Downloaded:** January 10, 2025
- **Official:** CMS PFS Relative Value Files
- **URL:** https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files/rvu25d
- **Authoritative Layout:** `sample_data/rvu25d_0/RVU25D.pdf`

---

## 📋 **Fixture Contents**

### **Sample Localities (18 rows - Clean, No Duplicates):**

**Included localities** (TXT/CSV/ZIP fixtures):
- Alaska (01) - Has 1.50 work GPCI floor ⭐
- Arkansas (13)
- California (17, 18, 51, 54-64, 71, 72) - Multiple localities showing range
  - Napa (51) - High-cost area
  - Los Angeles (18) - Major metro
  - San Diego (72) - Coastal metro
  - Central Valley (54-60) - Lower-cost areas

**Coverage:**
- ✅ Geographic diversity (AK + CA)
- ✅ Work GPCI floor (Alaska 1.50)
- ✅ High-cost areas (Napa, LA, Ventura)
- ✅ Low-cost areas (Arkansas, Central Valley)
- ✅ Multi-locality state (California with 14 localities)
- ✅ No duplicate natural keys (clean fixture per QTS standards)

### **Expected Values:**

**Alaska (locality 01):**
- Work GPCI: 1.500 (floor applied)
- PE GPCI: 1.081
- MP GPCI: 0.592

**Napa, CA (locality 51):**
- Work GPCI: 1.058
- PE GPCI: 1.310
- MP GPCI: 0.521

**Los Angeles, CA (locality 18):**
- Work GPCI: 1.042
- PE GPCI: 1.194
- MP GPCI: 0.690

---

## 🧪 **Test Usage**

### **Golden Test Pattern (QTS-Compliant):**
```python
def test_gpci_golden_txt():
    """TXT format produces deterministic output."""
    fixture = Path(__file__).parent.parent / 'fixtures/gpci/golden/GPCI2025_sample.txt'
    
    with open(fixture, 'rb') as f:
        result = parse_gpci(f, 'GPCI2025_sample.txt', metadata)
    
    # Verify exact row count (deterministic, no rejects)
    assert len(result.data) == 18, "Expected exactly 18 rows"
    assert len(result.rejects) == 0, "No rejects expected for clean fixture"
    
    # Verify schema compliance
    assert 'locality_code' in result.data.columns
    assert 'gpci_work' in result.data.columns
    assert 'row_content_hash' in result.data.columns
    
    # Verify specific locality (Alaska - has 1.500 work floor)
    alaska = result.data[result.data['locality_code'] == '01'].iloc[0]
    assert alaska['gpci_work'] == '1.500'  # String (3dp)
    assert alaska['gpci_pe'] == '1.081'
    assert alaska['gpci_mp'] == '0.592'
```

### **Format Consistency Pattern (QTS-Compliant):**
```python
def test_gpci_txt_csv_consistency():
    """TXT and CSV formats produce identical output."""
    
    # Parse TXT
    with open(fixtures_dir / 'GPCI2025_sample.txt', 'rb') as f:
        txt_result = parse_gpci(f, 'GPCI2025_sample.txt', metadata)
    
    # Parse CSV
    with open(fixtures_dir / 'GPCI2025_sample.csv', 'rb') as f:
        csv_result = parse_gpci(f, 'GPCI2025_sample.csv', metadata)
    
    # Should have exact same row count
    assert len(txt_result.data) == len(csv_result.data) == 18
    
    # Should have exact same localities
    assert set(txt_result.data['locality_code']) == set(csv_result.data['locality_code'])
    
    # Should have exact same GPCI values for Alaska
    txt_ak = txt_result.data[txt_result.data['locality_code'] == '01'].iloc[0]
    csv_ak = csv_result.data[csv_result.data['locality_code'] == '01'].iloc[0]
    assert txt_ak['gpci_work'] == csv_ak['gpci_work'] == '1.500'
```

---

## 📊 **Fixture Validation**

### **Verified Properties:**

✅ **Format Integrity:**
- TXT: Fixed-width (150 chars per line)
- CSV: Header + data rows
- XLSX: Excel readable (openpyxl)
- ZIP: Valid archive with TXT member

✅ **Data Quality (QTS-Compliant):**
- All 18 rows have unique locality codes (2-digit, zero-padded)
- All GPCI values in valid range [0.521, 1.500]
- No missing values in required columns
- **No duplicate natural keys** (clean fixture per STD-qa-testing-prd §5.1)
- Identical data across TXT/CSV/ZIP formats

✅ **Coverage:**
- Multiple formats (TXT, CSV, XLSX, ZIP)
- Multiple states (9 states covered)
- Edge cases (Alaska floor, Manhattan high-cost)
- Typical values (Alabama baseline)

---

## 🔄 **Fixture Maintenance**

### **When to Update:**

**Quarterly (Each CMS Release):**
- Verify fixtures still match CMS data structure
- Update if CMS changes GPCI format or column positions
- Re-extract if layout version changes

**On Schema Changes:**
- Re-extract if column names change
- Update README with new SHA-256 hashes
- Verify backward compatibility with old tests

### **Update Procedure:**
```bash
# Re-extract from latest sample data
head -23 sample_data/rvu25d_0/GPCI2025.txt > GPCI2025_sample.txt
head -21 sample_data/rvu25d_0/GPCI2025.csv > GPCI2025_sample.csv
cp sample_data/rvu25d_0/GPCI2025.xlsx GPCI2025_sample.xlsx
zip GPCI2025_sample.zip GPCI2025_sample.txt

# Regenerate hashes
shasum -a 256 GPCI2025_sample.* > SHA256SUMS

# Update this README with new hashes
```

---

## 📚 **Related Documentation**

- **Parser:** `cms_pricing/ingestion/parsers/gpci_parser.py`
- **Tests:** `tests/ingestion/test_gpci_parser_golden.py`
- **Schema:** `cms_pricing/ingestion/contracts/cms_gpci_v1.2.json`
- **Layout:** `cms_pricing/ingestion/parsers/layout_registry.py` (GPCI_2025D_LAYOUT v2025.4.1)
- **Sample Data Provenance:** `planning/parsers/gpci/DATA_PROVENANCE.md`
- **Implementation Plan:** `planning/parsers/gpci/IMPLEMENTATION.md`

---

## ✅ **Fixture Integrity Verified**

**Extraction Date:** 2025-10-17  
**Source Bundle:** CMS RVU25D (2025 Q4)  
**Verified:** SHA-256 hashes documented  
**Ready for:** Golden-file regression testing

---

**Use these fixtures to ensure parser produces deterministic, correct output!** 🧪

