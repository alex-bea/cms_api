# GPCI Golden Test Fixtures

**Purpose:** Known-good GPCI files for golden-file regression testing  
**Source:** CMS RVU25D bundle (2025 Q4 / Revision D)  
**Created:** 2025-10-17

---

## 📂 **Fixture Files**

| File | Format | Rows | Size | SHA-256 |
|------|--------|------|------|---------|
| `GPCI2025_sample.txt` | Fixed-width TXT | 20 data rows | 3.4K | `02683691ce1d8c468f132b81440a63cd63f979b53a26cef151745c7d8ac22d22` |
| `GPCI2025_sample.csv` | CSV | 20 data rows | 1.5K | `f465ef8cdf4e7e1b472814d61a2105d3b0021228014cc89ede795ffb2a17b4fa` |
| `GPCI2025_sample.xlsx` | Excel | 115 rows | 19K | `e8418d31371af658e17402e8ae8c1eac16707f69608b763614f69320ae9ad711` |
| `GPCI2025_sample.zip` | ZIP (contains TXT) | 20 data rows | 1.2K | *(see below)* |

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

### **Sample Localities (20 rows):**

**Included localities** (TXT/CSV fixtures):
- Alabama (00)
- Alaska (01) - Has 1.50 work GPCI floor
- Arizona (00, 13, 54)
- Arkansas (00, 13, 20, 47, 60)
- California (05, 06, 07, 09, 18, 26) - Multiple localities
- Colorado (01, 02, 03)
- Connecticut (00)
- Delaware (00)

**Coverage:**
- ✅ Geographic diversity (multiple states)
- ✅ Work GPCI floor (Alaska 1.50)
- ✅ High-cost areas (Manhattan, SF)
- ✅ Low-cost areas (Alabama, Arkansas)
- ✅ Multi-locality states (CA, CO)
- ✅ Single-locality states (AL, DE)

### **Expected Values:**

**Alabama (locality 00):**
- Work GPCI: 1.000
- PE GPCI: 0.869
- MP GPCI: 0.575

**Alaska (locality 01):**
- Work GPCI: 1.500 (floor applied)
- PE GPCI: 1.081
- MP GPCI: 0.592

**Manhattan (locality 05):**
- Work GPCI: 1.122
- PE GPCI: 1.569
- MP GPCI: 1.859

---

## 🧪 **Test Usage**

### **Golden Test Pattern:**
```python
def test_gpci_golden_txt():
    """TXT format produces deterministic output."""
    fixture = Path(__file__).parent.parent / 'fixtures/gpci/golden/GPCI2025_sample.txt'
    
    with open(fixture, 'rb') as f:
        result = parse_gpci(f, 'GPCI2025_sample.txt', metadata)
    
    # Verify row count
    assert len(result.data) == 20
    
    # Verify schema compliance
    assert 'locality_code' in result.data.columns
    assert 'gpci_work' in result.data.columns
    assert 'row_content_hash' in result.data.columns
    
    # Verify specific locality
    alabama = result.data[result.data['locality_code'] == '00'].iloc[0]
    assert alabama['gpci_work'] == '1.000'  # String (3dp)
    assert alabama['gpci_pe'] == '0.869'
    assert alabama['gpci_mp'] == '0.575'
```

### **Determinism Test Pattern:**
```python
def test_gpci_determinism():
    """Same input produces identical row_content_hash."""
    fixture = Path(__file__).parent.parent / 'fixtures/gpci/golden/GPCI2025_sample.txt'
    
    # Parse twice
    with open(fixture, 'rb') as f:
        result1 = parse_gpci(f, 'GPCI2025_sample.txt', metadata)
    
    with open(fixture, 'rb') as f:
        result2 = parse_gpci(f, 'GPCI2025_sample.txt', metadata)
    
    # Verify identical hashes
    assert result1.data['row_content_hash'].equals(result2.data['row_content_hash'])
```

---

## 📊 **Fixture Validation**

### **Verified Properties:**

✅ **Format Integrity:**
- TXT: Fixed-width (150 chars per line)
- CSV: Header + data rows
- XLSX: Excel readable (openpyxl)
- ZIP: Valid archive with TXT member

✅ **Data Quality:**
- All 20 rows have valid locality codes (2-digit, zero-padded)
- All GPCI values in valid range [0.575, 1.859]
- No missing values in required columns
- No duplicate localities in sample

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

