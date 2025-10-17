# GPCI Line Length Analysis (2025-10-16)

**Sample file:** `sample_data/rvu25d_0/GPCI2025.txt`  
**Sample file SHA-256:** `6c267d2a8fe83bf06a698b18474c3cdff8505fd626e2b2f9e022438899c4ee0d`

---

## 📊 **Measurements**

### Line Lengths
- **Window (head-40) all lines:** 151 characters
- **Full-file data lines MIN:** 151 characters  
- **Full-file data lines MAX:** 151 characters
- **Actual data line (stripped):** 150 characters (151 with \n)

**Conclusion:** All data lines are uniform at **150 characters** (excluding newline).

**Recommended `min_line_length`:** **100** (conservative with 50-char margin)

---

## 📍 **Column Positions (Verified)**

| Column | Start | End | Width | Sample Value | Notes |
|--------|-------|-----|-------|--------------|-------|
| `mac` | 0 | 5 | 5 | `'10112'` | 5-digit MAC code |
| `state` | 16 | 18 | 2 | `'AL'` | 2-letter state (pos 15 is space) |
| `locality_code` | 24 | 26 | 2 | `'00'` | 2-digit locality |
| `locality_name` | 28 | 78 | 50 | `'ALABAMA   ...'` | Left-padded with spaces |
| `gpci_work` | 121 | 126 | 5 | `'1.000'` | Right-aligned decimal |
| `gpci_pe` | 133 | 138 | 5 | `'0.869'` | Right-aligned decimal |
| `gpci_mp` | 145 | 150 | 5 | `'0.575'` | Right-aligned decimal |

**CRITICAL CORRECTIONS from original layout:**
- ✅ State: 16:18 (NOT 15:17)
- ✅ GPCI Work: 121:126 (NOT 120:125)
- ✅ GPCI PE: 133:138 (matches original)
- ✅ GPCI MP: 145:150 (NOT 140:145)

---

## 🔍 **Data Start Pattern**

**Header lines:** 2 lines before data
- Line 0: "ADDENDUM E. FINAL CY 2025..."
- Line 1: (blank)
- Line 2: "Medicare Admi State  Locality..."
- Line 3+: Data starts (MAC code = 5 digits)

**Pattern:** Lines starting with 5 digits = data lines  
**Regex:** `^\d{5}` or `^[0-9]{5}`

**Data start detection:** Skip until line matches `^\d{5}`

---

## ✅ **Verification**

**Sample extraction:**
```
Line: '10112           AL      00   ALABAMA                                             1.000       0.869       0.575'
Positions verified:
  [0:5]     = '10112'  (MAC)
  [16:18]   = 'AL'     (State)
  [24:26]   = '00'     (Locality)
  [28:78]   = 'ALABAMA...' (Name, trimmed)
  [121:126] = '1.000'  (Work GPCI)
  [133:138] = '0.869'  (PE GPCI)
  [145:150] = '0.575'  (MP GPCI)
```

**All positions confirmed against multiple sample lines.**

---

## 🎯 **Layout Update Requirements**

**Change from original layout:**

| Column | Old Position | New Position | Reason |
|--------|--------------|--------------|--------|
| `state` | 15:17 | **16:18** | Pos 15 is space separator |
| `gpci_work` | 120:125 | **121:126** | Right-aligned with leading space |
| `gpci_mp` | 140:145 | **145:150** | Actual position in file |

**min_line_length:** Set to **100** (actual is 150, with 50-char margin for safety)

---

**Analysis Date:** 2025-10-16  
**Verified By:** Automated column position detection  
**Ready for:** Layout registry update

