"""Sample RVU data for testing - using real data from rvu25d_0 folder"""

import os
from pathlib import Path

# Path to the real RVU data files
RVU_DATA_DIR = Path(__file__).parent.parent.parent.parent / "sample_data" / "rvu25d_0"

# Real data file paths
PPRRVU_TXT_FILE = RVU_DATA_DIR / "PPRRVU2025_Oct.txt"
PPRRVU_CSV_FILE = RVU_DATA_DIR / "PPRRVU2025_Oct.csv"
GPCI_TXT_FILE = RVU_DATA_DIR / "GPCI2025.txt"
GPCI_CSV_FILE = RVU_DATA_DIR / "GPCI2025.csv"
OPPSCAP_TXT_FILE = RVU_DATA_DIR / "OPPSCAP_Oct.txt"
OPPSCAP_CSV_FILE = RVU_DATA_DIR / "OPPSCAP_Oct.csv"
ANES_TXT_FILE = RVU_DATA_DIR / "ANES2025.txt"
ANES_CSV_FILE = RVU_DATA_DIR / "ANES2025.csv"
LOCCO_TXT_FILE = RVU_DATA_DIR / "25LOCCO.txt"
LOCCO_CSV_FILE = RVU_DATA_DIR / "25LOCCO.csv"
RVU25D_PDF_FILE = RVU_DATA_DIR / "RVU25D.pdf"

# Sample records extracted from real data for testing
SAMPLE_PPRRVU_TXT_RECORDS = [
    "00100  Anesth salivary gland                             J   0.00   0.00     0.00    0.00   0.00  0.009XXX.00.00.0099999             32.3465 090 99      0.00   0.00   0.00",
    "00102  Anesth repair of cleft lip                        J   0.00   0.00     0.00    0.00   0.00  0.009XXX.00.00.0099999             32.3465 090 99      0.00   0.00   0.00",
    "00103  Anesth blepharoplasty                             J   0.00   0.00     0.00    0.00   0.00  0.009XXX.00.00.0099999             32.3465 090 99      0.00   0.00   0.00",
    "00104  Anesth electroshock                               J   0.00   0.00     0.00    0.00   0.00  0.009XXX.00.00.0099999             32.3465 090 99      0.00   0.00   0.00",
    "00120  Anesth ear surgery                                J   0.00   0.00     0.00    0.00   0.00  0.009XXX.00.00.0099999             32.3465 090 99      0.00   0.00   0.00",
]

SAMPLE_GPCI_TXT_RECORDS = [
    "10112           AL      00   ALABAMA                                                                                     1.000       0.869       0.575",
    "02102           AK      01   ALASKA*                                                                                     1.500       1.081       0.592",
    "03102           AZ      00   ARIZONA                                                                                     1.000       0.975       0.854",
    "07102           AR      13   ARKANSAS                                                                                    1.000       0.860       0.518",
    "01112           CA      54   BAKERSFIELD                                                                                 1.017       1.093       0.662",
]

SAMPLE_OPPSCAP_TXT_RECORDS = [
    "0633TTC C 01112  05   150.69    150.69",
    "0633TTC C 01112  09   152.38    152.38",
    "0633TTC C 01112  17   125.72    125.72",
    "0633TTC C 01112  18   126.90    126.90",
    "0633TTC C 01112  51   139.15    139.15",
]

SAMPLE_ANES_TXT_RECORDS = [
    "10112       00    ALABAMA                                                1931",
    "02102       01    ALASKA                                                 2786",
    "03102       00    ARIZONA                                                2004",
    "07102       13    ARKANSAS                                               1921",
    "01112       54    BAKERSFIELD                                            2045",
]

SAMPLE_LOCCO_TXT_RECORDS = [
    "    10112       00    ALABAMA                STATEWIDE                                                                                         ALL COUNTIES",
    "    02102       01    ALASKA                 STATEWIDE                                                                                         ALL COUNTIES",
    "    03102       00    ARIZONA                STATEWIDE                                                                                         ALL COUNTIES",
    "    07102       13    ARKANSAS               STATEWIDE                                                                                         ALL COUNTIES",
    "    01112       54    BAKERSFIELD            CALIFORNIA                                                                                        KERN",
]

# CSV sample records (proper CSV format with headers)
SAMPLE_PPRRVU_CSV_RECORDS = """HCPCS,Modifier,Description,Status,Work RVU,PE RVU Non-Facility,PE RVU Facility,MP RVU,NA Indicator,Global Days,Bilateral,Multiple Proc,Assistant Surg,Co Surg,Team Surg,Endoscopic Base,Conversion Factor,Physician Supervision,Diag Imaging Family,Total Non-Facility,Total Facility
0001F,,Heart failure composite,I,,0.00,0.00,,0.00,,0.00,0.00,0.00,9,XXX,0.00,0.00,0.00,9,9,9,9,9,,32.3465,09,0,99,0.00,0.00,0.00
0005F,,Osteoarthritis composite,I,,0.00,0.00,,0.00,,0.00,0.00,0.00,9,XXX,0.00,0.00,0.00,9,9,9,9,9,,32.3465,09,0,99,0.00,0.00,0.00
00100,,Anesth salivary gland,J,,0.00,0.00,,0.00,,0.00,0.00,0.00,9,XXX,0.00,0.00,0.00,9,9,9,9,9,,32.3465,09,0,99,0.00,0.00,0.00
00102,,Anesth repair of cleft lip,J,,0.00,0.00,,0.00,,0.00,0.00,0.00,9,XXX,0.00,0.00,0.00,9,9,9,9,9,,32.3465,09,0,99,0.00,0.00,0.00
00103,,Anesth blepharoplasty,J,,0.00,0.00,,0.00,,0.00,0.00,0.00,9,XXX,0.00,0.00,0.00,9,9,9,9,9,,32.3465,09,0,99,0.00,0.00,0.00"""

SAMPLE_GPCI_CSV_RECORDS = """Medicare Administrative Contractor (MAC),State,Locality Number,Locality Name,2025 PW GPCI (with 1.0 Floor),2025 PE GPCI,2025 MP GPCI
10112,AL,00,ALABAMA,1,0.869,0.575
02102,AK,01,ALASKA*,1.5,1.081,0.592
03102,AZ,00,ARIZONA,1,0.975,0.854
07102,AR,13,ARKANSAS,1,0.860,0.518
01112,CA,54,BAKERSFIELD,1.017,1.093,0.662"""

SAMPLE_OPPSCAP_CSV_RECORDS = """HCPCS,MOD,PROCSTAT,CARRIER,LOCALITY,FACILITY PRICE,NON-FACILTY PRICE
0633T,TC,C,01112,05,150.69,150.69
0633T,TC,C,01112,09,152.38,152.38
0633T,TC,C,01112,17,125.72,125.72
0633T,TC,C,01112,18,126.90,126.90
0633T,TC,C,01112,51,139.15,139.15"""

SAMPLE_ANES_CSV_RECORDS = """Contractor,Locality,Locality Name,National Anes CF of 20.3178
10112,00,ALABAMA,19.31
02102,01,ALASKA*,27.86
03102,00,ARIZONA,20.04
07102,13,ARKANSAS,19.21
01112,54,BAKERSFIELD,20.45"""

SAMPLE_LOCCO_CSV_RECORDS = """Medicare Adminstrative Contractor,Locality Number,State,Fee Schedule Area,Counties
10112,0,ALABAMA,STATEWIDE,ALL COUNTIES
02102,1,ALASKA,STATEWIDE,ALL COUNTIES
03102,0,ARIZONA,STATEWIDE,ALL COUNTIES
07102,13,ARKANSAS,STATEWIDE,ALL COUNTIES
01182,18,CALIFORNIA,LOS ANGELES-LONG BEACH-ANAHEIM (LOS ANGELES CNTY/ORANG CNTY),LOS ANGELES/ORANGE"""

# For backward compatibility with existing test structure
SAMPLE_PPRRVU_TXT_RECORDS = SAMPLE_PPRRVU_TXT_RECORDS
SAMPLE_PPRRVU_CSV_RECORDS = SAMPLE_PPRRVU_CSV_RECORDS
SAMPLE_GPCI_RECORDS = SAMPLE_GPCI_TXT_RECORDS
SAMPLE_OPPSCAP_RECORDS = SAMPLE_OPPSCAP_TXT_RECORDS
SAMPLE_ANES_RECORDS = SAMPLE_ANES_TXT_RECORDS
SAMPLE_LOCCO_RECORDS = SAMPLE_LOCCO_TXT_RECORDS