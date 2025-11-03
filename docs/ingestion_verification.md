# RVU Ingestion Verification Cheatsheet

Use these commands after a Render ingestion to confirm that each dataset landed correctly.  
They assume you are in the `/app` directory inside the Render shell.

---

## 1. Inspect Staged Parquet Files

```bash
python - <<'PY'
import pandas as pd
from pathlib import Path

datasets = ("gpci", "oppscap", "pprrvu", "localitycounty", "anescf")
base = Path("data/ingestion/production/stage/cms_rvu")
for release_dir in base.iterdir():
    for name in datasets:
        path = release_dir / f"{name}.parquet"
        if path.exists():
            df = pd.read_parquet(path)
            print(f"{name} columns:", df.columns.tolist()[:10])
            print(f"{name} sample rows:", len(df))
    break  # only inspect latest release
PY
```

Key things to check:
- Column names match expectations (`gpci_work`, `facility_price`, `state_name`, etc.).
- Row counts look sane (e.g., ~55 rows for GPCI, ~110 for OPPSCap, ~19k for PPRRVU).

---

## 2. Validate Database Tables

```bash
python - <<'PY'
from sqlalchemy import text
from cms_pricing.database import SessionLocal

db = SessionLocal()
def show(label, query):
    print(f"{label}: {db.execute(text(query)).scalar()}")

show("gpci_indices rows", "select count(*) from gpci_indices")
show("gpci_indices NULL rows", """
  select count(*) from gpci_indices
  where work_gpci is null or pe_gpci is null or mp_gpci is null
""")

show("opps_caps rows", "select count(*) from opps_caps")
show("opps_caps hcpcs_code NULL rows", "select count(*) from opps_caps where hcpcs_code is null")

show("locality_counties rows", "select count(*) from locality_counties")

show("rvu_items rows", "select count(*) from rvu_items")
show("rvu_items NULL modifiers", "select count(*) from rvu_items where modifier_key = ''")

db.close()
PY
```

Expected outcomes:
- `gpci_indices NULL rows` = 0.
- `opps_caps hcpcs_code NULL rows` = 0.
- `locality_counties rows` matches the staged parquet count (after alias fixes).
- `rvu_items` shows ~19k rows with no empty modifier keys.

---

## 3. Optional: Promote GPCI Index Values

Once `gpci_indices` looks correct, populate the denormalised `gpci` table:

```bash
python scripts/load_gpci_from_indices.py
```

Verify:

```bash
python - <<'PY'
from sqlalchemy import text
from cms_pricing.database import SessionLocal

db = SessionLocal()
count = db.execute(text("select count(*) from gpci")).scalar()
nulls = db.execute(text("select count(*) from gpci where gpci_work is null or gpci_pe is null or gpci_mp is null")).scalar()
print(f"gpci rows: {count}, rows with NULL: {nulls}")
db.close()
PY
```

---

## 4. Release Reset Snippet

If ingestion was short-circuited (release already present), clear the release before rerunning:

```bash
python - <<'PY'
from sqlalchemy import text
from cms_pricing.database import SessionLocal

db = SessionLocal()
release_ids = [
    row[0]
    for row in db.execute(
        text("select id from releases where type='RVU_FULL' and source_version='rvu_2025_p'")
    )
]
for rid in release_ids:
    for table in ("gpci_indices", "rvu_items", "opps_caps", "anes_cfs", "locality_counties"):
        db.execute(text(f"delete from {table} where release_id = :rid"), {"rid": rid})
    db.execute(text("delete from releases where id = :rid"), {"rid": rid})
db.commit()
db.close()
PY
```

---

Keep this cheatsheet alongside the ingestion PRDs so new contributors can validate runs quickly and consistently.
