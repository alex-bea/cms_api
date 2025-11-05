# Quick Render Data Check

Paste this into your Render shell:

```python
python3 << 'EOF'
from cms_pricing.database import SessionLocal
from sqlalchemy import text

db = SessionLocal()
try:
    print("📊 Data Check:")
    print("-" * 50)
    
    rvu = db.execute(text("SELECT COUNT(*) FROM rvu_items")).fetchone()[0]
    gpci = db.execute(text("SELECT COUNT(*) FROM gpci_indices")).fetchone()[0]
    opps = db.execute(text("SELECT COUNT(*) FROM opps_caps")).fetchone()[0]
    releases = db.execute(text("SELECT COUNT(*) FROM releases")).fetchone()[0]
    
    print(f"RVU Items:      {rvu:>12,}")
    print(f"GPCI Indices:   {gpci:>12,}")
    print(f"OPPS Caps:      {opps:>12,}")
    print(f"Releases:       {releases:>12,}")
    
    print("-" * 50)
    if rvu > 0:
        print("✅ Data is present!")
    else:
        print("❌ No data found")
finally:
    db.close()
EOF
```

## Even Shorter One-Liner

```python
python3 -c "from cms_pricing.database import SessionLocal; from sqlalchemy import text; db = SessionLocal(); print('RVU:', db.execute(text('SELECT COUNT(*) FROM rvu_items')).fetchone()[0], '| GPCI:', db.execute(text('SELECT COUNT(*) FROM gpci_indices')).fetchone()[0], '| OPPS:', db.execute(text('SELECT COUNT(*) FROM opps_caps')).fetchone()[0]); db.close()"
```

