# Check Downloaded ZIP Contents

Run this in Render to see what's actually in the downloaded ZIPs:

```bash
# Find downloaded ZIPs
find data/ingestion -name "*.zip" -type f

# List contents of each ZIP to see if GPCI files are there
for zip in $(find data/ingestion -name "*.zip" -type f 2>/dev/null | head -4); do 
  echo "=== $zip ===" 
  unzip -l "$zip" 2>/dev/null | head -20
  echo ""
done
```

Or check the land stage logs to see what files were extracted:

```bash
# Search for file extraction logs
python scripts/load_rvu_to_production.py 2>&1 | grep -E "zip_members_classified|recognized_members|inner_name" | head -50
```

This will show us:
- What files are actually in the ZIPs
- Whether GPCI files were extracted
- If they were classified correctly

