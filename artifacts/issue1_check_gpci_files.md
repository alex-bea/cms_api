# Check if GPCI files exist in downloaded ZIPs

No GPCI parsing logs found - need to verify if files exist.

## Step 1: Check what files are in the downloaded ZIPs

Run these commands in Render shell:

```bash
# Find downloaded ZIP files
find data -name "*.zip" -type f | head -5

# List contents of one ZIP to see what's inside
cd data
unzip -l rvu25d-*.zip 2>/dev/null | grep -i gpci || echo "No GPCI files in ZIP"

# Or check all ZIPs at once
for zip in $(find . -name "rvu*.zip" -type f | head -4); do 
  echo "=== $zip ===" 
  unzip -l "$zip" 2>/dev/null | grep -i -E "(gpci|GPCI)" || echo "No GPCI files"
done
```

## Step 2: Check land stage output

See what files were discovered:

```bash
python scripts/load_rvu_to_production.py 2>&1 | grep -E "filename=|file_type|source_files" -i | head -50
```

## Expected results:

If GPCI exists, you should see:
- `GPCI2025.txt` or similar
- `gpci` in filename patterns

If no GPCI files:
- The ZIPs don't contain GPCI files
- OR the files are named differently than expected
- Need to check actual CMS ZIP contents

