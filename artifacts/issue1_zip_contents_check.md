# Check ALL Files in Downloaded ZIPs

The ZIPs don't contain GPCI files. Let's see what they DO contain:

```bash
# In Render shell:
for zip in $(find data/ingestion -name "*.zip" -type f 2>/dev/null | head -4); do 
  echo "=== $zip ===" 
  unzip -l "$zip" 2>/dev/null | head -30
  echo ""
done
```

This will show us what files ARE in the ZIPs and help us understand if:
- CMS changed file naming
- Files are in a different location
- GPCI needs to be downloaded separately

