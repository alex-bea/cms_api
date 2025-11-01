# Check Normalize Stage Logs for GPCI

We need to check if GPCI is being parsed in the normalize stage. Look for:

## Key log patterns to search for:

```bash
python scripts/load_rvu_to_production.py 2>&1 | grep -E "invoking_parser|dataset_added|gpci|GPCI|normalize" -i -A 3 -B 1 | head -200
```

## What to look for:

1. **GPCI file discovery:**
   - `"invoking_parser"` with `dataset=gpci`
   - `"parser_result"` with `dataset=gpci`
   - `"dataframe_added"` with `dataset=gpci`

2. **Missing GPCI:**
   - Look for `"Skipping unclassified file"` 
   - Look for `"invoking_parser"` ONLY for other datasets (pprrvu, oppscap, etc.)

## Expected output if GPCI is being parsed:

```
invoking_parser dataset=gpci filename=GPCI2025.txt
parser_result dataset=gpci rows_parsed=109
dataframe_added dataset=gpci rows=109 columns=[...]
```

## If no GPCI logs appear:

- GPCI files aren't being discovered
- OR GPCI files are being skipped
- OR GPCI files aren't in the ZIP archives

Check if GPCI files exist in the downloaded ZIPs.

