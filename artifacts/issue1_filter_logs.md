# Filter GPCI Debug Logs

## One-liner command:

```bash
python scripts/load_rvu_to_production.py 2>&1 | grep -iE "(Loading GPCI|Sample row columns|gpci|df_columns|sample_columns_with_gpci|work_gpci|pe_gpci|mp_gpci)" | head -50
```

## Alternative with more context:

```bash
python scripts/load_rvu_to_production.py 2>&1 | grep -iE "(Loading GPCI|Sample row|gpci|GPCI)" -A 2 -B 2 | head -100
```

## Just show the key debug lines:

```bash
python scripts/load_rvu_to_production.py 2>&1 | grep -E "Loading GPCI data|Sample row columns" -A 10
```

This will show:
- The `"Loading GPCI data"` log with DataFrame columns
- The `"Sample row columns"` log with sample values
- 10 lines after each match for context

