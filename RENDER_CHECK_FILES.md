# Check Why Files Aren't Being Downloaded

Run this to see what's happening:

```python
python3 << 'EOF'
from pathlib import Path
import json

data_dir = Path("./data/rvu_scraper")
print("🔍 Checking File Status")
print("=" * 60)

# Check raw directory
raw_dirs = list(data_dir.glob("**/raw"))
print(f"Raw directories found: {len(raw_dirs)}")

for raw_dir in raw_dirs[:3]:
    print(f"\n📁 {raw_dir}")
    if raw_dir.exists():
        files = [f for f in raw_dir.glob("*") if f.is_file()]
        print(f"   Files: {len(files)}")
        for f in files[:5]:
            print(f"      {f.name} ({f.stat().st_size:,} bytes)")

# Check curated directory structure
curated_dir = data_dir / "curated" / "cms_rvu"
if curated_dir.exists():
    print(f"\n📁 Curated directory: {curated_dir}")
    subdirs = [d for d in curated_dir.iterdir() if d.is_dir()]
    print(f"   Subdirectories: {len(subdirs)}")
    for subdir in sorted(subdirs, key=lambda x: x.stat().st_mtime, reverse=True)[:3]:
        print(f"\n   {subdir.name}")
        manifest = subdir / "manifest.json"
        if manifest.exists():
            with open(manifest) as f:
                m = json.load(f)
                print(f"      Files in manifest: {len(m.get('files', []))}")
                print(f"      Release ID: {m.get('release_id', 'N/A')}")
                print(f"      Batch ID: {m.get('batch_id', 'N/A')}")
        
        # Check for data files
        data_files = list(subdir.glob("**/*.parquet"))
        print(f"      Parquet files: {len(data_files)}")
        
        # Check raw subdirectory
        raw_subdir = subdir / "raw"
        if raw_subdir.exists():
            raw_files = [f for f in raw_subdir.glob("*") if f.is_file()]
            print(f"      Raw files: {len(raw_files)}")
            for rf in raw_files[:3]:
                print(f"         {rf.name} ({rf.stat().st_size:,} bytes)")

# Check if files match what was discovered
print("\n" + "=" * 60)
print("Expected files:")
expected = [
    "rvu25a-20250110.zip",
    "rvu25b-20250605.zip", 
    "rvu25c-20250605.zip",
    "rvu25d-20250911.zip"
]

for exp in expected:
    found = False
    for raw_dir in raw_dirs:
        if (raw_dir / exp).exists():
            print(f"✅ {exp} exists in {raw_dir}")
            found = True
            break
    if not found:
        print(f"❌ {exp} not found locally")
EOF
```

This will show:
1. Whether files exist in raw directories
2. What's in the latest manifest
3. Whether discovered files match what's on disk

The issue is likely that the ingestion is detecting files already exist and skipping the download, but they're not being processed. This could be a caching/deduplication issue.

