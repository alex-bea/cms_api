#!/usr/bin/env python
"""
Simple Database Initialization for First Deployment

This script creates all tables and loads GPCI data without using Alembic.
Perfect for first-time deployment or testing.

Usage:
    python simple_init_db.py
    
Requirements:
    - DATABASE_URL environment variable set
    - PostgreSQL running (Docker or local)
    - Source file: sample_data/rvu25d_0/GPCI2025.txt
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from cms_pricing.database import Base, engine
from cms_pricing.models import *  # Import all models to register them
from cms_pricing.ingestion.parsers.gpci_parser import parse_gpci
from sqlalchemy import text

def main():
    print("╔══════════════════════════════════════════════════════════════════════╗")
    print("║         Simple Database Initialization                              ║")
    print("╚══════════════════════════════════════════════════════════════════════╝")
    print("")
    
    # Step 1: Drop and recreate all tables (fresh start)
    print("Step 1: Creating fresh tables...")
    print("   Dropping existing tables...")
    Base.metadata.drop_all(bind=engine)
    
    print("   Creating all tables from models...")
    Base.metadata.create_all(bind=engine)
    print("✅ Tables created")
    
    # Step 2: List created tables
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;"
        ))
        tables = [row[0] for row in result]
        print(f"✅ Created {len(tables)} tables")
    
    # Step 3: Verify GPCI table and v1.3 index
    print("")
    print("Step 2: Verifying GPCI v1.3 setup...")
    
    with engine.connect() as conn:
        # Check gpci_indices table
        result = conn.execute(text(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'gpci_indices' ORDER BY ordinal_position;"
        ))
        columns = [row[0] for row in result]
        print(f"✅ gpci_indices table has {len(columns)} columns")
        
        # Check for v1.3 unique index
        result = conn.execute(text(
            "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'gpci_indices' AND indexname = 'uq_gpci_mac_locality_effective';"
        ))
        index = result.fetchone()
        
        if index:
            print(f"✅ GPCI v1.3 unique index present:")
            print(f"   {index[0]}")
        else:
            print("⚠️  GPCI v1.3 unique index not found in model")
            print("   Check: cms_pricing/models/rvu.py __table_args__")
    
    # Step 3: Parse and load GPCI data
    print("")
    print("Step 3: Loading GPCI data...")
    
    source_file = Path("sample_data/rvu25d_0/GPCI2025.txt")
    if not source_file.exists():
        print(f"⚠️  Source file not found: {source_file}")
        print("   Skipping data load (run backfill script later)")
        return
    
    metadata = {
        'release_id': 'RVU25D',
        'schema_id': 'cms_gpci_v1.3',
        'product_year': '2025',
        'quarter_vintage': 'D',
        'vintage_date': datetime(2025, 10, 1),
        'file_sha256': 'simple_init_sha256',
        'source_uri': str(source_file),
        'source_release': 'RVU25D',
    }
    
    with open(source_file, 'rb') as f:
        result = parse_gpci(f, source_file.name, metadata)
    
    print(f"✅ Parsed {len(result.data)} GPCI rows")
    print(f"   Rejects: {len(result.rejects)}")
    
    # Check for duplicates
    nk_cols = ['mac', 'locality_code', 'effective_from']
    dupes = result.data.duplicated(subset=nk_cols, keep=False)
    if dupes.any():
        print(f"❌ ERROR: {dupes.sum()} duplicates found on v1.3 NK!")
        return
    
    print(f"✅ Verified: 0 duplicates on {nk_cols}")
    
    # Load into database
    print("")
    print("Step 4: Loading data into database...")
    
    # Map schema columns to database columns
    df_db = result.data.copy()
    df_db.rename(columns={
        'locality_code': 'locality_id',
        'effective_from': 'effective_start',
        'effective_to': 'effective_end'
    }, inplace=True)
    
    # Add required columns (simplified - you'd normally get these from releases table)
    import uuid
    df_db['id'] = [uuid.uuid4() for _ in range(len(df_db))]
    df_db['release_id'] = uuid.uuid4()  # Dummy release ID
    
    # Load
    from sqlalchemy.dialects.postgresql import UUID
    df_db.to_sql(
        'gpci_indices',
        engine,
        if_exists='append',
        index=False,
        chunksize=100,
        dtype={'id': UUID(as_uuid=True), 'release_id': UUID(as_uuid=True)}
    )
    
    print(f"✅ Loaded {len(df_db)} rows into gpci_indices")
    
    # Step 5: Verify
    print("")
    print("Step 5: Verification...")
    
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM gpci_indices;")).scalar()
        print(f"✅ Row count: {count}")
        
        # Check for duplicates in DB
        dupes_db = conn.execute(text("""
            SELECT mac, locality_id, effective_start, COUNT(*) 
            FROM gpci_indices 
            GROUP BY mac, locality_id, effective_start 
            HAVING COUNT(*) > 1;
        """)).fetchall()
        
        if dupes_db:
            print(f"❌ {len(dupes_db)} duplicates in database!")
        else:
            print("✅ No duplicates in database")
        
        # Sample data
        sample = conn.execute(text("SELECT mac, locality_id, locality_name, work_gpci FROM gpci_indices LIMIT 5;")).fetchall()
        print("")
        print("Sample data:")
        for row in sample:
            print(f"   {row}")
    
    print("")
    print("╔══════════════════════════════════════════════════════════════════════╗")
    print("║                     INITIALIZATION COMPLETE                          ║")
    print("╚══════════════════════════════════════════════════════════════════════╝")
    print("")
    print("✅ Database initialized with GPCI v1.3 schema")
    print(f"✅ Loaded {count} GPCI localities")
    print("✅ Ready for testing!")
    print("")
    print("Next steps:")
    print("  - Run parser tests: pytest tests/ingestion/test_gpci_parser_golden.py -v")
    print("  - Query data: psql $DATABASE_URL")
    print("  - For production: Migrate to Render/Railway with proper Alembic")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

