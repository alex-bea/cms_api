#!/usr/bin/env python3
"""
Create all database tables from SQLAlchemy models.

This script creates tables for database-only deployments where the
FastAPI app isn't running. It's equivalent to what main.py does on
startup with Base.metadata.create_all().

Usage:
    export DATABASE_URL="postgresql://..."
    python scripts/create_tables.py
"""
import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from cms_pricing.database import Base, engine

# Import all models to register them with Base
from cms_pricing.models import (
    Geography, ZipGeometry, GeographyResolutionTrace,
    Code, CodeStatus,
    FeeMPFS, FeeOPPS, FeeASC, FeeIPPS, FeeCLFS, FeeDMEPOS,
    GPCI, ConversionFactor, WageIndex, IPPSBaseRate,
    DrugASP, DrugNADAC, NDCHCPCSXwalk,
    Plan, PlanComponent,
    BenefitParams,
    Snapshot,
    Run, RunInput, RunOutput, RunTrace,
    HospitalMRFRate,
    Release, RVUItem, GPCIIndex, OPPSCap, AnesCF, LocalityCounty,
    ZCTACoords, ZipToZCTA, CMSZipLocality, ZIP9Overrides,
    ZCTADistances, NBERCentroids, ZipMetadata, IngestRun, NearestZipTrace,
)


def main():
    """Create all tables"""
    # Check DATABASE_URL is set
    if not os.getenv("DATABASE_URL"):
        print("❌ ERROR: DATABASE_URL environment variable not set")
        print("\nSet it with:")
        print("  export DATABASE_URL=$(cat .env | grep DATABASE_URL | cut -d'=' -f2-)")
        sys.exit(1)
    
    print("╔══════════════════════════════════════════════════════════════════════╗")
    print("║         Creating Database Tables from SQLAlchemy Models             ║")
    print("╚══════════════════════════════════════════════════════════════════════╝")
    print()
    print(f"Database: {os.getenv('DATABASE_URL', '')[:50]}...")
    print()
    
    # First, drop all existing tables if any (clean slate)
    print("Checking for existing objects...")
    from sqlalchemy import inspect
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    
    if existing_tables:
        print(f"⚠️  Found {len(existing_tables)} existing tables - dropping for clean slate...")
        Base.metadata.drop_all(bind=engine)
        print("✅ Cleaned up existing tables")
        print()
    
    try:
        print("Creating all tables...")
        Base.metadata.create_all(bind=engine, checkfirst=True)
        print("✅ Tables created successfully!")
        print()
        
        # List created tables
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        print(f"Total tables created: {len(tables)}")
        print("\nTables:")
        for table in sorted(tables):
            print(f"  ✅ {table}")
        
        print()
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print("✅ SUCCESS - All tables created!")
        print("\nNext steps:")
        print("  1. Run: alembic stamp head")
        print("  2. Continue with Part 4: Load GPCI data")
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        
    except Exception as e:
        print(f"❌ ERROR creating tables: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

