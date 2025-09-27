#!/usr/bin/env python3
"""Check and fix database schema"""

from cms_pricing.database import engine
from sqlalchemy import text

with engine.connect() as conn:
    # Check current schema
    result = conn.execute(text("""
        SELECT column_name, is_nullable 
        FROM information_schema.columns 
        WHERE table_name = 'fee_mpfs' 
        ORDER BY ordinal_position
    """))
    
    print("Current fee_mpfs schema:")
    for row in result:
        print(f"{row[0]}: {row[1]}")
    
    # Drop and recreate table
    print("\nDropping and recreating fee_mpfs table...")
    conn.execute(text("DROP TABLE IF EXISTS fee_mpfs CASCADE"))
    conn.commit()
    
    # Import and create the new table
    from cms_pricing.models.fee_schedules import FeeMPFS
    FeeMPFS.__table__.create(engine)
    
    print("Table recreated successfully!")
