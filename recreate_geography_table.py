#!/usr/bin/env python3
"""Recreate geography table with PRD-compliant schema"""

from cms_pricing.database import engine
from cms_pricing.models.geography import Geography
from sqlalchemy import text

# Drop and recreate the geography table
with engine.connect() as conn:
    print("Dropping existing geography table...")
    conn.execute(text("DROP TABLE IF EXISTS geography CASCADE"))
    conn.commit()
    
    print("Creating new PRD-compliant geography table...")
    Geography.__table__.create(engine)
    
    print("✅ Geography table recreated with PRD-compliant schema!")
    print("   - Added plus4, has_plus4 fields for ZIP+4 support")
    print("   - Added carrier, rural_flag fields per PRD")
    print("   - Added dataset_id, dataset_digest for traceability")
    print("   - Updated indexes per PRD section 7.1")
