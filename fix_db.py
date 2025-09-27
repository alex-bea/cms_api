#!/usr/bin/env python3
"""Fix database schema"""

from cms_pricing.database import engine
from sqlalchemy import text

with engine.connect() as conn:
    conn.execute(text('ALTER TABLE fee_mpfs ALTER COLUMN locality_id DROP NOT NULL'))
    conn.commit()
    print('Updated fee_mpfs table - locality_id is now nullable')
