"""Script to populate zip_geometry table with sample coordinates for testing"""

import asyncio
from datetime import date
from sqlalchemy.orm import Session
from cms_pricing.database import SessionLocal, engine, Base
from cms_pricing.models.zip_geometry import ZipGeometry
from cms_pricing.models.geography import Geography
import math


def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two points using Haversine formula"""
    R = 3959  # Earth's radius in miles
    
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    
    a = (math.sin(delta_lat / 2) ** 2 + 
         math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2)
    c = 2 * math.asin(math.sqrt(a))
    
    return R * c


def populate_zip_geometry():
    """Populate zip_geometry table with sample coordinates"""
    
    # Create the table if it doesn't exist
    Base.metadata.create_all(bind=engine)
    
    db = SessionLocal()
    
    try:
        # Clear existing data
        db.query(ZipGeometry).delete()
        
        # Sample ZIP code coordinates (major cities and some rural areas)
        sample_coordinates = [
            # California
            ("90210", 34.0901, -118.4065, "CA", False),  # Beverly Hills
            ("94110", 37.7749, -122.4194, "CA", False),  # San Francisco
            ("90210", 34.0901, -118.4065, "CA", True),   # Beverly Hills PO Box
            ("90001", 33.9716, -118.2451, "CA", False),  # Los Angeles
            ("92101", 32.7157, -117.1611, "CA", False),  # San Diego
            
            # New York
            ("10001", 40.7505, -73.9934, "NY", False),   # New York City
            ("10001", 40.7505, -73.9934, "NY", True),    # NYC PO Box
            ("11201", 40.6943, -73.9903, "NY", False),   # Brooklyn
            ("13201", 43.0481, -76.1474, "NY", False),   # Syracuse
            
            # Texas
            ("75201", 32.7767, -96.7970, "TX", False),   # Dallas
            ("77001", 29.7604, -95.3698, "TX", False),   # Houston
            ("78701", 30.2672, -97.7431, "TX", False),   # Austin
            
            # Florida
            ("33101", 25.7617, -80.1918, "FL", False),   # Miami
            ("32801", 28.5383, -81.3792, "FL", False),   # Orlando
            
            # Illinois
            ("60601", 41.8781, -87.6298, "IL", False),   # Chicago
            
            # Massachusetts
            ("02101", 42.3601, -71.0589, "MA", False),   # Boston
            ("01434", 42.4072, -71.3824, "MA", False),   # Lowell (from our test data)
            
            # Pennsylvania
            ("19101", 39.9526, -75.1652, "PA", False),   # Philadelphia
            
            # Washington
            ("98101", 47.6062, -122.3321, "WA", False),  # Seattle
            
            # Colorado
            ("80201", 39.7392, -104.9903, "CO", False),  # Denver
            
            # Some rural areas for testing PO Box exclusion
            ("12345", 42.6526, -73.7562, "NY", True),    # Rural NY PO Box
            ("54321", 44.2619, -88.4154, "WI", True),    # Rural WI PO Box
            ("98765", 40.2732, -86.1349, "IN", True),    # Rural IN PO Box
        ]
        
        # Add coordinates for existing geography records
        existing_zips = db.query(Geography.zip5, Geography.state).distinct().limit(50).all()
        
        for zip5, state in existing_zips:
            if not any(coord[0] == zip5 for coord in sample_coordinates):
                # Generate approximate coordinates based on ZIP5 pattern
                # This is a simplified approach for testing
                lat_base = 39.0 + (int(zip5[:2]) % 20) * 0.5
                lon_base = -95.0 + (int(zip5[2:]) % 30) * 0.5
                
                sample_coordinates.append((zip5, lat_base, lon_base, state, False))
        
        # Insert sample data
        for zip5, lat, lon, state, is_pobox in sample_coordinates:
            zip_geom = ZipGeometry(
                zip5=zip5,
                lat=lat,
                lon=lon,
                state=state,
                is_pobox=is_pobox,
                effective_from=date(2024, 1, 1),
                effective_to=None
            )
            db.add(zip_geom)
        
        db.commit()
        print(f"✅ Populated zip_geometry table with {len(sample_coordinates)} records")
        
        # Verify the data
        count = db.query(ZipGeometry).count()
        print(f"✅ Total zip_geometry records: {count}")
        
        # Show sample of data
        samples = db.query(ZipGeometry).limit(5).all()
        print("\nSample zip_geometry records:")
        for sample in samples:
            print(f"  {sample.zip5}: {sample.lat}, {sample.lon} ({sample.state}) PO Box: {sample.is_pobox}")
            
    except Exception as e:
        print(f"❌ Error populating zip_geometry: {e}")
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    populate_zip_geometry()

