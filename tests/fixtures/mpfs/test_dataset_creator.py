"""
MPFS Test Dataset Creator

Creates sample MPFS data for testing following QTS standards
"""

import json
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Any
import random
import uuid


class MPFSTestDatasetCreator:
    """Creates test datasets for MPFS ingestor testing"""
    
    def __init__(self, output_dir: str = "tests/fixtures/mpfs/test_data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Sample HCPCS codes for different categories
        self.surgery_codes = [
            "10021", "10040", "10060", "10061", "10120", "10121", "10140", "10160", "10161",
            "11000", "11001", "11004", "11005", "11006", "11007", "11008", "11010", "11011",
            "11012", "11042", "11043", "11044", "11045", "11046", "11047", "11055", "11056"
        ]
        
        self.evaluation_codes = [
            "99201", "99202", "99203", "99204", "99205", "99211", "99212", "99213", "99214", "99215",
            "99281", "99282", "99283", "99284", "99285", "99291", "99292", "99341", "99342", "99343",
            "99344", "99345", "99347", "99348", "99349", "99350", "99401", "99402", "99403", "99404"
        ]
        
        self.radiology_codes = [
            "70010", "70015", "70020", "70030", "70050", "70052", "70053", "70060", "70070", "70100",
            "70110", "70120", "70130", "70140", "70150", "70160", "70170", "70180", "70190", "70200",
            "70210", "70220", "70230", "70240", "70250", "70260", "70270", "70280", "70290", "70300"
        ]
        
        self.therapy_codes = [
            "97110", "97112", "97113", "97116", "97124", "97140", "97150", "97161", "97162", "97163",
            "97164", "97165", "97166", "97167", "97168", "97169", "97170", "97171", "97172", "97173",
            "97174", "97175", "97176", "97177", "97178", "97179", "97180", "97181", "97182", "97183"
        ]
        
        # Status codes and their meanings
        self.status_codes = {
            "A": "Active",
            "R": "Radiology", 
            "T": "Therapy",
            "P": "Pathology",
            "L": "Laboratory",
            "M": "Medicine",
            "S": "Surgery",
            "X": "Anesthesia"
        }
        
        # Modifier codes
        self.modifiers = ["", "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "62", "63", "66", "80", "81", "82"]
        
        # Payment categories
        self.payment_categories = ["surgery", "evaluation", "radiology", "therapy", "pathology", "laboratory", "medicine", "anesthesia"]
    
    def create_all(self) -> Path:
        """Create all test datasets"""
        print("Creating MPFS test datasets...")
        
        # Create RVU data
        rvu_data = self._create_rvu_data()
        
        # Create conversion factor data
        cf_data = self._create_conversion_factor_data()
        
        # Create abstract data
        abstract_data = self._create_abstract_data()
        
        # Create manifest
        manifest = self._create_manifest(rvu_data, cf_data, abstract_data)
        
        # Save all data
        self._save_data(rvu_data, cf_data, abstract_data, manifest)
        
        print(f"MPFS test datasets created in {self.output_dir}")
        return self.output_dir
    
    def _create_rvu_data(self) -> Dict[str, pd.DataFrame]:
        """Create sample RVU data"""
        print("Creating RVU data...")
        
        # Create 1000 RVU records
        records = []
        
        for i in range(1000):
            # Choose category and get appropriate codes
            category = random.choice(self.payment_categories)
            if category == "surgery":
                hcpcs = random.choice(self.surgery_codes)
                status_code = "S"
            elif category == "evaluation":
                hcpcs = random.choice(self.evaluation_codes)
                status_code = "A"
            elif category == "radiology":
                hcpcs = random.choice(self.radiology_codes)
                status_code = "R"
            elif category == "therapy":
                hcpcs = random.choice(self.therapy_codes)
                status_code = "T"
            else:
                hcpcs = random.choice(self.surgery_codes)  # Default
                status_code = random.choice(list(self.status_codes.keys()))
            
            # Generate RVU values
            rvu_work = round(random.uniform(0.0, 50.0), 3) if random.random() > 0.1 else None
            rvu_pe_nonfac = round(random.uniform(0.0, 30.0), 3) if random.random() > 0.1 else None
            rvu_pe_fac = round(random.uniform(0.0, 30.0), 3) if random.random() > 0.1 else None
            rvu_malp = round(random.uniform(0.0, 10.0), 3) if random.random() > 0.1 else None
            
            # Calculate total RVU
            total_rvu = None
            if all(x is not None for x in [rvu_work, rvu_pe_nonfac, rvu_malp]):
                total_rvu = round(rvu_work + rvu_pe_nonfac + rvu_malp, 3)
            
            # Determine if payable
            is_payable = status_code in ["A", "R", "T", "S"] and rvu_work is not None and rvu_work > 0
            
            # Generate effective dates
            effective_from = date(2025, 1, 1) + timedelta(days=random.randint(0, 365))
            effective_to = None
            if random.random() > 0.8:  # 20% chance of having end date
                effective_to = effective_from + timedelta(days=random.randint(30, 365))
            
            record = {
                "hcpcs": hcpcs,
                "modifier": random.choice(self.modifiers) if random.random() > 0.7 else "",
                "status_code": status_code,
                "global_days": random.choice(["000", "010", "090", "XXX"]) if random.random() > 0.5 else "",
                "rvu_work": rvu_work,
                "rvu_pe_nonfac": rvu_pe_nonfac,
                "rvu_pe_fac": rvu_pe_fac,
                "rvu_malp": rvu_malp,
                "na_indicator": "Y" if random.random() > 0.9 else "",
                "opps_cap_applicable": random.choice([True, False]),
                "is_payable": is_payable,
                "payment_category": category,
                "bilateral_indicator": random.choice([True, False]),
                "multiple_procedure_indicator": random.choice([True, False]),
                "assistant_surgery_indicator": random.choice([True, False]),
                "co_surgeon_indicator": random.choice([True, False]),
                "team_surgery_indicator": random.choice([True, False]),
                "total_rvu": total_rvu,
                "is_surgery": category == "surgery",
                "is_evaluation": category == "evaluation",
                "is_procedure": category in ["surgery", "radiology", "therapy"],
                "effective_from": effective_from,
                "effective_to": effective_to,
                "release_id": f"mpfs_2025_test_{i:04d}",
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "batch_id": str(uuid.uuid4())
            }
            
            records.append(record)
        
        # Create DataFrame
        df = pd.DataFrame(records)
        
        # Create both CSV and Parquet versions
        return {
            "mpfs_rvu": df,
            "mpfs_rvu_csv": df,
            "mpfs_rvu_parquet": df
        }
    
    def _create_conversion_factor_data(self) -> Dict[str, pd.DataFrame]:
        """Create sample conversion factor data"""
        print("Creating conversion factor data...")
        
        records = []
        
        # Create physician conversion factors for 2025
        for i in range(10):
            record = {
                "id": f"cf_physician_2025_{i:02d}",
                "cf_type": "physician",
                "cf_value": round(random.uniform(30.0, 40.0), 4),
                "cf_description": f"Physician conversion factor {i+1}",
                "effective_from": date(2025, 1, 1),
                "effective_to": None,
                "release_id": f"mpfs_cf_2025_{i:02d}",
                "vintage_year": "2025",
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "batch_id": str(uuid.uuid4())
            }
            records.append(record)
        
        # Create anesthesia conversion factors for 2025
        for i in range(5):
            record = {
                "id": f"cf_anesthesia_2025_{i:02d}",
                "cf_type": "anesthesia",
                "cf_value": round(random.uniform(20.0, 30.0), 4),
                "cf_description": f"Anesthesia conversion factor {i+1}",
                "effective_from": date(2025, 1, 1),
                "effective_to": None,
                "release_id": f"mpfs_cf_anesthesia_2025_{i:02d}",
                "vintage_year": "2025",
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "batch_id": str(uuid.uuid4())
            }
            records.append(record)
        
        # Create DataFrame
        df = pd.DataFrame(records)
        
        return {
            "mpfs_conversion_factors": df,
            "mpfs_conversion_factors_csv": df,
            "mpfs_conversion_factors_parquet": df
        }
    
    def _create_abstract_data(self) -> Dict[str, pd.DataFrame]:
        """Create sample abstract data"""
        print("Creating abstract data...")
        
        records = []
        
        # Create national payment abstracts
        for i in range(5):
            record = {
                "id": f"abstract_national_2025_{i:02d}",
                "abstract_type": "national",
                "title": f"National Payment Abstract {i+1}",
                "content": f"This is the content for national payment abstract {i+1} for 2025.",
                "national_payment_total": round(random.uniform(1000000.0, 10000000.0), 2),
                "payment_year": "2025",
                "effective_from": date(2025, 1, 1),
                "effective_to": None,
                "release_id": f"mpfs_abstract_2025_{i:02d}",
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "batch_id": str(uuid.uuid4())
            }
            records.append(record)
        
        # Create summary abstracts
        for i in range(3):
            record = {
                "id": f"abstract_summary_2025_{i:02d}",
                "abstract_type": "summary",
                "title": f"Summary Abstract {i+1}",
                "content": f"This is the content for summary abstract {i+1} for 2025.",
                "national_payment_total": None,
                "payment_year": "2025",
                "effective_from": date(2025, 1, 1),
                "effective_to": None,
                "release_id": f"mpfs_abstract_summary_2025_{i:02d}",
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "batch_id": str(uuid.uuid4())
            }
            records.append(record)
        
        # Create DataFrame
        df = pd.DataFrame(records)
        
        return {
            "mpfs_abstracts": df,
            "mpfs_abstracts_csv": df,
            "mpfs_abstracts_parquet": df
        }
    
    def _create_manifest(self, rvu_data: Dict[str, pd.DataFrame], 
                        cf_data: Dict[str, pd.DataFrame], 
                        abstract_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Create manifest file"""
        print("Creating manifest...")
        
        manifest = {
            "dataset_name": "MPFS Test Dataset",
            "version": "1.0.0",
            "created_at": datetime.now().isoformat(),
            "description": "Test dataset for MPFS ingestor",
            "total_records": {
                "rvu_items": len(rvu_data["mpfs_rvu"]),
                "conversion_factors": len(cf_data["mpfs_conversion_factors"]),
                "abstracts": len(abstract_data["mpfs_abstracts"])
            },
            "files": [
                {
                    "filename": "mpfs_rvu_test.csv",
                    "type": "rvu_data",
                    "record_count": len(rvu_data["mpfs_rvu"]),
                    "description": "MPFS RVU test data"
                },
                {
                    "filename": "mpfs_conversion_factors_test.csv", 
                    "type": "conversion_factors",
                    "record_count": len(cf_data["mpfs_conversion_factors"]),
                    "description": "MPFS conversion factors test data"
                },
                {
                    "filename": "mpfs_abstracts_test.csv",
                    "type": "abstracts", 
                    "record_count": len(abstract_data["mpfs_abstracts"]),
                    "description": "MPFS abstracts test data"
                }
            ],
            "schema_version": "1.0.0",
            "test_dataset": True
        }
        
        return manifest
    
    def _save_data(self, rvu_data: Dict[str, pd.DataFrame], 
                   cf_data: Dict[str, pd.DataFrame], 
                   abstract_data: Dict[str, pd.DataFrame], 
                   manifest: Dict[str, Any]):
        """Save all data to files"""
        print("Saving data files...")
        
        # Save RVU data
        rvu_data["mpfs_rvu_csv"].to_csv(self.output_dir / "mpfs_rvu_test.csv", index=False)
        rvu_data["mpfs_rvu_parquet"].to_parquet(self.output_dir / "mpfs_rvu_test.parquet", index=False)
        
        # Save conversion factor data
        cf_data["mpfs_conversion_factors_csv"].to_csv(self.output_dir / "mpfs_conversion_factors_test.csv", index=False)
        cf_data["mpfs_conversion_factors_parquet"].to_parquet(self.output_dir / "mpfs_conversion_factors_test.parquet", index=False)
        
        # Save abstract data
        abstract_data["mpfs_abstracts_csv"].to_csv(self.output_dir / "mpfs_abstracts_test.csv", index=False)
        abstract_data["mpfs_abstracts_parquet"].to_parquet(self.output_dir / "mpfs_abstracts_test.parquet", index=False)
        
        # Save manifest
        with open(self.output_dir / "manifest.json", 'w') as f:
            json.dump(manifest, f, indent=2)
        
        print("Data files saved successfully")


# Example usage
if __name__ == "__main__":
    creator = MPFSTestDatasetCreator()
    output_dir = creator.create_all()
    print(f"Test datasets created in: {output_dir}")
