"""
Test Dataset Creator for RVU End-to-End Testing

Creates a smaller, focused dataset for faster testing while maintaining
realistic data patterns and relationships.

QTS Compliance:
- Test ID: QA-RVU-FIXTURE-0001
- Owner: Data Engineering
- Purpose: Create deterministic test fixtures for E2E testing
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime, date
from decimal import Decimal
import random
import hashlib

class RVUTestDatasetCreator:
    """Creates focused test datasets for RVU end-to-end testing"""
    
    def __init__(self, output_dir: str = "tests/fixtures/rvu/test_data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Set random seed for deterministic results
        random.seed(42)
        
        # Test data parameters
        self.num_pprrvu_records = 1000
        self.num_gpci_records = 50
        self.num_oppscap_records = 200
        self.num_anes_records = 30
        self.num_locco_records = 40
        
        # Sample HCPCS codes for testing
        self.sample_hcpcs = [
            "99213", "99214", "99215",  # Office visits
            "00100", "00102", "00103",  # Anesthesia
            "0633T", "0634T", "0635T",  # OPPS procedures
            "G0001", "G0002", "G0003",  # G codes
            "D0120", "D0140", "D0150",  # Dental codes
        ]
        
        # Sample states and localities
        self.sample_states = ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]
        self.sample_localities = ["00", "01", "02", "03", "04", "05", "06", "07", "08", "09"]
        
        # Sample MAC codes
        self.sample_macs = ["01112", "02102", "03102", "04102", "05102", "06102", "07102", "08102", "09102", "10112"]
    
    def create_pprrvu_data(self) -> pd.DataFrame:
        """Create PPRRVU test data"""
        records = []
        
        for i in range(self.num_pprrvu_records):
            hcpcs = random.choice(self.sample_hcpcs)
            modifier = random.choice(["", "26", "TC", "50", "51", "52", "53", "54", "55", "56", "57", "58", "59"])
            
            # Generate realistic RVU values
            work_rvu = round(random.uniform(0.0, 10.0), 3) if random.random() > 0.1 else 0.0
            pe_rvu_nonfac = round(random.uniform(0.0, 8.0), 3) if random.random() > 0.1 else 0.0
            pe_rvu_fac = round(random.uniform(0.0, 6.0), 3) if random.random() > 0.1 else 0.0
            mp_rvu = round(random.uniform(0.0, 2.0), 3) if random.random() > 0.1 else 0.0
            
            # Generate status codes
            status_code = random.choice(["A", "R", "T", "I", "N", "X", "C", "J"])
            
            # Generate global days
            global_days = random.choice(["000", "010", "090", "XXX"])
            
            # Generate NA indicator
            na_indicator = random.choice(["0", "1", "9"])
            
            # Generate other fields
            bilateral_ind = random.choice(["0", "1"])
            multiple_proc_ind = random.choice(["0", "1"])
            assistant_surg_ind = random.choice(["0", "1"])
            co_surg_ind = random.choice(["0", "1"])
            team_surg_ind = random.choice(["0", "1"])
            endoscopic_base = random.choice(["0", "1"])
            
            # Calculate totals
            conversion_factor = 32.3465
            total_nonfac = round((work_rvu + pe_rvu_nonfac + mp_rvu) * conversion_factor, 2)
            total_fac = round((work_rvu + pe_rvu_fac + mp_rvu) * conversion_factor, 2)
            
            records.append({
                "hcpcs_code": hcpcs,
                "modifier": modifier if modifier else None,
                "description": f"Test procedure {hcpcs}",
                "status_code": status_code,
                "work_rvu": work_rvu,
                "pe_rvu_nonfac": pe_rvu_nonfac,
                "pe_rvu_fac": pe_rvu_fac,
                "mp_rvu": mp_rvu,
                "na_indicator": na_indicator,
                "global_days": global_days,
                "bilateral_ind": bilateral_ind,
                "multiple_proc_ind": multiple_proc_ind,
                "assistant_surg_ind": assistant_surg_ind,
                "co_surg_ind": co_surg_ind,
                "team_surg_ind": team_surg_ind,
                "endoscopic_base": endoscopic_base,
                "conversion_factor": conversion_factor,
                "physician_supervision": "01",
                "diag_imaging_family": "00",
                "total_nonfac": total_nonfac,
                "total_fac": total_fac,
                "effective_start": date(2025, 1, 1),
                "effective_end": date(2025, 12, 31),
                "source_file": "PPRRVU2025_Oct_test.txt",
                "row_num": i + 1
            })
        
        return pd.DataFrame(records)
    
    def create_gpci_data(self) -> pd.DataFrame:
        """Create GPCI test data"""
        records = []
        
        for i in range(self.num_gpci_records):
            mac = random.choice(self.sample_macs)
            state = random.choice(self.sample_states)
            locality_id = random.choice(self.sample_localities)
            locality_name = f"Test Locality {locality_id} {state}"
            
            # Generate realistic GPCI values
            work_gpci = round(random.uniform(0.8, 1.5), 3)
            pe_gpci = round(random.uniform(0.5, 1.2), 3)
            mp_gpci = round(random.uniform(0.4, 0.8), 3)
            
            records.append({
                "mac": mac,
                "state": state,
                "locality_id": locality_id,
                "locality_name": locality_name,
                "work_gpci": work_gpci,
                "pe_gpci": pe_gpci,
                "mp_gpci": mp_gpci,
                "effective_start": date(2025, 1, 1),
                "effective_end": date(2025, 12, 31),
                "source_file": "GPCI2025_test.txt",
                "row_num": i + 1
            })
        
        return pd.DataFrame(records)
    
    def create_oppscap_data(self) -> pd.DataFrame:
        """Create OPPS Cap test data"""
        records = []
        
        for i in range(self.num_oppscap_records):
            hcpcs = random.choice(self.sample_hcpcs)
            modifier = random.choice(["", "TC", "26", "50"])
            proc_status = random.choice(["C", "T", "B"])
            mac = random.choice(self.sample_macs)
            locality_id = random.choice(self.sample_localities)
            
            # Generate realistic prices
            base_price = round(random.uniform(50.0, 500.0), 2)
            price_fac = base_price
            price_nonfac = base_price * random.uniform(0.8, 1.2)
            
            records.append({
                "hcpcs_code": hcpcs,
                "modifier": modifier if modifier else None,
                "proc_status": proc_status,
                "mac": mac,
                "locality_id": locality_id,
                "price_fac": round(price_fac, 2),
                "price_nonfac": round(price_nonfac, 2),
                "effective_start": date(2025, 1, 1),
                "effective_end": date(2025, 12, 31),
                "source_file": "OPPSCAP_Oct_test.txt",
                "row_num": i + 1
            })
        
        return pd.DataFrame(records)
    
    def create_anes_data(self) -> pd.DataFrame:
        """Create Anesthesia CF test data"""
        records = []
        
        for i in range(self.num_anes_records):
            mac = random.choice(self.sample_macs)
            locality_id = random.choice(self.sample_localities)
            locality_name = f"Test Anesthesia Locality {locality_id}"
            
            # Generate realistic anesthesia CF values
            anesthesia_cf = round(random.uniform(15.0, 30.0), 2)
            
            records.append({
                "mac": mac,
                "locality_id": locality_id,
                "locality_name": locality_name,
                "anesthesia_cf": anesthesia_cf,
                "effective_start": date(2025, 1, 1),
                "effective_end": date(2025, 12, 31),
                "source_file": "ANES2025_test.txt",
                "row_num": i + 1
            })
        
        return pd.DataFrame(records)
    
    def create_locco_data(self) -> pd.DataFrame:
        """Create Locality-County test data"""
        records = []
        
        for i in range(self.num_locco_records):
            mac = random.choice(self.sample_macs)
            locality_id = random.choice(self.sample_localities)
            state = random.choice(self.sample_states)
            fee_schedule_area = random.choice(["STATEWIDE", "METRO", "RURAL", "SPECIAL"])
            county_name = f"Test County {i+1}"
            
            records.append({
                "mac": mac,
                "locality_id": locality_id,
                "state": state,
                "fee_schedule_area": fee_schedule_area,
                "county_name": county_name,
                "effective_start": date(2025, 1, 1),
                "effective_end": date(2025, 12, 31),
                "source_file": "25LOCCO_test.txt",
                "row_num": i + 1
            })
        
        return pd.DataFrame(records)
    
    def create_fixed_width_files(self):
        """Create fixed-width TXT files in CMS format"""
        
        # PPRRVU fixed-width format
        pprrvu_df = self.create_pprrvu_data()
        pprrvu_txt_path = self.output_dir / "PPRRVU2025_Oct_test.txt"
        
        with open(pprrvu_txt_path, 'w') as f:
            for _, row in pprrvu_df.iterrows():
                line = f"{row['hcpcs_code']:<5}{row['description']:<50}{row['status_code']:<1}{row['work_rvu']:>8.3f}{row['pe_rvu_nonfac']:>8.3f}{row['pe_rvu_fac']:>8.3f}{row['mp_rvu']:>8.3f}{row['na_indicator']:<1}{row['global_days']:<3}{row['bilateral_ind']:<1}{row['multiple_proc_ind']:<1}{row['assistant_surg_ind']:<1}{row['co_surg_ind']:<1}{row['team_surg_ind']:<1}{row['endoscopic_base']:<1}{row['conversion_factor']:>10.4f}{row['physician_supervision']:<2}{row['diag_imaging_family']:<2}{row['total_nonfac']:>8.2f}{row['total_fac']:>8.2f}\n"
                f.write(line)
        
        # GPCI fixed-width format
        gpci_df = self.create_gpci_data()
        gpci_txt_path = self.output_dir / "GPCI2025_test.txt"
        
        with open(gpci_txt_path, 'w') as f:
            for _, row in gpci_df.iterrows():
                line = f"{row['mac']:<5}{row['state']:<2}{row['locality_id']:<2}{row['locality_name']:<50}{row['work_gpci']:>8.3f}{row['pe_gpci']:>8.3f}{row['mp_gpci']:>8.3f}\n"
                f.write(line)
        
        # OPPS Cap fixed-width format
        oppscap_df = self.create_oppscap_data()
        oppscap_txt_path = self.output_dir / "OPPSCAP_Oct_test.txt"
        
        with open(oppscap_txt_path, 'w') as f:
            for _, row in oppscap_df.iterrows():
                line = f"{row['hcpcs_code']:<5}{row['modifier'] or '':<2}{row['proc_status']:<1}{row['mac']:<5}{row['locality_id']:<2}{row['price_fac']:>8.2f}{row['price_nonfac']:>8.2f}\n"
                f.write(line)
        
        # Anesthesia CF fixed-width format
        anes_df = self.create_anes_data()
        anes_txt_path = self.output_dir / "ANES2025_test.txt"
        
        with open(anes_txt_path, 'w') as f:
            for _, row in anes_df.iterrows():
                line = f"{row['mac']:<5}{row['locality_id']:<2}{row['locality_name']:<50}{row['anesthesia_cf']:>4.0f}\n"
                f.write(line)
        
        # Locality-County fixed-width format
        locco_df = self.create_locco_data()
        locco_txt_path = self.output_dir / "25LOCCO_test.txt"
        
        with open(locco_txt_path, 'w') as f:
            for _, row in locco_df.iterrows():
                line = f"{row['mac']:<5}{row['locality_id']:<2}{row['state']:<20}{row['fee_schedule_area']:<30}{row['county_name']:<50}\n"
                f.write(line)
    
    def create_csv_files(self):
        """Create CSV files with headers"""
        
        # PPRRVU CSV
        pprrvu_df = self.create_pprrvu_data()
        pprrvu_csv_path = self.output_dir / "PPRRVU2025_Oct_test.csv"
        pprrvu_df.to_csv(pprrvu_csv_path, index=False)
        
        # GPCI CSV
        gpci_df = self.create_gpci_data()
        gpci_csv_path = self.output_dir / "GPCI2025_test.csv"
        gpci_df.to_csv(gpci_csv_path, index=False)
        
        # OPPS Cap CSV
        oppscap_df = self.create_oppscap_data()
        oppscap_csv_path = self.output_dir / "OPPSCAP_Oct_test.csv"
        oppscap_df.to_csv(oppscap_csv_path, index=False)
        
        # Anesthesia CF CSV
        anes_df = self.create_anes_data()
        anes_csv_path = self.output_dir / "ANES2025_test.csv"
        anes_df.to_csv(anes_csv_path, index=False)
        
        # Locality-County CSV
        locco_df = self.create_locco_data()
        locco_csv_path = self.output_dir / "25LOCCO_test.csv"
        locco_df.to_csv(locco_csv_path, index=False)
    
    def create_manifest(self):
        """Create test manifest.json"""
        manifest = {
            "source": "cms_rvu_test",
            "discovered_at": datetime.now().isoformat(),
            "files": [
                {
                    "filename": "PPRRVU2025_Oct_test.txt",
                    "url": "file://tests/fixtures/rvu/test_data/PPRRVU2025_Oct_test.txt",
                    "sha256": self._calculate_file_hash(self.output_dir / "PPRRVU2025_Oct_test.txt"),
                    "size_bytes": (self.output_dir / "PPRRVU2025_Oct_test.txt").stat().st_size,
                    "content_type": "text/plain",
                    "last_modified": datetime.now().isoformat()
                },
                {
                    "filename": "GPCI2025_test.txt",
                    "url": "file://tests/fixtures/rvu/test_data/GPCI2025_test.txt",
                    "sha256": self._calculate_file_hash(self.output_dir / "GPCI2025_test.txt"),
                    "size_bytes": (self.output_dir / "GPCI2025_test.txt").stat().st_size,
                    "content_type": "text/plain",
                    "last_modified": datetime.now().isoformat()
                },
                {
                    "filename": "OPPSCAP_Oct_test.txt",
                    "url": "file://tests/fixtures/rvu/test_data/OPPSCAP_Oct_test.txt",
                    "sha256": self._calculate_file_hash(self.output_dir / "OPPSCAP_Oct_test.txt"),
                    "size_bytes": (self.output_dir / "OPPSCAP_Oct_test.txt").stat().st_size,
                    "content_type": "text/plain",
                    "last_modified": datetime.now().isoformat()
                },
                {
                    "filename": "ANES2025_test.txt",
                    "url": "file://tests/fixtures/rvu/test_data/ANES2025_test.txt",
                    "sha256": self._calculate_file_hash(self.output_dir / "ANES2025_test.txt"),
                    "size_bytes": (self.output_dir / "ANES2025_test.txt").stat().st_size,
                    "content_type": "text/plain",
                    "last_modified": datetime.now().isoformat()
                },
                {
                    "filename": "25LOCCO_test.txt",
                    "url": "file://tests/fixtures/rvu/test_data/25LOCCO_test.txt",
                    "sha256": self._calculate_file_hash(self.output_dir / "25LOCCO_test.txt"),
                    "size_bytes": (self.output_dir / "25LOCCO_test.txt").stat().st_size,
                    "content_type": "text/plain",
                    "last_modified": datetime.now().isoformat()
                }
            ],
            "metadata": {
                "test_dataset": True,
                "record_counts": {
                    "pprrvu": self.num_pprrvu_records,
                    "gpci": self.num_gpci_records,
                    "oppscap": self.num_oppscap_records,
                    "anes": self.num_anes_records,
                    "locco": self.num_locco_records
                },
                "total_records": (self.num_pprrvu_records + self.num_gpci_records + 
                                self.num_oppscap_records + self.num_anes_records + 
                                self.num_locco_records),
                "created_at": datetime.now().isoformat(),
                "version": "1.0.0"
            },
            "license": {
                "name": "Test Data License",
                "url": "https://example.com/test-license",
                "attribution_required": False
            }
        }
        
        manifest_path = self.output_dir / "manifest.json"
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2, default=str)
        
        return manifest_path
    
    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of file"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    
    def create_all(self):
        """Create all test data files"""
        print("Creating RVU test dataset...")
        
        # Create fixed-width files
        print("  Creating fixed-width TXT files...")
        self.create_fixed_width_files()
        
        # Create CSV files
        print("  Creating CSV files...")
        self.create_csv_files()
        
        # Create manifest
        print("  Creating manifest...")
        manifest_path = self.create_manifest()
        
        print(f"Test dataset created in: {self.output_dir}")
        print(f"Manifest: {manifest_path}")
        
        return self.output_dir

if __name__ == "__main__":
    creator = RVUTestDatasetCreator()
    creator.create_all()
