#!/usr/bin/env python3
"""
OPPS Test Dataset Creator
=========================

Creates realistic test datasets for OPPS ingester testing following QTS v1.0 standards.
Generates 2025 Q1/Q2 golden data with realistic HCPCS codes, APC mappings, and edge cases.

Author: CMS Pricing Platform Team
Version: 1.0.0
QTS Compliance: v1.0
"""

import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Tuple
import pandas as pd
import numpy as np


class OPPSTestDatasetCreator:
    """Creates realistic OPPS test datasets for QTS compliance."""
    
    def __init__(self, output_dir: Path = None):
        self.output_dir = output_dir or Path("tests/fixtures/opps")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Realistic HCPCS codes for testing
        self.hcpcs_codes = [
            "99213", "99214", "99215",  # Office visits
            "99281", "99282", "99283", "99284", "99285",  # Emergency visits
            "36415", "36416",  # Blood draws
            "80053", "80061", "80069",  # Lab panels
            "93000", "93010", "93015",  # EKGs
            "71020", "71021", "71022",  # Chest X-rays
            "76700", "76770", "76775",  # Ultrasounds
            "70450", "70460", "70470",  # CT scans
            "70551", "70552", "70553",  # MRI brain
            "93000", "93010", "93015",  # EKGs
            "36415", "36416", "36417",  # Blood draws
            "80053", "80061", "80069",  # Lab panels
            "99213", "99214", "99215",  # Office visits
            "99281", "99282", "99283", "99284", "99285",  # Emergency visits
        ]
        
        # Realistic APC codes
        self.apc_codes = [
            "0001", "0002", "0003", "0004", "0005",  # General
            "0101", "0102", "0103", "0104", "0105",  # Level 1
            "0201", "0202", "0203", "0204", "0205",  # Level 2
            "0301", "0302", "0303", "0304", "0305",  # Level 3
            "0401", "0402", "0403", "0404", "0405",  # Level 4
            "0501", "0502", "0503", "0504", "0505",  # Level 5
        ]
        
        # Status indicators with realistic distribution
        self.status_indicators = {
            "A": 0.40,  # Separately payable
            "B": 0.25,  # Packaged
            "C": 0.15,  # Packaged
            "D": 0.10,  # Packaged
            "E": 0.05,  # Packaged
            "F": 0.03,  # Packaged
            "G": 0.02,  # Packaged
        }
        
        # Modifiers
        self.modifiers = [None, "26", "TC", "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "62", "63", "66", "76", "77", "78", "79", "80", "81", "82", "90", "91", "92", "93", "94", "95", "96", "97", "98", "99"]
        
        # CBSA codes for wage index
        self.cbsa_codes = [
            "12060", "14460", "16980", "19100", "26420",  # Major metros
            "31080", "33100", "35620", "37980", "41860",  # Secondary metros
            "47900", "48620", "49340", "51060", "52420",  # Tertiary metros
        ]
        
        # CCN codes
        self.ccn_codes = [
            "010001", "010002", "010003", "010004", "010005",
            "020001", "020002", "020003", "020004", "020005",
            "030001", "030002", "030003", "030004", "030005",
        ]
    
    def create_2025_q1_golden_data(self) -> Dict[str, pd.DataFrame]:
        """Create 2025 Q1 golden dataset."""
        logger.info("Creating 2025 Q1 golden dataset")
        
        # Generate APC payment data
        apc_payment = self._create_apc_payment_data(2025, 1, 100)
        
        # Generate HCPCS crosswalk data
        hcpcs_crosswalk = self._create_hcpcs_crosswalk_data(2025, 1, 200)
        
        # Generate enriched rates data
        rates_enriched = self._create_rates_enriched_data(2025, 1, apc_payment)
        
        # Generate SI lookup data
        si_lookup = self._create_si_lookup_data()
        
        return {
            "opps_apc_payment": apc_payment,
            "opps_hcpcs_crosswalk": hcpcs_crosswalk,
            "opps_rates_enriched": rates_enriched,
            "ref_si_lookup": si_lookup
        }
    
    def create_2025_q2_golden_data(self) -> Dict[str, pd.DataFrame]:
        """Create 2025 Q2 golden dataset."""
        logger.info("Creating 2025 Q2 golden dataset")
        
        # Generate APC payment data
        apc_payment = self._create_apc_payment_data(2025, 2, 105)
        
        # Generate HCPCS crosswalk data
        hcpcs_crosswalk = self._create_hcpcs_crosswalk_data(2025, 2, 210)
        
        # Generate enriched rates data
        rates_enriched = self._create_rates_enriched_data(2025, 2, apc_payment)
        
        # Generate SI lookup data
        si_lookup = self._create_si_lookup_data()
        
        return {
            "opps_apc_payment": apc_payment,
            "opps_hcpcs_crosswalk": hcpcs_crosswalk,
            "opps_rates_enriched": rates_enriched,
            "ref_si_lookup": si_lookup
        }
    
    def create_edge_case_data(self) -> Dict[str, pd.DataFrame]:
        """Create edge case test data."""
        logger.info("Creating edge case test data")
        
        # Edge cases for APC payment
        apc_edge_cases = pd.DataFrame([
            {
                "year": 2025,
                "quarter": 1,
                "apc_code": "0000",  # Edge case: 0000
                "apc_description": "Test APC with zero code",
                "payment_rate_usd": 0.01,  # Edge case: very low rate
                "relative_weight": 0.0001,  # Edge case: very low weight
                "packaging_flag": "Y",
                "effective_from": date(2025, 1, 1),
                "effective_to": None,
                "release_id": "opps_2025q1_r01",
                "batch_id": "test_edge_cases"
            },
            {
                "year": 2025,
                "quarter": 1,
                "apc_code": "9999",  # Edge case: 9999
                "apc_description": "Test APC with max code",
                "payment_rate_usd": 99999.99,  # Edge case: very high rate
                "relative_weight": 999.9999,  # Edge case: very high weight
                "packaging_flag": "N",
                "effective_from": date(2025, 1, 1),
                "effective_to": None,
                "release_id": "opps_2025q1_r01",
                "batch_id": "test_edge_cases"
            }
        ])
        
        # Edge cases for HCPCS crosswalk
        hcpcs_edge_cases = pd.DataFrame([
            {
                "year": 2025,
                "quarter": 1,
                "hcpcs_code": "00000",  # Edge case: all zeros
                "modifier": None,
                "status_indicator": "A",
                "apc_code": "0001",
                "payment_context": "Edge case test",
                "effective_from": date(2025, 1, 1),
                "effective_to": None,
                "release_id": "opps_2025q1_r01",
                "batch_id": "test_edge_cases"
            },
            {
                "year": 2025,
                "quarter": 1,
                "hcpcs_code": "99999",  # Edge case: all nines
                "modifier": "99",  # Edge case: max modifier
                "status_indicator": "Z",  # Edge case: max status indicator
                "apc_code": "9999",  # Edge case: max APC
                "payment_context": "Edge case test with max values",
                "effective_from": date(2025, 1, 1),
                "effective_to": None,
                "release_id": "opps_2025q1_r01",
                "batch_id": "test_edge_cases"
            }
        ])
        
        return {
            "opps_apc_payment": apc_edge_cases,
            "opps_hcpcs_crosswalk": hcpcs_edge_cases
        }
    
    def create_mock_validation_data(self) -> Dict[str, pd.DataFrame]:
        """Create mock validation data for I/OCE notes simulation."""
        logger.info("Creating mock validation data")
        
        # Mock I/OCE notes data
        i_oce_notes = pd.DataFrame([
            {
                "quarter": "2025Q1",
                "change_type": "add",
                "hcpcs_code": "12345",
                "modifier": None,
                "status_indicator": "A",
                "apc_code": "0101",
                "description": "New HCPCS code added",
                "effective_date": date(2025, 1, 1)
            },
            {
                "quarter": "2025Q1",
                "change_type": "delete",
                "hcpcs_code": "54321",
                "modifier": None,
                "status_indicator": "B",
                "apc_code": "0201",
                "description": "HCPCS code deleted",
                "effective_date": date(2025, 1, 1)
            },
            {
                "quarter": "2025Q1",
                "change_type": "si_change",
                "hcpcs_code": "67890",
                "modifier": "26",
                "status_indicator": "C",
                "apc_code": "0301",
                "description": "Status indicator changed from B to C",
                "effective_date": date(2025, 1, 1)
            }
        ])
        
        return {
            "i_oce_notes": i_oce_notes
        }
    
    def _create_apc_payment_data(self, year: int, quarter: int, count: int) -> pd.DataFrame:
        """Create APC payment data."""
        data = []
        
        for i in range(count):
            apc_code = random.choice(self.apc_codes)
            
            # Generate realistic payment rates
            base_rate = random.uniform(50, 5000)
            payment_rate = round(base_rate, 2)
            
            # Generate realistic relative weights
            relative_weight = round(random.uniform(0.1, 50.0), 4)
            
            # Generate packaging flag
            packaging_flag = random.choice(["Y", "N", None])
            
            # Generate effective dates
            effective_from = self._get_quarter_start(year, quarter)
            effective_to = None  # Most records are ongoing
            
            data.append({
                "year": year,
                "quarter": quarter,
                "apc_code": apc_code,
                "apc_description": f"APC {apc_code} Description",
                "payment_rate_usd": payment_rate,
                "relative_weight": relative_weight,
                "packaging_flag": packaging_flag,
                "effective_from": effective_from,
                "effective_to": effective_to,
                "release_id": f"opps_{year}q{quarter}_r01",
                "batch_id": f"opps_{year}q{quarter}_r01"
            })
        
        return pd.DataFrame(data)
    
    def _create_hcpcs_crosswalk_data(self, year: int, quarter: int, count: int) -> pd.DataFrame:
        """Create HCPCS crosswalk data."""
        data = []
        
        for i in range(count):
            hcpcs_code = random.choice(self.hcpcs_codes)
            modifier = random.choice(self.modifiers)
            
            # Generate status indicator based on realistic distribution
            status_indicator = np.random.choice(
                list(self.status_indicators.keys()),
                p=list(self.status_indicators.values())
            )
            
            # Generate APC code (some HCPCS may not have APC)
            apc_code = random.choice(self.apc_codes) if random.random() > 0.1 else None
            
            # Generate payment context
            payment_context = random.choice([
                "Separately Payable", "Packaged", "Not Payable", 
                "Special Payment", "Device Pass-Through"
            ])
            
            # Generate effective dates
            effective_from = self._get_quarter_start(year, quarter)
            effective_to = None  # Most records are ongoing
            
            data.append({
                "year": year,
                "quarter": quarter,
                "hcpcs_code": hcpcs_code,
                "modifier": modifier,
                "status_indicator": status_indicator,
                "apc_code": apc_code,
                "payment_context": payment_context,
                "effective_from": effective_from,
                "effective_to": effective_to,
                "release_id": f"opps_{year}q{quarter}_r01",
                "batch_id": f"opps_{year}q{quarter}_r01"
            })
        
        return pd.DataFrame(data)
    
    def _create_rates_enriched_data(self, year: int, quarter: int, apc_data: pd.DataFrame) -> pd.DataFrame:
        """Create enriched rates data with wage index."""
        data = []
        
        for _, apc_row in apc_data.iterrows():
            # Generate multiple facility records per APC
            facility_count = random.randint(1, 5)
            
            for _ in range(facility_count):
                ccn = random.choice(self.ccn_codes)
                cbsa_code = random.choice(self.cbsa_codes)
                
                # Generate wage index
                wage_index = round(random.uniform(0.3, 2.0), 3)
                
                # Calculate wage-adjusted rate
                base_rate = apc_row["payment_rate_usd"]
                wage_adjusted_rate = round(base_rate * wage_index, 2)
                
                data.append({
                    "year": year,
                    "quarter": quarter,
                    "apc_code": apc_row["apc_code"],
                    "ccn": ccn,
                    "cbsa_code": cbsa_code,
                    "wage_index": wage_index,
                    "payment_rate_usd": base_rate,
                    "wage_adjusted_rate_usd": wage_adjusted_rate,
                    "effective_from": apc_row["effective_from"],
                    "effective_to": apc_row["effective_to"],
                    "release_id": apc_row["release_id"],
                    "batch_id": apc_row["batch_id"]
                })
        
        return pd.DataFrame(data)
    
    def _create_si_lookup_data(self) -> pd.DataFrame:
        """Create SI lookup reference data."""
        data = []
        
        si_descriptions = {
            "A": "Services furnished to hospital outpatients that are paid under the OPPS",
            "B": "Services furnished to hospital outpatients that are paid under the OPPS and are subject to the OPPS packaging rules",
            "C": "Services furnished to hospital outpatients that are paid under the OPPS and are subject to the OPPS packaging rules",
            "D": "Services furnished to hospital outpatients that are paid under the OPPS and are subject to the OPPS packaging rules",
            "E": "Services furnished to hospital outpatients that are paid under the OPPS and are subject to the OPPS packaging rules",
            "F": "Services furnished to hospital outpatients that are paid under the OPPS and are subject to the OPPS packaging rules",
            "G": "Services furnished to hospital outpatients that are paid under the OPPS and are subject to the OPPS packaging rules",
            "H": "Services furnished to hospital outpatients that are paid under the OPPS and are subject to the OPPS packaging rules",
            "J": "Services furnished to hospital outpatients that are paid under the OPPS and are subject to the OPPS packaging rules",
            "K": "Services furnished to hospital outpatients that are paid under the OPPS and are subject to the OPPS packaging rules",
            "L": "Services furnished to hospital outpatients that are paid under the OPPS and are subject to the OPPS packaging rules",
            "M": "Services furnished to hospital outpatients that are paid under the OPPS and are subject to the OPPS packaging rules",
            "N": "Services furnished to hospital outpatients that are paid under the OPPS and are subject to the OPPS packaging rules",
            "P": "Services furnished to hospital outpatients that are paid under the OPPS and are subject to the OPPS packaging rules",
            "Q": "Services furnished to hospital outpatients that are paid under the OPPS and are subject to the OPPS packaging rules",
            "R": "Services furnished to hospital outpatients that are paid under the OPPS and are subject to the OPPS packaging rules",
            "S": "Services furnished to hospital outpatients that are paid under the OPPS and are subject to the OPPS packaging rules",
            "T": "Services furnished to hospital outpatients that are paid under the OPPS and are subject to the OPPS packaging rules",
            "U": "Services furnished to hospital outpatients that are paid under the OPPS and are subject to the OPPS packaging rules",
            "V": "Services furnished to hospital outpatients that are paid under the OPPS and are subject to the OPPS packaging rules",
            "W": "Services furnished to hospital outpatients that are paid under the OPPS and are subject to the OPPS packaging rules",
            "X": "Services furnished to hospital outpatients that are paid under the OPPS and are subject to the OPPS packaging rules",
            "Y": "Services furnished to hospital outpatients that are paid under the OPPS and are subject to the OPPS packaging rules",
            "Z": "Services furnished to hospital outpatients that are paid under the OPPS and are subject to the OPPS packaging rules"
        }
        
        payment_categories = {
            "A": "Separately Payable",
            "B": "Packaged",
            "C": "Packaged",
            "D": "Packaged",
            "E": "Packaged",
            "F": "Packaged",
            "G": "Packaged",
            "H": "Packaged",
            "J": "Packaged",
            "K": "Packaged",
            "L": "Packaged",
            "M": "Packaged",
            "N": "Packaged",
            "P": "Packaged",
            "Q": "Packaged",
            "R": "Packaged",
            "S": "Packaged",
            "T": "Packaged",
            "U": "Packaged",
            "V": "Packaged",
            "W": "Packaged",
            "X": "Packaged",
            "Y": "Packaged",
            "Z": "Packaged"
        }
        
        for si_code, description in si_descriptions.items():
            data.append({
                "status_indicator": si_code,
                "description": description,
                "payment_category": payment_categories[si_code],
                "effective_from": date(2025, 1, 1),
                "effective_to": None
            })
        
        return pd.DataFrame(data)
    
    def _get_quarter_start(self, year: int, quarter: int) -> date:
        """Get quarter start date."""
        quarter_starts = {
            1: date(year, 1, 1),
            2: date(year, 4, 1),
            3: date(year, 7, 1),
            4: date(year, 10, 1)
        }
        return quarter_starts[quarter]
    
    def save_golden_datasets(self):
        """Save all golden datasets to files."""
        logger.info("Saving golden datasets")
        
        # Create 2025 Q1 data
        q1_data = self.create_2025_q1_golden_data()
        self._save_dataset(q1_data, "2025q1")
        
        # Create 2025 Q2 data
        q2_data = self.create_2025_q2_golden_data()
        self._save_dataset(q2_data, "2025q2")
        
        # Create edge case data
        edge_data = self.create_edge_case_data()
        self._save_dataset(edge_data, "edge_cases")
        
        # Create mock validation data
        validation_data = self.create_mock_validation_data()
        self._save_dataset(validation_data, "mock_validation")
        
        # Generate manifest
        self._generate_manifest()
        
        logger.info("Golden datasets saved successfully")
    
    def _save_dataset(self, data: Dict[str, pd.DataFrame], dataset_name: str):
        """Save a dataset to files."""
        dataset_dir = self.output_dir / dataset_name
        dataset_dir.mkdir(parents=True, exist_ok=True)
        
        for table_name, df in data.items():
            # Save as Parquet
            parquet_path = dataset_dir / f"{table_name}.parquet"
            df.to_parquet(parquet_path, index=False)
            
            # Save as CSV for human readability
            csv_path = dataset_dir / f"{table_name}.csv"
            df.to_csv(csv_path, index=False)
            
            logger.info(f"Saved {table_name} to {parquet_path} and {csv_path}")
    
    def _generate_manifest(self):
        """Generate manifest file for all datasets."""
        manifest = {
            "fixture_id": "opps_golden_datasets_v2025q1q2",
            "schema_version": "1.0.0",
            "source_digest": "sha256:generated",
            "generated_at": datetime.utcnow().isoformat(),
            "notes": "Generated OPPS golden datasets for 2025 Q1/Q2 with realistic HCPCS codes, APC mappings, and edge cases",
            "datasets": {
                "2025q1": {
                    "description": "2025 Q1 golden dataset",
                    "tables": ["opps_apc_payment", "opps_hcpcs_crosswalk", "opps_rates_enriched", "ref_si_lookup"],
                    "record_counts": {}
                },
                "2025q2": {
                    "description": "2025 Q2 golden dataset", 
                    "tables": ["opps_apc_payment", "opps_hcpcs_crosswalk", "opps_rates_enriched", "ref_si_lookup"],
                    "record_counts": {}
                },
                "edge_cases": {
                    "description": "Edge case test data",
                    "tables": ["opps_apc_payment", "opps_hcpcs_crosswalk"],
                    "record_counts": {}
                },
                "mock_validation": {
                    "description": "Mock I/OCE validation data",
                    "tables": ["i_oce_notes"],
                    "record_counts": {}
                }
            },
            "qts_compliance": "v1.0",
            "dis_compliance": "v1.0"
        }
        
        # Count records in each dataset
        for dataset_name in ["2025q1", "2025q2", "edge_cases", "mock_validation"]:
            dataset_dir = self.output_dir / dataset_name
            if dataset_dir.exists():
                for table_file in dataset_dir.glob("*.parquet"):
                    table_name = table_file.stem
                    df = pd.read_parquet(table_file)
                    manifest["datasets"][dataset_name]["record_counts"][table_name] = len(df)
        
        manifest_path = self.output_dir / "manifest.json"
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        logger.info(f"Generated manifest at {manifest_path}")


# CLI interface
def main():
    """CLI entry point for creating test datasets."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Create OPPS test datasets')
    parser.add_argument('--output-dir', type=Path, default=Path('tests/fixtures/opps'), help='Output directory')
    parser.add_argument('--dataset', choices=['q1', 'q2', 'edge', 'validation', 'all'], default='all', help='Dataset to create')
    
    args = parser.parse_args()
    
    creator = OPPSTestDatasetCreator(args.output_dir)
    
    if args.dataset == 'all':
        creator.save_golden_datasets()
    elif args.dataset == 'q1':
        data = creator.create_2025_q1_golden_data()
        creator._save_dataset(data, "2025q1")
    elif args.dataset == 'q2':
        data = creator.create_2025_q2_golden_data()
        creator._save_dataset(data, "2025q2")
    elif args.dataset == 'edge':
        data = creator.create_edge_case_data()
        creator._save_dataset(data, "edge_cases")
    elif args.dataset == 'validation':
        data = creator.create_mock_validation_data()
        creator._save_dataset(data, "mock_validation")
    
    print("Test datasets created successfully!")


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    main()
