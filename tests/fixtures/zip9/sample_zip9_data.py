"""
Sample ZIP9 data fixtures for testing following QA Testing Standard (QTS)

Test ID: QA-ZIP9-FIXT-0001
Owner: Data Engineering
Tier: fixture
Environments: dev, ci, staging
Dependencies: None
"""

import pandas as pd
from datetime import datetime, date
from pathlib import Path


def get_sample_zip9_data():
    """Get sample ZIP9 data for testing"""
    return pd.DataFrame([
        {
            'zip9_low': '902100000',
            'zip9_high': '902109999',
            'state': 'CA',
            'locality': '01',
            'rural_flag': 'A',
            'effective_from': '2025-08-14',
            'effective_to': None,
            'vintage': '2025-08-14'
        },
        {
            'zip9_low': '902110000',
            'zip9_high': '902119999',
            'state': 'CA',
            'locality': '01',
            'rural_flag': 'A',
            'effective_from': '2025-08-14',
            'effective_to': None,
            'vintage': '2025-08-14'
        },
        {
            'zip9_low': '100010000',
            'zip9_high': '100019999',
            'state': 'NY',
            'locality': '02',
            'rural_flag': 'B',
            'effective_from': '2025-08-14',
            'effective_to': None,
            'vintage': '2025-08-14'
        },
        {
            'zip9_low': '100020000',
            'zip9_high': '100029999',
            'state': 'NY',
            'locality': '02',
            'rural_flag': 'B',
            'effective_from': '2025-08-14',
            'effective_to': None,
            'vintage': '2025-08-14'
        },
        {
            'zip9_low': '606010000',
            'zip9_high': '606019999',
            'state': 'IL',
            'locality': '03',
            'rural_flag': None,
            'effective_from': '2025-08-14',
            'effective_to': None,
            'vintage': '2025-08-14'
        },
        {
            'zip9_low': '606020000',
            'zip9_high': '606029999',
            'state': 'IL',
            'locality': '03',
            'rural_flag': None,
            'effective_from': '2025-08-14',
            'effective_to': None,
            'vintage': '2025-08-14'
        },
        {
            'zip9_low': '770010000',
            'zip9_high': '770019999',
            'state': 'TX',
            'locality': '04',
            'rural_flag': 'A',
            'effective_from': '2025-08-14',
            'effective_to': None,
            'vintage': '2025-08-14'
        },
        {
            'zip9_low': '770020000',
            'zip9_high': '770029999',
            'state': 'TX',
            'locality': '04',
            'rural_flag': 'A',
            'effective_from': '2025-08-14',
            'effective_to': None,
            'vintage': '2025-08-14'
        },
        {
            'zip9_low': '331010000',
            'zip9_high': '331019999',
            'state': 'FL',
            'locality': '05',
            'rural_flag': 'B',
            'effective_from': '2025-08-14',
            'effective_to': None,
            'vintage': '2025-08-14'
        },
        {
            'zip9_low': '331020000',
            'zip9_high': '331029999',
            'state': 'FL',
            'locality': '05',
            'rural_flag': 'B',
            'effective_from': '2025-08-14',
            'effective_to': None,
            'vintage': '2025-08-14'
        }
    ])


def get_invalid_zip9_data():
    """Get invalid ZIP9 data for testing validation"""
    return pd.DataFrame([
        {
            'zip9_low': '9021',  # Too short
            'zip9_high': '902109999',
            'state': 'CA',
            'locality': '01',
            'rural_flag': 'A'
        },
        {
            'zip9_low': '902100000',
            'zip9_high': '9021',  # Too short
            'state': 'CA',
            'locality': '01',
            'rural_flag': 'A'
        },
        {
            'zip9_low': '90210000a',  # Non-numeric
            'zip9_high': '902109999',
            'state': 'CA',
            'locality': '01',
            'rural_flag': 'A'
        },
        {
            'zip9_low': '902109999',  # Low > High
            'zip9_high': '902100000',
            'state': 'CA',
            'locality': '01',
            'rural_flag': 'A'
        },
        {
            'zip9_low': '902100000',
            'zip9_high': '902109999',
            'state': 'XX',  # Invalid state
            'locality': '01',
            'rural_flag': 'A'
        }
    ])


def get_conflicting_zip9_data():
    """Get conflicting ZIP9 data for testing conflict detection"""
    return pd.DataFrame([
        {
            'zip9_low': '902100000',
            'zip9_high': '902109999',
            'state': 'CA',
            'locality': '99',  # Different locality than ZIP5
            'rural_flag': 'A',
            'effective_from': '2025-08-14',
            'effective_to': None,
            'vintage': '2025-08-14'
        },
        {
            'zip9_low': '100010000',
            'zip9_high': '100019999',
            'state': 'NY',
            'locality': '99',  # Different locality than ZIP5
            'rural_flag': 'B',
            'effective_from': '2025-08-14',
            'effective_to': None,
            'vintage': '2025-08-14'
        }
    ])


def get_zip9_manifest():
    """Get ZIP9 manifest for testing"""
    return {
        "fixture_id": "zip9_overrides_v2025q3",
        "schema_version": "1.0.0",
        "source_digest": "sha256:abc123def456...",
        "generated_at": "2025-09-29T12:34:56Z",
        "notes": "Sample ZIP9 overrides data for testing; covers 5 states with 2 ranges each",
        "record_count": 10,
        "states_covered": ["CA", "NY", "IL", "TX", "FL"],
        "zip5_prefixes": ["90210", "10001", "60601", "77001", "33101"]
    }


def get_zip9_schema_contract():
    """Get ZIP9 schema contract for testing"""
    return {
        "name": "cms_zip9_overrides",
        "version": "1.0",
        "description": "CMS ZIP9 overrides for precise locality mapping",
        "columns": {
            "zip9_low": {
                "name": "zip9_low",
                "type": "string",
                "nullable": False,
                "description": "Low end of ZIP9 range (9 digits)",
                "pattern": "^[0-9]{9}$",
                "min_length": 9,
                "max_length": 9
            },
            "zip9_high": {
                "name": "zip9_high", 
                "type": "string",
                "nullable": False,
                "description": "High end of ZIP9 range (9 digits)",
                "pattern": "^[0-9]{9}$",
                "min_length": 9,
                "max_length": 9
            },
            "state": {
                "name": "state",
                "type": "string",
                "nullable": False,
                "description": "Two-letter state code",
                "pattern": "^[A-Z]{2}$",
                "min_length": 2,
                "max_length": 2
            },
            "locality": {
                "name": "locality",
                "type": "string",
                "nullable": False,
                "description": "CMS locality code",
                "pattern": "^[0-9]{2}$",
                "min_length": 2,
                "max_length": 2
            },
            "rural_flag": {
                "name": "rural_flag",
                "type": "string",
                "nullable": True,
                "description": "Rural flag (A, B, or null)",
                "pattern": "^[AB]$",
                "min_length": 1,
                "max_length": 1
            },
            "effective_from": {
                "name": "effective_from",
                "type": "date",
                "nullable": False,
                "description": "Effective start date"
            },
            "effective_to": {
                "name": "effective_to",
                "type": "date",
                "nullable": True,
                "description": "Effective end date (null for ongoing)"
            },
            "vintage": {
                "name": "vintage",
                "type": "string",
                "nullable": False,
                "description": "Data vintage (YYYY-MM-DD)"
            }
        }
    }


def save_zip9_fixtures():
    """Save ZIP9 fixtures to disk for testing"""
    fixtures_dir = Path(__file__).parent
    fixtures_dir.mkdir(exist_ok=True)
    
    # Save sample data
    sample_data = get_sample_zip9_data()
    sample_data.to_csv(fixtures_dir / "sample_zip9_data.csv", index=False)
    
    # Save invalid data
    invalid_data = get_invalid_zip9_data()
    invalid_data.to_csv(fixtures_dir / "invalid_zip9_data.csv", index=False)
    
    # Save conflicting data
    conflicting_data = get_conflicting_zip9_data()
    conflicting_data.to_csv(fixtures_dir / "conflicting_zip9_data.csv", index=False)
    
    # Save manifest
    import json
    manifest = get_zip9_manifest()
    with open(fixtures_dir / "manifest.json", 'w') as f:
        json.dump(manifest, f, indent=2)
    
    # Save schema contract
    schema_contract = get_zip9_schema_contract()
    with open(fixtures_dir / "schema_contract.json", 'w') as f:
        json.dump(schema_contract, f, indent=2)
    
    print(f"ZIP9 fixtures saved to {fixtures_dir}")


if __name__ == "__main__":
    save_zip9_fixtures()
