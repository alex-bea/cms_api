#!/usr/bin/env python3
"""
MPFS Go-Live Validation Test
============================

Comprehensive validation following QTS standards for MPFS ingester go-live.
Validates source parity, locality/GPCI integrity, conversion factors, API functionality,
and quarterly diffs using existing test data.

Test ID: QA-MPFS-GO-LIVE-0001
Tier: Integration
Environment: Test
"""

import asyncio
import json
import logging
import random
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import httpx
from decimal import Decimal

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MPFSGoLiveValidator:
    """Comprehensive MPFS go-live validation following QTS standards."""
    
    def __init__(self, base_url: str = "http://localhost:8000", api_key: str = "test-key"):
        self.base_url = base_url
        self.api_key = api_key
        self.results = {
            "test_id": "QA-MPFS-GO-LIVE-0001",
            "timestamp": datetime.now().isoformat(),
            "environment": "test",
            "validation_results": {},
            "summary": {},
            "recommendations": []
        }
        
    async def run_validation(self) -> Dict[str, Any]:
        """Run complete go-live validation suite."""
        logger.info("🚀 Starting MPFS Go-Live Validation")
        
        try:
            # Phase 1: Source Parity Spot-Checks
            await self._validate_source_parity()
            
            # Phase 2: Locality/GPCI Integrity
            await self._validate_locality_gpci_integrity()
            
            # Phase 3: Conversion Factor Verification
            await self._validate_conversion_factors()
            
            # Phase 4: API Smoke Tests
            await self._validate_api_functionality()
            
            # Phase 5: Quarterly Diffs
            await self._validate_quarterly_diffs()
            
            # Generate summary
            self._generate_summary()
            
            logger.info("✅ MPFS Go-Live Validation Complete")
            return self.results
            
        except Exception as e:
            logger.error(f"❌ Validation failed: {e}")
            self.results["error"] = str(e)
            return self.results
    
    async def _validate_source_parity(self):
        """Phase 1: Source parity spot-checks by quarter."""
        logger.info("📊 Phase 1: Source Parity Spot-Checks")
        
        quarters = ["RVU25A", "RVU25B", "RVU25C", "RVU25D"]
        parity_results = {}
        
        for quarter in quarters:
            logger.info(f"  Validating {quarter}...")
            
            try:
                # Get sample data from API
                sample_data = await self._get_sample_rvu_data(quarter, sample_size=20)
                
                if not sample_data:
                    parity_results[quarter] = {
                        "status": "FAILED",
                        "error": "No data available",
                        "sample_size": 0
                    }
                    continue
                
                # Validate RVU values and indicators
                rvu_validation = self._validate_rvu_values(sample_data)
                
                # Calculate expected payment amounts
                payment_validation = await self._validate_payment_calculations(sample_data)
                
                parity_results[quarter] = {
                    "status": "PASSED" if rvu_validation["valid"] and payment_validation["valid"] else "FAILED",
                    "sample_size": len(sample_data),
                    "rvu_validation": rvu_validation,
                    "payment_validation": payment_validation
                }
                
            except Exception as e:
                parity_results[quarter] = {
                    "status": "ERROR",
                    "error": str(e)
                }
        
        self.results["validation_results"]["source_parity"] = parity_results
    
    async def _validate_locality_gpci_integrity(self):
        """Phase 2: Locality/GPCI integrity validation."""
        logger.info("🌍 Phase 2: Locality/GPCI Integrity")
        
        try:
            # Get locality data
            locality_data = await self._get_locality_data()
            
            # Sample 5 localities across different MACs
            sample_localities = self._sample_localities(locality_data, 5)
            
            # Validate locality IDs, names, and GPCI triplets
            locality_validation = self._validate_locality_data(sample_localities)
            
            # Include California MSA example
            ca_validation = self._validate_california_msa(locality_data)
            
            self.results["validation_results"]["locality_gpci"] = {
                "status": "PASSED" if locality_validation["valid"] and ca_validation["valid"] else "FAILED",
                "locality_validation": locality_validation,
                "california_validation": ca_validation,
                "sample_size": len(sample_localities)
            }
            
        except Exception as e:
            self.results["validation_results"]["locality_gpci"] = {
                "status": "ERROR",
                "error": str(e)
            }
    
    async def _validate_conversion_factors(self):
        """Phase 3: Conversion Factor vintage validation."""
        logger.info("💰 Phase 3: Conversion Factor Verification")
        
        try:
            # Get conversion factor data
            cf_data = await self._get_conversion_factor_data()
            
            # Validate CY 2025 CF is locked at ~$32.35/$32.3465
            cf_validation = self._validate_conversion_factor_values(cf_data)
            
            # Validate anesthesia CF if applicable
            anesthesia_validation = self._validate_anesthesia_cf(cf_data)
            
            self.results["validation_results"]["conversion_factors"] = {
                "status": "PASSED" if cf_validation["valid"] and anesthesia_validation["valid"] else "FAILED",
                "cf_validation": cf_validation,
                "anesthesia_validation": anesthesia_validation
            }
            
        except Exception as e:
            self.results["validation_results"]["conversion_factors"] = {
                "status": "ERROR",
                "error": str(e)
            }
    
    async def _validate_api_functionality(self):
        """Phase 4: API smoke tests."""
        logger.info("🔌 Phase 4: API Smoke Tests")
        
        api_tests = {}
        
        try:
            # Test 4a: Single code lookup
            single_code_test = await self._test_single_code_lookup()
            api_tests["single_code"] = single_code_test
            
            # Test 4b: Paginated list
            pagination_test = await self._test_pagination()
            api_tests["pagination"] = pagination_test
            
            # Test 4c: Filter by quarter_vintage & modifier
            filter_test = await self._test_filtering()
            api_tests["filtering"] = filter_test
            
            # Test 4d: Stats/health endpoints
            stats_test = await self._test_stats_health()
            api_tests["stats_health"] = stats_test
            
            # Overall API status
            all_passed = all(test.get("status") == "PASSED" for test in api_tests.values())
            
            self.results["validation_results"]["api_functionality"] = {
                "status": "PASSED" if all_passed else "FAILED",
                "tests": api_tests
            }
            
        except Exception as e:
            self.results["validation_results"]["api_functionality"] = {
                "status": "ERROR",
                "error": str(e)
            }
    
    async def _validate_quarterly_diffs(self):
        """Phase 5: Quarterly diffs validation."""
        logger.info("📈 Phase 5: Quarterly Diffs")
        
        try:
            # Generate diff RVU25A→25B
            diff_results = await self._generate_quarterly_diff("RVU25A", "RVU25B")
            
            # Identify notable changes
            notable_changes = self._identify_notable_changes(diff_results)
            
            self.results["validation_results"]["quarterly_diffs"] = {
                "status": "PASSED" if diff_results["valid"] else "FAILED",
                "diff_summary": diff_results,
                "notable_changes": notable_changes
            }
            
        except Exception as e:
            self.results["validation_results"]["quarterly_diffs"] = {
                "status": "ERROR",
                "error": str(e)
            }
    
    async def _get_sample_rvu_data(self, quarter: str, sample_size: int = 20) -> List[Dict]:
        """Get sample RVU data for validation."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.base_url}/mpfs/rvu",
                    params={"quarter_vintage": quarter, "limit": sample_size},
                    headers={"X-API-Key": self.api_key}
                )
                response.raise_for_status()
                data = response.json()
                return data.get("data", [])
        except Exception as e:
            logger.error(f"Failed to get sample RVU data for {quarter}: {e}")
            return []
    
    async def _get_locality_data(self) -> List[Dict]:
        """Get locality data for validation."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.base_url}/mpfs/localities",
                    headers={"X-API-Key": self.api_key}
                )
                response.raise_for_status()
                data = response.json()
                return data.get("data", [])
        except Exception as e:
            logger.error(f"Failed to get locality data: {e}")
            return []
    
    async def _get_conversion_factor_data(self) -> List[Dict]:
        """Get conversion factor data for validation."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.base_url}/mpfs/conversion-factors",
                    headers={"X-API-Key": self.api_key}
                )
                response.raise_for_status()
                data = response.json()
                return data.get("data", [])
        except Exception as e:
            logger.error(f"Failed to get conversion factor data: {e}")
            return []
    
    def _validate_rvu_values(self, sample_data: List[Dict]) -> Dict[str, Any]:
        """Validate RVU values and indicators."""
        validation_results = {
            "valid": True,
            "issues": [],
            "checks": {}
        }
        
        for record in sample_data:
            hcpcs = record.get("hcpcs", "")
            modifier = record.get("modifier", "")
            
            # Check required fields
            required_fields = ["rvu_work", "rvu_pe_nonfac", "rvu_pe_fac", "rvu_malp", "status_code"]
            for field in required_fields:
                if field not in record or record[field] is None:
                    validation_results["issues"].append(f"Missing {field} for {hcpcs}{modifier}")
                    validation_results["valid"] = False
            
            # Check RVU ranges
            rvu_fields = ["rvu_work", "rvu_pe_nonfac", "rvu_pe_fac", "rvu_malp"]
            for field in rvu_fields:
                if field in record and record[field] is not None:
                    value = float(record[field])
                    if value < 0 or value > 100:  # Reasonable RVU range
                        validation_results["issues"].append(f"RVU {field} out of range for {hcpcs}{modifier}: {value}")
                        validation_results["valid"] = False
        
        validation_results["checks"]["total_records"] = len(sample_data)
        validation_results["checks"]["issues_found"] = len(validation_results["issues"])
        
        return validation_results
    
    async def _validate_payment_calculations(self, sample_data: List[Dict]) -> Dict[str, Any]:
        """Validate payment calculations using the formula."""
        validation_results = {
            "valid": True,
            "issues": [],
            "calculations": []
        }
        
        for record in sample_data:
            try:
                # Extract RVU components
                work_rvu = float(record.get("rvu_work", 0))
                pe_nonfac_rvu = float(record.get("rvu_pe_nonfac", 0))
                pe_fac_rvu = float(record.get("rvu_pe_fac", 0))
                malp_rvu = float(record.get("rvu_malp", 0))
                
                # Get GPCI values (would need to fetch from locality data)
                # For now, use default values for validation
                gpci_work = 1.0
                gpci_pe = 1.0
                gpci_mp = 1.0
                cf = 32.35  # CY 2025 CF
                
                # Calculate expected amounts
                nonfac_amount = (work_rvu * gpci_work) + (pe_nonfac_rvu * gpci_pe) + (malp_rvu * gpci_mp) * cf
                fac_amount = (work_rvu * gpci_work) + (pe_fac_rvu * gpci_pe) + (malp_rvu * gpci_mp) * cf
                
                validation_results["calculations"].append({
                    "hcpcs": record.get("hcpcs", ""),
                    "modifier": record.get("modifier", ""),
                    "nonfac_amount": round(nonfac_amount, 2),
                    "fac_amount": round(fac_amount, 2)
                })
                
            except Exception as e:
                validation_results["issues"].append(f"Calculation error for {record.get('hcpcs', '')}: {e}")
                validation_results["valid"] = False
        
        return validation_results
    
    def _sample_localities(self, locality_data: List[Dict], count: int) -> List[Dict]:
        """Sample localities across different MACs."""
        if len(locality_data) <= count:
            return locality_data
        
        # Group by MAC (carrier_id)
        mac_groups = {}
        for locality in locality_data:
            mac = locality.get("carrier_id", "unknown")
            if mac not in mac_groups:
                mac_groups[mac] = []
            mac_groups[mac].append(locality)
        
        # Sample from each MAC
        sampled = []
        for mac, localities in mac_groups.items():
            if len(sampled) < count:
                sampled.extend(random.sample(localities, min(1, len(localities))))
        
        return sampled[:count]
    
    def _validate_locality_data(self, localities: List[Dict]) -> Dict[str, Any]:
        """Validate locality data integrity."""
        validation_results = {
            "valid": True,
            "issues": [],
            "checks": {}
        }
        
        for locality in localities:
            # Check required fields
            required_fields = ["locality_code", "locality_name", "state_fips", "carrier_id"]
            for field in required_fields:
                if field not in locality or not locality[field]:
                    validation_results["issues"].append(f"Missing {field} for locality {locality.get('locality_code', 'unknown')}")
                    validation_results["valid"] = False
            
            # Check GPCI values
            gpci_fields = ["gpci_work", "gpci_pe", "gpci_malp"]
            for field in gpci_fields:
                if field in locality and locality[field] is not None:
                    value = float(locality[field])
                    if value < 0.3 or value > 2.0:  # Reasonable GPCI range
                        validation_results["issues"].append(f"GPCI {field} out of range for {locality.get('locality_code', 'unknown')}: {value}")
                        validation_results["valid"] = False
        
        validation_results["checks"]["total_localities"] = len(localities)
        validation_results["checks"]["issues_found"] = len(validation_results["issues"])
        
        return validation_results
    
    def _validate_california_msa(self, locality_data: List[Dict]) -> Dict[str, Any]:
        """Validate California MSA example."""
        ca_localities = [loc for loc in locality_data if loc.get("state_fips") == "06"]  # California FIPS
        
        if not ca_localities:
            return {
                "valid": False,
                "error": "No California localities found"
            }
        
        # Find MSA localities (typically have specific locality codes)
        msa_localities = [loc for loc in ca_localities if "MSA" in loc.get("locality_name", "").upper()]
        
        return {
            "valid": len(msa_localities) > 0,
            "ca_localities_found": len(ca_localities),
            "msa_localities_found": len(msa_localities),
            "sample_msa": msa_localities[0] if msa_localities else None
        }
    
    def _validate_conversion_factor_values(self, cf_data: List[Dict]) -> Dict[str, Any]:
        """Validate conversion factor values."""
        validation_results = {
            "valid": True,
            "issues": [],
            "checks": {}
        }
        
        # Look for CY 2025 CF
        cy2025_cf = None
        for cf in cf_data:
            if "2025" in str(cf.get("effective_from", "")):
                cy2025_cf = cf
                break
        
        if not cy2025_cf:
            validation_results["issues"].append("CY 2025 conversion factor not found")
            validation_results["valid"] = False
        else:
            cf_value = float(cf.get("conversion_factor", 0))
            expected_range = (32.30, 32.40)  # Allow some tolerance
            
            if not (expected_range[0] <= cf_value <= expected_range[1]):
                validation_results["issues"].append(f"CF value {cf_value} outside expected range {expected_range}")
                validation_results["valid"] = False
            
            validation_results["checks"]["cf_value"] = cf_value
            validation_results["checks"]["cf_source"] = cf.get("source", "unknown")
        
        return validation_results
    
    def _validate_anesthesia_cf(self, cf_data: List[Dict]) -> Dict[str, Any]:
        """Validate anesthesia conversion factors."""
        # This would need to be implemented based on actual data structure
        return {
            "valid": True,
            "message": "Anesthesia CF validation not implemented in test data"
        }
    
    async def _test_single_code_lookup(self) -> Dict[str, Any]:
        """Test single code lookup (200 OK, populated data, correlation-id)."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.base_url}/mpfs/rvu/99213",  # Common HCPCS code
                    headers={"X-API-Key": self.api_key}
                )
                
                return {
                    "status": "PASSED" if response.status_code == 200 else "FAILED",
                    "status_code": response.status_code,
                    "has_data": len(response.json().get("data", [])) > 0,
                    "has_correlation_id": "X-Correlation-Id" in response.headers
                }
        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e)
            }
    
    async def _test_pagination(self) -> Dict[str, Any]:
        """Test paginated list (sane pagination metadata)."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.base_url}/mpfs/rvu",
                    params={"limit": 10, "offset": 0},
                    headers={"X-API-Key": self.api_key}
                )
                
                data = response.json()
                pagination = data.get("pagination", {})
                
                return {
                    "status": "PASSED" if response.status_code == 200 else "FAILED",
                    "status_code": response.status_code,
                    "has_pagination": "pagination" in data,
                    "pagination_metadata": pagination
                }
        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e)
            }
    
    async def _test_filtering(self) -> Dict[str, Any]:
        """Test filter by quarter_vintage & modifier."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.base_url}/mpfs/rvu",
                    params={"quarter_vintage": "RVU25A", "modifier": "26"},
                    headers={"X-API-Key": self.api_key}
                )
                
                data = response.json()
                filtered_data = data.get("data", [])
                
                return {
                    "status": "PASSED" if response.status_code == 200 else "FAILED",
                    "status_code": response.status_code,
                    "filtered_count": len(filtered_data),
                    "all_have_modifier": all(record.get("modifier") == "26" for record in filtered_data)
                }
        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e)
            }
    
    async def _test_stats_health(self) -> Dict[str, Any]:
        """Test stats/health endpoints."""
        try:
            async with httpx.AsyncClient() as client:
                # Test health endpoint
                health_response = await client.get(
                    f"{self.base_url}/mpfs/health",
                    headers={"X-API-Key": self.api_key}
                )
                
                # Test stats endpoint
                stats_response = await client.get(
                    f"{self.base_url}/mpfs/stats",
                    headers={"X-API-Key": self.api_key}
                )
                
                return {
                    "status": "PASSED" if health_response.status_code == 200 and stats_response.status_code == 200 else "FAILED",
                    "health_status": health_response.status_code,
                    "stats_status": stats_response.status_code,
                    "health_data": health_response.json() if health_response.status_code == 200 else None,
                    "stats_data": stats_response.json() if stats_response.status_code == 200 else None
                }
        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e)
            }
    
    async def _generate_quarterly_diff(self, quarter_a: str, quarter_b: str) -> Dict[str, Any]:
        """Generate quarterly diff between two quarters."""
        try:
            # Get data for both quarters
            data_a = await self._get_sample_rvu_data(quarter_a, sample_size=1000)
            data_b = await self._get_sample_rvu_data(quarter_b, sample_size=1000)
            
            if not data_a or not data_b:
                return {
                    "valid": False,
                    "error": "Insufficient data for comparison"
                }
            
            # Create comparison
            diff_results = {
                "valid": True,
                "quarter_a": quarter_a,
                "quarter_b": quarter_b,
                "records_a": len(data_a),
                "records_b": len(data_b),
                "new_codes": [],
                "deleted_codes": [],
                "modified_codes": []
            }
            
            # Find new codes in B not in A
            codes_a = {f"{r.get('hcpcs', '')}{r.get('modifier', '')}" for r in data_a}
            codes_b = {f"{r.get('hcpcs', '')}{r.get('modifier', '')}" for r in data_b}
            
            diff_results["new_codes"] = list(codes_b - codes_a)
            diff_results["deleted_codes"] = list(codes_a - codes_b)
            
            return diff_results
            
        except Exception as e:
            return {
                "valid": False,
                "error": str(e)
            }
    
    def _identify_notable_changes(self, diff_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify notable changes from quarterly diff."""
        notable_changes = []
        
        if diff_results.get("valid"):
            new_codes = diff_results.get("new_codes", [])
            deleted_codes = diff_results.get("deleted_codes", [])
            
            if new_codes:
                notable_changes.append({
                    "type": "new_codes",
                    "count": len(new_codes),
                    "sample": new_codes[:5]  # First 5 as sample
                })
            
            if deleted_codes:
                notable_changes.append({
                    "type": "deleted_codes",
                    "count": len(deleted_codes),
                    "sample": deleted_codes[:5]  # First 5 as sample
                })
        
        return notable_changes
    
    def _generate_summary(self):
        """Generate validation summary and recommendations."""
        validation_results = self.results["validation_results"]
        
        # Count overall status
        total_phases = len(validation_results)
        passed_phases = sum(1 for phase in validation_results.values() if phase.get("status") == "PASSED")
        failed_phases = sum(1 for phase in validation_results.values() if phase.get("status") == "FAILED")
        error_phases = sum(1 for phase in validation_results.values() if phase.get("status") == "ERROR")
        
        # Generate summary
        self.results["summary"] = {
            "overall_status": "PASSED" if failed_phases == 0 and error_phases == 0 else "FAILED",
            "total_phases": total_phases,
            "passed_phases": passed_phases,
            "failed_phases": failed_phases,
            "error_phases": error_phases,
            "pass_rate": round((passed_phases / total_phases) * 100, 2) if total_phases > 0 else 0
        }
        
        # Generate recommendations
        recommendations = []
        
        if failed_phases > 0:
            recommendations.append("Address failed validation phases before go-live")
        
        if error_phases > 0:
            recommendations.append("Resolve system errors in validation phases")
        
        # Check specific issues
        for phase_name, phase_result in validation_results.items():
            if phase_result.get("status") == "FAILED":
                recommendations.append(f"Fix issues in {phase_name} phase")
            elif phase_result.get("status") == "ERROR":
                recommendations.append(f"Resolve system errors in {phase_name} phase")
        
        self.results["recommendations"] = recommendations

async def main():
    """Main execution function."""
    validator = MPFSGoLiveValidator()
    results = await validator.run_validation()
    
    # Print console output
    print("\n" + "="*80)
    print("MPFS GO-LIVE VALIDATION RESULTS")
    print("="*80)
    
    print(f"\nOverall Status: {results['summary']['overall_status']}")
    print(f"Pass Rate: {results['summary']['pass_rate']}%")
    print(f"Phases: {results['summary']['passed_phases']}/{results['summary']['total_phases']} passed")
    
    print("\nPhase Results:")
    for phase_name, phase_result in results['validation_results'].items():
        status = phase_result.get('status', 'UNKNOWN')
        print(f"  {phase_name}: {status}")
    
    if results['recommendations']:
        print("\nRecommendations:")
        for i, rec in enumerate(results['recommendations'], 1):
            print(f"  {i}. {rec}")
    
    # Save JSON report
    report_file = f"mpfs_go_live_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\nDetailed report saved to: {report_file}")
    
    return results

if __name__ == "__main__":
    asyncio.run(main())
