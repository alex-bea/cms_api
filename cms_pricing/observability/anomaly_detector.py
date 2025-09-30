"""
Anomaly Detection Engine

Identifies data quality issues, outliers, and anomalies in RVU data
"""

import statistics
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, date, timedelta
from dataclasses import dataclass
from sqlalchemy.orm import Session
from cms_pricing.database import SessionLocal
from cms_pricing.models.rvu import Release, RVUItem, GPCIIndex, OPPSCap, AnesCF, LocalityCounty
import logging

logger = logging.getLogger(__name__)


@dataclass
class Anomaly:
    """Represents a detected anomaly"""
    anomaly_type: str
    severity: str  # 'low', 'medium', 'high', 'critical'
    description: str
    affected_records: int
    dataset: str
    field: Optional[str] = None
    expected_value: Optional[Any] = None
    actual_value: Optional[Any] = None
    confidence: float = 1.0


class AnomalyDetector:
    """Detects anomalies in RVU data"""
    
    def __init__(self):
        self.db = SessionLocal()
    
    def detect_anomalies(self, release_id: str) -> List[Anomaly]:
        """Detect all types of anomalies in a release"""
        
        print(f"🔍 Detecting anomalies for release {release_id}...")
        
        anomalies = []
        
        # RVU Items anomalies
        rvu_anomalies = self._detect_rvu_anomalies(release_id)
        anomalies.extend(rvu_anomalies)
        
        # GPCI anomalies
        gpci_anomalies = self._detect_gpci_anomalies(release_id)
        anomalies.extend(gpci_anomalies)
        
        # OPPSCAP anomalies
        oppscap_anomalies = self._detect_oppscap_anomalies(release_id)
        anomalies.extend(oppscap_anomalies)
        
        # ANES anomalies
        anes_anomalies = self._detect_anes_anomalies(release_id)
        anomalies.extend(anes_anomalies)
        
        # Locality-County anomalies
        locco_anomalies = self._detect_locco_anomalies(release_id)
        anomalies.extend(locco_anomalies)
        
        # Cross-dataset anomalies
        cross_anomalies = self._detect_cross_dataset_anomalies(release_id)
        anomalies.extend(cross_anomalies)
        
        # Sort by severity
        severity_order = {'critical': 4, 'high': 3, 'medium': 2, 'low': 1}
        anomalies.sort(key=lambda x: severity_order.get(x.severity, 0), reverse=True)
        
        print(f"✅ Detected {len(anomalies)} anomalies:")
        for severity in ['critical', 'high', 'medium', 'low']:
            count = len([a for a in anomalies if a.severity == severity])
            if count > 0:
                print(f"   - {severity.title()}: {count}")
        
        return anomalies
    
    def _detect_rvu_anomalies(self, release_id: str) -> List[Anomaly]:
        """Detect anomalies in RVU items"""
        
        anomalies = []
        rvu_items = self.db.query(RVUItem).filter(RVUItem.release_id == release_id).all()
        
        if not rvu_items:
            return anomalies
        
        # Anomaly 1: Unusual RVU value distributions
        work_rvus = [item.work_rvu for item in rvu_items if item.work_rvu is not None]
        if work_rvus:
            work_mean = statistics.mean(work_rvus)
            work_std = statistics.stdev(work_rvus) if len(work_rvus) > 1 else 0
            
            # Find outliers (values > 3 standard deviations from mean)
            outliers = [item for item in rvu_items 
                       if item.work_rvu is not None and 
                       abs(item.work_rvu - work_mean) > 3 * work_std]
            
            if outliers:
                anomalies.append(Anomaly(
                    anomaly_type="outlier_values",
                    severity="medium",
                    description=f"Found {len(outliers)} RVU items with work RVU values >3σ from mean",
                    affected_records=len(outliers),
                    dataset="rvu_items",
                    field="work_rvu",
                    expected_value=f"{work_mean:.2f} ± {work_std:.2f}",
                    actual_value=f"Range: {min(item.work_rvu for item in outliers):.2f} - {max(item.work_rvu for item in outliers):.2f}"
                ))
        
        # Anomaly 2: Missing critical fields
        missing_work_rvu = [item for item in rvu_items if item.work_rvu is None and item.status_code in ['A', 'R', 'T']]
        if missing_work_rvu:
            anomalies.append(Anomaly(
                anomaly_type="missing_critical_data",
                severity="high",
                description=f"Found {len(missing_work_rvu)} payable items missing work RVU values",
                affected_records=len(missing_work_rvu),
                dataset="rvu_items",
                field="work_rvu"
            ))
        
        # Anomaly 3: Unusual status code distribution
        status_counts = {}
        for item in rvu_items:
            status_counts[item.status_code] = status_counts.get(item.status_code, 0) + 1
        
        total_items = len(rvu_items)
        for status, count in status_counts.items():
            percentage = (count / total_items) * 100
            if status == 'I' and percentage > 50:  # More than 50% non-payable
                anomalies.append(Anomaly(
                    anomaly_type="unusual_status_distribution",
                    severity="medium",
                    description=f"Unusually high percentage of non-payable items: {percentage:.1f}%",
                    affected_records=count,
                    dataset="rvu_items",
                    field="status_code",
                    expected_value="<50%",
                    actual_value=f"{percentage:.1f}%"
                ))
        
        # Anomaly 4: Duplicate HCPCS codes with different RVUs
        hcpcs_groups = {}
        for item in rvu_items:
            if item.hcpcs_code not in hcpcs_groups:
                hcpcs_groups[item.hcpcs_code] = []
            hcpcs_groups[item.hcpcs_code].append(item)
        
        duplicates = []
        for hcpcs, items in hcpcs_groups.items():
            if len(items) > 1:
                work_rvus = [item.work_rvu for item in items if item.work_rvu is not None]
                if len(set(work_rvus)) > 1:  # Different work RVU values
                    duplicates.extend(items)
        
        if duplicates:
            anomalies.append(Anomaly(
                anomaly_type="duplicate_hcpcs_different_rvus",
                severity="high",
                description=f"Found {len(duplicates)} duplicate HCPCS codes with different RVU values",
                affected_records=len(duplicates),
                dataset="rvu_items",
                field="hcpcs_code"
            ))
        
        return anomalies
    
    def _detect_gpci_anomalies(self, release_id: str) -> List[Anomaly]:
        """Detect anomalies in GPCI data"""
        
        anomalies = []
        gpci_items = self.db.query(GPCIIndex).filter(GPCIIndex.release_id == release_id).all()
        
        if not gpci_items:
            return anomalies
        
        # Anomaly 1: GPCI values outside normal range
        work_gpcis = [item.work_gpci for item in gpci_items if item.work_gpci is not None]
        if work_gpcis:
            min_gpci = min(work_gpcis)
            max_gpci = max(work_gpcis)
            
            if min_gpci < 0.5 or max_gpci > 2.0:
                anomalies.append(Anomaly(
                    anomaly_type="gpci_value_out_of_range",
                    severity="medium",
                    description=f"GPCI values outside normal range (0.5-2.0): {min_gpci:.3f} - {max_gpci:.3f}",
                    affected_records=len([g for g in gpci_items if g.work_gpci < 0.5 or g.work_gpci > 2.0]),
                    dataset="gpci_indices",
                    field="work_gpci",
                    expected_value="0.5 - 2.0",
                    actual_value=f"{min_gpci:.3f} - {max_gpci:.3f}"
                ))
        
        # Anomaly 2: Missing locality data
        missing_localities = [item for item in gpci_items if not item.locality_name or item.locality_name.strip() == '']
        if missing_localities:
            anomalies.append(Anomaly(
                anomaly_type="missing_locality_names",
                severity="low",
                description=f"Found {len(missing_localities)} GPCI records missing locality names",
                affected_records=len(missing_localities),
                dataset="gpci_indices",
                field="locality_name"
            ))
        
        return anomalies
    
    def _detect_oppscap_anomalies(self, release_id: str) -> List[Anomaly]:
        """Detect anomalies in OPPSCAP data"""
        
        anomalies = []
        oppscap_items = self.db.query(OPPSCap).filter(OPPSCap.release_id == release_id).all()
        
        if not oppscap_items:
            return anomalies
        
        # Anomaly 1: Negative prices
        negative_prices = [item for item in oppscap_items 
                          if item.price_fac < 0 or item.price_nonfac < 0]
        if negative_prices:
            anomalies.append(Anomaly(
                anomaly_type="negative_prices",
                severity="high",
                description=f"Found {len(negative_prices)} OPPSCAP records with negative prices",
                affected_records=len(negative_prices),
                dataset="opps_caps",
                field="price_fac,price_nonfac"
            ))
        
        # Anomaly 2: Unusual price ratios
        unusual_ratios = []
        for item in oppscap_items:
            if item.price_fac > 0 and item.price_nonfac > 0:
                ratio = item.price_fac / item.price_nonfac
                if ratio < 0.5 or ratio > 2.0:  # Facility price should be 50%-200% of non-facility
                    unusual_ratios.append(item)
        
        if unusual_ratios:
            anomalies.append(Anomaly(
                anomaly_type="unusual_price_ratios",
                severity="medium",
                description=f"Found {len(unusual_ratios)} OPPSCAP records with unusual facility/non-facility price ratios",
                affected_records=len(unusual_ratios),
                dataset="opps_caps",
                field="price_fac,price_nonfac"
            ))
        
        return anomalies
    
    def _detect_anes_anomalies(self, release_id: str) -> List[Anomaly]:
        """Detect anomalies in ANES data"""
        
        anomalies = []
        anes_items = self.db.query(AnesCF).filter(AnesCF.release_id == release_id).all()
        
        if not anes_items:
            return anomalies
        
        # Anomaly 1: Anesthesia CF values outside normal range
        cf_values = [item.anesthesia_cf for item in anes_items if item.anesthesia_cf is not None]
        if cf_values:
            min_cf = min(cf_values)
            max_cf = max(cf_values)
            
            if min_cf < 15.0 or max_cf > 35.0:  # Normal range for anesthesia CF
                anomalies.append(Anomaly(
                    anomaly_type="anes_cf_out_of_range",
                    severity="medium",
                    description=f"Anesthesia CF values outside normal range (15-35): {min_cf:.2f} - {max_cf:.2f}",
                    affected_records=len([a for a in anes_items if a.anesthesia_cf < 15.0 or a.anesthesia_cf > 35.0]),
                    dataset="anes_cfs",
                    field="anesthesia_cf",
                    expected_value="15.0 - 35.0",
                    actual_value=f"{min_cf:.2f} - {max_cf:.2f}"
                ))
        
        return anomalies
    
    def _detect_locco_anomalies(self, release_id: str) -> List[Anomaly]:
        """Detect anomalies in Locality-County data"""
        
        anomalies = []
        locco_items = self.db.query(LocalityCounty).filter(LocalityCounty.release_id == release_id).all()
        
        if not locco_items:
            return anomalies
        
        # Anomaly 1: Missing county names
        missing_counties = [item for item in locco_items if not item.county_name or item.county_name.strip() == '']
        if missing_counties:
            anomalies.append(Anomaly(
                anomaly_type="missing_county_names",
                severity="low",
                description=f"Found {len(missing_counties)} Locality-County records missing county names",
                affected_records=len(missing_counties),
                dataset="locality_counties",
                field="county_name"
            ))
        
        # Anomaly 2: Duplicate locality-county combinations
        locality_county_pairs = {}
        for item in locco_items:
            key = (item.mac, item.locality_id, item.county_name)
            if key not in locality_county_pairs:
                locality_county_pairs[key] = []
            locality_county_pairs[key].append(item)
        
        duplicates = []
        for key, items in locality_county_pairs.items():
            if len(items) > 1:
                duplicates.extend(items)
        
        if duplicates:
            anomalies.append(Anomaly(
                anomaly_type="duplicate_locality_county",
                severity="medium",
                description=f"Found {len(duplicates)} duplicate locality-county combinations",
                affected_records=len(duplicates),
                dataset="locality_counties",
                field="mac,locality_id,county_name"
            ))
        
        return anomalies
    
    def _detect_cross_dataset_anomalies(self, release_id: str) -> List[Anomaly]:
        """Detect anomalies across datasets"""
        
        anomalies = []
        
        # Get all data for the release
        rvu_items = self.db.query(RVUItem).filter(RVUItem.release_id == release_id).all()
        gpci_items = self.db.query(GPCIIndex).filter(GPCIIndex.release_id == release_id).all()
        oppscap_items = self.db.query(OPPSCap).filter(OPPSCap.release_id == release_id).all()
        
        # Anomaly 1: Mismatched locality counts
        rvu_localities = set()
        gpci_localities = set()
        
        for item in rvu_items:
            # Extract locality from source_file or other fields if available
            pass  # Would need to implement locality extraction logic
        
        for item in gpci_items:
            gpci_localities.add((item.mac, item.locality_id))
        
        # This is a simplified check - in practice, would need more sophisticated matching
        if len(gpci_localities) == 0 and len(rvu_items) > 0:
            anomalies.append(Anomaly(
                anomaly_type="missing_gpci_data",
                severity="high",
                description="RVU data present but no GPCI data found",
                affected_records=len(rvu_items),
                dataset="cross_dataset"
            ))
        
        return anomalies
    
    def generate_anomaly_report(self, release_id: str) -> Dict[str, Any]:
        """Generate a comprehensive anomaly report"""
        
        anomalies = self.detect_anomalies(release_id)
        
        # Group by severity
        by_severity = {}
        for anomaly in anomalies:
            if anomaly.severity not in by_severity:
                by_severity[anomaly.severity] = []
            by_severity[anomaly.severity].append(anomaly)
        
        # Group by dataset
        by_dataset = {}
        for anomaly in anomalies:
            if anomaly.dataset not in by_dataset:
                by_dataset[anomaly.dataset] = []
            by_dataset[anomaly.dataset].append(anomaly)
        
        # Calculate summary statistics
        total_affected_records = sum(anomaly.affected_records for anomaly in anomalies)
        
        return {
            "release_id": release_id,
            "detected_at": datetime.now().isoformat(),
            "summary": {
                "total_anomalies": len(anomalies),
                "total_affected_records": total_affected_records,
                "by_severity": {severity: len(anoms) for severity, anoms in by_severity.items()},
                "by_dataset": {dataset: len(anoms) for dataset, anoms in by_dataset.items()}
            },
            "anomalies": [
                {
                    "type": anomaly.anomaly_type,
                    "severity": anomaly.severity,
                    "description": anomaly.description,
                    "affected_records": anomaly.affected_records,
                    "dataset": anomaly.dataset,
                    "field": anomaly.field,
                    "expected_value": anomaly.expected_value,
                    "actual_value": anomaly.actual_value,
                    "confidence": anomaly.confidence
                }
                for anomaly in anomalies
            ]
        }
    
    def close(self):
        """Clean up resources"""
        self.db.close()

