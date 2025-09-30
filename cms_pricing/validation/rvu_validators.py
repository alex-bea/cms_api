"""
RVU Data Validation Engine

Implements business rule validations as specified in PRD Section 2.4 and 14.2
"""

from typing import Dict, List, Any, Optional, Tuple
import logging
from .types import ValidationLevel, ValidationResult

logger = logging.getLogger(__name__)

# Import QAReportGenerator here to avoid circular import
try:
    from .qa_reporter import QAReportGenerator
except ImportError:
    QAReportGenerator = None


class RVUValidator:
    """Main validator for RVU data business rules"""
    
    def __init__(self):
        self.validators = [
            self._validate_payability_rules,
            self._validate_na_indicator_logic,
            self._validate_global_days_semantics,
            self._validate_supervision_codes,
            self._validate_opps_caps,
            self._validate_anesthesia_cf_usage,
        ]
    
    def validate_rvu_item(self, rvu_item: Dict[str, Any]) -> List[ValidationResult]:
        """Validate a single RVU item against all business rules"""
        results = []
        
        for validator in self.validators:
            try:
                validator_results = validator(rvu_item)
                results.extend(validator_results)
            except Exception as e:
                logger.error(f"Validation error in {validator.__name__}: {e}")
                results.append(ValidationResult(
                    level=ValidationLevel.ERROR,
                    rule_name=validator.__name__,
                    message=f"Validation error: {e}",
                    record_id=rvu_item.get('hcpcs_code')
                ))
        
        return results
    
    def _validate_payability_rules(self, rvu_item: Dict[str, Any]) -> List[ValidationResult]:
        """Validate that only A/R/T status codes are payable"""
        results = []
        
        status_code = rvu_item.get('status_code')
        work_rvu = rvu_item.get('work_rvu', 0)
        pe_rvu_nonfac = rvu_item.get('pe_rvu_nonfac', 0)
        pe_rvu_fac = rvu_item.get('pe_rvu_fac', 0)
        mp_rvu = rvu_item.get('mp_rvu', 0)
        
        # Check if item has RVU values (indicating it's payable)
        has_rvu_values = any([
            work_rvu and work_rvu > 0,
            pe_rvu_nonfac and pe_rvu_nonfac > 0,
            pe_rvu_fac and pe_rvu_fac > 0,
            mp_rvu and mp_rvu > 0
        ])
        
        if has_rvu_values and status_code not in ['A', 'R', 'T']:
            # Add status code explanation
            status_explanations = {
                'I': 'Not payable by Medicare',
                'N': 'Not covered by Medicare',
                'J': 'Not payable by Medicare (carrier discretion)',
                'K': 'Not payable by Medicare (carrier discretion)',
                'L': 'Not payable by Medicare (carrier discretion)',
                'M': 'Not payable by Medicare (carrier discretion)',
                'P': 'Not payable by Medicare (carrier discretion)',
                'Q': 'Not payable by Medicare (carrier discretion)',
                'S': 'Not payable by Medicare (carrier discretion)',
                'U': 'Not payable by Medicare (carrier discretion)',
                'V': 'Not payable by Medicare (carrier discretion)',
                'W': 'Not payable by Medicare (carrier discretion)',
                'X': 'Not payable by Medicare (carrier discretion)',
                'Y': 'Not payable by Medicare (carrier discretion)',
                'Z': 'Not payable by Medicare (carrier discretion)'
            }
            
            status_explanation = status_explanations.get(status_code, f"Unknown status code '{status_code}'")
            
            results.append(ValidationResult(
                level=ValidationLevel.ERROR,
                rule_name="payability_rules",
                message=f"Non-payable status code '{status_code}' has RVU values. Status Code '{status_code}' = \"{status_explanation}\"",
                record_id=rvu_item.get('hcpcs_code'),
                field_name="status_code",
                actual_value=status_code,
                expected_value="A, R, or T"
            ))
        
        return results
    
    def _validate_na_indicator_logic(self, rvu_item: Dict[str, Any]) -> List[ValidationResult]:
        """Validate NA indicator logic: when NA=1, non-facility PE must be null"""
        results = []
        
        na_indicator = rvu_item.get('na_indicator')
        pe_rvu_nonfac = rvu_item.get('pe_rvu_nonfac')
        
        if na_indicator == '1' and pe_rvu_nonfac is not None and pe_rvu_nonfac > 0:
            results.append(ValidationResult(
                level=ValidationLevel.ERROR,
                rule_name="na_indicator_logic",
                message="NA indicator is 1 but non-facility PE RVU is not null/zero",
                record_id=rvu_item.get('hcpcs_code'),
                field_name="pe_rvu_nonfac",
                actual_value=pe_rvu_nonfac,
                expected_value="null or 0"
            ))
        
        return results
    
    def _validate_global_days_semantics(self, rvu_item: Dict[str, Any]) -> List[ValidationResult]:
        """Validate global days values are in valid domain"""
        results = []
        
        global_days = rvu_item.get('global_days')
        valid_global_days = {
            '000', '010', '090', 'XXX', 'YYY', 'ZZZ', 'MMM', 'PPP', 'QQQ', 'RRR', 'SSS', 'TTT', 'UUU', 'VVV', 'WWW'
        }
        
        if global_days and global_days not in valid_global_days:
            results.append(ValidationResult(
                level=ValidationLevel.WARN,
                rule_name="global_days_semantics",
                message=f"Global days value '{global_days}' not in standard domain",
                record_id=rvu_item.get('hcpcs_code'),
                field_name="global_days",
                actual_value=global_days,
                expected_value="Standard global period codes"
            ))
        
        return results
    
    def _validate_supervision_codes(self, rvu_item: Dict[str, Any]) -> List[ValidationResult]:
        """Validate physician supervision codes are in valid domain"""
        results = []
        
        supervision_code = rvu_item.get('physician_supervision')
        valid_supervision_codes = {
            '01', '02', '03', '04', '05', '06', '21', '22', '66', '6A', '77', '7A', '09'
        }
        
        if supervision_code and supervision_code not in valid_supervision_codes:
            results.append(ValidationResult(
                level=ValidationLevel.ERROR,
                rule_name="supervision_codes",
                message=f"Invalid supervision code '{supervision_code}'",
                record_id=rvu_item.get('hcpcs_code'),
                field_name="physician_supervision",
                actual_value=supervision_code,
                expected_value="Valid supervision codes (01-09, 21-22, 66, 6A, 77, 7A)"
            ))
        
        return results
    
    def _validate_opps_caps(self, rvu_item: Dict[str, Any]) -> List[ValidationResult]:
        """Validate OPPS caps don't exceed computed components"""
        results = []
        
        # This validation requires cross-referencing with OPPSCAP data
        # For now, we'll implement a placeholder that can be enhanced
        # when we have access to OPPSCAP data during validation
        
        hcpcs_code = rvu_item.get('hcpcs_code')
        if hcpcs_code and hcpcs_code.startswith('7'):  # Imaging codes typically start with 7
            # Placeholder for OPPS cap validation
            # In a full implementation, this would:
            # 1. Look up the code in OPPSCAP data
            # 2. Compare computed total with cap amount
            # 3. Flag if computed > cap
            pass
        
        return results
    
    def _validate_anesthesia_cf_usage(self, rvu_item: Dict[str, Any]) -> List[ValidationResult]:
        """Validate anesthesia codes use ANES CF, not PFS CF"""
        results = []
        
        hcpcs_code = rvu_item.get('hcpcs_code')
        description = rvu_item.get('description', '').lower()
        
        # Check if this is an anesthesia code
        is_anesthesia = (
            hcpcs_code and hcpcs_code.startswith('00') or
            'anesth' in description or
            'anesthesia' in description
        )
        
        if is_anesthesia:
            # This validation requires cross-referencing with ANES data
            # For now, we'll implement a placeholder
            # In a full implementation, this would:
            # 1. Check if code exists in ANES data
            # 2. Verify the conversion factor matches ANES CF, not PFS CF
            pass
        
        return results


class ValidationEngine:
    """Main validation engine for RVU datasets"""
    
    def __init__(self):
        self.rvu_validator = RVUValidator()
        self.qa_reporter = QAReportGenerator() if QAReportGenerator else None
    
    def validate_release(self, release_id: str, db_session) -> Dict[str, Any]:
        """Validate all data in a release"""
        from cms_pricing.models.rvu import RVUItem, GPCIIndex, OPPSCap, AnesCF, LocalityCounty
        
        validation_results = {
            'release_id': release_id,
            'structural': self._validate_structural(release_id, db_session),
            'content': self._validate_content(release_id, db_session),
            'referential': self._validate_referential(release_id, db_session),
            'business_rules': self._validate_business_rules(release_id, db_session)
        }
        
        # Generate QA reports
        if self.qa_reporter:
            try:
                report_paths = self.qa_reporter.generate_reports(release_id, validation_results)
                validation_results['qa_reports'] = report_paths
                logger.info(f"QA reports generated: {report_paths}")
            except Exception as e:
                logger.error(f"Failed to generate QA reports: {e}")
                validation_results['qa_reports'] = None
        else:
            validation_results['qa_reports'] = None
        
        return validation_results
    
    def _validate_structural(self, release_id: str, db_session) -> Dict[str, Any]:
        """Validate structural integrity"""
        from cms_pricing.models.rvu import RVUItem
        
        errors = []
        
        # Check required fields
        rvu_items = db_session.query(RVUItem).filter(RVUItem.release_id == release_id).all()
        for item in rvu_items:
            if not item.hcpcs_code or len(item.hcpcs_code) != 5:
                errors.append(f"Invalid HCPCS code: {item.hcpcs_code}")
            if not item.status_code:
                errors.append(f"Missing status code for HCPCS: {item.hcpcs_code}")
        
        return {"errors": errors, "status": "pass" if not errors else "fail"}
    
    def _validate_content(self, release_id: str, db_session) -> Dict[str, Any]:
        """Validate content integrity"""
        from cms_pricing.models.rvu import RVUItem, GPCIIndex
        
        errors = []
        
        # Check RVU values are non-negative
        rvu_items = db_session.query(RVUItem).filter(RVUItem.release_id == release_id).all()
        for item in rvu_items:
            if item.work_rvu and item.work_rvu < 0:
                errors.append(f"Negative work RVU for HCPCS: {item.hcpcs_code}")
            if item.pe_rvu_nonfac and item.pe_rvu_nonfac < 0:
                errors.append(f"Negative PE RVU (non-fac) for HCPCS: {item.hcpcs_code}")
        
        # Check GPCI values are in reasonable range
        gpci_items = db_session.query(GPCIIndex).filter(GPCIIndex.release_id == release_id).all()
        for item in gpci_items:
            if item.work_gpci < 0.1 or item.work_gpci > 3.0:
                errors.append(f"GPCI work value out of range: {item.work_gpci}")
        
        return {"errors": errors, "status": "pass" if not errors else "fail"}
    
    def _validate_referential(self, release_id: str, db_session) -> Dict[str, Any]:
        """Validate referential integrity"""
        from cms_pricing.models.rvu import OPPSCap, GPCIIndex
        
        errors = []
        
        # Check that OPPSCAP localities exist in GPCI
        opps_caps = db_session.query(OPPSCap).filter(OPPSCap.release_id == release_id).all()
        gpci_localities = set()
        for gpci in db_session.query(GPCIIndex).filter(GPCIIndex.release_id == release_id).all():
            gpci_localities.add((gpci.mac, gpci.locality_id))
        
        for opps in opps_caps:
            if (opps.mac, opps.locality_id) not in gpci_localities:
                errors.append(f"OPPSCAP locality {opps.locality_id} in MAC {opps.mac} not found in GPCI")
        
        return {"errors": errors, "status": "pass" if not errors else "fail"}
    
    def _validate_business_rules(self, release_id: str, db_session) -> Dict[str, Any]:
        """Validate business rules for all RVU items"""
        from cms_pricing.models.rvu import RVUItem
        
        all_results = []
        rvu_items = db_session.query(RVUItem).filter(RVUItem.release_id == release_id).all()
        
        for item in rvu_items:
            # Convert SQLAlchemy object to dict for validation
            item_dict = {
                'hcpcs_code': item.hcpcs_code,
                'status_code': item.status_code,
                'work_rvu': item.work_rvu,
                'pe_rvu_nonfac': item.pe_rvu_nonfac,
                'pe_rvu_fac': item.pe_rvu_fac,
                'mp_rvu': item.mp_rvu,
                'na_indicator': item.na_indicator,
                'global_days': item.global_days,
                'physician_supervision': item.physician_supervision,
                'description': item.description,
            }
            
            item_results = self.rvu_validator.validate_rvu_item(item_dict)
            all_results.extend(item_results)
        
        # Categorize results by level
        errors = [r for r in all_results if r.level == ValidationLevel.ERROR]
        warnings = [r for r in all_results if r.level == ValidationLevel.WARN]
        
        return {
            "errors": errors,
            "warnings": warnings,
            "total_validated": len(rvu_items),
            "status": "pass" if not errors else "fail"
        }
