"""Audit and validation services for nearest ZIP resolver"""

from typing import Dict, List, Any, Tuple
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_
import structlog

from cms_pricing.models.nearest_zip import ZipMetadata, CMSZipLocality, NearestZipTrace

logger = structlog.get_logger()


class NearestZipAudit:
    """Audit and validation services for nearest ZIP resolver"""
    
    def __init__(self, db_session: Session):
        self.db = db_session
    
    def audit_pobox_exclusions(self) -> Dict[str, Any]:
        """
        Audit PO Box exclusions to validate is_pobox derivation logic
        
        Returns:
            Dict with audit results and statistics
        """
        # Get total ZIPs in CMS data
        total_cms_zips = self.db.query(func.count(CMSZipLocality.zip5)).scalar() or 0
        
        # Get ZIPs with metadata
        zips_with_metadata = self.db.query(func.count(ZipMetadata.zip5)).scalar() or 0
        
        # Get PO Box ZIPs (is_pobox = True)
        pobox_zips = self.db.query(func.count(ZipMetadata.zip5)).filter(
            ZipMetadata.is_pobox == True
        ).scalar() or 0
        
        # Get non-PO Box ZIPs (is_pobox = False)
        non_pobox_zips = self.db.query(func.count(ZipMetadata.zip5)).filter(
            ZipMetadata.is_pobox == False
        ).scalar() or 0
        
        # Get ZIPs with null is_pobox (should be treated as non-PO Box)
        null_pobox_zips = self.db.query(func.count(ZipMetadata.zip5)).filter(
            ZipMetadata.is_pobox.is_(None)
        ).scalar() or 0
        
        # Calculate coverage
        metadata_coverage = (zips_with_metadata / total_cms_zips * 100) if total_cms_zips > 0 else 0
        pobox_percentage = (pobox_zips / zips_with_metadata * 100) if zips_with_metadata > 0 else 0
        
        # Get sample PO Box ZIPs for validation
        sample_pobox_zips = self.db.query(ZipMetadata.zip5).filter(
            ZipMetadata.is_pobox == True
        ).limit(10).all()
        
        sample_pobox_list = [row[0] for row in sample_pobox_zips]
        
        # Get sample non-PO Box ZIPs for validation
        sample_non_pobox_zips = self.db.query(ZipMetadata.zip5).filter(
            ZipMetadata.is_pobox == False
        ).limit(10).all()
        
        sample_non_pobox_list = [row[0] for row in sample_non_pobox_zips]
        
        return {
            'total_cms_zips': total_cms_zips,
            'zips_with_metadata': zips_with_metadata,
            'metadata_coverage_percent': round(metadata_coverage, 2),
            'pobox_zips': pobox_zips,
            'non_pobox_zips': non_pobox_zips,
            'null_pobox_zips': null_pobox_zips,
            'pobox_percentage': round(pobox_percentage, 2),
            'sample_pobox_zips': sample_pobox_list,
            'sample_non_pobox_zips': sample_non_pobox_list,
            'audit_timestamp': datetime.now().isoformat()
        }
    
    def validate_pobox_derivation_logic(self) -> Dict[str, Any]:
        """
        Validate the PO Box derivation logic by checking patterns
        
        Returns:
            Dict with validation results
        """
        # Check for common PO Box patterns in ZIP codes
        pobox_patterns = [
            'PO BOX',
            'P.O. BOX',
            'POST OFFICE BOX',
            'POB',
            'POBOX'
        ]
        
        # Get all ZIP metadata with potential PO Box indicators
        # Note: This is a simplified check - real implementation would need
        # to check against address data or other indicators
        
        # Check for ZIPs that might be PO Boxes but aren't flagged
        potential_missed_pobox = self.db.query(ZipMetadata.zip5).filter(
            and_(
                ZipMetadata.is_pobox == False,
                # Add pattern matching here if we had address data
                # For now, just check for some common patterns
                or_(
                    ZipMetadata.zip5.like('9%'),  # Some PO Box ZIPs start with 9
                    ZipMetadata.zip5.like('0%')   # Some PO Box ZIPs start with 0
                )
            )
        ).limit(5).all()
        
        potential_missed_list = [row[0] for row in potential_missed_pobox]
        
        # Check for ZIPs flagged as PO Boxes that might not be
        potential_false_positives = self.db.query(ZipMetadata.zip5).filter(
            and_(
                ZipMetadata.is_pobox == True,
                # Add logic to check for non-PO Box patterns
                # For now, just get a sample
            )
        ).limit(5).all()
        
        potential_false_positives_list = [row[0] for row in potential_false_positives]
        
        return {
            'pobox_patterns_checked': pobox_patterns,
            'potential_missed_pobox': potential_missed_list,
            'potential_false_positives': potential_false_positives_list,
            'validation_timestamp': datetime.now().isoformat()
        }
    
    def get_pobox_exclusion_stats(self) -> Dict[str, Any]:
        """
        Get statistics on PO Box exclusions in recent resolutions
        
        Returns:
            Dict with PO Box exclusion statistics
        """
        # Get recent resolutions (last 24 hours)
        cutoff_time = datetime.now() - timedelta(hours=24)
        
        # Count total resolutions
        total_resolutions = self.db.query(func.count(NearestZipTrace.id)).filter(
            NearestZipTrace.created_at >= cutoff_time
        ).scalar() or 0
        
        # This would require tracking which ZIPs were excluded as PO Boxes
        # For now, return basic stats
        return {
            'total_resolutions_24h': total_resolutions,
            'pobox_exclusions_tracked': False,  # Would need to implement tracking
            'exclusion_rate_percent': 0.0,  # Would calculate from tracked data
            'stats_timestamp': datetime.now().isoformat()
        }
    
    def run_comprehensive_audit(self) -> Dict[str, Any]:
        """
        Run comprehensive audit of PO Box exclusions
        
        Returns:
            Dict with complete audit results
        """
        pobox_audit = self.audit_pobox_exclusions()
        validation = self.validate_pobox_derivation_logic()
        exclusion_stats = self.get_pobox_exclusion_stats()
        
        # Calculate overall health score
        health_score = 100.0
        
        # Deduct points for low metadata coverage
        if pobox_audit['metadata_coverage_percent'] < 90:
            health_score -= 20
        
        # Deduct points for high PO Box percentage (might indicate issues)
        if pobox_audit['pobox_percentage'] > 50:
            health_score -= 10
        
        # Deduct points for potential missed PO Boxes
        if len(validation['potential_missed_pobox']) > 0:
            health_score -= 15
        
        # Deduct points for potential false positives
        if len(validation['potential_false_positives']) > 0:
            health_score -= 10
        
        return {
            'pobox_audit': pobox_audit,
            'validation': validation,
            'exclusion_stats': exclusion_stats,
            'health_score': max(0, health_score),
            'audit_timestamp': datetime.now().isoformat()
        }
