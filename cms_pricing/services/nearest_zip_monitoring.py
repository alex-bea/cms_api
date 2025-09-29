"""Monitoring and alerting for nearest ZIP resolver"""

from typing import Dict, Any
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import func, and_
import structlog

from cms_pricing.models.nearest_zip import NearestZipTrace

logger = structlog.get_logger()


class NearestZipMonitoring:
    """Monitoring and alerting for nearest ZIP resolver"""
    
    def __init__(self, db_session: Session):
        self.db = db_session
    
    def get_gazetteer_fallback_rate(self, hours: int = 24) -> Dict[str, Any]:
        """
        Calculate Gazetteer fallback rate over specified hours
        
        Args:
            hours: Number of hours to look back (default 24)
        
        Returns:
            Dict with fallback rate and statistics
        """
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        # Count total resolutions in time period
        total_resolutions = self.db.query(func.count(NearestZipTrace.id)).filter(
            NearestZipTrace.created_at >= cutoff_time
        ).scalar() or 0
        
        # Count resolutions that used NBER fallback
        # This would need to be tracked in the trace data
        # For now, we'll estimate based on warning logs
        fallback_resolutions = 0  # TODO: Implement proper tracking
        
        fallback_rate = (fallback_resolutions / total_resolutions * 100) if total_resolutions > 0 else 0
        
        return {
            'total_resolutions': total_resolutions,
            'fallback_resolutions': fallback_resolutions,
            'fallback_rate_percent': round(fallback_rate, 4),
            'time_period_hours': hours,
            'threshold_percent': 0.1,
            'alert_triggered': fallback_rate > 0.1
        }
    
    def check_gazetteer_fallback_alert(self) -> Dict[str, Any]:
        """
        Check if Gazetteer fallback rate exceeds threshold
        
        Returns:
            Dict with alert status and details
        """
        stats = self.get_gazetteer_fallback_rate()
        
        if stats['alert_triggered']:
            logger.error(
                f"P1 ALERT: Gazetteer Fallback Rate {stats['fallback_rate_percent']:.4f}% "
                f"exceeds threshold of {stats['threshold_percent']}%",
                extra={
                    'alert_type': 'gazetteer_fallback_rate',
                    'severity': 'P1',
                    'fallback_rate': stats['fallback_rate_percent'],
                    'threshold': stats['threshold_percent'],
                    'total_resolutions': stats['total_resolutions'],
                    'fallback_resolutions': stats['fallback_resolutions']
                }
            )
        
        return stats
    
    def get_resolver_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive resolver statistics
        
        Returns:
            Dict with various resolver metrics
        """
        # Get basic counts
        total_resolutions = self.db.query(func.count(NearestZipTrace.id)).scalar() or 0
        
        # Count coincident resolutions (< 1.0 miles)
        coincident_resolutions = self.db.query(func.count(NearestZipTrace.id)).filter(
            NearestZipTrace.distance_miles < 1.0
        ).scalar() or 0
        
        # Count far neighbor resolutions (> 10.0 miles)
        far_neighbor_resolutions = self.db.query(func.count(NearestZipTrace.id)).filter(
            NearestZipTrace.distance_miles > 10.0
        ).scalar() or 0
        
        # Calculate percentages
        coincident_percentage = (coincident_resolutions / total_resolutions * 100) if total_resolutions > 0 else 0
        far_neighbor_percentage = (far_neighbor_resolutions / total_resolutions * 100) if total_resolutions > 0 else 0
        
        return {
            'total_resolutions': total_resolutions,
            'coincident_resolutions': coincident_resolutions,
            'far_neighbor_resolutions': far_neighbor_resolutions,
            'coincident_percentage': round(coincident_percentage, 2),
            'far_neighbor_percentage': round(far_neighbor_percentage, 2)
        }
    
    def get_data_source_counts(self) -> Dict[str, int]:
        """
        Get counts of records in each data source table
        
        Returns:
            Dict mapping table names to record counts
        """
        from cms_pricing.models.nearest_zip import (
            ZCTACoords, ZipToZCTA, CMSZipLocality, 
            ZipMetadata, NBERCentroids, ZCTADistances
        )
        
        return {
            'zcta_coords': self.db.query(func.count(ZCTACoords.zcta5)).scalar() or 0,
            'zip_to_zcta': self.db.query(func.count(ZipToZCTA.zip5)).scalar() or 0,
            'cms_zip_locality': self.db.query(func.count(CMSZipLocality.zip5)).scalar() or 0,
            'zip_metadata': self.db.query(func.count(ZipMetadata.zip5)).scalar() or 0,
            'nber_centroids': self.db.query(func.count(NBERCentroids.zcta5)).scalar() or 0,
            'zcta_distances': self.db.query(func.count(ZCTADistances.id)).scalar() or 0
        }
