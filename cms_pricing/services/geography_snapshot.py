"""Snapshot and digest management service for geography data reproducibility"""

import hashlib
import json
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func
import structlog

from cms_pricing.models.geography import Geography
from cms_pricing.models.snapshots import Snapshot

logger = structlog.get_logger()


class GeographySnapshotService:
    """Service for managing geography data snapshots and digest pinning"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def create_snapshot(
        self,
        snapshot_name: str,
        description: Optional[str] = None,
        tags: Optional[List[str]] = None
    ) -> Snapshot:
        """
        Create a new snapshot of current geography data
        
        Args:
            snapshot_name: Unique name for the snapshot
            description: Optional description
            tags: Optional tags for categorization
            
        Returns:
            Created snapshot record
        """
        
        # Calculate digest of current geography data
        geography_digest = self._calculate_geography_digest()
        
        # Get current dataset statistics
        stats = self._get_geography_stats()
        
        # Create snapshot record
        snapshot = Snapshot(
            dataset_id="geography",
            effective_from=date.fromisoformat(stats["effective_date_range"]["earliest_from"]),
            effective_to=date.fromisoformat(stats["effective_date_range"]["latest_to"]),
            digest=geography_digest,
            source_url=f"geography_snapshot_{snapshot_name}",
            manifest_json=json.dumps({
                "snapshot_name": snapshot_name,
                "description": description,
                "tags": tags or [],
                "row_count": stats["total_rows"],
                "unique_zips": stats["unique_zips"],
                "unique_states": stats["unique_states"],
                "effective_date_range": stats["effective_date_range"]
            }),
            created_at=datetime.utcnow().date()
        )
        
        self.db.add(snapshot)
        self.db.commit()
        
        logger.info(
            "Geography snapshot created",
            snapshot_name=snapshot_name,
            digest=geography_digest,
            row_count=stats["total_rows"]
        )
        
        return snapshot
    
    def pin_digest(
        self,
        digest: str,
        pin_name: Optional[str] = None,
        description: Optional[str] = None
    ) -> Snapshot:
        """
        Pin a specific dataset digest for reproducibility
        
        Args:
            digest: Dataset digest to pin
            pin_name: Optional name for the pin
            description: Optional description
            
        Returns:
            Snapshot record for the pinned digest
        """
        
        # Verify digest exists in geography data
        if not self._verify_digest_exists(digest):
            raise ValueError(f"Digest {digest} not found in geography data")
        
        # Get stats for the specific digest
        stats = self._get_geography_stats(digest=digest)
        
        # Create pin snapshot
        pin_name = pin_name or f"pin_{digest[:8]}"
        snapshot = Snapshot(
            dataset_id="geography",
            effective_from=date.fromisoformat(stats["effective_date_range"]["earliest_from"]),
            effective_to=date.fromisoformat(stats["effective_date_range"]["latest_to"]),
            digest=digest,
            source_url=f"geography_pin_{pin_name}",
            manifest_json=json.dumps({
                "snapshot_name": pin_name,
                "description": description or f"Pinned digest {digest}",
                "tags": ["pinned", "reproducibility"],
                "row_count": stats["total_rows"],
                "unique_zips": stats["unique_zips"],
                "unique_states": stats["unique_states"],
                "effective_date_range": stats["effective_date_range"]
            }),
            created_at=datetime.utcnow().date()
        )
        
        self.db.add(snapshot)
        self.db.commit()
        
        logger.info(
            "Dataset digest pinned",
            digest=digest,
            pin_name=pin_name,
            row_count=stats["total_rows"]
        )
        
        return snapshot
    
    def get_snapshot(self, snapshot_name: str) -> Optional[Snapshot]:
        """Get snapshot by name (stored in manifest_json)"""
        snapshots = self.db.query(Snapshot).filter(
            Snapshot.dataset_id == "geography"
        ).all()
        
        for snapshot in snapshots:
            manifest = json.loads(snapshot.manifest_json) if snapshot.manifest_json else {}
            if manifest.get("snapshot_name") == snapshot_name:
                return snapshot
        
        return None
    
    def list_snapshots(
        self,
        tags: Optional[List[str]] = None,
        limit: int = 50
    ) -> List[Snapshot]:
        """List snapshots with optional tag filtering"""
        
        snapshots = self.db.query(Snapshot).filter(
            Snapshot.dataset_id == "geography"
        ).order_by(Snapshot.created_at.desc()).all()
        
        # Filter by tags if provided
        if tags:
            filtered_snapshots = []
            for snapshot in snapshots:
                manifest = json.loads(snapshot.manifest_json) if snapshot.manifest_json else {}
                snapshot_tags = manifest.get("tags", [])
                if any(tag in snapshot_tags for tag in tags):
                    filtered_snapshots.append(snapshot)
            snapshots = filtered_snapshots
        
        return snapshots[:limit]
    
    def verify_reproducibility(
        self,
        snapshot_name: str,
        test_zips: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Verify reproducibility of a snapshot by testing resolution consistency
        
        Args:
            snapshot_name: Name of snapshot to verify
            test_zips: Optional list of ZIP codes to test
            
        Returns:
            Reproducibility test results
        """
        
        snapshot = self.get_snapshot(snapshot_name)
        if not snapshot:
            raise ValueError(f"Snapshot {snapshot_name} not found")
        
        # Get current digest
        current_digest = self._calculate_geography_digest()
        
        # Get snapshot digest before async operations
        snapshot_digest = snapshot.digest
        
        # Check if current data matches snapshot
        digest_match = current_digest == snapshot_digest
        
        # Test resolution consistency if digest matches
        resolution_tests = []
        if digest_match and test_zips:
            resolution_tests = self._test_resolution_consistency(
                snapshot_digest, test_zips
            )
        
        # Calculate reproducibility score
        reproducibility_score = self._calculate_reproducibility_score(
            digest_match, resolution_tests
        )
        
        result = {
            "snapshot_name": snapshot_name,
            "snapshot_digest": snapshot_digest,
            "current_digest": current_digest,
            "digest_match": digest_match,
            "reproducibility_score": reproducibility_score,
            "resolution_tests": resolution_tests,
            "verified_at": datetime.utcnow().isoformat()
        }
        
        logger.info(
            "Reproducibility verification completed",
            snapshot_name=snapshot_name,
            digest_match=digest_match,
            reproducibility_score=reproducibility_score
        )
        
        return result
    
    def _calculate_geography_digest(self) -> str:
        """Calculate SHA256 digest of current geography data"""
        
        # Get all geography records ordered by zip5, plus4, effective_from
        records = self.db.query(Geography).order_by(
            Geography.zip5,
            Geography.plus4,
            Geography.effective_from
        ).all()
        
        # Create deterministic string representation
        data_strings = []
        for record in records:
            data_strings.append(
                f"{record.zip5}|{record.plus4}|{record.has_plus4}|"
                f"{record.state}|{record.locality_id}|{record.rural_flag}|"
                f"{record.effective_from}|{record.effective_to}|"
                f"{record.dataset_digest}"
            )
        
        # Calculate SHA256 hash
        combined_data = "\n".join(data_strings)
        return hashlib.sha256(combined_data.encode('utf-8')).hexdigest()
    
    def _verify_digest_exists(self, digest: str) -> bool:
        """Verify that a digest exists in geography data"""
        
        # For geography data, we verify by checking if the current digest matches
        # or if we have a snapshot with this digest
        current_digest = self._calculate_geography_digest()
        if current_digest == digest:
            return True
        
        # Check if we have a snapshot with this digest
        snapshot_exists = self.db.query(Snapshot).filter(
            Snapshot.dataset_id == "geography",
            Snapshot.digest == digest
        ).count() > 0
        
        return snapshot_exists
    
    def _get_geography_stats(self, digest: Optional[str] = None) -> Dict[str, Any]:
        """Get statistics for geography data, optionally filtered by digest"""
        
        query = self.db.query(Geography)
        if digest:
            query = query.filter(Geography.dataset_digest == digest)
        
        # Basic counts
        total_rows = query.count()
        
        # Unique counts
        unique_zips = query.with_entities(Geography.zip5).distinct().count()
        unique_states = query.with_entities(Geography.state).distinct().count()
        
        # Effective date range
        date_range = query.with_entities(
            func.min(Geography.effective_from),
            func.max(Geography.effective_to)
        ).first()
        
        # Handle case where effective dates might be null
        earliest_from = date_range[0] if date_range[0] else date(2024, 1, 1)
        latest_to = date_range[1] if date_range[1] else date(2024, 12, 31)
        
        effective_date_range = {
            "earliest_from": earliest_from.isoformat(),
            "latest_to": latest_to.isoformat()
        }
        
        return {
            "total_rows": total_rows,
            "unique_zips": unique_zips,
            "unique_states": unique_states,
            "effective_date_range": effective_date_range
        }
    
    def _test_resolution_consistency(
        self,
        digest: str,
        test_zips: List[str]
    ) -> List[Dict[str, Any]]:
        """Test resolution consistency for specific ZIP codes"""
        
        from cms_pricing.services.geography import GeographyService
        import asyncio
        
        geography_service = GeographyService(self.db)
        
        async def test_resolution(zip5: str):
            try:
                # Test resolution with pinned digest
                result = await geography_service.resolve_zip(
                    zip5=zip5,
                    valuation_year=2025,
                    strict=False
                )
                
                return {
                    "zip5": zip5,
                    "success": True,
                    "locality_id": result.get("locality_id") if result else None,
                    "match_level": result.get("match_level") if result else None,
                    "dataset_digest": result.get("dataset_digest") if result else None,
                    "digest_match": result.get("dataset_digest") == digest if result else False
                }
                
            except Exception as e:
                return {
                    "zip5": zip5,
                    "success": False,
                    "error": str(e),
                    "digest_match": False
                }
        
        # Run all tests
        async def run_all_tests():
            tasks = [test_resolution(zip5) for zip5 in test_zips]
            return await asyncio.gather(*tasks)
        
        # Execute async tests
        test_results = asyncio.run(run_all_tests())
        
        return test_results
    
    def _calculate_reproducibility_score(
        self,
        digest_match: bool,
        resolution_tests: List[Dict[str, Any]]
    ) -> float:
        """Calculate reproducibility score (0.0 to 1.0)"""
        
        if not digest_match:
            return 0.0
        
        if not resolution_tests:
            return 1.0
        
        # Calculate score based on successful resolutions and digest matches
        successful_tests = [t for t in resolution_tests if t.get("success", False)]
        digest_matches = [t for t in successful_tests if t.get("digest_match", False)]
        
        if not successful_tests:
            return 0.0
        
        # Score is percentage of successful tests that match the expected digest
        score = len(digest_matches) / len(successful_tests)
        
        return round(score, 3)
    
    def cleanup_old_snapshots(self, days_to_keep: int = 30) -> int:
        """Clean up old snapshots, keeping only recent ones"""
        
        cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
        
        old_snapshots = self.db.query(Snapshot).filter(
            Snapshot.dataset_type == "geography",
            Snapshot.created_at < cutoff_date,
            ~Snapshot.tags.contains(["pinned"])  # Don't delete pinned snapshots
        ).all()
        
        count = len(old_snapshots)
        for snapshot in old_snapshots:
            self.db.delete(snapshot)
        
        self.db.commit()
        
        logger.info(
            "Cleaned up old snapshots",
            deleted_count=count,
            days_to_keep=days_to_keep
        )
        
        return count
