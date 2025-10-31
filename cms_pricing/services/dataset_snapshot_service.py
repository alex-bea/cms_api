"""Service for selecting dataset snapshots

Part of Quick Win #1: Dataset Snapshots Table
"""

from typing import Optional, Dict, Any, List
from datetime import date
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
import structlog

from cms_pricing.models.dataset_snapshots import DatasetSnapshot

logger = structlog.get_logger()


class DatasetSnapshotService:
    """Service for selecting dataset snapshots by valuation date"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def select_snapshot(
        self,
        dataset_id: str,
        valuation_date: Optional[date] = None,
        release_id: Optional[str] = None
    ) -> Optional[DatasetSnapshot]:
        """
        Select active snapshot for dataset at valuation date.
        
        Args:
            dataset_id: Dataset identifier (e.g., 'MPFS', 'OPPS')
            valuation_date: Date for which to select snapshot (defaults to today)
            release_id: Optional specific release_id to select
            
        Returns:
            DatasetSnapshot if found, None otherwise
        """
        if valuation_date is None:
            valuation_date = date.today()
        
        query = self.db.query(DatasetSnapshot).filter(
            DatasetSnapshot.dataset_id == dataset_id
        )
        
        # If specific release_id requested, use it directly
        if release_id:
            query = query.filter(DatasetSnapshot.release_id == release_id)
        else:
            # Otherwise, select active snapshot at valuation date
            query = query.filter(
                and_(
                    DatasetSnapshot.effective_from <= valuation_date,
                    or_(
                        DatasetSnapshot.effective_to.is_(None),
                        DatasetSnapshot.effective_to >= valuation_date
                    )
                )
            )
        
        # Order by effective_from desc, then created_at desc to get latest
        snapshot = query.order_by(
            DatasetSnapshot.effective_from.desc(),
            DatasetSnapshot.created_at.desc()
        ).first()
        
        if snapshot:
            logger.debug(
                "Selected snapshot",
                dataset_id=dataset_id,
                release_id=snapshot.release_id,
                effective_from=snapshot.effective_from,
                valuation_date=valuation_date
            )
        
        return snapshot
    
    def get_snapshot_by_release(
        self,
        dataset_id: str,
        release_id: str
    ) -> Optional[DatasetSnapshot]:
        """Get snapshot by exact dataset_id and release_id (uses composite PK)"""
        return self.db.get(DatasetSnapshot, (dataset_id, release_id))
    
    def list_snapshots(
        self,
        dataset_id: Optional[str] = None,
        limit: int = 50
    ) -> List[DatasetSnapshot]:
        """List snapshots, optionally filtered by dataset_id"""
        query = self.db.query(DatasetSnapshot)
        
        if dataset_id:
            query = query.filter(DatasetSnapshot.dataset_id == dataset_id)
        
        return query.order_by(
            DatasetSnapshot.dataset_id,
            DatasetSnapshot.effective_from.desc()
        ).limit(limit).all()
    
    def register_snapshot(
        self,
        dataset_id: str,
        release_id: str,
        digest: str,
        effective_from: date,
        effective_to: Optional[date] = None,
        manifest_url: Optional[str] = None
    ) -> DatasetSnapshot:
        """
        Register a new dataset snapshot.
        
        Args:
            dataset_id: Dataset identifier
            release_id: Release identifier (must match fee schedule tables)
            digest: SHA256 digest of dataset content
            effective_from: Date when snapshot becomes effective
            effective_to: Optional expiration date
            manifest_url: Optional URL to dataset manifest
            
        Returns:
            Created DatasetSnapshot
        """
        # Check if already exists (efficient PK lookup)
        existing = self.db.get(DatasetSnapshot, (dataset_id, release_id))
        if existing:
            logger.warning(
                "Snapshot already exists, updating",
                dataset_id=dataset_id,
                release_id=release_id
            )
            existing.digest = digest
            existing.effective_from = effective_from
            existing.effective_to = effective_to
            existing.manifest_url = manifest_url
            self.db.commit()
            return existing
        
        snapshot = DatasetSnapshot(
            dataset_id=dataset_id,
            release_id=release_id,
            digest=digest,
            effective_from=effective_from,
            effective_to=effective_to,
            manifest_url=manifest_url
        )
        
        self.db.add(snapshot)
        self.db.commit()
        self.db.refresh(snapshot)
        
        logger.info(
            "Registered dataset snapshot",
            dataset_id=dataset_id,
            release_id=release_id,
            effective_from=effective_from
        )
        
        return snapshot

