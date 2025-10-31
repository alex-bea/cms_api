"""Tests for DatasetSnapshot model

Part of Quick Win #1: Dataset Snapshots Table
"""

import pytest
from datetime import date, datetime
from sqlalchemy.exc import IntegrityError, ProgrammingError, OperationalError
from sqlalchemy import text

from cms_pricing.models.dataset_snapshots import DatasetSnapshot


def check_table_exists(db_session):
    """Check if dataset_snapshots table exists"""
    try:
        db_session.execute(text("SELECT 1 FROM dataset_snapshots LIMIT 1"))
        return True
    except (ProgrammingError, OperationalError) as e:
        if "does not exist" in str(e) or "relation" in str(e).lower():
            return False
        raise


def test_dataset_snapshot_model_creation(test_db_session):
    """Test creating a DatasetSnapshot"""
    if not check_table_exists(test_db_session):
        pytest.skip("dataset_snapshots table not yet created. Run: alembic upgrade head")
    
    snapshot = DatasetSnapshot(
        dataset_id="MPFS",
        release_id="mpfs_2025_annual_20250115",
        digest="sha256:abc123",
        effective_from=date(2025, 1, 1),
        effective_to=None,
        manifest_url="https://example.com/manifest.json"
    )
    
    test_db_session.add(snapshot)
    test_db_session.commit()
    test_db_session.refresh(snapshot)
    
    assert snapshot.dataset_id == "MPFS"
    assert snapshot.release_id == "mpfs_2025_annual_20250115"
    assert snapshot.digest == "sha256:abc123"
    assert snapshot.effective_from == date(2025, 1, 1)
    assert snapshot.effective_to is None
    assert snapshot.manifest_url == "https://example.com/manifest.json"
    assert snapshot.created_at is not None


def test_dataset_snapshot_composite_primary_key(test_db_session):
    """Test that (dataset_id, release_id) is unique"""
    if not check_table_exists(test_db_session):
        pytest.skip("dataset_snapshots table not yet created. Run: alembic upgrade head")
    
    snapshot1 = DatasetSnapshot(
        dataset_id="MPFS",
        release_id="mpfs_2025_annual_20250115",
        digest="sha256:abc123",
        effective_from=date(2025, 1, 1)
    )
    
    test_db_session.add(snapshot1)
    test_db_session.commit()
    
    # Try to create duplicate
    snapshot2 = DatasetSnapshot(
        dataset_id="MPFS",
        release_id="mpfs_2025_annual_20250115",
        digest="sha256:def456",
        effective_from=date(2025, 1, 1)
    )
    
    test_db_session.add(snapshot2)
    
    with pytest.raises(IntegrityError):
        test_db_session.commit()


def test_dataset_snapshot_to_dict(test_db_session):
    """Test to_dict() method"""
    if not check_table_exists(test_db_session):
        pytest.skip("dataset_snapshots table not yet created. Run: alembic upgrade head")
    
    snapshot = DatasetSnapshot(
        dataset_id="OPPS",
        release_id="opps_2025_q1_20250115",
        digest="sha256:xyz789",
        effective_from=date(2025, 1, 1),
        effective_to=date(2025, 3, 31),
        manifest_url="https://example.com/opps.json"
    )
    
    test_db_session.add(snapshot)
    test_db_session.commit()
    
    result = snapshot.to_dict()
    
    assert result["dataset_id"] == "OPPS"
    assert result["release_id"] == "opps_2025_q1_20250115"
    assert result["digest"] == "sha256:xyz789"
    assert result["effective_from"] == "2025-01-01"
    assert result["effective_to"] == "2025-03-31"
    assert result["manifest_url"] == "https://example.com/opps.json"
    assert "created_at" in result


def test_dataset_snapshot_is_active(test_db_session):
    """Test is_active() method"""
    snapshot = DatasetSnapshot(
        dataset_id="ASC",
        release_id="asc_2025_annual_20250115",
        digest="sha256:active123",
        effective_from=date(2025, 1, 1),
        effective_to=None  # No expiration
    )
    
    # Active for current date
    assert snapshot.is_active(date(2025, 6, 15)) is True
    
    # Active for date before effective_from
    assert snapshot.is_active(date(2024, 12, 31)) is False
    
    # Test with expiration
    snapshot.effective_to = date(2025, 12, 31)
    assert snapshot.is_active(date(2025, 6, 15)) is True
    assert snapshot.is_active(date(2026, 1, 1)) is False


def test_dataset_snapshot_defaults(test_db_session):
    """Test that created_at has default"""
    if not check_table_exists(test_db_session):
        pytest.skip("dataset_snapshots table not yet created. Run: alembic upgrade head")
    
    snapshot = DatasetSnapshot(
        dataset_id="CLFS",
        release_id="clfs_2025_annual_20250115",
        digest="sha256:default123",
        effective_from=date(2025, 1, 1)
    )
    
    # Don't set created_at explicitly
    test_db_session.add(snapshot)
    test_db_session.commit()
    test_db_session.refresh(snapshot)
    
    assert snapshot.created_at is not None
    assert isinstance(snapshot.created_at, datetime)

