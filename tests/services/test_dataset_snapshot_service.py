"""Tests for DatasetSnapshotService

Part of Quick Win #1: Dataset Snapshots Table
"""

import json
from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError, ProgrammingError

from cms_pricing.models.dataset_snapshots import DatasetSnapshot
from cms_pricing.services.dataset_snapshot_service import DatasetSnapshotService


def check_table_exists(db_session):
    """Check if dataset_snapshots table exists"""
    try:
        db_session.execute(text("SELECT 1 FROM dataset_snapshots LIMIT 1"))
        return True
    except (ProgrammingError, OperationalError) as e:
        if "does not exist" in str(e) or "relation" in str(e).lower():
            return False
        raise


@pytest.fixture(autouse=True)
def cleanup_snapshots(test_db_session):
    """Ensure dataset_snapshots table is empty between tests."""
    if not check_table_exists(test_db_session):
        yield
        return
    test_db_session.execute(text("TRUNCATE TABLE dataset_snapshots"))
    test_db_session.commit()
    yield


def test_select_snapshot_active(test_db_session):
    """Test selecting active snapshot by valuation date"""
    if not check_table_exists(test_db_session):
        pytest.skip("dataset_snapshots table not yet created. Run: alembic upgrade head")
    
    service = DatasetSnapshotService(test_db_session)
    
    # Create snapshots
    snapshot1 = DatasetSnapshot(
        dataset_id="MPFS",
        release_id="mpfs_2024_annual",
        digest="sha256:2024",
        effective_from=date(2024, 1, 1),
        effective_to=date(2024, 12, 31)
    )
    snapshot2 = DatasetSnapshot(
        dataset_id="MPFS",
        release_id="mpfs_2025_annual",
        digest="sha256:2025",
        effective_from=date(2025, 1, 1),
        effective_to=None  # Current
    )
    
    test_db_session.add_all([snapshot1, snapshot2])
    test_db_session.commit()
    
    # Select for 2025
    selected = service.select_snapshot("MPFS", date(2025, 6, 15))
    assert selected is not None
    assert selected.release_id == "mpfs_2025_annual"
    
    # Select for 2024
    selected = service.select_snapshot("MPFS", date(2024, 6, 15))
    assert selected is not None
    assert selected.release_id == "mpfs_2024_annual"


def test_select_snapshot_by_release_id(test_db_session):
    """Test selecting snapshot by specific release_id"""
    if not check_table_exists(test_db_session):
        pytest.skip("dataset_snapshots table not yet created. Run: alembic upgrade head")
    
    service = DatasetSnapshotService(test_db_session)
    
    snapshot = DatasetSnapshot(
        dataset_id="OPPS",
        release_id="opps_2025_q1",
        digest="sha256:q1",
        effective_from=date(2025, 1, 1),
        effective_to=date(2025, 3, 31)
    )
    
    test_db_session.add(snapshot)
    test_db_session.commit()
    
    # Select by release_id
    selected = service.select_snapshot("OPPS", release_id="opps_2025_q1")
    assert selected is not None
    assert selected.release_id == "opps_2025_q1"


def test_select_snapshot_not_found(test_db_session):
    """Test selecting snapshot that doesn't exist"""
    if not check_table_exists(test_db_session):
        pytest.skip("dataset_snapshots table not yet created. Run: alembic upgrade head")
    
    service = DatasetSnapshotService(test_db_session)
    
    selected = service.select_snapshot("UNKNOWN", date(2025, 1, 1))
    assert selected is None


def test_get_snapshot_by_release(test_db_session):
    """Test getting snapshot by exact dataset_id and release_id"""
    if not check_table_exists(test_db_session):
        pytest.skip("dataset_snapshots table not yet created. Run: alembic upgrade head")
    
    service = DatasetSnapshotService(test_db_session)
    
    snapshot = DatasetSnapshot(
        dataset_id="ASC",
        release_id="asc_2025_annual",
        digest="sha256:asc",
        effective_from=date(2025, 1, 1)
    )
    
    test_db_session.add(snapshot)
    test_db_session.commit()
    
    found = service.get_snapshot_by_release("ASC", "asc_2025_annual")
    assert found is not None
    assert found.release_id == "asc_2025_annual"
    
    not_found = service.get_snapshot_by_release("ASC", "unknown")
    assert not_found is None


def test_list_snapshots(test_db_session):
    """Test listing snapshots"""
    if not check_table_exists(test_db_session):
        pytest.skip("dataset_snapshots table not yet created. Run: alembic upgrade head")
    
    service = DatasetSnapshotService(test_db_session)
    
    snapshots = [
        DatasetSnapshot(
            dataset_id="MPFS",
            release_id=f"mpfs_2025_{i}",
            digest=f"sha256:{i}",
            effective_from=date(2025, 1, 1)
        )
        for i in range(3)
    ]
    
    test_db_session.add_all(snapshots)
    test_db_session.commit()
    
    listed = service.list_snapshots("MPFS", limit=10)
    assert len(listed) == 3
    
    # Test limit
    listed = service.list_snapshots("MPFS", limit=2)
    assert len(listed) == 2


def test_register_snapshot_new(test_db_session):
    """Test registering a new snapshot"""
    if not check_table_exists(test_db_session):
        pytest.skip("dataset_snapshots table not yet created. Run: alembic upgrade head")
    
    service = DatasetSnapshotService(test_db_session)
    
    snapshot = service.register_snapshot(
        dataset_id="CLFS",
        release_id="clfs_2025_annual",
        digest="sha256:new",
        effective_from=date(2025, 1, 1),
        effective_to=date(2025, 12, 31),
        manifest_url="https://example.com/manifest.json"
    )
    
    assert snapshot.dataset_id == "CLFS"
    assert snapshot.release_id == "clfs_2025_annual"
    assert snapshot.digest == "sha256:new"
    assert snapshot.manifest_url == "https://example.com/manifest.json"
    
    # Verify it was saved
    found = test_db_session.query(DatasetSnapshot).filter(
        DatasetSnapshot.dataset_id == "CLFS",
        DatasetSnapshot.release_id == "clfs_2025_annual"
    ).first()
    assert found is not None


def test_register_snapshot_update_existing(test_db_session):
    """Test that registering existing snapshot updates it"""
    if not check_table_exists(test_db_session):
        pytest.skip("dataset_snapshots table not yet created. Run: alembic upgrade head")
    
    service = DatasetSnapshotService(test_db_session)
    
    # Create initial snapshot
    snapshot1 = service.register_snapshot(
        dataset_id="DMEPOS",
        release_id="dmepos_2025_annual",
        digest="sha256:old",
        effective_from=date(2025, 1, 1)
    )
    
    # Register again with different digest
    snapshot2 = service.register_snapshot(
        dataset_id="DMEPOS",
        release_id="dmepos_2025_annual",
        digest="sha256:new",
        effective_from=date(2025, 1, 1),
        manifest_url="https://example.com/updated.json"
    )
    
    # Should be the same object (updated)
    assert snapshot1.release_id == snapshot2.release_id
    assert snapshot2.digest == "sha256:new"
    assert snapshot2.manifest_url == "https://example.com/updated.json"
    
    # Verify only one record exists
    count = test_db_session.query(DatasetSnapshot).filter(
        DatasetSnapshot.dataset_id == "DMEPOS",
        DatasetSnapshot.release_id == "dmepos_2025_annual"
    ).count()
    assert count == 1


def test_resolve_path_from_manifest_dataset_list(tmp_path, test_db_session):
    """Manifests with dataset lists should resolve parquet paths via alias mapping."""
    if not check_table_exists(test_db_session):
        pytest.skip("dataset_snapshots table not yet created. Run: alembic upgrade head")

    release_dir = tmp_path / "rvu_release"
    release_dir.mkdir()
    parquet_path = release_dir / "pprrvu_snapshot.parquet"
    parquet_path.write_text("")  # touch file for existence check

    manifest_payload = {
        "datasets": [
            {
                "name": "pprrvu",
                "parquet_path": parquet_path.name,  # relative path in manifest
            }
        ]
    }
    manifest_path = release_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest_payload))

    snapshot = DatasetSnapshot(
        dataset_id="rvu_items",
        release_id="rvu_2025_D",
        digest="sha256:test",
        effective_from=date(2025, 1, 1),
        manifest_url=str(manifest_path),
    )

    service = DatasetSnapshotService(test_db_session)
    try:
        resolved = service._resolve_curated_path(snapshot)  # pylint: disable=protected-access
    finally:
        service.close()

    assert resolved == str(parquet_path)


def test_resolve_prefers_manifest_path_even_when_missing(tmp_path, test_db_session):
    if not check_table_exists(test_db_session):
        pytest.skip("dataset_snapshots table not yet created. Run: alembic upgrade head")

    missing_path = tmp_path / "rvu_output" / "pprrvu_snapshot.parquet"
    snapshot = DatasetSnapshot(
        dataset_id="rvu_items",
        release_id="rvu_2025_D",
        digest="sha256:test",
        effective_from=date(2025, 1, 1),
        manifest_url=str(missing_path),
    )

    service = DatasetSnapshotService(test_db_session)
    try:
        resolved = service._resolve_curated_path(snapshot)  # pylint: disable=protected-access
    finally:
        service.close()

    assert resolved == str(missing_path)


def test_manifest_relative_entry_returns_missing_path(tmp_path, test_db_session):
    if not check_table_exists(test_db_session):
        pytest.skip("dataset_snapshots table not yet created. Run: alembic upgrade head")

    release_dir = tmp_path / "rvu_release"
    release_dir.mkdir()
    manifest_payload = {
        "datasets": [
            {
                "name": "pprrvu",
                "parquet_path": "missing_relative.parquet",
            }
        ]
    }
    manifest_path = release_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest_payload))

    expected_path = release_dir / "missing_relative.parquet"
    snapshot = DatasetSnapshot(
        dataset_id="rvu_items",
        release_id="rvu_2025_D",
        digest="sha256:test",
        effective_from=date(2025, 1, 1),
        manifest_url=str(manifest_path),
    )

    service = DatasetSnapshotService(test_db_session)
    try:
        resolved = service._resolve_curated_path(snapshot)  # pylint: disable=protected-access
    finally:
        service.close()

    assert resolved == str(expected_path)


def test_normalize_ingestion_path_handles_app_curated(test_db_session):
    if not check_table_exists(test_db_session):
        pytest.skip("dataset_snapshots table not yet created. Run: alembic upgrade head")

    normalized = DatasetSnapshotService._normalize_ingestion_path(
        Path("/app/data/curated/rvu/rvu_2025_D/pprrvu.parquet")
    )
    assert normalized == Path("data/curated/rvu/rvu_2025_D/pprrvu.parquet")
