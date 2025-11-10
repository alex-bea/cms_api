"""Tests for the snapshot repair CLI helpers."""

import json
from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from cms_pricing.models.dataset_snapshots import DatasetSnapshot
from cms_pricing.ops import repair_snapshot_paths


@pytest.fixture()
def snapshot_session_factory(test_engine):
    """Provide a session factory targeting the dataset_snapshots table."""
    Session = sessionmaker(bind=test_engine)

    # Ensure table exists; skip if migrations haven't run
    probe = Session()
    try:
        try:
            probe.execute(text("SELECT 1 FROM dataset_snapshots LIMIT 1"))
        except Exception as exc:  # pragma: no cover - defensive skip
            pytest.skip(f"dataset_snapshots table unavailable: {exc}")
    finally:
        probe.close()

    # Clean table before yielding factory
    cleanup = Session()
    cleanup.execute(text("DELETE FROM dataset_snapshots"))
    cleanup.commit()
    cleanup.close()

    def factory():
        return Session()

    yield factory

    cleanup = Session()
    cleanup.execute(text("DELETE FROM dataset_snapshots"))
    cleanup.commit()
    cleanup.close()


def _insert_snapshot(session, *, dataset_id: str, manifest_url: str) -> None:
    snapshot = DatasetSnapshot(
        dataset_id=dataset_id,
        release_id="rvu_2025_D",
        digest="sha256:test",
        effective_from=date(2025, 1, 1),
        manifest_url=manifest_url,
    )
    session.merge(snapshot)
    session.commit()
    session.close()


def test_repair_updates_manifest_from_manifest_json(tmp_path, monkeypatch, snapshot_session_factory):
    monkeypatch.setattr(repair_snapshot_paths, "SessionLocal", snapshot_session_factory)

    repo_root = tmp_path
    parquet_path = repo_root / "data" / "ingestion" / "rvu" / "curated" / "cms_rvu" / "2025-11-10" / "data" / "pprrvu_snapshot.parquet"
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    parquet_path.write_text("placeholder")

    manifest_payload = {
        "datasets": [
            {
                "name": "pprrvu",
                "parquet_path": "data/ingestion/rvu/curated/cms_rvu/2025-11-10/data/pprrvu_snapshot.parquet",
            }
        ]
    }
    manifest_path = repo_root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest_payload))
    monkeypatch.chdir(repo_root)

    session = snapshot_session_factory()
    _insert_snapshot(
        session,
        dataset_id="rvu_items",
        manifest_url=str(manifest_path),
    )

    backup_path = tmp_path / "backup.csv"
    rc = repair_snapshot_paths.audit_and_repair(
        dataset_id="rvu_items",
        release_id=None,
        limit=None,
        dry_run=False,
        confirm=True,
        backup_path=backup_path,
    )
    assert rc == 0
    assert backup_path.exists()

    verify_session = snapshot_session_factory()
    stored = verify_session.get(DatasetSnapshot, ("rvu_items", "rvu_2025_D"))
    assert stored is not None
    assert stored.manifest_url == "data/ingestion/rvu/curated/cms_rvu/2025-11-10/data/pprrvu_snapshot.parquet"
    verify_session.close()


def test_repair_dedupes_repo_relative_paths(tmp_path, monkeypatch, snapshot_session_factory):
    monkeypatch.setattr(repair_snapshot_paths, "SessionLocal", snapshot_session_factory)

    repo_root = tmp_path
    data_dir = repo_root / "data" / "ingestion" / "rvu"
    data_dir.mkdir(parents=True)
    parquet_path = data_dir / "pprrvu.parquet"
    parquet_path.write_text("placeholder")

    monkeypatch.chdir(repo_root)

    duplicated_path = "data/ingestion/rvu/data/ingestion/rvu/pprrvu.parquet"

    session = snapshot_session_factory()
    _insert_snapshot(
        session,
        dataset_id="rvu_items",
        manifest_url=duplicated_path,
    )

    rc = repair_snapshot_paths.audit_and_repair(
        dataset_id="rvu_items",
        release_id=None,
        limit=None,
        dry_run=False,
        confirm=True,
        backup_path=repo_root / "backup.csv",
    )
    assert rc == 0

    verify_session = snapshot_session_factory()
    stored = verify_session.get(DatasetSnapshot, ("rvu_items", "rvu_2025_D"))
    assert stored is not None
    assert stored.manifest_url == "data/ingestion/rvu/pprrvu.parquet"
    verify_session.close()
