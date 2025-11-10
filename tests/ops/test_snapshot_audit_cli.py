"""Tests for the snapshot audit CLI helpers."""

from datetime import date

import pytest
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from cms_pricing.models.dataset_snapshots import DatasetSnapshot
from cms_pricing.ops.audit_snapshot_paths import audit_snapshots


@pytest.fixture()
def snapshot_session_factory(test_engine):
    """Provide a session factory targeting the dataset_snapshots table."""
    Session = sessionmaker(bind=test_engine)

    # Ensure table exists; skip if migrations haven't run
    test_session = Session()
    try:
        try:
            test_session.execute(text("SELECT 1 FROM dataset_snapshots LIMIT 1"))
        except Exception as exc:  # pragma: no cover - defensive skip
            pytest.skip(f"dataset_snapshots table unavailable: {exc}")
        finally:
            test_session.close()

        cleanup = Session()
        cleanup.execute(text("DELETE FROM dataset_snapshots"))
        cleanup.commit()
        cleanup.close()

        def factory():
            return Session()

        yield factory
    finally:
        cleanup = Session()
        cleanup.execute(text("DELETE FROM dataset_snapshots"))
        cleanup.commit()
        cleanup.close()


def _insert_snapshot(session, dataset_id: str, manifest_url: str) -> None:
    snapshot = DatasetSnapshot(
        dataset_id=dataset_id,
        release_id="rvu_2025_D",
        digest="sha256:test",
        effective_from=date(2025, 11, 10),
        manifest_url=manifest_url,
    )
    session.merge(snapshot)
    session.commit()
    session.close()


def test_audit_reports_missing_when_parquet_absent(tmp_path, snapshot_session_factory, capsys):
    manifest = tmp_path / "missing.parquet"
    session = snapshot_session_factory()
    _insert_snapshot(session, "rvu_items", str(manifest))

    rc = audit_snapshots("rvu_items", limit=10, show_all=True, session_factory=snapshot_session_factory)
    output = capsys.readouterr().out

    assert rc == 1
    assert "missing_target" in output


def test_audit_reports_ok_when_parquet_present(tmp_path, snapshot_session_factory, capsys):
    manifest = tmp_path / "present.parquet"
    manifest.write_text("placeholder")
    session = snapshot_session_factory()
    _insert_snapshot(session, "rvu_items", str(manifest))

    rc = audit_snapshots("rvu_items", limit=10, show_all=True, session_factory=snapshot_session_factory)
    output = capsys.readouterr().out

    assert rc == 0
    assert "status=ok" in output
