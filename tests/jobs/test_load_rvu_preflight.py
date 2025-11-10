from cms_pricing.ingestion.jobs import load_rvu_preflight


def test_run_preflight_invokes_rvu_ingestor(tmp_path, monkeypatch):
    calls = {}

    class FakeIngestor:
        def __init__(self, output_dir, db_session, enable_snapshot_registration):
            calls["output_dir"] = output_dir
            calls["db_session"] = db_session
            calls["enable_snapshot_registration"] = enable_snapshot_registration

        async def ingest(self, release_id, batch_id):
            calls["release_id"] = release_id
            calls["batch_id"] = batch_id
            return {"status": "success", "total_records": 1}

    monkeypatch.setattr(load_rvu_preflight, "RVUIngestor", FakeIngestor)

    rc = load_rvu_preflight.run_preflight("rvu_2025_D", tmp_path.as_posix())

    assert rc == 0
    assert calls["output_dir"] == tmp_path.as_posix()
    assert calls["db_session"] is None
    assert calls["enable_snapshot_registration"] is False
    assert calls["release_id"] == "rvu_2025_D"
    assert calls["batch_id"].startswith("preflight_")
