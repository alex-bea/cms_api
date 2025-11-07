import json
from pathlib import Path

import pandas as pd

from cms_pricing.ingestion.ingestors.mpfs_ingestor import MPFSIngestor


class _DummySnapshotService:
    def close(self) -> None:
        pass


def test_mpfs_load_snapshot_dataframe_from_manifest(tmp_path):
    # Prepare a small parquet file for rvu_items
    df = pd.DataFrame([
        {"hcpcs_code": "A1000", "work_rvu": 1.0}
    ])
    parquet_path = tmp_path / "rvu_items.parquet"
    df.to_parquet(parquet_path, index=False)

    # Create a manifest with datasets mapping
    manifest = {
        "datasets": {
            "rvu_items": {
                "parquet_path": str(parquet_path)
            }
        }
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))

    # Initialize ingestor with a dummy snapshot service to avoid DB usage
    ingestor = MPFSIngestor(output_dir=str(tmp_path), snapshot_service=_DummySnapshotService())

    # Should resolve the parquet path via manifest and load successfully
    loaded = ingestor._load_snapshot_dataframe("rvu_items", {"path": str(manifest_path)})
    assert len(loaded) == 1
    assert list(loaded.columns) == ["hcpcs_code", "work_rvu"]

