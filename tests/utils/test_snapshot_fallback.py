from pathlib import Path

from cms_pricing.utils import snapshot_fallback


def test_resolve_repo_path_strips_repo_prefix(tmp_path):
    curated_root = tmp_path / "curated"
    target = curated_root / "cms_rvu" / "2025-11-06" / "data"
    target.mkdir(parents=True)
    parquet = target / "pprrvu_2025-11-06.parquet"
    parquet.write_text("content")

    repo_path = Path("data/ingestion/rvu/curated/cms_rvu/2025-11-06/data/pprrvu_2025-11-06.parquet")

    resolved = snapshot_fallback.resolve_repo_path(
        repo_path,
        [curated_root],
        dataset_hint="cms_rvu",
    )

    assert resolved == parquet
