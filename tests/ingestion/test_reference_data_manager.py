import json
from pathlib import Path
from datetime import date

from cms_pricing.ingestion.enrichers.dis_reference_data_integration import (
    ReferenceDataManager,
    ReferenceDataSource,
)


def test_reference_metadata_serialization(tmp_path):
    manager = ReferenceDataManager(output_dir=str(tmp_path))

    manager.register_reference_source(
        source_name="cms_gpci",
        source_type=ReferenceDataSource.CMS_OFFICIAL,
        version="1.0",
        effective_from=date(2025, 1, 1),
        effective_to=None,
        record_count=0,
        quality_score=0.99,
        confidence_level="high",
        coverage_scope="national",
    )

    metadata_path = tmp_path / "reference_metadata.json"
    assert metadata_path.exists()

    payload = json.loads(metadata_path.read_text())
    assert payload["cms_gpci"]["source_type"] == "cms_official"
    assert payload["cms_gpci"]["effective_from"] == "2025-01-01"
