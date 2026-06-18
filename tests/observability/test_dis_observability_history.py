import json
import uuid
from datetime import datetime, timedelta

from cms_pricing.ingestion.observability.dis_observability import (
    DISObservabilityCollector,
)


def _build_report(collector: DISObservabilityCollector, run_id: str, release_id: str):
    dataset_name = "test_dataset"
    now = datetime.utcnow()

    freshness = collector.collect_freshness_metrics(
        dataset_name=dataset_name,
        last_updated=now,
        expected_frequency_hours=24.0,
        previous_update=now - timedelta(hours=24),
    )

    volume = collector.collect_volume_metrics(
        total_records=100,
        total_size_bytes=1024,
        expected_records=100,
        expected_size_bytes=1024,
    )

    schema = collector.collect_schema_metrics(
        schema_version="v1",
        validation_results={"valid": True, "breaking_changes": 0, "non_breaking_changes": 0},
    )

    quality = collector.collect_quality_metrics(
        validation_results={
            "quality_score": 0.99,
            "rules_passed": 10,
            "rules_failed": 0,
            "metrics": {"null_rate": 0.0, "duplicate_rate": 0.0},
        },
        quality_threshold=0.95,
    )

    lineage = collector.collect_lineage_metrics(
        source_files=[{"url": "s3://bucket/file.csv", "sha256": "abc"}],
        transformation_steps=["Land", "Validate", "Normalize", "Publish"],
        processing_timestamp=now,
        ingest_run_id=run_id,
        batch_id=run_id,
        release_id=release_id,
    )

    return collector.generate_observability_report(
        dataset_name=dataset_name,
        freshness=freshness,
        volume=volume,
        schema=schema,
        quality=quality,
        lineage=lineage,
    )


def test_history_persists_across_restart(tmp_path):
    output_dir = tmp_path / "observability"
    collector = DISObservabilityCollector(str(output_dir))

    run_id = str(uuid.uuid4())
    release_id = "2025.01"
    report = _build_report(collector, run_id=run_id, release_id=release_id)

    history_file = output_dir / "observability_history.json"
    assert history_file.exists()

    with history_file.open() as fh:
        persisted_entries = json.load(fh)

    assert persisted_entries
    assert persisted_entries[-1]["run_id"] == run_id
    assert persisted_entries[-1]["dataset"] == report.dataset_name

    restarted_collector = DISObservabilityCollector(str(output_dir))
    latest = restarted_collector.get_latest_report(report.dataset_name)

    assert latest is not None
    assert latest.lineage.ingest_run_id == run_id
    assert latest.lineage.release_id == release_id

    summary = restarted_collector.get_observability_summary()
    assert summary["datasets"][report.dataset_name]["alert_severity"] == "normal"
    assert summary["datasets"][report.dataset_name]["block_run"] is False


def test_critical_alerts_block_and_persist(tmp_path):
    output_dir = tmp_path / "observability_critical"
    collector = DISObservabilityCollector(str(output_dir))

    run_id = "run-critical"
    release_id = "2025.02"

    now = datetime.utcnow()
    freshness = collector.collect_freshness_metrics(
        dataset_name="critical_dataset",
        last_updated=now - timedelta(hours=72),
        expected_frequency_hours=24.0,
        previous_update=now - timedelta(hours=24 * 4),
    )

    volume = collector.collect_volume_metrics(
        total_records=50,
        total_size_bytes=2048,
        expected_records=100,
        expected_size_bytes=4096,
    )

    schema = collector.collect_schema_metrics(
        schema_version="v2",
        validation_results={"valid": False, "breaking_changes": 1, "non_breaking_changes": 0},
    )

    quality = collector.collect_quality_metrics(
        validation_results={
            "quality_score": 0.4,
            "rules_passed": 2,
            "rules_failed": 8,
            "metrics": {"null_rate": 0.2, "duplicate_rate": 0.1},
        },
        quality_threshold=0.95,
    )

    lineage = collector.collect_lineage_metrics(
        source_files=[{"url": "s3://bucket/bad.csv", "sha256": "def"}],
        transformation_steps=["Land", "Validate", "Publish"],
        processing_timestamp=now,
        ingest_run_id=run_id,
        batch_id=run_id,
        release_id=release_id,
    )

    report = collector.generate_observability_report(
        dataset_name="critical_dataset",
        freshness=freshness,
        volume=volume,
        schema=schema,
        quality=quality,
        lineage=lineage,
    )

    assert report.alert_decision.severity == "critical"
    assert report.alert_decision.should_block_run is True
    assert collector.last_alert_decision is report.alert_decision

    history_file = output_dir / "observability_history.json"
    with history_file.open() as fh:
        entries = json.load(fh)

    assert entries[-1]["alert_decision"]["severity"] == "critical"
    assert entries[-1]["alert_decision"]["should_block_run"] is True

    restarted = DISObservabilityCollector(str(output_dir))
    latest = restarted.get_latest_report("critical_dataset")
    assert latest.alert_decision.severity == "critical"
    assert latest.alert_decision.should_block_run is True
