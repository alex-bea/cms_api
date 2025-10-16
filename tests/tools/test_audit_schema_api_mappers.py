from pathlib import Path

import json
import sys
import types

import tools.audit_schema_api_mappers as audit_mappers


def test_audit_passes_with_real_mapping(tmp_path, monkeypatch):
    errors = audit_mappers.audit_schema_api_mappers()
    assert errors == []


def test_audit_detects_missing_column(tmp_path, monkeypatch):
    # Create temporary schema file directory
    contracts_dir = tmp_path / "contracts"
    contracts_dir.mkdir()
    schema_path = contracts_dir / "cms_sample_v1.0.json"
    schema_path.write_text(json.dumps({"columns": {"schema_col": {}}}), encoding="utf-8")

    # Create fake module
    module = types.SimpleNamespace(SAMPLE_SCHEMA_TO_API={"missing_col": "api_col"})

    monkeypatch.setitem(sys.modules, "fake_mappers", module)

    errors = audit_mappers.audit_schema_api_mappers(
        contracts_dir=contracts_dir, mappers_module_name="fake_mappers"
    )
    assert len(errors) == 1
    assert "missing_col" in errors[0]
