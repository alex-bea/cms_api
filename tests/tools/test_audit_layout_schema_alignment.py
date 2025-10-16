import json
import sys
import types
from pathlib import Path

import tools.audit_layout_schema_alignment as audit_layouts


def test_audit_passes_with_real_layouts():
    errors = audit_layouts.audit_layout_schema_alignment()
    assert errors == []


def test_audit_detects_missing_columns(tmp_path, monkeypatch):
    contracts_dir = tmp_path / "contracts"
    contracts_dir.mkdir()
    schema = {
        "columns": {"col_a": {}, "col_b": {}},
        "natural_keys": ["col_a"],
    }
    schema_path = contracts_dir / "cms_sample_v1.0.json"
    schema_path.write_text(json.dumps(schema), encoding="utf-8")

    layout = {
        "columns": {
            "col_a": {"start": 0, "end": 5},
            "extra": {"start": 6, "end": 10},
        },
    }
    layout_module = types.SimpleNamespace(SAMPLE_LAYOUT=layout)
    monkeypatch.setitem(sys.modules, "fake_layouts", layout_module)

    errors = audit_layouts.audit_layout_schema_alignment(
        contracts_dir=contracts_dir,
        layout_module_name="fake_layouts",
    )

    assert len(errors) == 2
    assert "missing schema columns" in errors[0].lower()
    assert "not defined in schema" in errors[1].lower()
