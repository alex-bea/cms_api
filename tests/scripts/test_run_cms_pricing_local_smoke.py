import json

from scripts import run_cms_pricing_local_smoke as smoke


def _config(tmp_path, *, dry_run_plan=True):
    return smoke.SmokeConfig(
        database_url="postgresql://cms_user:secret@localhost:5432/cms_pricing",
        report_json=tmp_path / "reports" / "smoke.json",
        geography_report_json=tmp_path / "reports" / "geography.json",
        rvu_report_json=tmp_path / "reports" / "rvu.json",
        output_dir=tmp_path / "rvu",
        start_year=2026,
        end_year=2026,
        release="latest",
        valuation_date="2026-07-01",
        python_executable=".venv/bin/python",
        dry_run_plan=dry_run_plan,
    )


def test_build_step_plan_uses_production_style_geography_and_rvu_load(tmp_path):
    steps = smoke.build_step_plan(_config(tmp_path))

    assert [step["name"] for step in steps] == [
        "geography_readiness_load",
        "rvu_local_load",
        "post_rvu_smoke_94110",
        "post_rvu_smoke_special_state_66012",
    ]

    geography_command = steps[0]["command"]
    assert "scripts/load_cms_geography_local.py" in geography_command
    assert "--production-readiness-gates" in geography_command
    assert "--open-ended-latest" in geography_command
    assert "--require-valuation-date-coverage" in geography_command

    rvu_command = steps[1]["command"]
    assert "scripts/load_latest_cms_rvu_local.py" in rvu_command
    assert rvu_command[rvu_command.index("--start-year") + 1] == "2026"
    assert rvu_command[rvu_command.index("--end-year") + 1] == "2026"
    assert rvu_command[rvu_command.index("--release") + 1] == "latest"


def test_build_step_plan_smokes_standard_and_special_state_zips(tmp_path):
    steps = smoke.build_step_plan(_config(tmp_path))
    standard_smoke = steps[2]["command"]
    special_state_smoke = steps[3]["command"]

    assert "scripts/post_rvu_load_api_smoke.py" in standard_smoke
    assert standard_smoke[standard_smoke.index("--proof-path") + 1] == (
        "production_style_local_smoke"
    )
    assert "--zip" not in standard_smoke

    assert special_state_smoke[special_state_smoke.index("--zip") + 1] == "66012"
    assert special_state_smoke[special_state_smoke.index("--expected-state") + 1] == "EK"
    assert (
        special_state_smoke[special_state_smoke.index("--expected-locality") + 1]
        == "00"
    )
    assert (
        special_state_smoke[special_state_smoke.index("--expected-carrier") + 1]
        == "05202"
    )
    assert special_state_smoke[special_state_smoke.index("--proof-path") + 1] == (
        "production_style_local_smoke"
    )


def test_safe_command_masks_database_password(tmp_path):
    command = smoke.build_step_plan(_config(tmp_path))[0]["command"]

    masked = smoke.safe_command(command)

    assert "secret" not in " ".join(masked)
    assert "postgresql://cms_user:***@localhost:5432/cms_pricing" in masked


def test_dry_run_plan_writes_consolidated_report_without_running_steps(tmp_path):
    config = _config(tmp_path)

    report = smoke.run(config)

    assert report["status"] == "planned"
    assert report["database_url"] == (
        "postgresql://cms_user:***@localhost:5432/cms_pricing"
    )
    assert report["proof_path"] == "production_style_local_smoke"
    assert [step["name"] for step in report["steps"]] == [
        "geography_readiness_load",
        "rvu_local_load",
        "post_rvu_smoke_94110",
        "post_rvu_smoke_special_state_66012",
    ]
    assert config.report_json.exists()
    saved_report = json.loads(config.report_json.read_text(encoding="utf-8"))
    assert saved_report["status"] == "planned"
    assert "secret" not in config.report_json.read_text(encoding="utf-8")
