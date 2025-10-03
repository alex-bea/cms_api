import yaml
from pathlib import Path

def test_openapi_file_exists():
    p = Path("api-contracts/openapi.yaml")
    assert p.exists(), "OpenAPI contract is missing"

def test_required_sections_present():
    spec = yaml.safe_load(Path("api-contracts/openapi.yaml").read_text())
    assert "openapi" in spec and "info" in spec and "paths" in spec
    assert "components" in spec and "schemas" in spec["components"]
