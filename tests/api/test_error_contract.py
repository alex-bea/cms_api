from pathlib import Path

import yaml
from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator

from cms_pricing.main import app

SPEC = yaml.safe_load(Path("api-contracts/openapi.yaml").read_text())
ERROR_SCHEMA = "#/components/schemas/Error"


def schema(ref):
    parts = ref.replace("#/", "").split("/")
    cur = SPEC
    for p in parts:
        cur = cur[p]
    return cur


def validate(instance, schema_obj):
    Draft202012Validator(schema_obj).validate(instance)


def test_error_schema_minimal_fields():
    err = {"code": "VALIDATION_FAILED", "message": "Bad input", "trace_id": "abc123"}
    validate(err, schema(ERROR_SCHEMA))


def test_error_responses_exist():
    responses = SPEC["components"]["responses"]
    for key in [
        "BadRequest",
        "Unauthorized",
        "PaymentRequired",
        "NotFound",
        "TooManyRequests",
    ]:
        assert key in responses, f"Missing {key}"


def test_paths_reference_canonical_errors():
    for path, item in SPEC["paths"].items():
        for method, op in item.items():
            if method.lower() not in (
                "get",
                "post",
                "put",
                "patch",
                "delete",
                "options",
                "head",
            ):
                continue
            resps = op.get("responses", {})
            for status, body in resps.items():
                if status.startswith("4") or status.startswith("5"):
                    # ensure it's a $ref to components.responses.*
                    assert (
                        "$ref" in body
                    ), f"{path} {method} {status} must $ref canonical response"
                    assert body["$ref"].startswith("#/components/responses/")


def test_missing_api_key_response_matches_error_schema():
    response = TestClient(app).get("/pricing/estimate?hcpcs_code=99213&zip_code=94110")

    assert response.status_code == 401
    validate(response.json(), schema(ERROR_SCHEMA))


def test_routing_404_response_matches_error_schema():
    response = TestClient(app).get(
        "/__contract_missing_route__",
        headers={"X-API-Key": "dev-key-123"},
    )

    assert response.status_code == 404
    validate(response.json(), schema(ERROR_SCHEMA))
