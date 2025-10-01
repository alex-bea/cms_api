import yaml, json
from pathlib import Path
from jsonschema import Draft202012Validator

SPEC = yaml.safe_load(Path("api-contracts/openapi.yaml").read_text())

def schema(ref):
    parts = ref.replace("#/","").split("/")
    cur = SPEC
    for p in parts:
        cur = cur[p]
    return cur

def validate(instance, schema_obj):
    Draft202012Validator(schema_obj).validate(instance)

def test_error_schema_minimal_fields():
    err = {"code":"VALIDATION_FAILED","message":"Bad input","trace_id":"abc123"}
    validate(err, schema("#/components/schemas/Error"))

def test_error_responses_exist():
    responses = SPEC["components"]["responses"]
    for key in ["BadRequest","Unauthorized","PaymentRequired","NotFound","TooManyRequests"]:
        assert key in responses, f"Missing {key}"

def test_paths_reference_canonical_errors():
    for path, item in SPEC["paths"].items():
        for method, op in item.items():
            if method.lower() not in ("get","post","put","patch","delete","options","head"): 
                continue
            resps = op.get("responses", {})
            for status, body in resps.items():
                if status.startswith("4") or status.startswith("5"):
                    # ensure it's a $ref to components.responses.*
                    assert "$ref" in body, f"{path} {method} {status} must $ref canonical response"
                    assert body["$ref"].startswith("#/components/responses/")
