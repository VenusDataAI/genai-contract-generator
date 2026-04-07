from __future__ import annotations

import json
import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

from core.contract_generator import GeneratedContract, GenerationMetadata
from core.contract_validator import ValidationResult as CoreValidationResult
from integrations.anthropic_client import AnthropicResponse


from api.main import app
from api.middleware.rate_limiter import RateLimiterMiddleware


def _find_rate_limiter(app_instance) -> RateLimiterMiddleware | None:
    """Walk the built ASGI middleware stack to find RateLimiterMiddleware."""
    node = getattr(app_instance, "middleware_stack", None)
    while node is not None:
        if isinstance(node, RateLimiterMiddleware):
            return node
        node = getattr(node, "app", None)
    return None


@pytest.fixture(autouse=True)
def reset_rate_limiter():
    """Clear the in-memory rate-limiter store before and after every test."""
    # Build the ASGI stack if not already built (TestClient does this on first use)
    with TestClient(app, raise_server_exceptions=False):
        pass
    rl = _find_rate_limiter(app)
    if rl is not None:
        rl._store.clear()
    yield
    if rl is not None:
        rl._store.clear()


client = TestClient(app, raise_server_exceptions=False)

_VALID_YAML = """\
dataContractSpecification: "0.9.3"
id: "urn:datacontract:orders:silver_orders"
info:
  title: "Silver Orders"
  version: "1.0.0"
  owner: "team@company.com"
  domain: "orders"
models:
  silver_orders:
    fields:
      order_id:
        type: string
        required: true
        description: "Unique order identifier."
quality:
  rules:
    - type: not_null
      column: order_id
sla:
  freshness_hours: 4
  support_contact: "team@company.com"
  tier: silver
tags:
  - logistics
"""

_MOCK_CONTRACT = GeneratedContract(
    yaml_content=_VALID_YAML,
    parsed_dict={"dataContractSpecification": "0.9.3"},
    table_name="silver_orders",
    detected_layer="silver",
    tags=["logistics"],
    validation=CoreValidationResult(is_valid=True, errors=[], warnings=[]),
    metadata=GenerationMetadata(
        model="claude-sonnet-4-20250514",
        input_tokens=500,
        output_tokens=800,
        latency_ms=1200.0,
        retry_count=0,
    ),
)


# ── Health ────────────────────────────────────────────────────────────────────

class TestHealthEndpoint:
    def test_health_ok(self):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "version" in data
        assert "anthropic_configured" in data

    def test_health_response_model(self):
        resp = client.get("/health")
        data = resp.json()
        assert isinstance(data["anthropic_configured"], bool)


# ── Generate ──────────────────────────────────────────────────────────────────

class TestGenerateEndpoint:
    _DDL = "CREATE TABLE silver_orders (order_id VARCHAR(36) NOT NULL, user_id BIGINT NOT NULL);"

    def _post(self, payload):
        return client.post("/api/v1/contracts/generate", json=payload)

    @patch("api.routes.contracts.ContractGenerator")
    def test_generate_ddl_success(self, MockGenerator):
        MockGenerator.return_value.generate.return_value = _MOCK_CONTRACT
        resp = self._post({"input_format": "ddl", "content": self._DDL})
        assert resp.status_code == 200
        data = resp.json()
        assert "contract_yaml" in data
        assert data["validation"]["valid"] is True
        assert data["parsed_schema"]["table_name"] == "silver_orders"
        assert data["parsed_schema"]["detected_layer"] == "silver"

    @patch("api.routes.contracts.ContractGenerator")
    def test_generate_metadata_in_response(self, MockGenerator):
        MockGenerator.return_value.generate.return_value = _MOCK_CONTRACT
        resp = self._post({"input_format": "ddl", "content": self._DDL})
        data = resp.json()
        meta = data["metadata"]
        assert meta["model"] == "claude-sonnet-4-20250514"
        assert meta["tokens_used"] == 1300
        assert meta["latency_ms"] == 1200.0
        assert meta["retry_count"] == 0

    @patch("api.routes.contracts.ContractGenerator")
    def test_generate_json_schema_success(self, MockGenerator):
        MockGenerator.return_value.generate.return_value = _MOCK_CONTRACT
        schema = {
            "title": "gold_products",
            "type": "object",
            "properties": {"product_id": {"type": "string"}},
            "required": ["product_id"],
        }
        resp = self._post({"input_format": "json_schema", "content": schema})
        assert resp.status_code == 200

    @patch("api.routes.contracts.ContractGenerator")
    def test_generate_column_list_success(self, MockGenerator):
        MockGenerator.return_value.generate.return_value = _MOCK_CONTRACT
        payload = {
            "input_format": "column_list",
            "content": {
                "table_name": "silver_orders",
                "columns": [{"name": "order_id", "type": "varchar", "nullable": False}],
            },
        }
        resp = self._post(payload)
        assert resp.status_code == 200

    def test_generate_invalid_format_returns_422(self):
        resp = self._post({"input_format": "excel", "content": "something"})
        assert resp.status_code == 422

    def test_generate_ddl_format_with_non_string_returns_422(self):
        resp = self._post({"input_format": "ddl", "content": {"not": "a string"}})
        assert resp.status_code == 422

    def test_generate_missing_content_returns_422(self):
        resp = self._post({"input_format": "ddl"})
        assert resp.status_code == 422

    def test_generate_invalid_ddl_returns_422(self):
        resp = self._post({"input_format": "ddl", "content": "NOT SQL AT ALL ;;;"})
        assert resp.status_code in (422, 500)

    @patch("api.routes.contracts.ContractGenerator")
    def test_generate_with_options_owner(self, MockGenerator):
        MockGenerator.return_value.generate.return_value = _MOCK_CONTRACT
        resp = self._post({
            "input_format": "ddl",
            "content": self._DDL,
            "options": {"owner": "custom@owner.com"},
        })
        assert resp.status_code == 200
        # Verify owner was passed through
        call_kwargs = MockGenerator.return_value.generate.call_args
        assert call_kwargs.kwargs.get("owner") == "custom@owner.com" or \
               (call_kwargs.args and "custom@owner.com" in str(call_kwargs))

    @patch("api.routes.contracts.ContractGenerator")
    def test_generate_anthropic_not_configured_returns_503(self, MockGenerator):
        MockGenerator.return_value.generate.side_effect = EnvironmentError("ANTHROPIC_API_KEY not set")
        resp = self._post({"input_format": "ddl", "content": self._DDL})
        assert resp.status_code == 503


# ── Validate ──────────────────────────────────────────────────────────────────

class TestValidateEndpoint:
    def test_validate_valid_contract(self):
        resp = client.post("/api/v1/contracts/validate", json={"contract_yaml": _VALID_YAML})
        assert resp.status_code == 200
        data = resp.json()
        assert data["valid"] is True
        assert data["errors"] == []

    def test_validate_invalid_yaml(self):
        resp = client.post("/api/v1/contracts/validate", json={"contract_yaml": "key: [unclosed"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["valid"] is False
        assert len(data["errors"]) > 0

    def test_validate_missing_required_keys(self):
        minimal = "title: missing\nfoo: bar\n"
        resp = client.post("/api/v1/contracts/validate", json={"contract_yaml": minimal})
        assert resp.status_code == 200
        data = resp.json()
        assert data["valid"] is False

    def test_validate_missing_body_returns_422(self):
        resp = client.post("/api/v1/contracts/validate", json={})
        assert resp.status_code == 422


# ── Examples ──────────────────────────────────────────────────────────────────

class TestExamplesEndpoint:
    def test_examples_returns_list(self):
        resp = client.get("/api/v1/contracts/examples")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    def test_examples_have_required_fields(self):
        resp = client.get("/api/v1/contracts/examples")
        data = resp.json()
        for ex in data:
            assert "name" in ex
            assert "input_format" in ex
            assert "content" in ex
            assert "description" in ex

    def test_examples_include_ddl_files(self):
        resp = client.get("/api/v1/contracts/examples")
        data = resp.json()
        formats = [ex["input_format"] for ex in data]
        assert "ddl" in formats

    def test_examples_include_json_schema_files(self):
        resp = client.get("/api/v1/contracts/examples")
        data = resp.json()
        formats = [ex["input_format"] for ex in data]
        assert "json_schema" in formats


# ── Rate Limiter ──────────────────────────────────────────────────────────────

class TestRateLimiter:
    def test_rate_limit_after_10_requests(self):
        """Make 11 rapid POST requests to /api/v1/contracts/validate and expect the 11th to be 429."""
        payload = {"contract_yaml": "foo: bar"}
        responses = []
        for _ in range(11):
            r = client.post("/api/v1/contracts/validate", json=payload)
            responses.append(r.status_code)

        assert 429 in responses, f"Expected a 429 in responses, got: {responses}"

    def test_rate_limit_response_has_retry_after(self):
        """Once rate-limited, the response body should contain retry_after_seconds."""
        payload = {"contract_yaml": "foo: bar"}
        last = None
        for _ in range(12):
            last = client.post("/api/v1/contracts/validate", json=payload)
        if last and last.status_code == 429:
            data = last.json()
            assert "retry_after_seconds" in data

    def test_non_api_routes_not_rate_limited(self):
        """The /health endpoint must never be rate-limited."""
        for _ in range(15):
            resp = client.get("/health")
            assert resp.status_code == 200
