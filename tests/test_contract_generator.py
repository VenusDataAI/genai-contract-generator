from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch
from core.contract_generator import ContractGenerator, GeneratedContract
from core.schema_parser import ColumnDef, ParsedSchema
from integrations.anthropic_client import AnthropicResponse

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

_MALFORMED_YAML = "key: [unclosed bracket\nnot: valid: yaml: at all"


def _make_schema():
    return ParsedSchema(
        table_name="silver_orders",
        columns=[ColumnDef(name="order_id", data_type="string", nullable=False, primary_key=True)],
        source_format="ddl",
        detected_layer="silver",
    )


def _mock_response(content: str) -> AnthropicResponse:
    return AnthropicResponse(
        content=content,
        model="claude-sonnet-4-20250514",
        input_tokens=500,
        output_tokens=800,
        latency_ms=1200.0,
    )


class TestContractGenerator:
    def _generator(self, mock_client):
        return ContractGenerator(anthropic_client=mock_client)

    def test_successful_generation_returns_contract(self):
        client = MagicMock()
        client.complete.return_value = _mock_response(_VALID_YAML)
        gen = self._generator(client)
        result = gen.generate(_make_schema())
        assert isinstance(result, GeneratedContract)
        assert result.yaml_content == _VALID_YAML
        assert result.table_name == "silver_orders"
        assert result.detected_layer == "silver"

    def test_metadata_populated(self):
        client = MagicMock()
        client.complete.return_value = _mock_response(_VALID_YAML)
        result = self._generator(client).generate(_make_schema())
        assert result.metadata.model == "claude-sonnet-4-20250514"
        assert result.metadata.input_tokens == 500
        assert result.metadata.output_tokens == 800
        assert result.metadata.latency_ms == 1200.0
        assert result.metadata.retry_count == 0

    def test_validation_result_attached(self):
        client = MagicMock()
        client.complete.return_value = _mock_response(_VALID_YAML)
        result = self._generator(client).generate(_make_schema())
        assert result.validation.is_valid is True
        assert result.validation.errors == []

    def test_parsed_dict_populated(self):
        client = MagicMock()
        client.complete.return_value = _mock_response(_VALID_YAML)
        result = self._generator(client).generate(_make_schema())
        assert isinstance(result.parsed_dict, dict)
        assert "models" in result.parsed_dict

    def test_retry_on_malformed_yaml(self):
        """First call returns malformed YAML, second returns valid — should succeed with retry_count=1."""
        client = MagicMock()
        client.complete.side_effect = [
            _mock_response(_MALFORMED_YAML),
            _mock_response(_VALID_YAML),
        ]
        result = self._generator(client).generate(_make_schema())
        assert client.complete.call_count == 2
        assert result.metadata.retry_count == 1
        assert result.validation.is_valid is True

    def test_error_correction_prompt_used_on_retry(self):
        """Verify the second call uses the error-correction prompt (contains the broken output)."""
        client = MagicMock()
        client.complete.side_effect = [
            _mock_response(_MALFORMED_YAML),
            _mock_response(_VALID_YAML),
        ]
        self._generator(client).generate(_make_schema())
        second_call_prompt = client.complete.call_args_list[1][0][0]
        assert "Parse error" in second_call_prompt or "fix" in second_call_prompt.lower()

    def test_all_retries_exhausted_returns_degraded_result(self):
        """If all retries fail, returns a GeneratedContract with is_valid=False."""
        client = MagicMock()
        client.complete.return_value = _mock_response(_MALFORMED_YAML)
        result = self._generator(client).generate(_make_schema())
        # 1 original + 2 retries = 3 calls
        assert client.complete.call_count == 3
        assert result.validation.is_valid is False
        assert len(result.validation.errors) > 0
        assert result.metadata.retry_count == 2

    def test_accumulated_tokens_across_retries(self):
        """Tokens should be summed across all attempts."""
        client = MagicMock()
        client.complete.side_effect = [
            _mock_response(_MALFORMED_YAML),
            _mock_response(_VALID_YAML),
        ]
        result = self._generator(client).generate(_make_schema())
        # Each mock response: 500 input + 800 output = 1300; two calls = 2600 total
        assert result.metadata.input_tokens == 1000
        assert result.metadata.output_tokens == 1600

    def test_accumulated_latency_across_retries(self):
        client = MagicMock()
        client.complete.side_effect = [
            _mock_response(_MALFORMED_YAML),
            _mock_response(_VALID_YAML),
        ]
        result = self._generator(client).generate(_make_schema())
        assert result.metadata.latency_ms == pytest.approx(2400.0)

    def test_strips_markdown_fences(self):
        """Claude sometimes wraps output in ```yaml ... ``` despite instructions."""
        fenced = f"```yaml\n{_VALID_YAML}\n```"
        client = MagicMock()
        client.complete.return_value = _mock_response(fenced)
        result = self._generator(client).generate(_make_schema())
        assert "```" not in result.yaml_content
        assert result.validation.is_valid is True

    def test_strips_plain_fences(self):
        fenced = f"```\n{_VALID_YAML}\n```"
        client = MagicMock()
        client.complete.return_value = _mock_response(fenced)
        result = self._generator(client).generate(_make_schema())
        assert "```" not in result.yaml_content

    def test_tags_extracted_from_yaml(self):
        client = MagicMock()
        client.complete.return_value = _mock_response(_VALID_YAML)
        result = self._generator(client).generate(_make_schema())
        assert "logistics" in result.tags

    def test_owner_passed_to_prompt_builder(self):
        client = MagicMock()
        client.complete.return_value = _mock_response(_VALID_YAML)
        gen = self._generator(client)
        gen.generate(_make_schema(), owner="custom@owner.com")
        called_prompt = client.complete.call_args[0][0]
        assert "custom@owner.com" in called_prompt

    def test_domain_override_passed_to_prompt_builder(self):
        client = MagicMock()
        client.complete.return_value = _mock_response(_VALID_YAML)
        gen = self._generator(client)
        gen.generate(_make_schema(), domain="payments")
        called_prompt = client.complete.call_args[0][0]
        assert "payments" in called_prompt
