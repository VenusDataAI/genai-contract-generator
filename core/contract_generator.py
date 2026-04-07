from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Any

import yaml
import structlog

from core.contract_validator import ContractValidator, ValidationResult
from core.prompt_builder import PromptBuilder
from core.schema_parser import ParsedSchema
from integrations.anthropic_client import AnthropicClient

logger = structlog.get_logger(__name__)

_MAX_RETRIES = 2


@dataclass
class GenerationMetadata:
    model: str
    input_tokens: int
    output_tokens: int
    latency_ms: float
    retry_count: int


@dataclass
class GeneratedContract:
    yaml_content: str
    parsed_dict: dict[str, Any]
    table_name: str
    detected_layer: str | None
    tags: list[str]
    validation: ValidationResult
    metadata: GenerationMetadata
    warnings: list[str] = field(default_factory=list)


def _strip_fences(text: str) -> str:
    """Remove markdown code fences if Claude accidentally adds them."""
    text = text.strip()
    # Remove ```yaml ... ``` or ``` ... ```
    text = re.sub(r"^```[a-zA-Z]*\n?", "", text)
    text = re.sub(r"\n?```\s*$", "", text)
    # Preserve a single trailing newline (standard for YAML files)
    return text.rstrip() + "\n" if text.strip() else text


class ContractGenerator:
    def __init__(
        self,
        anthropic_client: AnthropicClient | None = None,
        prompt_builder: PromptBuilder | None = None,
        validator: ContractValidator | None = None,
    ) -> None:
        self._client = anthropic_client or AnthropicClient()
        self._builder = prompt_builder or PromptBuilder()
        self._validator = validator or ContractValidator()

    def generate(
        self,
        parsed_schema: ParsedSchema,
        owner: str = "data-team@company.com",
        domain: str | None = None,
    ) -> GeneratedContract:
        log = logger.bind(table=parsed_schema.table_name)
        prompt = self._builder.build(parsed_schema, owner=owner, domain=domain)

        total_input_tokens = 0
        total_output_tokens = 0
        total_latency_ms = 0.0
        retry_count = 0
        last_model = ""
        last_broken_yaml = ""
        last_error = ""

        for attempt in range(_MAX_RETRIES + 1):
            if attempt == 0:
                current_prompt = prompt
            else:
                log.warning("contract_retry", attempt=attempt, error=last_error)
                current_prompt = self._builder.build_error_correction_prompt(
                    original_prompt=prompt,
                    broken_yaml=last_broken_yaml,
                    parse_error=last_error,
                )
                retry_count += 1

            response = self._client.complete(current_prompt)
            total_input_tokens += response.input_tokens
            total_output_tokens += response.output_tokens
            total_latency_ms += response.latency_ms
            last_model = response.model

            raw = _strip_fences(response.content)

            try:
                parsed_dict = yaml.safe_load(raw)
                if not isinstance(parsed_dict, dict):
                    raise yaml.YAMLError("Top-level element is not a mapping.")
            except yaml.YAMLError as exc:
                last_broken_yaml = raw
                last_error = str(exc)
                if attempt < _MAX_RETRIES:
                    continue
                # Final attempt failed — return a degraded result
                log.error("contract_generation_failed", error=last_error, retries=retry_count)
                metadata = GenerationMetadata(
                    model=last_model,
                    input_tokens=total_input_tokens,
                    output_tokens=total_output_tokens,
                    latency_ms=round(total_latency_ms, 2),
                    retry_count=retry_count,
                )
                return GeneratedContract(
                    yaml_content=raw,
                    parsed_dict={},
                    table_name=parsed_schema.table_name,
                    detected_layer=parsed_schema.detected_layer,
                    tags=[],
                    validation=ValidationResult(
                        is_valid=False,
                        errors=[f"Could not produce valid YAML after {_MAX_RETRIES} retries: {last_error}"],
                    ),
                    metadata=metadata,
                    warnings=[f"Final output may be malformed: {last_error}"],
                )

            # YAML parsed successfully — validate
            expected_cols = [c.name for c in parsed_schema.columns]
            validation = self._validator.validate(raw, expected_columns=expected_cols)

            tags = parsed_dict.get("tags", [])
            if not isinstance(tags, list):
                tags = []

            metadata = GenerationMetadata(
                model=last_model,
                input_tokens=total_input_tokens,
                output_tokens=total_output_tokens,
                latency_ms=round(total_latency_ms, 2),
                retry_count=retry_count,
            )

            log.info(
                "contract_generated",
                is_valid=validation.is_valid,
                retries=retry_count,
                tokens=total_input_tokens + total_output_tokens,
            )

            return GeneratedContract(
                yaml_content=raw,
                parsed_dict=parsed_dict,
                table_name=parsed_schema.table_name,
                detected_layer=parsed_schema.detected_layer,
                tags=tags,
                validation=validation,
                metadata=metadata,
            )

        # Unreachable but satisfies type checker
        raise RuntimeError("Unexpected exit from generation loop")
