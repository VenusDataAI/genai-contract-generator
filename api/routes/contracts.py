from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import structlog
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from core.contract_generator import ContractGenerator
from core.contract_validator import ContractValidator
from core.schema_parser import DDLParser, JSONSchemaParser, ColumnListParser
from models.output_models import ValidationResult

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/api/v1/contracts", tags=["Contracts"])

_EXAMPLES_DIR = Path(__file__).parent.parent.parent / "examples"


# --- Request/Response Models ---

class GenerationOptions(BaseModel):
    owner: str = "data-team@company.com"
    domain: str | None = None
    strict_quality: bool = False


class GenerationRequest(BaseModel):
    input_format: str = Field(..., description="ddl | json_schema | column_list")
    content: str | dict[str, Any] = Field(..., description="Raw DDL string, JSON Schema object, or column list dict")
    options: GenerationOptions = Field(default_factory=GenerationOptions)

    model_config = {"json_schema_extra": {
        "examples": [{
            "input_format": "ddl",
            "content": "CREATE TABLE silver_orders (order_id VARCHAR(36) NOT NULL, user_id BIGINT NOT NULL);",
            "options": {"owner": "orders@company.com"},
        }]
    }}


class ParsedSchemaInfo(BaseModel):
    table_name: str
    detected_layer: str | None
    column_count: int


class GenerationMetadataOut(BaseModel):
    model: str
    tokens_used: int
    latency_ms: float
    retry_count: int


class GenerationResponse(BaseModel):
    contract_yaml: str
    validation: ValidationResult
    metadata: GenerationMetadataOut
    parsed_schema: ParsedSchemaInfo


class ValidateRequest(BaseModel):
    contract_yaml: str


class ExampleEntry(BaseModel):
    name: str
    input_format: str
    content: str | dict[str, Any]
    description: str


# --- Helpers ---

def _parse_input(input_format: str, content: str | dict[str, Any]):
    fmt = input_format.lower().strip()
    if fmt == "ddl":
        if not isinstance(content, str):
            raise HTTPException(status_code=422, detail="For 'ddl' format, 'content' must be a string.")
        return DDLParser().parse(content)
    elif fmt == "json_schema":
        if isinstance(content, str):
            try:
                content = json.loads(content)
            except json.JSONDecodeError as exc:
                raise HTTPException(status_code=422, detail=f"Invalid JSON in content: {exc}") from exc
        if not isinstance(content, dict):
            raise HTTPException(status_code=422, detail="For 'json_schema' format, 'content' must be a JSON object.")
        return JSONSchemaParser().parse(content)
    elif fmt == "column_list":
        if isinstance(content, str):
            try:
                content = json.loads(content)
            except json.JSONDecodeError as exc:
                raise HTTPException(status_code=422, detail=f"Invalid JSON in content: {exc}") from exc
        if not isinstance(content, dict):
            raise HTTPException(status_code=422, detail="For 'column_list' format, 'content' must be a JSON object.")
        return ColumnListParser().parse(content)
    else:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown input_format '{input_format}'. Must be one of: ddl, json_schema, column_list.",
        )


# --- Routes ---

@router.post("/generate", response_model=GenerationResponse)
async def generate_contract(request: GenerationRequest) -> GenerationResponse:
    log = logger.bind(input_format=request.input_format)
    log.info("generate_contract_request")

    try:
        parsed_schema = _parse_input(request.input_format, request.content)
    except HTTPException:
        raise
    except Exception as exc:
        log.error("schema_parse_error", error=str(exc))
        raise HTTPException(status_code=422, detail=f"Schema parsing failed: {exc}") from exc

    try:
        generator = ContractGenerator()
        result = generator.generate(
            parsed_schema,
            owner=request.options.owner,
            domain=request.options.domain,
        )
    except EnvironmentError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        log.error("generation_error", error=str(exc))
        raise HTTPException(status_code=500, detail=f"Contract generation failed: {exc}") from exc

    return GenerationResponse(
        contract_yaml=result.yaml_content,
        validation=ValidationResult(
            valid=result.validation.is_valid,
            errors=result.validation.errors,
            warnings=result.validation.warnings,
        ),
        metadata=GenerationMetadataOut(
            model=result.metadata.model,
            tokens_used=result.metadata.input_tokens + result.metadata.output_tokens,
            latency_ms=result.metadata.latency_ms,
            retry_count=result.metadata.retry_count,
        ),
        parsed_schema=ParsedSchemaInfo(
            table_name=result.table_name,
            detected_layer=result.detected_layer,
            column_count=len(parsed_schema.columns),
        ),
    )


@router.post("/validate", response_model=ValidationResult)
async def validate_contract(request: ValidateRequest) -> ValidationResult:
    validator = ContractValidator()
    result = validator.validate(request.contract_yaml)
    return ValidationResult(
        valid=result.is_valid,
        errors=result.errors,
        warnings=result.warnings,
    )


@router.get("/examples", response_model=list[ExampleEntry])
async def list_examples() -> list[ExampleEntry]:
    examples: list[ExampleEntry] = []

    # DDL examples
    for sql_file in sorted(_EXAMPLES_DIR.glob("input_ddl_*.sql")):
        examples.append(ExampleEntry(
            name=sql_file.stem,
            input_format="ddl",
            content=sql_file.read_text(encoding="utf-8"),
            description=f"SQL DDL example: {sql_file.stem.replace('_', ' ')}",
        ))

    # JSON Schema examples
    for json_file in sorted(_EXAMPLES_DIR.glob("input_json_schema_*.json")):
        try:
            content = json.loads(json_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            content = json_file.read_text(encoding="utf-8")
        examples.append(ExampleEntry(
            name=json_file.stem,
            input_format="json_schema",
            content=content,
            description=f"JSON Schema example: {json_file.stem.replace('_', ' ')}",
        ))

    return examples
