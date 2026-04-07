from __future__ import annotations

from pydantic import BaseModel, Field


class GeneratedContract(BaseModel):
    contract_yaml: str = Field(..., description="Generated YAML data contract")
    table_name: str
    detected_layer: str | None = None
    inferred_domain: str | None = None
    tags: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class ValidationResult(BaseModel):
    valid: bool
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class HealthResponse(BaseModel):
    status: str
    version: str
    anthropic_configured: bool
