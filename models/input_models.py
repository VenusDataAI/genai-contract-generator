from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class InputFormat(str, Enum):
    DDL = "ddl"
    JSON_SCHEMA = "json_schema"
    COLUMN_LIST = "column_list"


class ColumnInput(BaseModel):
    name: str
    type: str
    nullable: bool = True
    description: str | None = None
    default: str | None = None


class ColumnListInput(BaseModel):
    columns: list[ColumnInput]


class ContractGenerationRequest(BaseModel):
    format: InputFormat = Field(..., description="Input format: ddl, json_schema, column_list")
    content: str | dict[str, Any] | ColumnListInput = Field(
        ..., description="Raw DDL string, JSON Schema object, or column list payload"
    )
    owner: str = Field(default="data-team@company.com", description="Data owner contact")
    domain: str | None = Field(default=None, description="Domain override (inferred if omitted)")

    model_config = {"json_schema_extra": {
        "examples": [
            {
                "format": "ddl",
                "content": "CREATE TABLE silver_orders (order_id VARCHAR(36) NOT NULL, user_id BIGINT NOT NULL, total_amount NUMERIC(12,2));",
                "owner": "orders-team@company.com",
            }
        ]
    }}


class ValidateContractRequest(BaseModel):
    contract_yaml: str = Field(..., description="YAML string of the data contract to validate")
