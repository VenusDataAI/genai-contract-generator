from __future__ import annotations

import re
from typing import Any

import yaml

from core.schema_parser import ColumnDef, ParsedSchema

_PII_PATTERNS = [
    "email", "phone", "cpf", "ssn", "name", "address",
    "birth", "gender", "nationality",
]

_TAG_RULES: dict[str, list[str]] = {
    "finance": ["amount", "price", "revenue", "cost", "payment", "billing", "invoice", "tax"],
    "analytics": ["session", "page", "click", "view", "event", "impression", "funnel"],
    "identity": ["user_id", "customer_id", "account_id", "profile"],
    "logistics": ["order", "shipment", "delivery", "tracking", "warehouse"],
}

_SLA_FRESHNESS: dict[str, int] = {
    "bronze": 2,
    "silver": 4,
    "gold": 8,
}

_UUID_PATTERN = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
_EMAIL_PATTERN = r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
_ISO_DATE_PATTERN = r"^\d{4}-\d{2}-\d{2}$"


def _is_pii(col_name: str) -> bool:
    lower = col_name.lower()
    return any(p in lower for p in _PII_PATTERNS)


def _infer_tags(columns: list[ColumnDef]) -> list[str]:
    all_names = " ".join(c.name.lower() for c in columns)
    tags: list[str] = []
    for tag, keywords in _TAG_RULES.items():
        if any(kw in all_names for kw in keywords):
            tags.append(tag)
    return sorted(tags)


def _infer_domain(table_name: str) -> str:
    parts = table_name.lower().split("_")
    skip = {"bronze", "silver", "gold", "raw", "stg", "int", "mart", "dim", "fact", "agg"}
    for part in parts:
        if part not in skip and len(part) > 2:
            return part
    return table_name


def _build_quality_rules(col: ColumnDef) -> list[dict[str, Any]]:
    rules: list[dict[str, Any]] = []
    lower = col.name.lower()

    if not col.nullable:
        rules.append({"type": "not_null", "column": col.name})

    if lower.endswith("_id") or col.primary_key:
        rules.append({"type": "unique", "column": col.name})

    if col.data_type in ("decimal", "double", "float", "integer", "long"):
        if "amount" in lower or "price" in lower or "revenue" in lower or "cost" in lower:
            rules.append({"type": "min", "column": col.name, "value": 0})
        if "age" in lower:
            rules.append({"type": "min", "column": col.name, "value": 0})
            rules.append({"type": "max", "column": col.name, "value": 150})

    if "email" in lower:
        rules.append({"type": "regex", "column": col.name, "pattern": _EMAIL_PATTERN})
    elif lower.endswith("_id") and col.data_type == "string":
        rules.append({"type": "regex", "column": col.name, "pattern": _UUID_PATTERN})
    elif "date" in lower and col.data_type == "string":
        rules.append({"type": "regex", "column": col.name, "pattern": _ISO_DATE_PATTERN})

    return rules


def _col_description(col: ColumnDef) -> str:
    if col.description:
        return col.description
    name = col.name.replace("_", " ").strip()
    parts = []
    if col.primary_key:
        parts.append(f"Primary key for {name}.")
    else:
        parts.append(f"The {name} of the record.")
    if _is_pii(col.name):
        parts.append("Contains personally identifiable information (PII).")
    if not col.nullable:
        parts.append("This field is mandatory and cannot be null.")
    return " ".join(parts)


class PromptBuilder:
    def build(self, schema: ParsedSchema, owner: str = "data-team@company.com", domain: str | None = None) -> str:
        resolved_domain = domain or _infer_domain(schema.table_name)
        layer = schema.detected_layer or "unknown"
        freshness = _SLA_FRESHNESS.get(layer, 24)
        tags = _infer_tags(schema.columns)

        # --- Build the contract skeleton as a Python dict ---
        models_fields: dict[str, Any] = {}
        for col in schema.columns:
            field_def: dict[str, Any] = {
                "type": col.data_type,
                "required": not col.nullable,
                "description": _col_description(col),
            }
            if _is_pii(col.name):
                field_def["pii"] = True
            if col.default is not None:
                field_def["default"] = col.default
            models_fields[col.name] = field_def

        all_quality_rules: list[dict[str, Any]] = []
        for col in schema.columns:
            all_quality_rules.extend(_build_quality_rules(col))

        contract_skeleton = {
            "dataContractSpecification": "0.9.3",
            "id": f"urn:datacontract:{resolved_domain}:{schema.table_name}",
            "info": {
                "title": schema.table_name.replace("_", " ").title(),
                "version": "1.0.0",
                "owner": owner,
                "domain": resolved_domain,
                "description": f"Data contract for the {schema.table_name} table in the {layer} layer.",
            },
            "models": {
                schema.table_name: {
                    "description": f"The {schema.table_name} model.",
                    "fields": models_fields,
                }
            },
            "quality": {
                "rules": all_quality_rules
            },
            "sla": {
                "freshness_hours": freshness,
                "support_contact": owner,
                "tier": layer,
            },
            "tags": tags,
        }

        skeleton_yaml = yaml.dump(contract_skeleton, default_flow_style=False, allow_unicode=True, sort_keys=False)

        prompt = f"""You are a data governance expert producing a YAML data contract.

Use the datacontract.com specification version 0.9.3.

Below is a pre-filled skeleton contract in YAML. Your task is to:
1. Keep every field already present — do NOT remove or rename any key.
2. Improve every `description` field with clear, professional, domain-aware natural language.
3. Ensure the `quality.rules` list is complete and accurate:
   - Add `not_null` rules for every field marked `required: true`.
   - Add `unique` rules for fields whose name ends with `_id`.
   - Add `regex` rules for email fields, UUID id fields (string type), and date string fields.
   - Add `min: 0` for amount/price/revenue/cost numeric fields.
4. Do NOT change type values, required values, or structural keys.
5. Output ONLY the final valid YAML. No markdown code fences. No explanation. No comments.

Skeleton to improve:
{skeleton_yaml}

Respond with ONLY the complete, valid YAML data contract."""

        return prompt

    def build_error_correction_prompt(self, original_prompt: str, broken_yaml: str, parse_error: str) -> str:
        return f"""Your previous response contained invalid YAML that could not be parsed.

Parse error: {parse_error}

Broken output:
{broken_yaml}

Please fix the YAML and output ONLY the corrected, complete, valid YAML data contract.
No markdown code fences. No explanation. No comments.

Original instructions for reference:
{original_prompt}"""
