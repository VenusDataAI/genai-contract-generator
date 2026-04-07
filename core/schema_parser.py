from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import sqlglot
import sqlglot.expressions as exp


@dataclass
class ColumnDef:
    name: str
    data_type: str
    nullable: bool = True
    default: str | None = None
    primary_key: bool = False
    description: str | None = None


@dataclass
class ParsedSchema:
    table_name: str
    columns: list[ColumnDef]
    source_format: str
    detected_layer: str | None = None
    raw_source: str | None = None


_LAYER_PREFIXES = {
    "bronze": ["bronze_", "raw_", "stg_landing_"],
    "silver": ["silver_", "stg_", "int_"],
    "gold": ["gold_", "mart_", "dim_", "fact_", "agg_"],
}


def _detect_layer(table_name: str) -> str | None:
    lower = table_name.lower()
    for layer, prefixes in _LAYER_PREFIXES.items():
        if any(lower.startswith(p) for p in prefixes):
            return layer
    return None


def _normalize_type(sqlglot_type: str) -> str:
    mapping = {
        "TEXT": "string",
        "VARCHAR": "string",
        "CHAR": "string",
        "INT": "integer",
        "INTEGER": "integer",
        "BIGINT": "long",
        "SMALLINT": "integer",
        "TINYINT": "integer",
        "FLOAT": "float",
        "DOUBLE": "double",
        "REAL": "double",
        "NUMERIC": "decimal",
        "DECIMAL": "decimal",
        "BOOLEAN": "boolean",
        "BOOL": "boolean",
        "DATE": "date",
        "TIMESTAMP": "timestamp",
        "DATETIME": "timestamp",
        "JSON": "object",
        "JSONB": "object",
        "UUID": "string",
        "BYTEA": "bytes",
    }
    upper = sqlglot_type.upper()
    for k, v in mapping.items():
        if upper.startswith(k):
            return v
    return sqlglot_type.lower()


class DDLParser:
    def parse(self, ddl: str) -> ParsedSchema:
        try:
            statements = sqlglot.parse(ddl, error_level=sqlglot.ErrorLevel.WARN)
        except Exception as exc:
            raise ValueError(f"Failed to parse DDL: {exc}") from exc

        create_stmt = next(
            (s for s in statements if isinstance(s, exp.Create)), None
        )
        if create_stmt is None:
            raise ValueError("No CREATE TABLE statement found in DDL")

        table_expr = create_stmt.find(exp.Table)
        table_name = table_expr.name if table_expr else "unknown_table"

        columns: list[ColumnDef] = []
        pk_columns: set[str] = set()

        schema_expr = create_stmt.find(exp.Schema)
        if schema_expr:
            for constraint in schema_expr.find_all(exp.PrimaryKey):
                for expr in constraint.expressions:
                    # In sqlglot v30+, pk expressions are Identifier nodes
                    name = getattr(expr, "name", None) or str(expr)
                    if name:
                        pk_columns.add(name.lower())

        for col_def in (create_stmt.find(exp.Schema) or create_stmt).find_all(exp.ColumnDef):
            col_name = col_def.name
            type_node = col_def.find(exp.DataType)
            raw_type = type_node.sql() if type_node else "string"
            normalized_type = _normalize_type(raw_type)

            not_null = any(
                isinstance(c.args.get("kind"), exp.NotNullColumnConstraint)
                for c in col_def.find_all(exp.ColumnConstraint)
            )
            is_pk = col_name.lower() in pk_columns or any(
                isinstance(c.args.get("kind"), exp.PrimaryKeyColumnConstraint)
                for c in col_def.find_all(exp.ColumnConstraint)
            )

            default_node = col_def.find(exp.DefaultColumnConstraint)
            default_val = default_node.find(exp.Literal).sql() if default_node and default_node.find(exp.Literal) else None

            columns.append(ColumnDef(
                name=col_name,
                data_type=normalized_type,
                nullable=not not_null and not is_pk,
                default=default_val,
                primary_key=is_pk,
            ))

        return ParsedSchema(
            table_name=table_name,
            columns=columns,
            source_format="ddl",
            detected_layer=_detect_layer(table_name),
            raw_source=ddl,
        )


_JSON_SCHEMA_TYPE_MAP = {
    "string": "string",
    "integer": "integer",
    "number": "double",
    "boolean": "boolean",
    "array": "array",
    "object": "object",
    "null": "string",
}


class JSONSchemaParser:
    def parse(self, schema: dict[str, Any]) -> ParsedSchema:
        title = schema.get("title") or schema.get("$id", "unknown_table")
        table_name = re.sub(r"[^a-zA-Z0-9_]", "_", title).lower().strip("_")

        properties: dict[str, Any] = schema.get("properties", {})
        required_fields: set[str] = set(schema.get("required", []))

        columns: list[ColumnDef] = []
        for prop_name, prop_schema in properties.items():
            raw_type = prop_schema.get("type", "string")
            if isinstance(raw_type, list):
                non_null = [t for t in raw_type if t != "null"]
                raw_type = non_null[0] if non_null else "string"
            normalized = _JSON_SCHEMA_TYPE_MAP.get(raw_type, raw_type)

            # Respect format hints
            fmt = prop_schema.get("format", "")
            if fmt in ("date-time", "timestamp"):
                normalized = "timestamp"
            elif fmt == "date":
                normalized = "date"
            elif fmt == "uuid":
                normalized = "string"

            columns.append(ColumnDef(
                name=prop_name,
                data_type=normalized,
                nullable=prop_name not in required_fields,
                description=prop_schema.get("description"),
            ))

        return ParsedSchema(
            table_name=table_name,
            columns=columns,
            source_format="json_schema",
            detected_layer=_detect_layer(table_name),
            raw_source=str(schema),
        )


class ColumnListParser:
    def parse(self, payload: dict[str, Any]) -> ParsedSchema:
        table_name = payload.get("table_name", "unknown_table")
        raw_columns = payload.get("columns", [])

        columns: list[ColumnDef] = []
        for c in raw_columns:
            columns.append(ColumnDef(
                name=c["name"],
                data_type=_normalize_type(c.get("type", "string")),
                nullable=c.get("nullable", True),
                description=c.get("description"),
                default=c.get("default"),
            ))

        return ParsedSchema(
            table_name=table_name,
            columns=columns,
            source_format="column_list",
            detected_layer=_detect_layer(table_name),
        )
