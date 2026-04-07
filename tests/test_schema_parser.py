from __future__ import annotations

import pytest
from core.schema_parser import DDLParser, JSONSchemaParser, ColumnListParser, ParsedSchema


# ── DDLParser ────────────────────────────────────────────────────────────────

class TestDDLParser:
    def setup_method(self):
        self.parser = DDLParser()

    def test_basic_table_name(self):
        ddl = "CREATE TABLE silver_orders (id BIGINT NOT NULL);"
        schema = self.parser.parse(ddl)
        assert schema.table_name == "silver_orders"

    def test_column_count(self):
        ddl = """
        CREATE TABLE bronze_events (
            event_id VARCHAR(36) NOT NULL,
            user_id  BIGINT      NOT NULL,
            payload  TEXT
        );
        """
        schema = self.parser.parse(ddl)
        assert len(schema.columns) == 3

    def test_nullability_not_null(self):
        ddl = "CREATE TABLE t (col1 VARCHAR(50) NOT NULL, col2 TEXT);"
        schema = self.parser.parse(ddl)
        col1 = next(c for c in schema.columns if c.name == "col1")
        col2 = next(c for c in schema.columns if c.name == "col2")
        assert col1.nullable is False
        assert col2.nullable is True

    def test_primary_key_column(self):
        ddl = "CREATE TABLE t (id BIGINT NOT NULL, PRIMARY KEY (id));"
        schema = self.parser.parse(ddl)
        col = schema.columns[0]
        assert col.primary_key is True
        assert col.nullable is False

    def test_type_normalization_varchar(self):
        ddl = "CREATE TABLE t (name VARCHAR(255));"
        schema = self.parser.parse(ddl)
        assert schema.columns[0].data_type == "string"

    def test_type_normalization_numeric(self):
        ddl = "CREATE TABLE t (amount NUMERIC(12,2));"
        schema = self.parser.parse(ddl)
        assert schema.columns[0].data_type == "decimal"

    def test_type_normalization_bigint(self):
        ddl = "CREATE TABLE t (id BIGINT NOT NULL);"
        schema = self.parser.parse(ddl)
        assert schema.columns[0].data_type == "long"

    def test_type_normalization_boolean(self):
        ddl = "CREATE TABLE t (flag BOOLEAN NOT NULL DEFAULT FALSE);"
        schema = self.parser.parse(ddl)
        assert schema.columns[0].data_type == "boolean"

    def test_type_normalization_timestamp(self):
        ddl = "CREATE TABLE t (created_at TIMESTAMP NOT NULL);"
        schema = self.parser.parse(ddl)
        assert schema.columns[0].data_type == "timestamp"

    def test_source_format(self):
        ddl = "CREATE TABLE t (id INT);"
        schema = self.parser.parse(ddl)
        assert schema.source_format == "ddl"

    def test_invalid_ddl_raises(self):
        with pytest.raises(ValueError, match="No CREATE TABLE"):
            self.parser.parse("SELECT 1;")

    def test_layer_detection_silver(self):
        ddl = "CREATE TABLE silver_orders (id INT);"
        schema = self.parser.parse(ddl)
        assert schema.detected_layer == "silver"

    def test_layer_detection_bronze(self):
        ddl = "CREATE TABLE bronze_raw_events (id INT);"
        schema = self.parser.parse(ddl)
        assert schema.detected_layer == "bronze"

    def test_layer_detection_gold(self):
        ddl = "CREATE TABLE gold_revenue (id INT);"
        schema = self.parser.parse(ddl)
        assert schema.detected_layer == "gold"

    def test_layer_detection_fact(self):
        ddl = "CREATE TABLE fact_sales (id INT);"
        schema = self.parser.parse(ddl)
        assert schema.detected_layer == "gold"

    def test_layer_detection_dim(self):
        ddl = "CREATE TABLE dim_customers (id INT);"
        schema = self.parser.parse(ddl)
        assert schema.detected_layer == "gold"

    def test_layer_detection_unknown(self):
        ddl = "CREATE TABLE my_custom_table (id INT);"
        schema = self.parser.parse(ddl)
        assert schema.detected_layer is None

    def test_full_orders_ddl(self):
        ddl = """
        CREATE TABLE silver_orders (
            order_id        VARCHAR(36)     NOT NULL,
            user_id         BIGINT          NOT NULL,
            customer_email  VARCHAR(255)    NOT NULL,
            total_amount    NUMERIC(12, 2)  NOT NULL,
            is_gift         BOOLEAN         NOT NULL DEFAULT FALSE,
            created_at      TIMESTAMP       NOT NULL,
            PRIMARY KEY (order_id)
        );
        """
        schema = self.parser.parse(ddl)
        assert len(schema.columns) == 6
        order_id = next(c for c in schema.columns if c.name == "order_id")
        assert order_id.primary_key is True
        email = next(c for c in schema.columns if c.name == "customer_email")
        assert email.nullable is False
        assert email.data_type == "string"


# ── JSONSchemaParser ──────────────────────────────────────────────────────────

class TestJSONSchemaParser:
    def setup_method(self):
        self.parser = JSONSchemaParser()

    def _schema(self, title="gold_products", props=None, required=None):
        return {
            "title": title,
            "type": "object",
            "properties": props or {},
            "required": required or [],
        }

    def test_table_name_from_title(self):
        schema = self._schema(title="gold_products")
        result = self.parser.parse(schema)
        assert result.table_name == "gold_products"

    def test_required_field_not_nullable(self):
        schema = self._schema(
            props={"product_id": {"type": "string"}},
            required=["product_id"],
        )
        result = self.parser.parse(schema)
        assert result.columns[0].nullable is False

    def test_optional_field_nullable(self):
        schema = self._schema(props={"description": {"type": "string"}})
        result = self.parser.parse(schema)
        assert result.columns[0].nullable is True

    def test_type_mapping_number_to_double(self):
        schema = self._schema(props={"price": {"type": "number"}})
        result = self.parser.parse(schema)
        assert result.columns[0].data_type == "double"

    def test_type_mapping_integer(self):
        schema = self._schema(props={"qty": {"type": "integer"}})
        result = self.parser.parse(schema)
        assert result.columns[0].data_type == "integer"

    def test_format_datetime_becomes_timestamp(self):
        schema = self._schema(props={"created_at": {"type": "string", "format": "date-time"}})
        result = self.parser.parse(schema)
        assert result.columns[0].data_type == "timestamp"

    def test_format_date_becomes_date(self):
        schema = self._schema(props={"birth_date": {"type": "string", "format": "date"}})
        result = self.parser.parse(schema)
        assert result.columns[0].data_type == "date"

    def test_nullable_type_array(self):
        schema = self._schema(props={"col": {"type": ["string", "null"]}})
        result = self.parser.parse(schema)
        assert result.columns[0].data_type == "string"

    def test_description_preserved(self):
        schema = self._schema(props={"sku": {"type": "string", "description": "Stock keeping unit"}})
        result = self.parser.parse(schema)
        assert result.columns[0].description == "Stock keeping unit"

    def test_source_format(self):
        result = self.parser.parse(self._schema())
        assert result.source_format == "json_schema"

    def test_layer_detection_gold(self):
        result = self.parser.parse(self._schema(title="gold_products"))
        assert result.detected_layer == "gold"


# ── ColumnListParser ──────────────────────────────────────────────────────────

class TestColumnListParser:
    def setup_method(self):
        self.parser = ColumnListParser()

    def _payload(self, table="silver_orders", cols=None):
        return {
            "table_name": table,
            "columns": cols or [],
        }

    def test_table_name(self):
        result = self.parser.parse(self._payload(table="silver_orders"))
        assert result.table_name == "silver_orders"

    def test_column_parsed(self):
        payload = self._payload(cols=[{"name": "order_id", "type": "varchar", "nullable": False}])
        result = self.parser.parse(payload)
        assert len(result.columns) == 1
        assert result.columns[0].name == "order_id"
        assert result.columns[0].nullable is False

    def test_type_normalized(self):
        payload = self._payload(cols=[{"name": "amount", "type": "NUMERIC"}])
        result = self.parser.parse(payload)
        assert result.columns[0].data_type == "decimal"

    def test_description_preserved(self):
        payload = self._payload(cols=[{"name": "x", "type": "int", "description": "my desc"}])
        result = self.parser.parse(payload)
        assert result.columns[0].description == "my desc"

    def test_source_format(self):
        result = self.parser.parse(self._payload())
        assert result.source_format == "column_list"

    def test_layer_detection(self):
        result = self.parser.parse(self._payload(table="silver_orders"))
        assert result.detected_layer == "silver"
