from __future__ import annotations

import pytest
from core.schema_parser import ColumnDef, ParsedSchema
from core.prompt_builder import PromptBuilder, _is_pii, _infer_tags, _build_quality_rules


def _make_schema(table="silver_orders", columns=None, layer="silver"):
    return ParsedSchema(
        table_name=table,
        columns=columns or [],
        source_format="ddl",
        detected_layer=layer,
    )


def _col(name, dtype="string", nullable=True, pk=False):
    return ColumnDef(name=name, data_type=dtype, nullable=nullable, primary_key=pk)


# ── PII detection ─────────────────────────────────────────────────────────────

class TestPIIDetection:
    @pytest.mark.parametrize("col_name", [
        "customer_email", "email", "user_email",
        "phone", "phone_number", "mobile_phone",
        "cpf", "user_cpf",
        "ssn", "social_ssn",
        "full_name", "first_name", "last_name",
        "address", "billing_address",
        "birth_date", "date_of_birth",
    ])
    def test_pii_detected(self, col_name):
        assert _is_pii(col_name) is True

    @pytest.mark.parametrize("col_name", [
        "order_id", "total_amount", "status", "created_at",
        "currency_code", "is_gift", "session_id", "page_url",
    ])
    def test_non_pii(self, col_name):
        assert _is_pii(col_name) is False


# ── Tag inference ─────────────────────────────────────────────────────────────

class TestTagInference:
    def test_finance_tag_from_amount(self):
        cols = [_col("total_amount", "decimal"), _col("order_id")]
        tags = _infer_tags(cols)
        assert "finance" in tags

    def test_finance_tag_from_price(self):
        cols = [_col("price"), _col("product_id")]
        tags = _infer_tags(cols)
        assert "finance" in tags

    def test_analytics_tag_from_session(self):
        cols = [_col("session_id"), _col("page_url")]
        tags = _infer_tags(cols)
        assert "analytics" in tags

    def test_logistics_tag_from_order(self):
        cols = [_col("order_id"), _col("status")]
        tags = _infer_tags(cols)
        assert "logistics" in tags

    def test_multiple_tags(self):
        cols = [_col("order_id"), _col("total_amount"), _col("session_id")]
        tags = _infer_tags(cols)
        assert "finance" in tags
        assert "analytics" in tags
        assert "logistics" in tags

    def test_no_tags_for_generic_columns(self):
        cols = [_col("id"), _col("created_at"), _col("updated_at")]
        tags = _infer_tags(cols)
        assert tags == []


# ── Quality rule inference ────────────────────────────────────────────────────

class TestQualityRules:
    def test_not_null_rule_for_not_nullable_column(self):
        col = _col("order_id", nullable=False)
        rules = _build_quality_rules(col)
        types = [r["type"] for r in rules]
        assert "not_null" in types

    def test_no_not_null_rule_for_nullable_column(self):
        col = _col("notes", nullable=True)
        rules = _build_quality_rules(col)
        types = [r["type"] for r in rules]
        assert "not_null" not in types

    def test_unique_rule_for_id_column(self):
        col = _col("order_id", nullable=False)
        rules = _build_quality_rules(col)
        types = [r["type"] for r in rules]
        assert "unique" in types

    def test_no_unique_rule_for_non_id_column(self):
        col = _col("status", nullable=True)
        rules = _build_quality_rules(col)
        types = [r["type"] for r in rules]
        assert "unique" not in types

    def test_unique_rule_for_primary_key(self):
        col = _col("id", nullable=False, pk=True)
        rules = _build_quality_rules(col)
        types = [r["type"] for r in rules]
        assert "unique" in types

    def test_min_rule_for_amount_column(self):
        col = _col("total_amount", dtype="decimal", nullable=False)
        rules = _build_quality_rules(col)
        types = [r["type"] for r in rules]
        assert "min" in types
        min_rule = next(r for r in rules if r["type"] == "min")
        assert min_rule["value"] == 0

    def test_no_min_rule_for_string_amount(self):
        col = _col("total_amount", dtype="string")
        rules = _build_quality_rules(col)
        types = [r["type"] for r in rules]
        assert "min" not in types

    def test_regex_for_email_column(self):
        col = _col("customer_email", nullable=False)
        rules = _build_quality_rules(col)
        types = [r["type"] for r in rules]
        assert "regex" in types
        regex_rule = next(r for r in rules if r["type"] == "regex")
        assert "@" in regex_rule["pattern"]

    def test_regex_for_uuid_id_column(self):
        col = _col("order_id", dtype="string", nullable=False)
        rules = _build_quality_rules(col)
        types = [r["type"] for r in rules]
        assert "regex" in types

    def test_age_min_max_rules(self):
        col = _col("age", dtype="integer", nullable=True)
        rules = _build_quality_rules(col)
        types = [r["type"] for r in rules]
        assert "min" in types
        assert "max" in types


# ── SLA defaults per layer ────────────────────────────────────────────────────

class TestSLADefaults:
    def setup_method(self):
        self.builder = PromptBuilder()

    def _extract_freshness(self, schema):
        import yaml
        prompt = self.builder.build(schema)
        # The skeleton YAML is embedded in the prompt — parse it out
        lines = prompt.split("\n")
        skeleton_start = next(i for i, l in enumerate(lines) if l.startswith("dataContractSpecification"))
        skeleton_end = next(
            (i for i, l in enumerate(lines[skeleton_start:], start=skeleton_start)
             if l.strip().startswith("Respond with")),
            len(lines)
        )
        skeleton_yaml = "\n".join(lines[skeleton_start:skeleton_end]).strip()
        contract = yaml.safe_load(skeleton_yaml)
        return contract["sla"]["freshness_hours"]

    def test_bronze_freshness_2h(self):
        schema = _make_schema("bronze_events", [_col("id")], layer="bronze")
        assert self._extract_freshness(schema) == 2

    def test_silver_freshness_4h(self):
        schema = _make_schema("silver_orders", [_col("id")], layer="silver")
        assert self._extract_freshness(schema) == 4

    def test_gold_freshness_8h(self):
        schema = _make_schema("gold_revenue", [_col("id")], layer="gold")
        assert self._extract_freshness(schema) == 8

    def test_unknown_layer_freshness_24h(self):
        schema = _make_schema("custom_table", [_col("id")], layer=None)
        schema.detected_layer = None
        assert self._extract_freshness(schema) == 24


# ── PromptBuilder output ──────────────────────────────────────────────────────

class TestPromptBuilder:
    def setup_method(self):
        self.builder = PromptBuilder()

    def test_prompt_contains_no_markdown_fences_instruction(self):
        schema = _make_schema(columns=[_col("id")])
        prompt = self.builder.build(schema)
        assert "No markdown code fences" in prompt

    def test_prompt_contains_yaml_only_instruction(self):
        schema = _make_schema(columns=[_col("id")])
        prompt = self.builder.build(schema)
        assert "ONLY" in prompt

    def test_pii_flag_in_skeleton_for_email_column(self):
        schema = _make_schema(columns=[_col("customer_email", nullable=False)])
        prompt = self.builder.build(schema)
        assert "pii: true" in prompt

    def test_error_correction_prompt_includes_broken_yaml(self):
        broken = "key: [unclosed"
        err = "unexpected end of stream"
        prompt = self.builder.build_error_correction_prompt("original", broken, err)
        assert broken in prompt
        assert err in prompt

    def test_owner_in_skeleton(self):
        schema = _make_schema(columns=[_col("id")])
        prompt = self.builder.build(schema, owner="test-owner@acme.com")
        assert "test-owner@acme.com" in prompt

    def test_domain_override(self):
        schema = _make_schema(columns=[_col("id")])
        prompt = self.builder.build(schema, domain="payments")
        assert "payments" in prompt
