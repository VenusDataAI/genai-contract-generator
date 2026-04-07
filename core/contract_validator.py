from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import yaml
import structlog

logger = structlog.get_logger(__name__)

_REQUIRED_TOP_LEVEL = {"dataContractSpecification", "id", "info", "models"}
_REQUIRED_INFO_KEYS = {"title", "version", "owner"}
_VALID_TYPES = {
    "string", "integer", "long", "float", "double", "decimal",
    "boolean", "date", "timestamp", "object", "array", "bytes",
}


@dataclass
class ValidationResult:
    is_valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


class ContractValidator:
    def validate(self, yaml_content: str, expected_columns: list[str] | None = None) -> ValidationResult:
        errors: list[str] = []
        warnings: list[str] = []

        # 1. Parse YAML
        try:
            parsed: dict[str, Any] = yaml.safe_load(yaml_content)
        except yaml.YAMLError as exc:
            return ValidationResult(is_valid=False, errors=[f"Invalid YAML: {exc}"])

        if not isinstance(parsed, dict):
            return ValidationResult(is_valid=False, errors=["Contract must be a YAML mapping at the top level."])

        # 2. Required top-level keys
        missing_top = _REQUIRED_TOP_LEVEL - set(parsed.keys())
        for key in sorted(missing_top):
            errors.append(f"Missing required top-level key: '{key}'")

        # 3. Spec version
        spec_ver = parsed.get("dataContractSpecification")
        if spec_ver and str(spec_ver) != "0.9.3":
            warnings.append(f"dataContractSpecification is '{spec_ver}', expected '0.9.3'.")

        # 4. Info block
        info = parsed.get("info", {})
        if isinstance(info, dict):
            missing_info = _REQUIRED_INFO_KEYS - set(info.keys())
            for key in sorted(missing_info):
                errors.append(f"Missing required info key: '{key}'")
        else:
            errors.append("'info' block must be a mapping.")

        # 5. Models block
        models = parsed.get("models", {})
        if not isinstance(models, dict) or not models:
            errors.append("'models' block must be a non-empty mapping.")
        else:
            for model_name, model_def in models.items():
                fields = model_def.get("fields", {}) if isinstance(model_def, dict) else {}
                if not fields:
                    warnings.append(f"Model '{model_name}' has no fields defined.")
                    continue

                # Type validation
                for field_name, field_def in fields.items():
                    if not isinstance(field_def, dict):
                        errors.append(f"Field '{field_name}' in model '{model_name}' must be a mapping.")
                        continue
                    ftype = field_def.get("type", "")
                    if ftype and ftype not in _VALID_TYPES:
                        warnings.append(
                            f"Field '{field_name}' has unrecognized type '{ftype}'. "
                            f"Valid types: {sorted(_VALID_TYPES)}."
                        )

                # Expected columns check
                if expected_columns:
                    defined = set(fields.keys())
                    for col in expected_columns:
                        if col not in defined:
                            errors.append(
                                f"Column '{col}' from input schema is missing in model '{model_name}'."
                            )

        # 6. Quality block warnings
        quality = parsed.get("quality", {})
        if not quality:
            warnings.append("No 'quality' block defined. Consider adding data quality rules.")
        elif isinstance(quality, dict):
            rules = quality.get("rules", [])
            if not rules:
                warnings.append("'quality.rules' is empty. No quality rules defined.")
            else:
                # Warn about columns with no quality rule
                if expected_columns and isinstance(models, dict):
                    for model_def in models.values():
                        if not isinstance(model_def, dict):
                            continue
                        fields = model_def.get("fields", {})
                        covered = {r.get("column") for r in rules if isinstance(r, dict)}
                        for col in (expected_columns or []):
                            if col not in covered:
                                warnings.append(f"Column '{col}' has no quality rule defined.")

        # 7. SLA block
        sla = parsed.get("sla", {})
        if not sla:
            warnings.append("No 'sla' block defined. Consider adding SLA definitions.")

        is_valid = len(errors) == 0
        logger.info("contract_validated", is_valid=is_valid, errors=len(errors), warnings=len(warnings))
        return ValidationResult(is_valid=is_valid, errors=errors, warnings=warnings)
