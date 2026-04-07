from __future__ import annotations

"""
Thin wrapper around the datacontract-cli tool for optional extended validation.
Falls back gracefully if the CLI is not installed.
"""

import shutil
import subprocess
import tempfile
import os
from dataclasses import dataclass, field

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class CLIValidationResult:
    available: bool
    valid: bool = True
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    raw_output: str = ""


class DataContractCLI:
    """Wraps the `datacontract` CLI binary for deep spec validation."""

    def __init__(self) -> None:
        self._binary = shutil.which("datacontract")
        if not self._binary:
            logger.warning("datacontract_cli_not_found", hint="pip install datacontract-cli")

    @property
    def available(self) -> bool:
        return self._binary is not None

    def validate(self, yaml_content: str) -> CLIValidationResult:
        if not self.available:
            return CLIValidationResult(
                available=False,
                warnings=["datacontract-cli not installed; skipping deep CLI validation."],
            )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False, encoding="utf-8") as tmp:
            tmp.write(yaml_content)
            tmp_path = tmp.name

        try:
            result = subprocess.run(
                [self._binary, "lint", tmp_path],
                capture_output=True,
                text=True,
                timeout=30,
            )
            success = result.returncode == 0
            errors: list[str] = []
            warnings: list[str] = []

            if not success:
                for line in result.stdout.splitlines() + result.stderr.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    if "error" in line.lower() or "fail" in line.lower():
                        errors.append(line)
                    elif "warn" in line.lower():
                        warnings.append(line)
                    else:
                        errors.append(line)

            return CLIValidationResult(
                available=True,
                valid=success,
                errors=errors,
                warnings=warnings,
                raw_output=result.stdout + result.stderr,
            )
        except subprocess.TimeoutExpired:
            return CLIValidationResult(
                available=True,
                valid=False,
                errors=["datacontract-cli validation timed out after 30s."],
            )
        except Exception as exc:
            logger.error("datacontract_cli_error", error=str(exc))
            return CLIValidationResult(
                available=True,
                valid=False,
                errors=[f"CLI execution error: {exc}"],
            )
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
