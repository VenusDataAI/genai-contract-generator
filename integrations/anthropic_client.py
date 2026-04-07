from __future__ import annotations

import os
import time
from dataclasses import dataclass

import structlog
from anthropic import Anthropic, APIError, APIStatusError
from dotenv import load_dotenv

load_dotenv()

logger = structlog.get_logger(__name__)

_MODEL = "claude-sonnet-4-20250514"
_MAX_TOKENS = 4096
_SYSTEM_PROMPT = (
    "You are a data governance expert. "
    "You output only valid YAML data contracts following the datacontract.com 0.9.3 specification."
)


@dataclass
class AnthropicResponse:
    content: str
    model: str
    input_tokens: int
    output_tokens: int
    latency_ms: float


class AnthropicClient:
    def __init__(self) -> None:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise EnvironmentError("ANTHROPIC_API_KEY environment variable is not set.")
        self._client = Anthropic(api_key=api_key)

    def complete(self, prompt: str) -> AnthropicResponse:
        log = logger.bind(model=_MODEL)
        log.info("anthropic_request_start", prompt_length=len(prompt))
        start = time.perf_counter()
        try:
            message = self._client.messages.create(
                model=_MODEL,
                max_tokens=_MAX_TOKENS,
                system=_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
        except APIStatusError as exc:
            log.error("anthropic_api_status_error", status_code=exc.status_code, message=str(exc))
            raise
        except APIError as exc:
            log.error("anthropic_api_error", message=str(exc))
            raise

        latency_ms = (time.perf_counter() - start) * 1000
        content = message.content[0].text if message.content else ""
        response = AnthropicResponse(
            content=content,
            model=message.model,
            input_tokens=message.usage.input_tokens,
            output_tokens=message.usage.output_tokens,
            latency_ms=round(latency_ms, 2),
        )
        log.info(
            "anthropic_request_complete",
            input_tokens=response.input_tokens,
            output_tokens=response.output_tokens,
            latency_ms=response.latency_ms,
        )
        return response
