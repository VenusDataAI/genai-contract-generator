from __future__ import annotations

import os

from fastapi import APIRouter
from models.output_models import HealthResponse

router = APIRouter()

_MODEL = "claude-sonnet-4-20250514"


@router.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check() -> HealthResponse:
    return HealthResponse(
        status="ok",
        version="0.1.0",
        anthropic_configured=bool(os.environ.get("ANTHROPIC_API_KEY")),
    )
