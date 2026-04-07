from __future__ import annotations

import structlog
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path

from api.middleware.rate_limiter import RateLimiterMiddleware
from api.routes import contracts, health

load_dotenv()

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ]
)

logger = structlog.get_logger(__name__)

_UI_DIR = Path(__file__).parent.parent / "ui"


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("app_startup", version="0.1.0", docs="/docs", ui="/")
    yield


app = FastAPI(
    title="GenAI Data Contract Generator",
    description="Generate production-ready data contracts from DDL, JSON Schema, or column lists using Claude AI.",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# CORS — allow all origins for local dev tool use
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rate limiter
app.add_middleware(RateLimiterMiddleware, max_requests=10, window_seconds=60)

# Routers
app.include_router(health.router)
app.include_router(contracts.router)

# Serve UI
if _UI_DIR.exists():
    app.mount("/ui", StaticFiles(directory=str(_UI_DIR), html=True), name="ui")

    @app.get("/", include_in_schema=False)
    async def root() -> FileResponse:
        return FileResponse(str(_UI_DIR / "index.html"))
