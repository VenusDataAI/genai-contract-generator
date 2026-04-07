from __future__ import annotations

import time
from collections import defaultdict, deque
from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

import structlog

logger = structlog.get_logger(__name__)

_MAX_REQUESTS = 10
_WINDOW_SECONDS = 60


class RateLimiterMiddleware(BaseHTTPMiddleware):
    """
    Sliding-window in-memory rate limiter.
    Limits each IP to _MAX_REQUESTS per _WINDOW_SECONDS.
    """

    def __init__(self, app, max_requests: int = _MAX_REQUESTS, window_seconds: int = _WINDOW_SECONDS) -> None:
        super().__init__(app)
        self._max_requests = max_requests
        self._window = window_seconds
        # ip -> deque of request timestamps
        self._store: dict[str, deque[float]] = defaultdict(deque)

    def _get_ip(self, request: Request) -> str:
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()
        return request.client.host if request.client else "unknown"

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Only rate-limit API routes
        if not request.url.path.startswith("/api/"):
            return await call_next(request)

        ip = self._get_ip(request)
        now = time.monotonic()
        window_start = now - self._window

        queue = self._store[ip]
        # Evict timestamps outside the window
        while queue and queue[0] < window_start:
            queue.popleft()

        if len(queue) >= self._max_requests:
            oldest = queue[0]
            retry_after = int(self._window - (now - oldest)) + 1
            logger.warning("rate_limit_exceeded", ip=ip, count=len(queue))
            return JSONResponse(
                status_code=429,
                content={
                    "detail": "Too many requests. Please slow down.",
                    "retry_after_seconds": retry_after,
                },
                headers={"Retry-After": str(retry_after)},
            )

        queue.append(now)
        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(self._max_requests)
        response.headers["X-RateLimit-Remaining"] = str(self._max_requests - len(queue))
        response.headers["X-RateLimit-Reset"] = str(int(now + self._window))
        return response
