"""
Public health-check endpoint at GET /health.

Railway uses this to validate every deployment. Returns 200 OK as long
as the FastAPI app is running. Does NOT validate database connectivity
or scraper sources — use /status for a richer view.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, status

from src.config import settings
from src.utils.errors import success_envelope


_STARTUP_MONOTONIC: float = time.monotonic()
_SERVICE_NAME: str = "distress-properties-scraper"
_SERVICE_VERSION: str = "0.1.0"


router = APIRouter(tags=["health"])


@router.get(
    "/health",
    status_code=status.HTTP_200_OK,
    summary="Service health check (probe)",
    description=(
        "Lightweight health check used by Railway's deployment probe. "
        "Returns 200 OK as long as the FastAPI app is running."
    ),
)
async def health_check() -> dict[str, Any]:
    """Shallow health check — fast, dependency-free."""
    uptime_seconds = time.monotonic() - _STARTUP_MONOTONIC

    return success_envelope({
        "ok": True,
        "service": _SERVICE_NAME,
        "version": _SERVICE_VERSION,
        "environment": settings.environment,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "uptime_seconds": round(uptime_seconds, 2),
    })


@router.head(
    "/health",
    status_code=status.HTTP_200_OK,
    include_in_schema=False,
)
async def health_check_head() -> None:
    """HEAD variant for orchestrators that prefer body-less probes."""
    return None


__all__ = ["router"]
