"""
FastAPI application entry point — the file Railway invokes to start
the service.

uvicorn discovers the app via: src.main:app
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from src.config import settings
from src.db.supabase_client import ping_supabase
from src.routes import (
    access,
    admin,
    ai,
    connect,
    discover,
    health,
    marketplace,
    properties,
    saved_searches,
    status,
    trigger,
)
from src.scheduler.cron import start_scheduler, stop_scheduler
from src.utils.errors import ServiceError, error_envelope
from src.utils.logger import logger


# ============================================================
# OPENAPI METADATA
# ============================================================

_API_TITLE: str = "Distress Properties Scraper Service"
_API_DESCRIPTION: str = """
Background scraper service for Minnesota distressed-property signals.

This service runs daily/weekly/monthly cron jobs that pull data from:
  - Minneapolis 311 (code violations)
  - Hennepin & Ramsey County sheriff offices (foreclosure sales)
  - Minneapolis VBR + Saint Paul DSI (vacant buildings)
  - MN Court Records Online (probate filings)
  - HUD/USPS Vacancy Indicator
  - MN tax-forfeit county pages

## Authentication

  - GET  /health  → no auth (Railway probe)
  - GET  /status  → no auth (operator dashboard)
  - GET  /trigger → X-Admin-Key required
  - POST /trigger/{scraper_name} → X-Admin-Key required
""".strip()

_API_VERSION: str = "0.1.0"


# ============================================================
# LIFESPAN HANDLER
# ============================================================


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """
    FastAPI lifespan handler — runs once at startup and once at shutdown.

    Startup:
      1. Log service starting
      2. Validate Supabase connectivity (fail-soft)
      3. Start the APScheduler
    Shutdown:
      1. Stop the APScheduler
    """
    # ----- STARTUP -----
    logger.info(
        "Service starting",
        environment=settings.environment,
        api_version=_API_VERSION,
        log_level=settings.log_level,
    )

    try:
        ping_supabase()
        logger.info("Supabase connectivity verified")
    except Exception as e:
        logger.error(
            "Supabase ping failed during startup — service will start anyway",
            error_type=type(e).__name__,
            error_message=str(e),
            hint="Scrapers will fail until Supabase is reachable",
        )

    try:
        start_scheduler()
        logger.info("Scheduler started successfully")
    except Exception as e:
        logger.exception(
            "Scheduler failed to start — service will start anyway",
            error_type=type(e).__name__,
        )

    logger.info("Service ready", environment=settings.environment)

    yield

    # ----- SHUTDOWN -----
    logger.info("Service shutting down")

    try:
        stop_scheduler()
    except Exception as e:
        logger.exception(
            "Error stopping scheduler during shutdown",
            error_type=type(e).__name__,
        )

    logger.info("Service shutdown complete")


# ============================================================
# APPLICATION INSTANCE
# ============================================================

app = FastAPI(
    title=_API_TITLE,
    description=_API_DESCRIPTION,
    version=_API_VERSION,
    lifespan=lifespan,
    # Public API docs are disabled — the auto-generated schema would expose
    # every endpoint, field, and data source. Set any of these back to a path
    # string to re-enable (e.g. for local debugging).
    openapi_url=None,
    docs_url=None,
    redoc_url=None,
)

# ============================================================
# CORS
# ============================================================

_cors_allowed_origins: list[str] = [
    "https://govire.com",
    "https://www.govire.com",
    "https://distress-properties-frontend.vercel.app",
]

# Optional override via FRONTEND_ORIGIN env var (e.g. a Vercel preview URL)
if settings.frontend_origin:
    _cors_allowed_origins.append(str(settings.frontend_origin))

if settings.environment != "production":
    _cors_allowed_origins.extend([
        "http://localhost:3000",
        "http://localhost:3001",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:3001",
    ])

if not _cors_allowed_origins:
    logger.warning(
        "No CORS origins configured",
        hint="Set FRONTEND_ORIGIN if a frontend will call this API",
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_allowed_origins,
    allow_credentials=False,
    # DELETE added 2026-08-11 for DELETE /saved-searches/{id}. Without it the
    # browser fails the preflight and blocks the request before it is sent —
    # no status code, no server log, a bare "CORS error" in devtools. Same
    # trap the X-Connect-Session header note below describes.
    allow_methods=["GET", "POST", "DELETE", "HEAD", "OPTIONS"],
    # X-Connect-Session is what makes POST /connect/raise-hand a preflighted
    # request. Omitting it here fails the OPTIONS check, so the browser blocks
    # the POST before sending it — no status code, no server-side log, and a
    # bare "CORS error" in devtools.
    allow_headers=[
        "Content-Type",
        "X-Admin-Key",
        "X-Access-Key",
        "X-Connect-Session",
        "Accept",
        "Authorization",
    ],
    max_age=3600,
)

# ============================================================
# ROUTERS
# ============================================================

app.include_router(health.router)
app.include_router(status.router)
app.include_router(trigger.router)
app.include_router(discover.router)
app.include_router(properties.router)
app.include_router(access.router)
app.include_router(admin.router)
app.include_router(ai.router)
# Govire Connect — the public redemption calculator. Deliberately NOT behind
# resolve_tier or redact_property: different consent basis, and gating it
# would kill the organic search that is the whole point.
app.include_router(connect.router)
# Govire marketplace — the INVESTOR side of Connect. Separate router from
# connect.py because it is the opposite side of the wall: app_auth JWT
# identity instead of an owner magic-link session, and its job is to WITHHOLD
# the numbers connect.py exists to show. Deliberately NOT behind resolve_tier
# either — resolve_tier never raises and falls through to "free", which is
# right for browsing locked cards and wrong for anything that writes. It uses
# resolve_investor, which 401s.
app.include_router(marketplace.router)
# Saved searches — an investor's standing criteria plus an alert cadence.
# Uses resolve_tier for the tier claim, but every endpoint calls
# _require_user() first, which 401s when the context carries no user_id.
# resolve_tier alone would not be enough here for the reason given above:
# it falls through to "free" rather than raising, and these endpoints write.
app.include_router(saved_searches.router)


# ============================================================
# GLOBAL EXCEPTION HANDLERS
# ============================================================


@app.exception_handler(StarletteHTTPException)
async def starlette_http_exception_handler(
    request: Request, exc: StarletteHTTPException
) -> JSONResponse:
    """Handle HTTPException raised explicitly by route handlers."""
    if isinstance(exc.detail, dict):
        body = exc.detail
    else:
        body = error_envelope(
            ServiceError(
                str(exc.detail),
                context={"status_code": exc.status_code},
            )
        )

    return JSONResponse(status_code=exc.status_code, content=body)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """Handle Pydantic validation errors (422)."""
    return JSONResponse(
        status_code=422,
        content={
            "success": False,
            "error": {
                "type": "ValidationError",
                "message": "Request validation failed",
                "details": exc.errors(),
            },
        },
    )


@app.exception_handler(ServiceError)
async def service_error_handler(
    request: Request, exc: ServiceError
) -> JSONResponse:
    """Handle uncaught ServiceError instances."""
    logger.exception(
        "Unhandled ServiceError escaped to global handler",
        path=str(request.url.path),
        method=request.method,
        error_type=type(exc).__name__,
    )
    return JSONResponse(status_code=500, content=error_envelope(exc))


@app.exception_handler(Exception)
async def unexpected_exception_handler(
    request: Request, exc: Exception
) -> JSONResponse:
    """Catch-all handler. Logs detail; returns generic 500 to client."""
    logger.exception(
        "Unhandled exception escaped to global handler",
        path=str(request.url.path),
        method=request.method,
        error_type=type(exc).__name__,
    )

    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "error": {
                "type": "InternalServerError",
                "message": "An unexpected error occurred. Check service logs for details.",
            },
        },
    )


# ============================================================
# ROOT ENDPOINT
# ============================================================


@app.get("/", include_in_schema=False)
async def root() -> dict[str, str]:
    """Root endpoint — friendly message and docs pointer."""
    return {
        "service": "distress-properties-scraper",
        "version": _API_VERSION,
        "status": "/status",
        "health": "/health",
    }

# Access gate router registered above (access.router).
__all__ = ["app"]
