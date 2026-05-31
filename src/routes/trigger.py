"""
Admin-protected manual scraper trigger endpoints.

Two trigger styles:

  POST /trigger/{scraper_name}
      Synchronous — runs the scraper and waits for it to finish before
      returning. Good for fast scrapers (Saint Paul, small test runs).
      Accepts ?max_records=N to limit fetch (ArcGIS scrapers only).

  POST /trigger-async/{scraper_name}
      Fire-and-forget — starts the scraper in a background task and returns
      immediately with a run_id. Good for LONG scrapes (Hennepin's 448K
      parcels, ~18 min) where holding an HTTP connection open the whole time
      is fragile. Poll /status to watch progress.
      Accepts ?max_records=N too.
"""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, HTTPException, Path, Query, status as http_status

from src.middleware.auth import AdminKeyRequired
from src.scrapers.anoka_sheriff import AnokaSheriffScraper
from src.scrapers.base_scraper import BaseScraper
from src.scrapers.dakota_sheriff import DakotaSheriffScraper
from src.scrapers.hennepin_parcels import HennepinParcelsScraper
from src.scrapers.hennepin_sheriff import HennepinSheriffScraper
from src.scrapers.hennepin_tax_roll import HennepinTaxRollScraper
from src.scrapers.mcro_probate import McroProbateScraper
from src.scrapers.mpls_311 import MplsThreeOneOneScraper
from src.scrapers.mpls_vbr import MplsVacantBuildingScraper
from src.scrapers.ramsey_sheriff import RamseySheriffScraper
from src.scrapers.saint_paul_vacant import SaintPaulVacantBuildingScraper
from src.scrapers.tax_forfeit import TaxForfeitScraper
from src.scrapers.usps_vacancy import UspsVacancyScraper
from src.utils.errors import (
    ScraperAlreadyRunningError,
    ScraperDisabledError,
    ScraperNotFoundError,
    ServiceError,
    error_envelope,
    success_envelope,
)
from src.utils.logger import logger


# Scraper registry — name → class
_SCRAPER_REGISTRY: dict[str, type[BaseScraper]] = {
    "mpls_311": MplsThreeOneOneScraper,
    "hennepin_sheriff": HennepinSheriffScraper,
    "hennepin_parcels": HennepinParcelsScraper,
    "hennepin_tax_roll": HennepinTaxRollScraper,
    "dakota_sheriff": DakotaSheriffScraper,
    "anoka_sheriff": AnokaSheriffScraper,
    "ramsey_sheriff": RamseySheriffScraper,
    "mpls_vbr": MplsVacantBuildingScraper,
    "saint_paul_vacant": SaintPaulVacantBuildingScraper,
    "mcro_probate": McroProbateScraper,
    "usps_vacancy": UspsVacancyScraper,
    "tax_forfeit": TaxForfeitScraper,
}

# Track background tasks so they aren't garbage-collected mid-run.
# Keyed by scraper_name → asyncio.Task.
_BACKGROUND_TASKS: dict[str, asyncio.Task] = {}


router = APIRouter(tags=["trigger"])


def _resolve_scraper(scraper_name: str) -> type[BaseScraper]:
    scraper_class = _SCRAPER_REGISTRY.get(scraper_name)
    if scraper_class is None:
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail=error_envelope(
                ScraperNotFoundError(
                    f"No scraper registered with name '{scraper_name}'",
                    context={
                        "scraper_name": scraper_name,
                        "available_scrapers": sorted(_SCRAPER_REGISTRY.keys()),
                    },
                )
            ),
        )
    return scraper_class


@router.post(
    "/trigger/{scraper_name}",
    status_code=http_status.HTTP_200_OK,
    summary="Manually trigger a scraper run (synchronous)",
    dependencies=[AdminKeyRequired],
)
async def trigger_scraper(
    scraper_name: str = Path(
        ...,
        min_length=1,
        max_length=100,
        pattern=r"^[a-z][a-z0-9_]*$",
    ),
    max_records: int | None = Query(
        default=None,
        ge=1,
        le=1_000_000,
        description=(
            "Optional: limit number of records fetched. Used for test runs "
            "on large ArcGIS datasets. Only honored by ArcGIS scrapers."
        ),
    ),
) -> dict[str, Any]:
    """Trigger a scraper run on demand and wait for it to finish."""
    scraper_class = _resolve_scraper(scraper_name)

    logger.info(
        "Manual trigger received (sync)",
        scraper_name=scraper_name,
        max_records=max_records,
    )

    try:
        scraper = scraper_class()
        if max_records is not None and hasattr(scraper, "_max_records_override"):
            scraper._max_records_override = max_records  # type: ignore[attr-defined]

        metadata: dict[str, Any] = {"trigger_source": "trigger_endpoint"}
        if max_records is not None:
            metadata["max_records"] = max_records

        result = await scraper.run(trigger="manual", metadata=metadata)
    except ScraperDisabledError as exc:
        raise HTTPException(
            status_code=http_status.HTTP_403_FORBIDDEN,
            detail=error_envelope(exc),
        ) from exc
    except ScraperAlreadyRunningError as exc:
        raise HTTPException(
            status_code=http_status.HTTP_409_CONFLICT,
            detail=error_envelope(exc),
        ) from exc
    except ServiceError as exc:
        logger.exception(
            "Manual trigger service error",
            scraper_name=scraper_name,
            error_type=type(exc).__name__,
        )
        raise HTTPException(
            status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=error_envelope(exc),
        ) from exc
    except Exception as exc:
        logger.exception(
            "Manual trigger unexpected error",
            scraper_name=scraper_name,
            error_type=type(exc).__name__,
        )
        raise HTTPException(
            status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "type": type(exc).__name__,
                "message": f"Internal error: {exc}",
                "scraper_name": scraper_name,
            },
        ) from exc

    logger.info(
        "Manual trigger complete (sync)",
        scraper_name=scraper_name,
        run_id=result.run_id,
        status=result.status,
        duration_seconds=result.duration_seconds,
    )

    return success_envelope({
        "scraper_name": result.scraper_name,
        "run_id": result.run_id,
        "status": result.status,
        "duration_seconds": result.duration_seconds,
        "records_fetched": result.records_fetched,
        "records_new": result.records_new,
        "records_updated": result.records_updated,
        "records_failed": result.records_failed,
        "error_message": result.error_message,
    })


async def _run_scraper_background(
    scraper_class: type[BaseScraper],
    scraper_name: str,
    max_records: int | None,
) -> None:
    """
    Run a scraper to completion in the background.

    Exceptions are logged (not raised) — there's no client waiting for a
    response. The run's outcome is recorded in audit.scraper_runs and visible
    via /status.
    """
    try:
        scraper = scraper_class()
        if max_records is not None and hasattr(scraper, "_max_records_override"):
            scraper._max_records_override = max_records  # type: ignore[attr-defined]

        metadata: dict[str, Any] = {"trigger_source": "trigger_async_endpoint"}
        if max_records is not None:
            metadata["max_records"] = max_records

        result = await scraper.run(trigger="manual", metadata=metadata)
        logger.info(
            "Background scraper run complete",
            scraper_name=scraper_name,
            run_id=result.run_id,
            status=result.status,
            duration_seconds=round(result.duration_seconds, 2),
            records_new=result.records_new,
            records_failed=result.records_failed,
        )
    except Exception as exc:
        logger.exception(
            "Background scraper run failed",
            scraper_name=scraper_name,
            error_type=type(exc).__name__,
        )
    finally:
        # Remove ourselves from the task registry
        _BACKGROUND_TASKS.pop(scraper_name, None)


@router.post(
    "/trigger-async/{scraper_name}",
    status_code=http_status.HTTP_202_ACCEPTED,
    summary="Trigger a scraper run in the background (fire-and-forget)",
    dependencies=[AdminKeyRequired],
)
async def trigger_scraper_async(
    scraper_name: str = Path(
        ...,
        min_length=1,
        max_length=100,
        pattern=r"^[a-z][a-z0-9_]*$",
    ),
    max_records: int | None = Query(
        default=None,
        ge=1,
        le=1_000_000,
        description="Optional record cap (ArcGIS scrapers only).",
    ),
) -> dict[str, Any]:
    """
    Start a scraper in the background and return immediately.

    Use this for long scrapes (e.g., Hennepin's 448K parcels) where keeping
    an HTTP connection open for ~18 minutes is fragile. Poll GET /status to
    watch progress (parcel_count climbs as the scrape writes pages).
    """
    scraper_class = _resolve_scraper(scraper_name)

    # Don't start a second background run of the same scraper
    existing = _BACKGROUND_TASKS.get(scraper_name)
    if existing is not None and not existing.done():
        raise HTTPException(
            status_code=http_status.HTTP_409_CONFLICT,
            detail=error_envelope(
                ScraperAlreadyRunningError(
                    f"Scraper '{scraper_name}' already running in background",
                    source=scraper_name,
                    context={"scraper_name": scraper_name},
                )
            ),
        )

    logger.info(
        "Background trigger received",
        scraper_name=scraper_name,
        max_records=max_records,
    )

    task = asyncio.create_task(
        _run_scraper_background(scraper_class, scraper_name, max_records)
    )
    _BACKGROUND_TASKS[scraper_name] = task

    return success_envelope({
        "scraper_name": scraper_name,
        "status": "started",
        "message": (
            "Scraper started in background. Poll GET /status to watch "
            "progress — parcel_count and scraper_runs will update as it runs."
        ),
        "max_records": max_records,
    })


@router.get(
    "/trigger",
    status_code=http_status.HTTP_200_OK,
    summary="List scrapers available for manual triggering",
    dependencies=[AdminKeyRequired],
)
async def list_available_scrapers() -> dict[str, Any]:
    """Return the list of registered scrapers."""
    scrapers_info = [
        {
            "name": name,
            "class": cls.__name__,
            "signal_type": getattr(cls, "signal_type", None),
            "running_in_background": (
                name in _BACKGROUND_TASKS
                and not _BACKGROUND_TASKS[name].done()
            ),
        }
        for name, cls in sorted(_SCRAPER_REGISTRY.items())
    ]

    return success_envelope({
        "scrapers": scrapers_info,
        "count": len(scrapers_info),
    })


__all__ = ["router"]
