"""
Admin-protected manual scraper trigger endpoint.

Operators call POST /trigger/{scraper_name} to run a specific scraper
on demand, outside its scheduled cron window.

For ArcGIS-based scrapers, an optional `max_records` query parameter
limits the fetch — useful for test runs (e.g., loading just 100 parcels
to validate field mapping before the full 448K scrape).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Path, Query, status as http_status

from src.middleware.auth import AdminKeyRequired
from src.scrapers.base_scraper import BaseScraper
from src.scrapers.hennepin_parcels import HennepinParcelsScraper
from src.scrapers.hennepin_sheriff import HennepinSheriffScraper
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
    "ramsey_sheriff": RamseySheriffScraper,
    "mpls_vbr": MplsVacantBuildingScraper,
    "saint_paul_vacant": SaintPaulVacantBuildingScraper,
    "mcro_probate": McroProbateScraper,
    "usps_vacancy": UspsVacancyScraper,
    "tax_forfeit": TaxForfeitScraper,
}


router = APIRouter(tags=["trigger"])


@router.post(
    "/trigger/{scraper_name}",
    status_code=http_status.HTTP_200_OK,
    summary="Manually trigger a scraper run",
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
            "on large ArcGIS datasets (e.g., max_records=100 to validate "
            "field mapping before a full scrape). Only honored by ArcGIS "
            "scrapers — other scrapers ignore it."
        ),
    ),
) -> dict[str, Any]:
    """Trigger a scraper run on demand."""
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

    logger.info(
        "Manual trigger received",
        scraper_name=scraper_name,
        max_records=max_records,
    )

    try:
        scraper = scraper_class()
        # Apply max_records override if set and the scraper supports it
        if max_records is not None and hasattr(
            scraper, "_max_records_override"
        ):
            scraper._max_records_override = max_records  # type: ignore[attr-defined]

        metadata: dict[str, Any] = {"trigger_source": "trigger_endpoint"}
        if max_records is not None:
            metadata["max_records"] = max_records

        result = await scraper.run(
            trigger="manual",
            metadata=metadata,
        )
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
        "Manual trigger complete",
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
        }
        for name, cls in sorted(_SCRAPER_REGISTRY.items())
    ]

    return success_envelope({
        "scrapers": scrapers_info,
        "count": len(scrapers_info),
    })


__all__ = ["router"]
