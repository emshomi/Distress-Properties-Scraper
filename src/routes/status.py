"""
Public operator dashboard endpoint at GET /status.

Unlike /health (shallow), this is the DEEP check that queries Supabase,
reports per-scraper health, last run times, and database freshness.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, status as http_status

from src.config import settings
from src.db.supabase_client import core_table, signals_table
from src.services.audit_logger import get_latest_run_for_scraper
from src.services.source_health_tracker import UNHEALTHY_THRESHOLD, get_all_health
from src.utils.errors import success_envelope
from src.utils.logger import logger


# Authoritative list of scrapers in the system
_KNOWN_SCRAPERS: tuple[dict[str, str], ...] = (
    {"name": "mpls_311", "display_name": "Minneapolis 311 Code Violations",
     "schedule_hint": "Daily 06:00 CST", "signal_type": "code_violation"},
    {"name": "hennepin_sheriff", "display_name": "Hennepin County Sheriff Sales",
     "schedule_hint": "Daily 06:15 CST", "signal_type": "sheriff_sale"},
    {"name": "ramsey_sheriff", "display_name": "Ramsey County Sheriff Sales",
     "schedule_hint": "Daily 06:30 CST", "signal_type": "sheriff_sale"},
    {"name": "mpls_vbr", "display_name": "Minneapolis Vacant Building Registry",
     "schedule_hint": "Daily 07:00 CST", "signal_type": "vbr_listing"},
    {"name": "saint_paul_vacant", "display_name": "Saint Paul Vacant Buildings",
     "schedule_hint": "Daily 07:15 CST", "signal_type": "vbr_listing"},
    {"name": "mcro_probate", "display_name": "MN Court Records — Probate Filings",
     "schedule_hint": "Daily 08:00 CST (disabled by default)", "signal_type": "probate_filing"},
    {"name": "usps_vacancy", "display_name": "HUD/USPS Vacancy Indicator",
     "schedule_hint": "Weekly Sunday 02:00 CST", "signal_type": "usps_vacancy"},
    {"name": "tax_forfeit", "display_name": "MN Tax-Forfeit Properties",
     "schedule_hint": "Monthly 1st 03:00 CST", "signal_type": "tax_forfeit"},
)


router = APIRouter(tags=["status"])


@dataclass(slots=True)
class ScraperStatus:
    """Per-scraper status data."""

    name: str
    display_name: str
    schedule_hint: str
    enabled: bool
    is_healthy: bool | None
    consecutive_failures: int
    last_successful_run_at: str | None
    last_failed_run_at: str | None
    last_run_status: str | None
    last_run_at: str | None
    last_run_duration_seconds: float | None
    last_run_records_new: int | None
    last_run_records_updated: int | None
    last_run_records_failed: int | None
    notes: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "display_name": self.display_name,
            "schedule_hint": self.schedule_hint,
            "enabled": self.enabled,
            "is_healthy": self.is_healthy,
            "consecutive_failures": self.consecutive_failures,
            "last_successful_run_at": self.last_successful_run_at,
            "last_failed_run_at": self.last_failed_run_at,
            "last_run": {
                "status": self.last_run_status,
                "started_at": self.last_run_at,
                "duration_seconds": self.last_run_duration_seconds,
                "records_new": self.last_run_records_new,
                "records_updated": self.last_run_records_updated,
                "records_failed": self.last_run_records_failed,
            } if self.last_run_at else None,
            "notes": self.notes,
        }


@router.get(
    "/status",
    status_code=http_status.HTTP_200_OK,
    summary="Operator dashboard — full system status",
)
async def status_endpoint() -> dict[str, Any]:
    """Build a complete picture of the scraper service's state."""
    scrapers, scraper_warning = _build_scraper_statuses()
    summary = _build_summary(scrapers)
    database, database_warning = _build_database_section()

    response: dict[str, Any] = {
        "summary": summary,
        "scrapers": [s.to_dict() for s in scrapers],
        "database": database,
        "service": {
            "environment": settings.environment,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    }

    warnings = [w for w in (scraper_warning, database_warning) if w]
    if warnings:
        response["warnings"] = warnings

    return success_envelope(response)


def _build_scraper_statuses() -> tuple[list[ScraperStatus], str | None]:
    try:
        all_health = get_all_health()
    except Exception as e:
        logger.exception("Failed to fetch source health", error_type=type(e).__name__)
        return [], f"Could not load scraper health: {type(e).__name__}"

    health_by_name = {h.source_name: h for h in all_health}
    statuses: list[ScraperStatus] = []

    for meta in _KNOWN_SCRAPERS:
        name = meta["name"]
        enabled = settings.scraper_enabled(name)
        health = health_by_name.get(name)

        if health is None:
            statuses.append(ScraperStatus(
                name=name,
                display_name=meta["display_name"],
                schedule_hint=meta["schedule_hint"],
                enabled=enabled,
                is_healthy=None,
                consecutive_failures=0,
                last_successful_run_at=None,
                last_failed_run_at=None,
                last_run_status=None,
                last_run_at=None,
                last_run_duration_seconds=None,
                last_run_records_new=None,
                last_run_records_updated=None,
                last_run_records_failed=None,
                notes=None,
            ))
            continue

        try:
            last_run = get_latest_run_for_scraper(name)
        except Exception:
            last_run = None

        statuses.append(ScraperStatus(
            name=name,
            display_name=meta["display_name"],
            schedule_hint=meta["schedule_hint"],
            enabled=enabled,
            is_healthy=health.is_healthy,
            consecutive_failures=health.consecutive_failures,
            last_successful_run_at=_iso(health.last_successful_run_at),
            last_failed_run_at=_iso(health.last_failed_run_at),
            last_run_status=(last_run.get("status") if last_run else None),
            last_run_at=(_iso(last_run.get("started_at")) if last_run else None),
            last_run_duration_seconds=(last_run.get("duration_seconds") if last_run else None),
            last_run_records_new=(last_run.get("records_new") if last_run else None),
            last_run_records_updated=(last_run.get("records_updated") if last_run else None),
            last_run_records_failed=(last_run.get("records_failed") if last_run else None),
            notes=health.notes,
        ))

    return statuses, None


def _build_summary(scrapers: list[ScraperStatus]) -> dict[str, Any]:
    total = len(scrapers)
    healthy = unhealthy = disabled = never_run = 0

    for s in scrapers:
        if not s.enabled:
            disabled += 1
        elif s.is_healthy is None:
            never_run += 1
        elif s.is_healthy is True:
            healthy += 1
        else:
            unhealthy += 1

    return {
        "total_scrapers": total,
        "healthy_scrapers": healthy,
        "unhealthy_scrapers": unhealthy,
        "disabled_scrapers": disabled,
        "never_run_scrapers": never_run,
        "unhealthy_threshold": UNHEALTHY_THRESHOLD,
    }


def _build_database_section() -> tuple[dict[str, Any], str | None]:
    section: dict[str, Any] = {}
    warnings: list[str] = []

    try:
        result = (
            core_table("parcels")
            .select("parcel_id", count="exact")
            .limit(1)
            .execute()
        )
        section["parcel_count"] = result.count or 0
    except Exception as e:
        section["parcel_count"] = None
        warnings.append(f"parcel_count: {type(e).__name__}")

    try:
        result = (
            signals_table("distress_events")
            .select("id", count="exact")
            .limit(1)
            .execute()
        )
        section["distress_event_count"] = result.count or 0
    except Exception as e:
        section["distress_event_count"] = None
        warnings.append(f"distress_event_count: {type(e).__name__}")

    section["freshness"] = _compute_freshness()

    warning = "; ".join(warnings) if warnings else None
    return section, warning


def _compute_freshness() -> dict[str, Any]:
    freshness: dict[str, Any] = {
        "newest_signal_at": None,
        "oldest_signal_at": None,
    }

    try:
        newest = (
            signals_table("distress_events")
            .select("observed_at")
            .order("observed_at", desc=True)
            .limit(1)
            .execute()
        )
        if newest.data:
            freshness["newest_signal_at"] = newest.data[0].get("observed_at")
    except Exception:
        pass

    try:
        oldest = (
            signals_table("distress_events")
            .select("observed_at")
            .order("observed_at", desc=False)
            .limit(1)
            .execute()
        )
        if oldest.data:
            freshness["oldest_signal_at"] = oldest.data[0].get("observed_at")
    except Exception:
        pass

    return freshness


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        return value
    try:
        return str(value)
    except Exception:
        return None


__all__ = ["router"]
