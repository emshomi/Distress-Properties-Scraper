"""
Audit logging service.

Writes to audit.scraper_runs and audit.scraper_errors. Provides helpers
that BaseScraper uses to open a run, log per-record errors, and close
the run with final counts.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from src.db.supabase_client import audit_table
from src.models.audit import (
    ScraperErrorEntry,
    ScraperRunFinish,
    ScraperRunStart,
    ScraperRunStatus,
)
from src.utils.logger import logger


# ============================================================
# RUN LIFECYCLE
# ============================================================


def start_run(
    scraper_name: str,
    *,
    metadata: dict[str, Any] | None = None,
) -> int | None:
    """
    Open a new scraper_runs row with status='running'.

    Returns the new row's id, or None if the insert failed (logged but
    not raised — scrapers can still run without an audit trail).
    """
    payload = ScraperRunStart(
        scraper_name=scraper_name,
        started_at=datetime.now(timezone.utc),
        status="running",
        metadata=metadata,
    )

    try:
        result = audit_table("scraper_runs").insert(
            payload.model_dump(mode="json", exclude_none=True)
        ).execute()
        if result.data and len(result.data) > 0:
            run_id = result.data[0].get("id")
            logger.debug(
                "Opened scraper_runs row",
                scraper=scraper_name,
                run_id=run_id,
            )
            return run_id
    except Exception as e:
        logger.warning(
            "Failed to open scraper_runs row",
            scraper=scraper_name,
            error=str(e),
        )

    return None


def finish_run(
    run_id: int,
    *,
    status: ScraperRunStatus,
    records_fetched: int = 0,
    records_new: int = 0,
    records_updated: int = 0,
    records_failed: int = 0,
    error_message: str | None = None,
    duration_seconds: float | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Close out a scraper_runs row with final counts and status."""
    payload = ScraperRunFinish(
        status=status,
        finished_at=datetime.now(timezone.utc),
        duration_seconds=duration_seconds,
        records_fetched=records_fetched,
        records_new=records_new,
        records_updated=records_updated,
        records_failed=records_failed,
        error_message=error_message,
        metadata=metadata,
    )

    try:
        audit_table("scraper_runs").update(
            payload.model_dump(mode="json", exclude_none=True)
        ).eq("id", run_id).execute()
    except Exception as e:
        logger.warning(
            "Failed to finalize scraper_runs row",
            run_id=run_id,
            error=str(e),
        )


def mark_run_failed(
    run_id: int | None,
    *,
    error_message: str,
    duration_seconds: float | None = None,
) -> None:
    """Convenience: mark a run as failed with an error message."""
    if run_id is None:
        return
    finish_run(
        run_id,
        status="failed",
        error_message=error_message,
        duration_seconds=duration_seconds,
    )


# ============================================================
# ERROR LOGGING
# ============================================================


def log_error(
    *,
    run_id: int | None,
    error_type: str,
    error_message: str,
    raw_record: dict[str, Any] | None = None,
) -> None:
    """
    Insert a row into audit.scraper_errors for a per-record failure.

    Never raises — error logging itself should not crash a scraper.
    """
    # Truncate excessively long error messages
    if len(error_message) > 5000:
        error_message = error_message[:4997] + "..."

    payload = ScraperErrorEntry(
        scraper_run_id=run_id,
        error_type=error_type,  # type: ignore[arg-type]
        error_message=error_message,
        raw_record=raw_record,
        occurred_at=datetime.now(timezone.utc),
    )

    try:
        audit_table("scraper_errors").insert(
            payload.model_dump(mode="json", exclude_none=True)
        ).execute()
    except Exception as e:
        logger.warning(
            "Failed to insert scraper_errors row",
            run_id=run_id,
            error_type=error_type,
            insert_error=str(e),
        )


# ============================================================
# READ HELPERS (for /status endpoint)
# ============================================================


def get_latest_run_for_scraper(scraper_name: str) -> dict[str, Any] | None:
    """
    Fetch the most recent scraper_runs row for a given scraper.

    Used by /status to surface "last run" details in the dashboard.
    Returns the raw dict (not a model) for direct JSON projection.
    """
    try:
        result = (
            audit_table("scraper_runs")
            .select("*")
            .eq("scraper_name", scraper_name)
            .order("started_at", desc=True)
            .limit(1)
            .execute()
        )
        if result.data and len(result.data) > 0:
            return result.data[0]
    except Exception as e:
        logger.warning(
            "Failed to fetch latest run",
            scraper=scraper_name,
            error=str(e),
        )
    return None


__all__ = [
    "start_run",
    "finish_run",
    "mark_run_failed",
    "log_error",
    "get_latest_run_for_scraper",
]
