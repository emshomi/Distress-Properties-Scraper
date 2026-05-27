"""
Source health tracker.

Maintains the audit.source_health row for each scraper, tracking
consecutive_failures and flipping is_healthy to False once the threshold
is crossed. Used by /status to surface "this scraper is broken" warnings.
"""

from __future__ import annotations

from datetime import datetime, timezone

from src.db.supabase_client import audit_table
from src.models.audit import SourceHealth, SourceHealthUpdate
from src.utils.logger import logger

# Number of consecutive failures before a source is marked unhealthy
UNHEALTHY_THRESHOLD: int = 3


# ============================================================
# READ
# ============================================================


def get_health(source_name: str) -> SourceHealth | None:
    """Fetch the source_health row for a given source, or None if not yet tracked."""
    try:
        result = (
            audit_table("source_health")
            .select("*")
            .eq("source_name", source_name)
            .limit(1)
            .execute()
        )
        if result.data and len(result.data) > 0:
            return SourceHealth.model_validate(result.data[0])
    except Exception as e:
        logger.warning(
            "Failed to fetch source_health",
            source=source_name,
            error=str(e),
        )
    return None


def get_all_health() -> list[SourceHealth]:
    """Fetch all source_health rows. Used by /status to build the dashboard."""
    try:
        result = audit_table("source_health").select("*").execute()
        return [SourceHealth.model_validate(row) for row in (result.data or [])]
    except Exception as e:
        logger.warning("Failed to fetch all source_health", error=str(e))
        return []


# ============================================================
# WRITE
# ============================================================


def _upsert(payload: SourceHealthUpdate) -> None:
    """Upsert a source_health row keyed on source_name."""
    try:
        audit_table("source_health").upsert(
            payload.model_dump(mode="json", exclude_none=True),
            on_conflict="source_name",
        ).execute()
    except Exception as e:
        logger.warning(
            "Failed to upsert source_health",
            source=payload.source_name,
            error=str(e),
        )


def record_success(source_name: str, notes: str | None = None) -> None:
    """Mark a scraper as having completed successfully. Resets consecutive failures."""
    now = datetime.now(timezone.utc)
    payload = SourceHealthUpdate(
        source_name=source_name,
        last_successful_run_at=now,
        consecutive_failures=0,
        is_healthy=True,
        notes=notes,
        updated_at=now,
    )
    _upsert(payload)


def record_failure(source_name: str, notes: str | None = None) -> None:
    """
    Increment consecutive_failures for a scraper. If we cross the threshold,
    flip is_healthy to False so /status surfaces the warning.
    """
    existing = get_health(source_name)
    new_consecutive = (existing.consecutive_failures + 1) if existing else 1
    is_healthy = new_consecutive < UNHEALTHY_THRESHOLD

    now = datetime.now(timezone.utc)
    payload = SourceHealthUpdate(
        source_name=source_name,
        last_successful_run_at=existing.last_successful_run_at if existing else None,
        last_failed_run_at=now,
        consecutive_failures=new_consecutive,
        is_healthy=is_healthy,
        notes=notes,
        updated_at=now,
    )
    _upsert(payload)

    if not is_healthy:
        logger.warning(
            "Source crossed unhealthy threshold",
            source=source_name,
            consecutive_failures=new_consecutive,
            threshold=UNHEALTHY_THRESHOLD,
        )


def record_partial(source_name: str, notes: str | None = None) -> None:
    """
    Mark a partial run — some records succeeded, some failed.

    Partial runs don't increment consecutive_failures but they do update
    last_successful_run_at because something useful was accomplished.
    """
    now = datetime.now(timezone.utc)
    payload = SourceHealthUpdate(
        source_name=source_name,
        last_successful_run_at=now,
        consecutive_failures=0,
        is_healthy=True,
        notes=notes or "partial success — see scraper_errors for details",
        updated_at=now,
    )
    _upsert(payload)


__all__ = [
    "UNHEALTHY_THRESHOLD",
    "get_health",
    "get_all_health",
    "record_success",
    "record_failure",
    "record_partial",
]
