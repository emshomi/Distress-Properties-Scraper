"""
Source health tracker.

Maintains the audit.source_health row for each scraper, tracking
consecutive_failures and flipping is_healthy to False once the threshold
is crossed. Used by /status to surface "this scraper is broken" warnings.

=== is_healthy MUST AGREE WITH THE DIGEST (2026-08-02) ===
Two things read this table and they used to disagree.

scripts/health_alert.py — the daily digest — deliberately IGNORES is_healthy
and calls a source BROKEN on `consecutive_failures > 0`, i.e. one failure.
Its header explains why: it would rather flag a borderline source than let a
silent failure hide.

/status reads is_healthy, which stayed True until THREE consecutive failures.
So for a daily scraper there was a two-day window where the digest said broken
and the status page said healthy, about the same source, on the same data. A
status page that is optimistic about a feed which is actively failing is worse
than no status page: it actively reassures.

UNHEALTHY_THRESHOLD is now 1, so both consumers mean the same thing by
"healthy". See the constant for why flapping is the right trade.
"""

from __future__ import annotations

from datetime import datetime, timezone

from src.db.supabase_client import audit_table
from src.models.audit import SourceHealth, SourceHealthUpdate
from src.utils.logger import logger

# Number of consecutive failures before a source is marked unhealthy.
#
# 1, not 3 (changed 2026-08-02). One failure marks the source unhealthy, which
# is what scripts/health_alert.py has always meant by broken.
#
# The objection to 1 is flapping: a source that fails once transiently and
# recovers on its next run flips False then True. hennepin_sheriff did exactly
# that on 2026-08-02 — 503 records fetched, ONE Supabase write timeout, clean
# on the next run.
#
# Accepted, because the flap is self-correcting and the alternative is not.
# record_success() resets the counter and the flag, so a transient failure is
# visible for exactly one run cycle. At a threshold of 3, a genuinely broken
# DAILY scraper reads healthy for two more days — and the sheriff feeds are
# where redemption expiry dates come from, so two days of false reassurance is
# two days of owners potentially seeing a stale deadline.
#
# A brief false alarm costs a second glance. A silent window costs the thing
# this table exists to prevent.
UNHEALTHY_THRESHOLD: int = 1


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
    """Mark a scraper as having completed successfully. Resets consecutive failures.

    IMPORTANT: on success we CLEAR notes (empty string) rather than leaving them
    None. Because _upsert excludes None fields, passing None would leave whatever
    error message was last written lingering on a now-healthy row -- a stale note
    that misleads both the /status dashboard and the health-digest alert (a
    recovered source kept showing an old 404 / "writes failed" message for weeks).
    An explicit empty string overwrites and clears it. If a caller passes real
    notes, we honor them.
    """
    now = datetime.now(timezone.utc)
    payload = SourceHealthUpdate(
        source_name=source_name,
        last_successful_run_at=now,
        consecutive_failures=0,
        is_healthy=True,
        notes=notes if notes is not None else "",
        updated_at=now,
    )
    _upsert(payload)


def record_failure(source_name: str, notes: str | None = None) -> None:
    """
    Increment consecutive_failures for a scraper and flip is_healthy to False
    so /status surfaces the warning.

    With UNHEALTHY_THRESHOLD = 1 this now fires on the FIRST failure, matching
    what the daily digest already reports. The next successful run clears both
    the counter and the flag.
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
