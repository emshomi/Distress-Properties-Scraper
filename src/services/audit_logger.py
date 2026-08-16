"""
Audit logging service.

Writes to audit.scraper_runs and audit.scraper_errors. Provides helpers
that BaseScraper uses to open a run, log per-record errors, and close
the run with final counts.

Also provides sweep_orphaned_runs(), which closes rows left behind when a
scraper's PROCESS is killed rather than its code failing. See that
function for why that case needs its own handling.
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
    """Close out a scraper_runs row with final counts and status.

    NOTE: this update is unconditional — it does not check the row's
    current status, so it will happily overwrite an already-closed run.
    Callers that might touch a finished row must filter for themselves.
    sweep_orphaned_runs does exactly that.
    """
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
# ORPHANED RUN SWEEP
# ============================================================
#
# ADDED 2026-08-16.
#
# THE FAILURE THIS EXISTS FOR
# BaseScraper._run_locked calls finish_run at step 8 and updates
# source_health at step 9. Both are Python. Neither runs if the PROCESS
# dies — a Railway redeploy, an OOM kill, a container recycle. The row
# then stays status='running' with all counters at zero, FOREVER, and
# source_health is never touched. The daily digest reads only
# source_health, so the source keeps reporting HEALTHY while producing
# nothing.
#
# Measured 2026-08-16: 20 such rows across 7 sources, the oldest from
# 2026-05-27. parcel_enrich_mngeo alone had 8, including three
# consecutive nights (08-13, 08-14, 08-15) — and appeared in the HEALTHY
# list of every digest throughout. A try/except cannot catch this;
# process death is not an exception.
#
# WHY 'interrupted' AND NOT 'failed'
# The counters on an orphaned row measure nothing — they were never
# written. Run 641 wrote 446 hennepin parcels before it died, proven by
# their shared last_observed_at fingerprint (2026-08-15 13:34:41.851716)
# — write() stamps one timestamp per run. Recording that as 'failed'
# with records_updated=0 would assert something false. 'interrupted'
# says what is actually known: the run ended without reporting.
#
# WHY ONLY THE LATEST RUN MARKS A SOURCE UNHEALTHY
# saint_paul_vacant carries 5 orphans from May and June yet runs cleanly
# every day. Calling record_failure for each would mark a working source
# broken and put five false BROKEN lines in tomorrow's digest — the
# exact noise problem that let a real failure hide for ten weeks. A
# newer completed run supersedes an older orphan, so only an orphan that
# is still a source's most recent run says anything about its health.
#
# SINGLE-REPLICA ASSUMPTION — READ BEFORE SCALING
# At startup this treats EVERY 'running' row as orphaned, because with
# one replica no run can outlive a restart. Railway is at 1 Replica
# (checked 2026-08-16). With two or more, a booting replica would close
# a sibling's in-flight run. start_run records nothing identifying the
# process, so there is no way to tell them apart today. If replicas are
# ever increased, this must gain an instance id before it is safe.


def get_orphaned_runs() -> list[dict[str, Any]]:
    """Return every scraper_runs row still marked 'running'.

    Read-only. Used by the sweep and safe to call for inspection.
    """
    try:
        result = (
            audit_table("scraper_runs")
            .select("id,scraper_name,started_at,status")
            .eq("status", "running")
            .order("started_at", desc=False)
            .execute()
        )
        return list(result.data or [])
    except Exception as e:
        logger.warning("Failed to fetch orphaned runs", error=str(e))
        return []


def sweep_orphaned_runs(*, reason: str = "process restart") -> dict[str, Any]:
    """Close orphaned 'running' rows as 'interrupted'; flag live failures.

    Returns a summary dict. Never raises — a failure to sweep must not
    stop the service from starting.
    """
    summary: dict[str, Any] = {
        "found": 0,
        "closed": 0,
        "close_failed": 0,
        "marked_unhealthy": [],
    }

    orphans = get_orphaned_runs()
    summary["found"] = len(orphans)
    if not orphans:
        logger.info("Orphaned run sweep: nothing to close")
        return summary

    # Which orphans are still their scraper's most recent run? Only those
    # say anything about current health — see the note above.
    latest_orphan_by_scraper: dict[str, int] = {}
    for row in orphans:
        name = row.get("scraper_name")
        run_id = row.get("id")
        if not name or run_id is None:
            continue
        latest = get_latest_run_for_scraper(name)
        if latest and latest.get("id") == run_id:
            latest_orphan_by_scraper[name] = run_id

    finished_at = datetime.now(timezone.utc).isoformat()

    for row in orphans:
        run_id = row.get("id")
        name = row.get("scraper_name", "?")
        started = row.get("started_at")
        if run_id is None:
            continue

        # Update ONLY these three columns. finish_run would also write
        # zeros into records_* — see its docstring — and those zeros
        # would look like a measurement rather than an absence of one.
        # The filter on status='running' makes this idempotent: a second
        # boot cannot re-close a row this one already closed.
        try:
            result = (
                audit_table("scraper_runs")
                .update({
                    "status": "interrupted",
                    "finished_at": finished_at,
                    "error_message": (
                        f"Run never reported completion ({reason}); closed by "
                        f"startup sweep. Counters were never written and are "
                        f"NOT a measurement of work done. started_at={started}"
                    ),
                })
                .eq("id", run_id)
                .eq("status", "running")
                .execute()
            )
            if result.data:
                summary["closed"] += 1
            else:
                # Already closed by someone else between read and write.
                logger.debug("Orphan already closed", run_id=run_id)
        except Exception as e:
            summary["close_failed"] += 1
            logger.warning(
                "Failed to close orphaned run",
                run_id=run_id, scraper=name, error=str(e),
            )
            continue

        if latest_orphan_by_scraper.get(name) == run_id:
            try:
                from src.services import source_health_tracker

                source_health_tracker.record_failure(
                    name,
                    notes=(
                        f"Run {run_id} interrupted — process ended before the "
                        f"run reported ({reason}). Started {started}."
                    ),
                )
                summary["marked_unhealthy"].append(name)
            except Exception as e:
                logger.warning(
                    "Failed to record health failure for interrupted run",
                    scraper=name, run_id=run_id, error=str(e),
                )

    logger.info(
        "Orphaned run sweep complete",
        found=summary["found"],
        closed=summary["closed"],
        close_failed=summary["close_failed"],
        marked_unhealthy=summary["marked_unhealthy"],
    )
    return summary


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

    created_at, NOT occurred_at (fixed 2026-08-16). The column on
    audit.scraper_errors is created_at; this passed occurred_at, which
    does not exist, so PostgREST rejected every insert and the rejection
    was swallowed by the except below. That is why the table held 0 rows
    from May until today. ScraperErrorEntry still accepts the old
    spelling via an alias, so this file and the model can ship in either
    order; the alias comes out once both are deployed.
    """
    # Truncate excessively long error messages
    if len(error_message) > 5000:
        error_message = error_message[:4997] + "..."

    payload = ScraperErrorEntry(
        scraper_run_id=run_id,
        error_type=error_type,  # type: ignore[arg-type]
        error_message=error_message,
        raw_record=raw_record,
        created_at=datetime.now(timezone.utc),
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
    "get_orphaned_runs",
    "sweep_orphaned_runs",
    "log_error",
    "get_latest_run_for_scraper",
]
