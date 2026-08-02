"""
APScheduler-based cron coordinator for the scraper service.

Registers each scraper with a specific cron expression and ensures runs
happen automatically without operator intervention.

=== WHY JOBS RUN ON A WORKER THREAD (2026-08-02) ===
AsyncIOScheduler awaits coroutine jobs directly ON THE EVENT LOOP. Every
scraper method is `async def`, but the WRITE path inside them is entirely
synchronous — the sync supabase client, audit_logger, source_health_tracker
and (in parcel_enrich) plain `requests`. None of it ever yields, so the loop
was pinned for the whole run and uvicorn could not accept a single request.

Measured live 2026-08-02. parcel_enrich_mngeo started 08:30 CDT and held the
loop until 08:50. In that window GET /health timed out at 21s, an offer POST
hung until the client gave up, and NOTHING appeared in the Railway logs for
either request, because the server never processed them. The container was
Active and healthy the entire time. The same shape had happened at 08:30 the
previous day and nobody noticed — the only reason it surfaced was that we
happened to be calling the API mid-run.

The daily cost was ~20 minutes of dead API at 08:30 plus ~10 at 06:15, on a
product whose users are homeowners in foreclosure looking up a deadline.

Fix: dispatch the whole run to a worker thread via asyncio.to_thread, and
give it its own event loop with asyncio.run. The API loop stays free. The
async parts of a scrape (base_arcgis_scraper uses httpx.AsyncClient properly)
work fine on the worker's loop — that class creates and closes its client
inside fetch(), holding no loop-bound state across runs.

The per-scraper concurrency guard moved from asyncio.Lock to threading.Lock
in base_scraper.py for exactly this reason: an asyncio.Lock guards nothing
once runs happen on different threads, and it fails silently.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import (
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MISSED,
    JobExecutionEvent,
)

from src.config import settings
from src.scrapers.base_scraper import BaseScraper
from src.scrapers.hennepin_sheriff import HennepinSheriffScraper
from src.scrapers.mcro_probate import McroProbateScraper
from src.scrapers.mpls_311 import MplsThreeOneOneScraper
from src.scrapers.mpls_vbr import MplsVacantBuildingScraper
from src.scrapers.saint_paul_vacant import SaintPaulVacantBuildingScraper
from src.scrapers.tax_forfeit import TaxForfeitScraper
from src.scrapers.usps_vacancy import UspsVacancyScraper
from src.scrapers.parcel_enrich import ParcelEnrichScraper
from src.utils.logger import logger


_SCHEDULE_TIMEZONE = settings.scheduler_timezone


_SCRAPER_SCHEDULES: tuple[tuple[type[BaseScraper], dict[str, Any]], ...] = (
    # Daily scrapers, staggered 06:00–08:00 CST
    # SHELVED 2026-07-06: Minneapolis retired the Socrata dataset (rmpv-bp76);
       # code-violation data now lives only in Tableau dashboards with no JSON/REST
       # API. No scrapable endpoint exists. Re-enable if a violations Feature
       # Service appears on services.arcgis.com/afSMGVsC7QlRK1kZ/.
       # (MplsThreeOneOneScraper, {"hour": 6, "minute": 0}),
    (HennepinSheriffScraper, {"hour": 6, "minute": 15}),
    (MplsVacantBuildingScraper, {"hour": 7, "minute": 0}),
    (SaintPaulVacantBuildingScraper, {"hour": 7, "minute": 15}),
    (McroProbateScraper, {"hour": 8, "minute": 0}),
    (ParcelEnrichScraper, {"hour": 8, "minute": 30}),

    # Weekly: USPS vacancy, Sunday 02:00 CST
    (UspsVacancyScraper, {"day_of_week": "sun", "hour": 2, "minute": 0}),

    # Monthly: tax forfeit, 1st of month 03:00 CST
    (TaxForfeitScraper, {"day": 1, "hour": 3, "minute": 0}),
)


_scheduler: AsyncIOScheduler | None = None
_JOB_ID_PREFIX: str = "scraper_"
_RUNTIME_CLASS_REGISTRY: dict[str, type[BaseScraper]] = {
    cls.__name__: cls for cls, _ in _SCRAPER_SCHEDULES
}


def _run_scraper_blocking(scraper_class: type[BaseScraper]) -> Any:
    """Run one scrape to completion on THIS thread, with its own event loop.

    Called only from a worker thread via asyncio.to_thread. asyncio.run
    creates a fresh loop, runs the coroutine, and closes the loop — so the
    scrape never touches the API's loop, and nothing loop-bound survives
    between runs.

    Returns the RunResult. Exceptions propagate to the caller, which is the
    async wrapper below; letting them out here keeps this function honest and
    the logging in one place.
    """
    scraper = scraper_class()
    return asyncio.run(
        scraper.run(
            trigger="scheduler",
            metadata={"trigger_source": "cron"},
        )
    )


async def _run_scraper_job(scraper_class_name: str) -> None:
    """APScheduler job function — dispatches the run to a worker thread.

    Still `async def` because AsyncIOScheduler awaits its jobs, but the body
    yields at `await asyncio.to_thread(...)`, which is the entire point: the
    event loop is released for the duration of the scrape instead of being
    pinned by its synchronous write path.

    asyncio.to_thread uses the default ThreadPoolExecutor, which FastAPI also
    uses for sync route handlers. Scrapers are staggered an hour apart and
    APScheduler enforces max_instances=1, so at most one long scrape occupies
    a pool thread at a time; the default pool is min(32, cpu_count + 4)
    workers. If scrapers are ever scheduled to overlap, give them a dedicated
    executor rather than letting them compete with request handling.
    """
    scraper_class = _RUNTIME_CLASS_REGISTRY.get(scraper_class_name)
    if scraper_class is None:
        logger.error(
            "Scheduled job fired with unknown scraper class",
            class_name=scraper_class_name,
        )
        return

    try:
        result = await asyncio.to_thread(_run_scraper_blocking, scraper_class)
        logger.info(
            "Scheduled scraper run complete",
            scraper=result.scraper_name,
            run_id=result.run_id,
            status=result.status,
            duration_seconds=result.duration_seconds,
            records_new=result.records_new,
            records_failed=result.records_failed,
        )
    except Exception as exc:
        logger.exception(
            "Scheduled scraper crashed unexpectedly",
            scraper_class=scraper_class_name,
            error_type=type(exc).__name__,
        )


def _on_job_executed(event: JobExecutionEvent) -> None:
    logger.debug("APScheduler job executed", job_id=event.job_id)


def _on_job_error(event: JobExecutionEvent) -> None:
    logger.error(
        "APScheduler job raised exception",
        job_id=event.job_id,
        exception_type=type(event.exception).__name__ if event.exception else None,
        exception_message=str(event.exception) if event.exception else None,
    )


def _on_job_missed(event: JobExecutionEvent) -> None:
    logger.warning(
        "APScheduler job missed its scheduled time",
        job_id=event.job_id,
        scheduled_run_time=str(event.scheduled_run_time),
    )


def start_scheduler() -> AsyncIOScheduler:
    """Start the APScheduler and register all scraper jobs."""
    global _scheduler

    if _scheduler is not None:
        logger.warning("start_scheduler called but scheduler already exists")
        return _scheduler

    logger.info(
        "Starting APScheduler",
        timezone=_SCHEDULE_TIMEZONE,
        job_count=len(_SCRAPER_SCHEDULES),
    )

    scheduler = AsyncIOScheduler(
        timezone=_SCHEDULE_TIMEZONE,
        job_defaults={
            "misfire_grace_time": 300,
            "coalesce": True,
            "max_instances": 1,
        },
    )

    scheduler.add_listener(_on_job_executed, EVENT_JOB_EXECUTED)
    scheduler.add_listener(_on_job_error, EVENT_JOB_ERROR)
    scheduler.add_listener(_on_job_missed, EVENT_JOB_MISSED)

    for scraper_class, cron_kwargs in _SCRAPER_SCHEDULES:
        job_id = f"{_JOB_ID_PREFIX}{scraper_class.source_name}"
        trigger = CronTrigger(timezone=_SCHEDULE_TIMEZONE, **cron_kwargs)

        scheduler.add_job(
            func=_run_scraper_job,
            trigger=trigger,
            args=[scraper_class.__name__],
            id=job_id,
            name=f"Scrape {scraper_class.source_name}",
            replace_existing=True,
        )

        try:
            next_run = trigger.get_next_fire_time(None, datetime.now(tz=ZoneInfo(_SCHEDULE_TIMEZONE)))
        except Exception as next_run_error:  # noqa: BLE001
            logger.warning(
                "Could not compute next fire time",
                scraper=scraper_class.source_name,
                error=str(next_run_error),
            )
            next_run = None
        logger.info(
            "Registered scheduled scraper job",
            scraper=scraper_class.source_name,
            job_id=job_id,
            cron=str(cron_kwargs),
            next_run=str(next_run) if next_run else "unknown",
        )

    scheduler.start()
    _scheduler = scheduler

    logger.info(
        "APScheduler started",
        job_count=len(scheduler.get_jobs()),
        timezone=_SCHEDULE_TIMEZONE,
    )

    return scheduler


def stop_scheduler() -> None:
    """Stop the APScheduler gracefully."""
    global _scheduler

    if _scheduler is None:
        return

    logger.info("Stopping APScheduler", active_jobs=len(_scheduler.get_jobs()))

    try:
        _scheduler.shutdown(wait=False)
    except Exception as e:
        logger.exception(
            "Error during APScheduler shutdown",
            error_type=type(e).__name__,
        )

    _scheduler = None
    logger.info("APScheduler stopped")


def get_scheduler() -> AsyncIOScheduler | None:
    """Return the active scheduler, or None if not started."""
    return _scheduler


def get_next_run_times() -> dict[str, str | None]:
    """Return {scraper_source_name: next_run_time_str}."""
    if _scheduler is None:
        return {cls.source_name: None for cls, _ in _SCRAPER_SCHEDULES}

    result: dict[str, str | None] = {}
    for cls, _ in _SCRAPER_SCHEDULES:
        job_id = f"{_JOB_ID_PREFIX}{cls.source_name}"
        job = _scheduler.get_job(job_id)
        if job is None:
            result[cls.source_name] = None
            continue
        next_run = job.next_run_time
        result[cls.source_name] = str(next_run) if next_run else None

    return result


__all__ = [
    "start_scheduler",
    "stop_scheduler",
    "get_scheduler",
    "get_next_run_times",
]
