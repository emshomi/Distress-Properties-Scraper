"""
Abstract base class for all scrapers.

Every scraper inherits from BaseScraper and implements:
  - source_name: ClassVar[str]
  - signal_type: ClassVar[str]
  - fetch() → list of raw records
  - parse() → list of typed signal models
  - write() → (records_new, records_updated, records_failed)

The base class provides the run() lifecycle:
  1. Check if scraper is enabled in settings
  2. Acquire per-class lock (prevents concurrent invocations). The lock is a
     threading.Lock, not an asyncio.Lock — runs are dispatched to worker
     threads so the synchronous write path cannot block the API event loop.
  3. Open audit.scraper_runs row
  4. Call fetch() → parse() → write()
  5. Record SOURCE FRESHNESS from what the source actually served
  6. Close the run with final counts
  7. Update source_health
  8. Release the lock

=== SOURCE FRESHNESS (step 5, added 2026-08-16) ===
Everything else in this file answers "did the scraper run?". None of it
answers "is the source still producing?" — and on 2026-08-16 six sources
were found frozen while reporting HEALTHY in every daily digest, three of
them for over a year (mpls_vbr 653 days, hennepin_tax_roll 592,
saint_paul_vacant 412).

Three measures were tried against the events table and all three failed:

  event_date gap analysis   — broken by registry sources, where event_date
                              is a historical attribute, not a publication
                              date. mpls_vbr's records start in 1998, so
                              its largest "gap" is 4,929 days and a 653-day
                              silence scores as normal.
  records_new               — counts write calls that did not raise, not
                              rows changed. mpls_vbr reports 311 new every
                              run against a table holding 311 rows total.
  observed_at               — records when WE last touched a row. The VBR
                              cleanup on 2026-08-07 rewrote all 311 rows
                              and reset the signal on a source that has
                              published nothing since 2024.

Every measure derived from our own table is contaminated by our own writes.
The one thing that is not is the SOURCE'S OWN CONTENT. So this records the
newest date present in what the source served on THIS run, per run, into
audit.scraper_runs.metadata.

A frozen source is then one whose source_max_date STOPS MOVING across runs.
No migration of ours can touch that number, because it is computed from the
fetched payload and never read back from the database. dakota_sheriff has
served a maximum of 2026-05-28 since May; hennepin_sheriff's advances every
few days. That difference is the signal.

Failure here must NEVER fail a scrape: a monitoring feature that can break
the fleet is worse than no monitoring. The whole block is wrapped, and on
any error the run proceeds exactly as it did before.
"""

from __future__ import annotations

import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, ClassVar, Generic, TypeVar

from src.config import settings
from src.services import audit_logger, source_health_tracker
from src.utils.errors import (
    ScraperAlreadyRunningError,
    ScraperDisabledError,
    ServiceError,
)
from src.utils.logger import logger

# Generic type vars for raw record and parsed signal
RAW = TypeVar("RAW")
SIGNAL = TypeVar("SIGNAL")


# ============================================================
# RUN RESULT
# ============================================================


@dataclass(slots=True)
class RunResult:
    """Summary returned by BaseScraper.run()."""

    scraper_name: str
    run_id: int | None
    status: str  # 'success' | 'partial' | 'failed' | 'skipped'
    duration_seconds: float
    records_fetched: int = 0
    records_new: int = 0
    records_updated: int = 0
    records_failed: int = 0
    error_message: str | None = None


# ============================================================
# BASE CLASS
# ============================================================


class BaseScraper(ABC, Generic[RAW, SIGNAL]):
    """
    Abstract base class for all scrapers.

    Subclasses MUST set:
      - source_name: ClassVar[str]   (e.g., 'mpls_311')
      - signal_type: ClassVar[str]   (e.g., 'code_violation')

    Subclasses MUST implement:
      - fetch(trigger) → list[RAW]
      - parse(raw_records) → list[SIGNAL]
      - write(signals) → (new, updated, failed)
    """

    source_name: ClassVar[str] = ""
    signal_type: ClassVar[str] = ""

    # Per-class lock prevents concurrent invocations of the same scraper.
    # Subclasses inherit this; each class gets its own lock.
    #
    # threading.Lock, NOT asyncio.Lock — CHANGED 2026-08-02.
    #
    # An asyncio.Lock only serialises coroutines running on ONE event loop.
    # Scraper runs are now dispatched to worker threads, each with its own
    # loop (see scheduler/cron.py and routes/trigger.py), because the write
    # path is synchronous — the sync supabase client, audit_logger and
    # source_health_tracker all block inside `async def`. Left on the API's
    # loop they made uvicorn deaf for the whole run: measured 2026-08-02,
    # parcel_enrich_mngeo held it for 20 minutes and every request in that
    # window, /health included, timed out with nothing in the logs.
    #
    # Once runs happen on different threads an asyncio.Lock guards NOTHING,
    # and it fails silently: `.locked()` would keep answering, the guard
    # would keep appearing to work, and two runs of the same scraper could
    # write concurrently. That guard is what returns 409 from
    # POST /trigger/{name} while a scheduled run is in flight, so losing it
    # means a manual trigger can collide with the cron.
    #
    # threading.Lock is loop-agnostic and thread-safe, which is exactly the
    # property needed once execution crosses threads. It is only ever used
    # via .locked() and as a context manager, so the change is contained.
    _class_lock: ClassVar[threading.Lock]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Each subclass gets its own lock
        cls._class_lock = threading.Lock()

    # ----- ABSTRACT METHODS -----

    @abstractmethod
    async def fetch(self, trigger: str) -> list[RAW]:
        """Fetch raw records from the source."""

    @abstractmethod
    async def parse(self, raw_records: list[RAW]) -> list[SIGNAL]:
        """Parse raw records into typed signal models."""

    @abstractmethod
    async def write(self, signals: list[SIGNAL]) -> tuple[int, int, int]:
        """
        Write signals to the database.

        Returns (records_new, records_updated, records_failed).
        """

    # ----- SOURCE FRESHNESS -----

    @staticmethod
    def _to_date_str(value: Any) -> str | None:
        """Coerce a date/datetime/ISO-string to a plain YYYY-MM-DD string."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, str) and len(value) >= 10:
            return value[:10]
        return None

    def source_freshness(self, signals: list[SIGNAL]) -> dict[str, Any]:
        """Summarise the date range the SOURCE served on this run.

        Reads `event_date` off each parsed signal. Signals without one — the
        parcel loaders, which carry no event date at all — yield nulls, which
        is honest: those sources have no freshness signal to report and must
        be judged some other way.

        Override in a subclass whose signal type dates itself differently.
        Must never raise; the caller guards it as well, belt and braces.
        """
        dates: list[str] = []
        for sig in signals:
            d = self._to_date_str(getattr(sig, "event_date", None))
            if d:
                dates.append(d)

        if not dates:
            return {
                "source_max_date": None,
                "source_min_date": None,
                "source_dated_signals": 0,
                "source_signals_parsed": len(signals),
            }

        return {
            # ISO strings sort lexicographically, so min/max are correct.
            "source_max_date": max(dates),
            "source_min_date": min(dates),
            "source_dated_signals": len(dates),
            "source_signals_parsed": len(signals),
        }

    # ----- LIFECYCLE -----

    async def run(
        self,
        *,
        trigger: str = "scheduler",
        metadata: dict[str, Any] | None = None,
    ) -> RunResult:
        """
        Execute the full scrape lifecycle.

        Args:
            trigger: 'scheduler' or 'manual' — recorded in audit metadata.
            metadata: Additional metadata to record on the audit run.
        """
        start_time = time.monotonic()

        # 1. Check enabled
        if not settings.scraper_enabled(self.source_name):
            if trigger == "manual":
                raise ScraperDisabledError(
                    f"Scraper '{self.source_name}' is disabled in settings",
                    source=self.source_name,
                )
            # Scheduled: silent skip
            return RunResult(
                scraper_name=self.source_name,
                run_id=None,
                status="skipped",
                duration_seconds=0.0,
                error_message="Scraper disabled in settings",
            )

        # 2. Acquire lock — non-blocking
        if self._class_lock.locked():
            raise ScraperAlreadyRunningError(
                f"Scraper '{self.source_name}' is already running",
                source=self.source_name,
                context={"scraper_name": self.source_name},
            )

        # `with`, not `async with`: threading.Lock is a plain context manager.
        # The acquire cannot block here in practice — .locked() above already
        # rejected a contended lock — but a plain `with` is still correct if
        # two threads race that check, because the loser simply waits rather
        # than running a second concurrent scrape.
        with self._class_lock:
            return await self._run_locked(trigger, metadata, start_time)

    async def _run_locked(
        self,
        trigger: str,
        metadata: dict[str, Any] | None,
        start_time: float,
    ) -> RunResult:
        """Run the actual scrape inside the class lock."""
        # 3. Open audit run
        run_metadata = dict(metadata or {})
        run_metadata["trigger"] = trigger
        run_id = audit_logger.start_run(self.source_name, metadata=run_metadata)

        logger.info(
            "Scraper run starting",
            scraper=self.source_name,
            trigger=trigger,
            run_id=run_id,
        )

        signals: list[SIGNAL] = []
        records_fetched = 0
        records_new = 0
        records_updated = 0
        records_failed = 0
        error_message: str | None = None
        status: str = "success"

        try:
            # 4. Fetch
            raw_records = await self.fetch(trigger)
            records_fetched = len(raw_records)
            logger.debug(
                "Scraper fetch complete",
                scraper=self.source_name,
                records=records_fetched,
            )

            # 5. Parse
            signals = await self.parse(raw_records)
            logger.debug(
                "Scraper parse complete",
                scraper=self.source_name,
                signals=len(signals),
            )

            # 6. Write
            records_new, records_updated, records_failed = await self.write(signals)

            # 7. Determine final status
            if records_failed > 0 and records_new + records_updated == 0:
                status = "failed"
                error_message = (
                    f"All {records_failed} record writes failed"
                )
            elif records_failed > 0:
                status = "partial"
                error_message = (
                    f"{records_failed} of "
                    f"{records_new + records_updated + records_failed} records failed"
                )

        except Exception as e:
            status = "failed"
            error_message = f"{type(e).__name__}: {e}"
            logger.exception(
                "Scraper run failed",
                scraper=self.source_name,
                error_type=type(e).__name__,
            )

        # 8. Source freshness — see the module docstring for why this exists
        #    and why it is computed from the PARSED SIGNALS rather than from
        #    anything already in the database.
        #
        #    Guarded in its own try/except and deliberately placed AFTER the
        #    main try block: this is monitoring, and monitoring must not be
        #    able to fail a scrape. On any error the run reports exactly what
        #    it would have reported before this existed.
        final_metadata = dict(run_metadata)
        try:
            final_metadata.update(self.source_freshness(signals))
        except Exception as freshness_error:  # noqa: BLE001
            logger.warning(
                "Source freshness capture failed — run unaffected",
                scraper=self.source_name,
                run_id=run_id,
                error_type=type(freshness_error).__name__,
                error=str(freshness_error),
            )

        # 9. Close audit run
        #
        #    final_metadata carries the ORIGINAL run_metadata keys as well as
        #    the freshness ones. finish_run writes the metadata column whole,
        #    so passing only the new keys would erase `trigger` and
        #    `trigger_source` — and those are what proved the ramsey_parcels
        #    collision on 2026-08-01 came from two GitHub Actions runners.
        duration = time.monotonic() - start_time

        if run_id is not None:
            audit_logger.finish_run(
                run_id,
                status=status,  # type: ignore[arg-type]
                records_fetched=records_fetched,
                records_new=records_new,
                records_updated=records_updated,
                records_failed=records_failed,
                error_message=error_message,
                duration_seconds=duration,
                metadata=final_metadata,
            )

        # 10. Update source health
        if status == "success":
            source_health_tracker.record_success(self.source_name)
        elif status == "partial":
            source_health_tracker.record_partial(self.source_name, notes=error_message)
        else:
            source_health_tracker.record_failure(self.source_name, notes=error_message)

        logger.info(
            "Scraper run complete",
            scraper=self.source_name,
            status=status,
            duration_seconds=round(duration, 2),
            records_new=records_new,
            records_updated=records_updated,
            records_failed=records_failed,
            source_max_date=final_metadata.get("source_max_date"),
        )

        return RunResult(
            scraper_name=self.source_name,
            run_id=run_id,
            status=status,
            duration_seconds=duration,
            records_fetched=records_fetched,
            records_new=records_new,
            records_updated=records_updated,
            records_failed=records_failed,
            error_message=error_message,
        )


__all__ = ["BaseScraper", "RunResult"]
