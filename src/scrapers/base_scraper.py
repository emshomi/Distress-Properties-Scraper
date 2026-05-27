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
  2. Acquire per-class lock (prevents concurrent invocations)
  3. Open audit.scraper_runs row
  4. Call fetch() → parse() → write()
  5. Close the run with final counts
  6. Update source_health
  7. Release the lock
"""

from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
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
    _class_lock: ClassVar[asyncio.Lock]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Each subclass gets its own lock
        cls._class_lock = asyncio.Lock()

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

        async with self._class_lock:
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

        # 8. Close audit run
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
            )

        # 9. Update source health
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
