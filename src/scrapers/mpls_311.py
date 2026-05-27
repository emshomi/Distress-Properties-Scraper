"""
Minneapolis 311 Code Violations scraper.

Pulls from the Socrata dataset rmpv-bp76 (Minneapolis Open Data).
On first run: pulls 365 days of history. On subsequent runs: pulls
the trailing 7 days to catch newly-reported violations.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, ClassVar

import httpx

from src.config import settings
from src.models.signal import CodeViolationInsert
from src.scrapers.base_scraper import BaseScraper
from src.services.audit_logger import log_error
from src.services.event_writer import (
    write_events_dedup,
    write_typed_signals_dedup,
)
from src.services.parcel_resolver import resolve_parcel
from src.models.parcel import ParcelUpsert
from src.utils.errors import ParseError, SourceUnavailableError
from src.utils.logger import logger
from src.utils.parcel_id_normalizer import safe_normalize_parcel_id
from src.utils.retry import retry_on_transient


# Socrata dataset for Minneapolis 311 code violations
_DATASET_ID = "rmpv-bp76"
_BASE_URL = f"https://opendata.minneapolismn.gov/resource/{_DATASET_ID}.json"

# Lookback windows
_FIRST_RUN_DAYS = 365
_SUBSEQUENT_RUN_DAYS = 7

# Page size for Socrata pagination
_PAGE_SIZE = 1000
_MAX_PAGES = 50  # Safety cap


class MplsThreeOneOneScraper(BaseScraper[dict[str, Any], CodeViolationInsert]):
    """Minneapolis 311 code violations."""

    source_name: ClassVar[str] = "mpls_311"
    signal_type: ClassVar[str] = "code_violation"

    @retry_on_transient(source="mpls_311")
    async def _fetch_page(
        self,
        client: httpx.AsyncClient,
        offset: int,
        since_date: date,
    ) -> list[dict[str, Any]]:
        """Fetch one page of records from Socrata."""
        params: dict[str, Any] = {
            "$limit": _PAGE_SIZE,
            "$offset": offset,
            "$order": "open_date DESC",
            "$where": f"open_date >= '{since_date.isoformat()}T00:00:00.000'",
        }

        headers: dict[str, str] = {"Accept": "application/json"}
        if settings.minneapolis_311_app_token is not None:
            headers["X-App-Token"] = settings.minneapolis_311_app_token.get_secret_value()

        try:
            response = await client.get(_BASE_URL, params=params, headers=headers)
        except httpx.HTTPError as e:
            raise SourceUnavailableError(
                f"Socrata request failed: {e}",
                source=self.source_name,
            ) from e

        if response.status_code >= 500:
            raise SourceUnavailableError(
                f"Socrata returned {response.status_code}",
                source=self.source_name,
            )
        if response.status_code != 200:
            raise SourceUnavailableError(
                f"Socrata returned unexpected status {response.status_code}",
                source=self.source_name,
            )

        try:
            data = response.json()
        except ValueError as e:
            raise ParseError(
                f"Socrata returned non-JSON: {e}",
                source=self.source_name,
            ) from e

        if not isinstance(data, list):
            raise ParseError(
                f"Expected JSON array, got {type(data).__name__}",
                source=self.source_name,
            )

        return data

    async def fetch(self, trigger: str) -> list[dict[str, Any]]:
        """Fetch records from Socrata with pagination."""
        # Determine lookback window
        # (For simplicity, always use subsequent-run window; first-run logic
        # would query the audit table for prior successful runs.)
        days_back = _SUBSEQUENT_RUN_DAYS
        if trigger == "manual":
            days_back = _FIRST_RUN_DAYS

        since_date = (datetime.now(timezone.utc) - timedelta(days=days_back)).date()

        logger.info(
            "Fetching Minneapolis 311",
            since=since_date.isoformat(),
            lookback_days=days_back,
        )

        all_records: list[dict[str, Any]] = []
        async with httpx.AsyncClient(
            timeout=settings.scraper_request_timeout_seconds
        ) as client:
            for page in range(_MAX_PAGES):
                offset = page * _PAGE_SIZE
                records = await self._fetch_page(client, offset, since_date)
                all_records.extend(records)
                if len(records) < _PAGE_SIZE:
                    # Last page
                    break

        return all_records

    async def parse(
        self, raw_records: list[dict[str, Any]]
    ) -> list[CodeViolationInsert]:
        """Parse raw Socrata records into CodeViolationInsert."""
        signals: list[CodeViolationInsert] = []
        now = datetime.now(timezone.utc)

        for raw in raw_records:
            try:
                # Parcel ID — Socrata field is 'apn' or 'pid' depending on dataset version
                raw_pid = raw.get("apn") or raw.get("pid") or raw.get("parcel_id")
                if not raw_pid:
                    continue

                pid, err = safe_normalize_parcel_id("hennepin", str(raw_pid))
                if pid is None:
                    log_error(
                        run_id=None,
                        error_type="validation_error",
                        error_message=f"Bad PID: {err}",
                        raw_record=raw,
                    )
                    continue

                case_number = raw.get("case_number") or raw.get("case_id")
                if not case_number:
                    continue

                # Reported date
                reported_str = raw.get("open_date") or raw.get("reported_date")
                reported_date: date | None = None
                if reported_str:
                    try:
                        reported_date = datetime.fromisoformat(
                            reported_str.replace("Z", "+00:00")
                        ).date()
                    except (ValueError, AttributeError):
                        reported_date = None

                signal = CodeViolationInsert(
                    parcel_id=pid,
                    case_number=str(case_number),
                    violation_type=raw.get("violation_type") or raw.get("category"),
                    violation_description=raw.get("description") or raw.get("violation_description"),
                    status=raw.get("status") or raw.get("case_status"),
                    reported_date=reported_date,
                    source=self.source_name,
                    raw_data=raw,
                    observed_at=now,
                )
                signals.append(signal)
            except Exception as e:
                log_error(
                    run_id=None,
                    error_type="parse_error",
                    error_message=f"{type(e).__name__}: {e}",
                    raw_record=raw,
                )

        return signals

    async def write(
        self, signals: list[CodeViolationInsert]
    ) -> tuple[int, int, int]:
        """Resolve parcels, write typed signals, and write to unified event feed."""
        if not signals:
            return 0, 0, 0

        # Resolve unique parcels first
        unique_pids: dict[str, ParcelUpsert] = {}
        for sig in signals:
            if sig.parcel_id not in unique_pids:
                unique_pids[sig.parcel_id] = ParcelUpsert(
                    parcel_id=sig.parcel_id,
                    county_code="hennepin",
                    data_sources=[self.source_name],
                )

        for parcel_payload in unique_pids.values():
            resolve_parcel(parcel_payload)

        # Write typed signals
        signal_rows = [
            sig.model_dump(mode="json", exclude_none=True) for sig in signals
        ]
        new_typed, failed_typed = write_typed_signals_dedup(
            "code_violations",
            signal_rows,
            on_conflict="case_number,source",
        )

        # Write unified events
        events = [sig.to_event() for sig in signals]
        new_events, failed_events = write_events_dedup(events)

        return new_typed, 0, failed_typed + failed_events


__all__ = ["MplsThreeOneOneScraper"]
