"""
MN Court Records Online (MCRO) Probate Filings scraper.

Disabled by default — MCRO has aggressive CAPTCHA protection and our use
must be careful and slow. Operators enable this scraper deliberately
after reviewing MCRO's terms of use.

Rate-limited to 1 request per 3.5 seconds across all queries.
"""

from __future__ import annotations

import asyncio
import re
import time
from datetime import date, datetime, timezone
from typing import Any, ClassVar

import httpx

from src.config import settings
from src.models.signal import ProbateFilingInsert
from src.scrapers.base_scraper import BaseScraper
from src.services.audit_logger import log_error
from src.services.event_writer import (
    write_events_dedup,
    write_typed_signals_dedup,
)
from src.utils.errors import ParseError, SourceUnavailableError
from src.utils.logger import logger
from src.utils.retry import retry_on_transient


# 3.5 second rate limit between MCRO requests
_REQUEST_INTERVAL_SECONDS: float = 3.5

# Counties we currently support for MCRO probate lookups
_SUPPORTED_COUNTIES: tuple[str, ...] = (
    "hennepin",
    "ramsey",
    "dakota",
    "anoka",
    "washington",
    "scott",
    "carver",
    "wright",
    "st_louis",
)

# Process-wide lock + last request time
_MCRO_LOCK = asyncio.Lock()
_MCRO_LAST_REQUEST_TIME: float = 0.0


# CAPTCHA detection patterns
_CAPTCHA_PATTERNS = (
    "captcha",
    "human verification",
    "are you a robot",
    "please verify",
)


def _looks_like_captcha(html: str) -> bool:
    """Heuristic CAPTCHA detection."""
    lower = html.lower()
    return any(pattern in lower for pattern in _CAPTCHA_PATTERNS)


class McroProbateScraper(BaseScraper[dict[str, Any], ProbateFilingInsert]):
    """MN Court Records Online probate filings."""

    source_name: ClassVar[str] = "mcro_probate"
    signal_type: ClassVar[str] = "probate_filing"

    @retry_on_transient(source="mcro_probate")
    async def _fetch_county(
        self, client: httpx.AsyncClient, county: str
    ) -> list[dict[str, Any]]:
        """Fetch probate filings for one county. Rate-limited."""
        global _MCRO_LAST_REQUEST_TIME

        async with _MCRO_LOCK:
            elapsed = time.monotonic() - _MCRO_LAST_REQUEST_TIME
            if elapsed < _REQUEST_INTERVAL_SECONDS:
                await asyncio.sleep(_REQUEST_INTERVAL_SECONDS - elapsed)

            url = "https://publicaccess.courts.state.mn.us/CaseSearch"
            params = {
                "county": county,
                "caseType": "probate",
            }

            try:
                response = await client.get(
                    url,
                    params=params,
                    headers={
                        "User-Agent": "DistressProperties/1.0 (research)",
                    },
                )
                _MCRO_LAST_REQUEST_TIME = time.monotonic()
            except httpx.HTTPError as e:
                raise SourceUnavailableError(
                    f"MCRO request failed: {e}", source=self.source_name
                ) from e

        if response.status_code == 429:
            raise SourceUnavailableError(
                "MCRO rate-limited us (429)", source=self.source_name
            )
        if response.status_code != 200:
            raise SourceUnavailableError(
                f"MCRO returned {response.status_code}", source=self.source_name
            )

        if _looks_like_captcha(response.text):
            logger.warning(
                "MCRO showed CAPTCHA — pausing this scraper",
                county=county,
            )
            raise SourceUnavailableError(
                "MCRO CAPTCHA detected — manual intervention required",
                source=self.source_name,
            )

        # Parse the response — this is illustrative; real MCRO HTML parsing
        # would need site-specific selectors that may change over time
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.text, "lxml")
            records: list[dict[str, Any]] = []
            for row in soup.select("table.results tr"):
                cells = row.find_all("td")
                if len(cells) < 4:
                    continue
                records.append({
                    "county": county,
                    "case_number": cells[0].get_text(strip=True),
                    "decedent_name": cells[1].get_text(strip=True),
                    "filing_date": cells[2].get_text(strip=True),
                    "filing_type": cells[3].get_text(strip=True),
                })
            return records
        except Exception as e:
            raise ParseError(
                f"MCRO HTML parse failed: {e}", source=self.source_name
            ) from e

    async def fetch(self, trigger: str) -> list[dict[str, Any]]:
        """Fetch probate filings for each supported county, sequentially."""
        all_records: list[dict[str, Any]] = []

        async with httpx.AsyncClient(
            timeout=settings.scraper_request_timeout_seconds * 2
        ) as client:
            for county in _SUPPORTED_COUNTIES:
                try:
                    records = await self._fetch_county(client, county)
                    all_records.extend(records)
                    logger.debug(
                        "MCRO county fetched",
                        county=county,
                        records=len(records),
                    )
                except SourceUnavailableError as e:
                    logger.warning(
                        "Skipping MCRO county due to error",
                        county=county,
                        error=str(e),
                    )
                    # If we hit CAPTCHA, stop entirely
                    if "captcha" in str(e).lower():
                        break

        return all_records

    async def parse(
        self, raw_records: list[dict[str, Any]]
    ) -> list[ProbateFilingInsert]:
        signals: list[ProbateFilingInsert] = []
        now = datetime.now(timezone.utc)

        for raw in raw_records:
            try:
                case_number = raw.get("case_number")
                if not case_number:
                    continue

                filing_date_str = raw.get("filing_date")
                filing_date: date | None = None
                if filing_date_str:
                    # Try common formats
                    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
                        try:
                            filing_date = datetime.strptime(
                                filing_date_str, fmt
                            ).date()
                            break
                        except ValueError:
                            continue

                signals.append(
                    ProbateFilingInsert(
                        case_number=str(case_number),
                        decedent_name=raw.get("decedent_name"),
                        filing_date=filing_date,
                        county=str(raw.get("county", "unknown")),
                        filing_type=raw.get("filing_type"),
                        source=self.source_name,
                        raw_data=raw,
                        observed_at=now,
                    )
                )
            except Exception as e:
                log_error(
                    run_id=None,
                    error_type="parse_error",
                    error_message=f"{type(e).__name__}: {e}",
                    raw_record=raw,
                )

        return signals

    async def write(
        self, signals: list[ProbateFilingInsert]
    ) -> tuple[int, int, int]:
        if not signals:
            return 0, 0, 0

        signal_rows = [sig.model_dump(mode="json", exclude_none=True) for sig in signals]
        new_typed, failed_typed = write_typed_signals_dedup(
            "probate_filings",
            signal_rows,
            on_conflict="case_number,county",
        )

        # Project to events ONLY when parcel_id is set
        events = [e for sig in signals if (e := sig.to_event()) is not None]
        new_events, failed_events = write_events_dedup(events)

        return new_typed, 0, failed_typed + failed_events


__all__ = ["McroProbateScraper"]
