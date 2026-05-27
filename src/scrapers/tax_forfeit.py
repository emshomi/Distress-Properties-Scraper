"""
MN tax-forfeit property scraper.

Pulls tax-forfeit inventory from county auditor / land commissioner pages.
Currently covers Hennepin, Ramsey, St. Louis, Otter Tail, and Cass counties.

CRITICAL CONTEXT — MN Tyler v. Hennepin precedent (2023):
  Pre-2024 tax forfeitures may be subject to the forced-liquidation
  window: counties must remit 75% of sale surplus to former owners
  through June 2027, and 85% July 2027 - June 2029. This is OUR
  competitive insight — pre-2024 forfeit properties are time-boxed
  opportunities.

The pre_2024_forfeit flag in TaxForfeitInsert marks these for higher
event severity ('critical') so they surface prominently.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, ClassVar

import httpx
from bs4 import BeautifulSoup

from src.config import settings
from src.models.parcel import ParcelUpsert
from src.models.signal import TaxForfeitInsert
from src.scrapers.base_scraper import BaseScraper
from src.services.audit_logger import log_error
from src.services.event_writer import (
    write_events_dedup,
    write_typed_signals_dedup,
)
from src.services.parcel_resolver import resolve_parcel
from src.utils.errors import ParseError, SourceUnavailableError
from src.utils.logger import logger
from src.utils.parcel_id_normalizer import safe_normalize_parcel_id
from src.utils.retry import retry_on_transient


# Per-county tax-forfeit inventory URLs (illustrative — real URLs may differ)
_COUNTY_URLS: dict[str, str] = {
    "hennepin": "https://www.hennepin.us/your-government/property/tax-forfeited-land",
    "ramsey": "https://www.ramseycounty.us/residents/property/tax-forfeited-property",
    "st_louis": "https://www.stlouiscountymn.gov/departments-a-z/auditor/tax-forfeited-land",
    "otter_tail": "https://ottertailcounty.gov/department/auditor-treasurer/tax-forfeited-land/",
    "cass": "https://www.casscountymn.gov/departments/auditor_treasurer/tax_forfeited_land.php",
}

# Cutoff: forfeitures before this date are subject to Tyler v. Hennepin remediation
_TYLER_HENNEPIN_CUTOFF = date(2024, 1, 1)


def _parse_decimal(text: str) -> Decimal | None:
    """Extract a decimal from text (strips $, commas)."""
    cleaned = text.replace("$", "").replace(",", "").strip()
    try:
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None


def _parse_date_loose(text: str) -> date | None:
    """Try several date formats."""
    text = text.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


class TaxForfeitScraper(BaseScraper[dict[str, Any], TaxForfeitInsert]):
    """MN tax-forfeit property scraper."""

    source_name: ClassVar[str] = "tax_forfeit"
    signal_type: ClassVar[str] = "tax_forfeit"

    @retry_on_transient(source="tax_forfeit")
    async def _fetch_county_page(
        self, client: httpx.AsyncClient, county: str, url: str
    ) -> str:
        try:
            response = await client.get(url, follow_redirects=True)
            response.raise_for_status()
            return response.text
        except httpx.HTTPError as e:
            raise SourceUnavailableError(
                f"Fetch failed for {county}: {e}",
                source=self.source_name,
                context={"county": county, "url": url},
            ) from e

    def _parse_county_page(
        self, html: str, county: str
    ) -> list[dict[str, Any]]:
        """
        Best-effort parse of a county tax-forfeit page.

        Real parsing would need per-county selectors that may change.
        This skeleton looks for tables with PID-like patterns.
        """
        records: list[dict[str, Any]] = []
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception as e:
            raise ParseError(
                f"HTML parse failed for {county}: {e}",
                source=self.source_name,
            ) from e

        # Look for tables
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) < 3:
                    continue
                cell_texts = [c.get_text(strip=True) for c in cells]
                # Heuristic: row is a forfeit record if first cell looks like a PID
                first = cell_texts[0]
                if any(ch.isdigit() for ch in first) and len(first) >= 8:
                    records.append({
                        "county": county,
                        "parcel_id": first,
                        "cells": cell_texts,
                    })

        return records

    async def fetch(self, trigger: str) -> list[dict[str, Any]]:
        all_records: list[dict[str, Any]] = []
        async with httpx.AsyncClient(
            timeout=settings.scraper_request_timeout_seconds * 2
        ) as client:
            for county, url in _COUNTY_URLS.items():
                try:
                    html = await self._fetch_county_page(client, county, url)
                    records = self._parse_county_page(html, county)
                    all_records.extend(records)
                    logger.debug(
                        "Tax-forfeit county fetched",
                        county=county,
                        records=len(records),
                    )
                except (SourceUnavailableError, ParseError) as e:
                    log_error(
                        run_id=None,
                        error_type="fetch_error",
                        error_message=f"{county}: {e}",
                        raw_record={"county": county, "url": url},
                    )
        return all_records

    async def parse(self, raw_records: list[dict[str, Any]]) -> list[TaxForfeitInsert]:
        signals: list[TaxForfeitInsert] = []
        now = datetime.now(timezone.utc)

        for raw in raw_records:
            try:
                county = raw["county"]
                raw_pid = raw["parcel_id"]
                pid, err = safe_normalize_parcel_id(county, raw_pid)
                if pid is None:
                    log_error(
                        run_id=None,
                        error_type="validation_error",
                        error_message=f"Bad PID for {county}: {err}",
                        raw_record=raw,
                    )
                    continue

                # Try to extract forfeit date and amounts from remaining cells
                cells = raw.get("cells", [])
                forfeit_date: date | None = None
                appraised_value: Decimal | None = None
                minimum_bid: Decimal | None = None

                for cell in cells[1:]:
                    if forfeit_date is None:
                        forfeit_date = _parse_date_loose(cell)
                    if "$" in cell:
                        amount = _parse_decimal(cell)
                        if amount is not None:
                            if appraised_value is None:
                                appraised_value = amount
                            elif minimum_bid is None:
                                minimum_bid = amount

                pre_2024 = (
                    forfeit_date is not None and forfeit_date < _TYLER_HENNEPIN_CUTOFF
                )

                signals.append(
                    TaxForfeitInsert(
                        parcel_id=pid,
                        county=county,
                        forfeit_date=forfeit_date,
                        appraised_value=appraised_value,
                        minimum_bid=minimum_bid,
                        pre_2024_forfeit=pre_2024,
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

    async def write(self, signals: list[TaxForfeitInsert]) -> tuple[int, int, int]:
        if not signals:
            return 0, 0, 0

        # Resolve parcels (county varies per signal)
        unique_pids: dict[tuple[str, str], ParcelUpsert] = {}
        for sig in signals:
            key = (sig.parcel_id, sig.county)
            if key not in unique_pids:
                unique_pids[key] = ParcelUpsert(
                    parcel_id=sig.parcel_id,
                    county_code=sig.county,
                    data_sources=[self.source_name],
                )

        for parcel_payload in unique_pids.values():
            resolve_parcel(parcel_payload)

        signal_rows = [sig.model_dump(mode="json", exclude_none=True) for sig in signals]
        new_typed, failed_typed = write_typed_signals_dedup(
            "tax_forfeit",
            signal_rows,
            on_conflict="parcel_id,county",
        )

        events = [sig.to_event() for sig in signals]
        new_events, failed_events = write_events_dedup(events)

        return new_typed, 0, failed_typed + failed_events


__all__ = ["TaxForfeitScraper"]
