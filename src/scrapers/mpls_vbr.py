"""
Minneapolis Vacant Building Registry (VBR) + Problem Vacant Earnings (PVE) scraper.

Pulls from Socrata dataset qa8g-rqe7. The registry tracks vacant
buildings flagged by the city. PVE is a monthly fee assessed against
chronic problem properties.

VBR fee schedule (as of late 2024): $7,228.70/year base.
PVE fee schedule: $2,000/month per problem-property designation.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, ClassVar

import httpx

from src.config import settings
from src.models.parcel import ParcelUpsert
from src.models.signal import VbrListingInsert
from src.scrapers.base_scraper import BaseScraper
from src.services.audit_logger import log_error
from src.services.event_writer import (
    write_events_dedup,
    write_typed_signals_dedup,
)
from src.services.parcel_resolver import resolve_parcel
from src.utils.errors import ParseError, SourceUnavailableError
from src.utils.parcel_id_normalizer import safe_normalize_parcel_id
from src.utils.retry import retry_on_transient


_DATASET_ID = "qa8g-rqe7"
_BASE_URL = f"https://opendata.minneapolismn.gov/resource/{_DATASET_ID}.json"

_VBR_ANNUAL_FEE = Decimal("7228.70")
_PVE_MONTHLY_FEE = Decimal("2000.00")

_PAGE_SIZE = 1000
_MAX_PAGES = 20


class MplsVacantBuildingScraper(BaseScraper[dict[str, Any], VbrListingInsert]):
    """Minneapolis VBR + PVE scraper."""

    source_name: ClassVar[str] = "mpls_vbr"
    signal_type: ClassVar[str] = "vbr_listing"

    @retry_on_transient(source="mpls_vbr")
    async def _fetch_page(
        self, client: httpx.AsyncClient, offset: int
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "$limit": _PAGE_SIZE,
            "$offset": offset,
            "$order": "registration_date DESC",
        }
        headers: dict[str, str] = {"Accept": "application/json"}
        if settings.minneapolis_311_app_token is not None:
            headers["X-App-Token"] = settings.minneapolis_311_app_token.get_secret_value()

        try:
            response = await client.get(_BASE_URL, params=params, headers=headers)
        except httpx.HTTPError as e:
            raise SourceUnavailableError(
                f"VBR fetch failed: {e}", source=self.source_name
            ) from e

        if response.status_code != 200:
            raise SourceUnavailableError(
                f"VBR returned status {response.status_code}",
                source=self.source_name,
            )

        try:
            return response.json()
        except ValueError as e:
            raise ParseError(
                f"VBR returned non-JSON: {e}", source=self.source_name
            ) from e

    async def fetch(self, trigger: str) -> list[dict[str, Any]]:
        all_records: list[dict[str, Any]] = []
        async with httpx.AsyncClient(
            timeout=settings.scraper_request_timeout_seconds
        ) as client:
            for page in range(_MAX_PAGES):
                offset = page * _PAGE_SIZE
                batch = await self._fetch_page(client, offset)
                all_records.extend(batch)
                if len(batch) < _PAGE_SIZE:
                    break
        return all_records

    async def parse(self, raw_records: list[dict[str, Any]]) -> list[VbrListingInsert]:
        signals: list[VbrListingInsert] = []
        now = datetime.now(timezone.utc)

        for raw in raw_records:
            try:
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

                # Registration date
                reg_str = raw.get("registration_date")
                reg_date: date | None = None
                if reg_str:
                    try:
                        reg_date = datetime.fromisoformat(
                            reg_str.replace("Z", "+00:00")
                        ).date()
                    except (ValueError, AttributeError):
                        reg_date = None

                category = raw.get("category") or raw.get("classification")
                status = raw.get("status")

                # Heuristic flags
                category_lower = (category or "").lower()
                status_lower = (status or "").lower()
                boarded = "boarded" in category_lower or "boarded" in status_lower
                condemned = "condemned" in category_lower or "condemned" in status_lower

                signal = VbrListingInsert(
                    parcel_id=pid,
                    registration_number=raw.get("registration_number") or raw.get("id"),
                    category=category,
                    status=status,
                    registered_date=reg_date,
                    vbr_fee_assessed=_VBR_ANNUAL_FEE,
                    pve_monthly_fee=_PVE_MONTHLY_FEE if "pve" in (status_lower + category_lower) else None,
                    boarded=boarded,
                    condemned=condemned,
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

    async def write(self, signals: list[VbrListingInsert]) -> tuple[int, int, int]:
        if not signals:
            return 0, 0, 0

        unique_pids: dict[str, ParcelUpsert] = {}
        for sig in signals:
            if sig.parcel_id not in unique_pids:
                unique_pids[sig.parcel_id] = ParcelUpsert(
                    parcel_id=sig.parcel_id,
                    county_code="hennepin",
                    data_sources=[self.source_name],
                    vacancy_status="vacant",
                )

        for parcel_payload in unique_pids.values():
            resolve_parcel(parcel_payload)

        signal_rows = [sig.model_dump(mode="json", exclude_none=True) for sig in signals]
        new_typed, failed_typed = write_typed_signals_dedup(
            "vbr_listings",
            signal_rows,
            on_conflict="parcel_id,source,registered_date",
        )

        events = [sig.to_event() for sig in signals]
        new_events, failed_events = write_events_dedup(events)

        return new_typed, 0, failed_typed + failed_events


__all__ = ["MplsVacantBuildingScraper"]
