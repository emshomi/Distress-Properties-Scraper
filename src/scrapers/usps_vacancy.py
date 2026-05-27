"""
HUD/USPS Vacancy Indicator scraper.

HUD publishes quarterly USPS vacancy data at the ZIP+4 level. We filter
to Minnesota ZIP codes (550xx, 551xx, 553xx, 554xx, 555xx, 556xx, 557xx,
558xx, 559xx).

This scraper is OPTIONAL — it requires HUD_USPS_VACANCY_URL to be set
(per-account download URL). When unset, the scraper logs and returns
empty results.
"""

from __future__ import annotations

import csv
import io
from datetime import date, datetime, timezone
from typing import Any, ClassVar

import httpx

from src.config import settings
from src.models.signal import UspsVacancyInsert
from src.scrapers.base_scraper import BaseScraper
from src.services.audit_logger import log_error
from src.services.event_writer import write_typed_signals_dedup
from src.utils.errors import ParseError, SourceUnavailableError
from src.utils.logger import logger
from src.utils.retry import retry_on_transient


# Minnesota ZIP prefixes
_MN_ZIP_PREFIXES: tuple[str, ...] = (
    "550", "551", "553", "554", "555", "556", "557", "558", "559",
)

# Vacancy rate threshold for emitting an event (30%)
_VACANCY_RATE_EVENT_THRESHOLD: float = 0.30


def _is_mn_zip(zip5: str) -> bool:
    """True if a 5-digit zip is in Minnesota."""
    if not zip5 or len(zip5) < 5:
        return False
    return zip5.startswith(_MN_ZIP_PREFIXES)


class UspsVacancyScraper(BaseScraper[dict[str, Any], UspsVacancyInsert]):
    """HUD/USPS vacancy indicator (ZIP+4 level)."""

    source_name: ClassVar[str] = "usps_vacancy"
    signal_type: ClassVar[str] = "usps_vacancy"

    @retry_on_transient(source="usps_vacancy")
    async def _fetch_csv(self, client: httpx.AsyncClient, url: str) -> str:
        try:
            response = await client.get(url, follow_redirects=True)
            response.raise_for_status()
            return response.text
        except httpx.HTTPError as e:
            raise SourceUnavailableError(
                f"HUD USPS fetch failed: {e}", source=self.source_name
            ) from e

    async def fetch(self, trigger: str) -> list[dict[str, Any]]:
        if settings.hud_usps_vacancy_url is None:
            logger.info(
                "HUD_USPS_VACANCY_URL not configured — skipping fetch",
                source=self.source_name,
            )
            return []

        url = str(settings.hud_usps_vacancy_url)
        async with httpx.AsyncClient(
            timeout=settings.scraper_request_timeout_seconds * 3
        ) as client:
            content = await self._fetch_csv(client, url)

        try:
            reader = csv.DictReader(io.StringIO(content))
            all_rows = list(reader)
        except Exception as e:
            raise ParseError(
                f"HUD USPS CSV parse failed: {e}", source=self.source_name
            ) from e

        # Filter to MN ZIPs only
        mn_rows = [
            row
            for row in all_rows
            if _is_mn_zip(str(row.get("ZIP", row.get("zip", ""))))
        ]

        logger.info(
            "Filtered HUD USPS rows to MN",
            total=len(all_rows),
            mn_rows=len(mn_rows),
        )

        return mn_rows

    async def parse(self, raw_records: list[dict[str, Any]]) -> list[UspsVacancyInsert]:
        signals: list[UspsVacancyInsert] = []
        now = datetime.now(timezone.utc)
        quarter = _detect_quarter(raw_records)

        for raw in raw_records:
            try:
                zip_val = (
                    raw.get("ZIP+4")
                    or raw.get("ZIP4")
                    or raw.get("zip4")
                    or raw.get("ZIP")
                    or raw.get("zip")
                    or ""
                )
                zip_val = str(zip_val).strip().replace("-", "")
                if len(zip_val) < 5:
                    continue

                zip5 = zip_val[:5]
                zip4 = zip_val[5:9] if len(zip_val) >= 9 else None

                residential_total = int(
                    raw.get("RES_TOTAL") or raw.get("residential_total") or 0
                )
                residential_vacant = int(
                    raw.get("RES_VAC") or raw.get("residential_vacant") or 0
                )
                business_total = int(
                    raw.get("BUS_TOTAL") or raw.get("business_total") or 0
                )
                business_vacant = int(
                    raw.get("BUS_VAC") or raw.get("business_vacant") or 0
                )

                res_rate = (
                    residential_vacant / residential_total
                    if residential_total > 0
                    else 0.0
                )
                bus_rate = (
                    business_vacant / business_total if business_total > 0 else 0.0
                )

                signals.append(
                    UspsVacancyInsert(
                        zip5=zip5,
                        zip4=zip4,
                        quarter=quarter,
                        residential_total=residential_total,
                        residential_vacant=residential_vacant,
                        residential_vacancy_rate=min(res_rate, 1.0),
                        business_total=business_total,
                        business_vacant=business_vacant,
                        business_vacancy_rate=min(bus_rate, 1.0),
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

    async def write(self, signals: list[UspsVacancyInsert]) -> tuple[int, int, int]:
        if not signals:
            return 0, 0, 0

        signal_rows = [sig.model_dump(mode="json", exclude_none=True) for sig in signals]
        new_typed, failed_typed = write_typed_signals_dedup(
            "usps_vacancy",
            signal_rows,
            on_conflict="zip5,zip4,quarter",
        )

        # USPS vacancy is ZIP+4 level, not parcel level — no events emitted
        # from this scraper. The data joins to parcels at query time via
        # ZIP code.
        return new_typed, 0, failed_typed


def _detect_quarter(rows: list[dict[str, Any]]) -> str:
    """Best-effort quarter detection from any row's metadata."""
    if not rows:
        return datetime.now(timezone.utc).strftime("%YQ%m")

    first = rows[0]
    quarter = (
        first.get("QUARTER")
        or first.get("quarter")
        or first.get("PERIOD")
    )
    if quarter:
        return str(quarter).strip()

    # Fallback: current year-quarter from system date
    now = datetime.now(timezone.utc)
    q = (now.month - 1) // 3 + 1
    return f"{now.year}Q{q}"


__all__ = ["UspsVacancyScraper"]
