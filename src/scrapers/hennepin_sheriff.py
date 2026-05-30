"""
Hennepin County Sheriff Foreclosure Sales scraper.

Source: Hennepin County public foreclosure API (clean JSON, no auth wall
        beyond an Azure APIM subscription key).
    Frontend:  https://foreclosure.hennepin.us/
    List:      POST https://api.hennepincounty.gov/hcso-public-services-api/v1/Foreclosure/Search
    Detail:    GET  https://api.hennepincounty.gov/hcso-public-services-api/v1/Foreclosure/{saleRecordNumber}

License / posture: official Hennepin County government API. Public foreclosure
notice data under the Minnesota Government Data Practices Act. No anti-bot
terms, no robots restriction on the API host. We fetch politely (small delays).

=== WHY THIS IS THE EASIEST SHERIFF SOURCE WE HAVE ===
Unlike Anoka (bot-resistant ASP.NET WebForms requiring Playwright) and the
HTML-scraping counties, Hennepin exposes a modern JSON API:
  * List endpoint paginates cleanly (10/page; ~47 pages; ~465 records).
  * Detail endpoint returns the full record by saleRecordNumber, INCLUDING a
    server-computed `redemptionExpirationDate` — we READ it rather than
    computing sale_date + 6 months. This handles the 5-week / 2-month /
    12-month redemption edge cases automatically and correctly.

=== AUTH ===
The API sits behind Azure API Management and requires a subscription key
header: `Ocp-Apim-Subscription-Key`. It is a public, client-side key (shipped
in the site's JS), but we read it from settings/env so it can be rotated
without a code change. Falls back to the known published value if unset.

=== DATA AVAILABLE ===
List record:    saleRecordNumber, dateOfSale, typeOfSale, address, city,
                mortgagors[].display
Detail record:  + mortgagee, toWhomSold, finalBidAmount,
                redemptionExpirationDate, lawFirm, mortgageDocumentNumber,
                comments, noticeOfIntent

=== ARCHITECTURE ===
  fetch():
    1. POST Search with {pagination:{activePage, pageSize}} until all pages
       are collected. Read totalPages from the first response.
    2. For each saleRecordNumber, GET the detail endpoint. Detail failures
       are tolerated (we keep the list row's basic fields).
  parse():  convert each enriched record into a DistressEventInsert
            (sheriff_sale / completed_sale). The full detail JSON is stored
            in raw_data so redemptionExpirationDate is preserved for the
            redemption-window UI work.
  write():  synthesize a stable parcel_id (HENNEPIN-FC-{saleRecordNumber});
            resolve_parcel + write_events_dedup, mirroring the Anoka scraper.

Severity:
  redemption window still open (future expiration)  -> high  (actionable)
  redemption expired / unknown                       -> low/medium
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, ClassVar

import httpx

from src.config import settings
from src.models.parcel import ParcelUpsert
from src.models.signal import DistressEventInsert
from src.scrapers.base_scraper import BaseScraper
from src.services.event_writer import write_events_dedup
from src.services.parcel_resolver import resolve_parcel
from src.utils.errors import SourceUnavailableError
from src.utils.logger import logger


_API_BASE = "https://api.hennepincounty.gov/hcso-public-services-api/v1/Foreclosure"
_LIST_URL = f"{_API_BASE}/Search"
_DETAIL_URL = f"{_API_BASE}/{{record}}"

# Public client-side APIM subscription key shipped in the site's JS. Read from
# settings/env when available so it can be rotated without a redeploy; this
# literal is only a fallback to avoid hard-blocking if the env var is unset.
_FALLBACK_SUBSCRIPTION_KEY = "e522a816143443189f09de85c4288b98"

_PAGE_SIZE = 10
# Defensive ceiling so a malformed totalPages can never spin forever.
# ~47 pages today; 200 leaves enormous headroom.
_MAX_PAGES = 200

# Politeness: small delay between detail-record fetches.
_DETAIL_DELAY_SECONDS = 0.25
# Small delay between list-page POSTs.
_LIST_DELAY_SECONDS = 0.2


def _subscription_key() -> str:
    """Read the APIM key from settings if present, else the fallback."""
    for attr in ("hennepin_api_key", "HENNEPIN_API_KEY"):
        val = getattr(settings, attr, None)
        if val:
            return str(val)
    return _FALLBACK_SUBSCRIPTION_KEY


def _headers() -> dict[str, str]:
    return {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://foreclosure.hennepin.us",
        "Referer": "https://foreclosure.hennepin.us/",
        "Ocp-Apim-Subscription-Key": _subscription_key(),
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ),
    }


def _search_body(active_page: int) -> dict[str, Any]:
    """Mirror the exact payload the site sends; only activePage varies."""
    return {
        "dateOfSale": {"minDate": None, "maxDate": None},
        "address": None,
        "city": None,
        "mortgagorName": None,
        "pagination": {"activePage": active_page, "pageSize": _PAGE_SIZE},
    }


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        d = Decimal(str(value).replace(",", "").strip())
        return d if d >= 0 else None
    except (InvalidOperation, ValueError):
        return None


def _parse_iso_date(value: Any) -> date | None:
    """Parse the API's ISO datetimes (e.g. '2025-06-03T00:00:00')."""
    if not value:
        return None
    s = str(value).strip()
    # Strip a trailing 'Z' if present so fromisoformat is happy.
    if s.endswith("Z"):
        s = s[:-1]
    try:
        return datetime.fromisoformat(s).date()
    except ValueError:
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
    return None


def _mortgagor_names(mortgagors: Any) -> str | None:
    """Join mortgagors[].display into a single owner string."""
    if not isinstance(mortgagors, list):
        return None
    names = [
        _safe_str(m.get("display"))
        for m in mortgagors
        if isinstance(m, dict) and _safe_str(m.get("display"))
    ]
    return "; ".join(n for n in names if n) or None


class HennepinSheriffScraper(BaseScraper[dict[str, Any], DistressEventInsert]):
    """Hennepin County sheriff foreclosure sales — clean JSON API source."""

    source_name: ClassVar[str] = "hennepin_sheriff"
    signal_type: ClassVar[str] = "sheriff_sale"
    county_code: ClassVar[str] = "hennepin"

    # ---- Fetch (paginated JSON list + per-record detail) ----

    async def fetch(self, trigger: str) -> list[dict[str, Any]]:
        timeout = httpx.Timeout(connect=20.0, read=60.0, write=30.0, pool=30.0)
        async with httpx.AsyncClient(
            timeout=timeout,
            headers=_headers(),
            follow_redirects=True,
        ) as client:
            # 1. First page — also tells us totalPages / totalRecords.
            first = await self._post_search(client, active_page=1)
            data = first.get("data") or []
            pagination = first.get("pagination") or {}
            total_pages = int(pagination.get("totalPages") or 1)
            total_records = int(pagination.get("totalRecords") or len(data))

            records: list[dict[str, Any]] = list(data)

            logger.info(
                "Hennepin list page fetched",
                source=self.source_name,
                page=1,
                total_pages=total_pages,
                total_records=total_records,
                rows=len(data),
            )

            # 2. Remaining pages.
            pages_to_fetch = min(total_pages, _MAX_PAGES)
            for page in range(2, pages_to_fetch + 1):
                await asyncio.sleep(_LIST_DELAY_SECONDS)
                try:
                    resp = await self._post_search(client, active_page=page)
                except SourceUnavailableError:
                    # One flaky page should not kill the whole run; log and
                    # continue. We keep whatever we've gathered so far.
                    logger.warning(
                        "Hennepin list page failed; continuing",
                        source=self.source_name,
                        page=page,
                    )
                    continue
                page_rows = resp.get("data") or []
                records.extend(page_rows)
                logger.info(
                    "Hennepin list page fetched",
                    source=self.source_name,
                    page=page,
                    rows=len(page_rows),
                )

            logger.info(
                "Hennepin list collection complete",
                source=self.source_name,
                collected=len(records),
                expected=total_records,
            )

            # 3. Detail enrichment per saleRecordNumber.
            enriched = await self._enrich_details(client, records)

        logger.info(
            "Hennepin fetch complete",
            source=self.source_name,
            total_rows=len(enriched),
        )
        return enriched

    async def _post_search(
        self, client: httpx.AsyncClient, active_page: int
    ) -> dict[str, Any]:
        """POST the Search endpoint for one page, with light retries."""
        last_err: Exception | None = None
        for attempt in range(3):
            try:
                resp = await client.post(
                    _LIST_URL, json=_search_body(active_page)
                )
                if resp.status_code != 200:
                    raise SourceUnavailableError(
                        f"Hennepin Search page {active_page} returned "
                        f"{resp.status_code}",
                        source=self.source_name,
                    )
                return resp.json()
            except (httpx.HTTPError, ValueError) as e:
                last_err = e
                logger.warning(
                    "Hennepin Search POST attempt failed",
                    source=self.source_name,
                    page=active_page,
                    attempt=attempt + 1,
                    error_type=type(e).__name__,
                    error_repr=repr(e),
                )
                await asyncio.sleep(2.0)
        raise SourceUnavailableError(
            f"Hennepin Search page {active_page} failed after retries: "
            f"{type(last_err).__name__}: {last_err!r}",
            source=self.source_name,
        )

    async def _enrich_details(
        self,
        client: httpx.AsyncClient,
        records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """GET each record's detail endpoint and merge it onto the list row."""
        detail_ok = 0
        detail_errors = 0

        for rec in records:
            record_no = _safe_str(rec.get("saleRecordNumber"))
            if not record_no:
                continue
            url = _DETAIL_URL.format(record=record_no)
            try:
                await asyncio.sleep(_DETAIL_DELAY_SECONDS)
                resp = await client.get(url)
                if resp.status_code != 200:
                    detail_errors += 1
                    logger.warning(
                        "Hennepin detail non-200",
                        source=self.source_name,
                        record=record_no,
                        status_code=resp.status_code,
                    )
                    continue
                detail = resp.json()
                if isinstance(detail, dict):
                    # Detail is the authoritative record; merge it over the
                    # list fields (which it fully supersets).
                    rec.update(detail)
                    detail_ok += 1
            except (httpx.HTTPError, ValueError) as e:
                detail_errors += 1
                logger.warning(
                    "Hennepin detail fetch error",
                    source=self.source_name,
                    record=record_no,
                    error_type=type(e).__name__,
                    error=str(e),
                )

        logger.info(
            "Hennepin detail enrichment complete",
            source=self.source_name,
            detail_ok=detail_ok,
            detail_errors=detail_errors,
            total=len(records),
        )
        return records

    # ---- Parse records → signals ----

    async def parse(
        self, raw_records: list[dict[str, Any]]
    ) -> list[DistressEventInsert]:
        signals: list[DistressEventInsert] = []
        today = date.today()

        for r in raw_records:
            record_no = _safe_str(r.get("saleRecordNumber"))
            if not record_no:
                continue

            sale_date = _parse_iso_date(r.get("dateOfSale"))
            if sale_date is None:
                # No usable sale date → cannot form a sheriff_sale event.
                continue

            parcel_id = f"HENNEPIN-FC-{record_no}"
            redemption_date = _parse_iso_date(
                r.get("redemptionExpirationDate")
            )
            owner = _mortgagor_names(r.get("mortgagors"))
            address = _safe_str(r.get("address"))
            city = _safe_str(r.get("city"))
            final_bid = _safe_decimal(r.get("finalBidAmount"))

            # Severity from the redemption window: an open (future) redemption
            # period is the actionable window — the prior owner can still
            # redeem and a buyer can engage. Expired/unknown is lower priority.
            if redemption_date is not None and redemption_date >= today:
                severity = "high"
            elif redemption_date is not None and redemption_date < today:
                severity = "low"
            else:
                severity = "medium"

            title_bits = ["Sheriff foreclosure sale"]
            if address:
                title_bits.append(f"— {address}")
            if city:
                title_bits.append(f", {city}")
            title = " ".join(title_bits)[:500]

            desc_parts = [
                f"Completed Hennepin County sheriff sale on "
                f"{sale_date.isoformat()}."
            ]
            if owner:
                desc_parts.append(f"Mortgagor: {owner}.")
            if final_bid is not None:
                desc_parts.append(f"Final bid: ${final_bid:,.0f}.")
            if redemption_date is not None:
                desc_parts.append(
                    f"Redemption expires {redemption_date.isoformat()}."
                )
            if _safe_str(r.get("typeOfSale")):
                desc_parts.append(f"Type: {r['typeOfSale']}.")
            description = " ".join(desc_parts)[:2000]

            signals.append(DistressEventInsert(
                parcel_id=parcel_id,
                event_type="sheriff_sale",
                event_subtype="completed_sale",
                event_date=sale_date,
                event_value=final_bid,
                source=self.source_name,
                source_id=record_no,
                severity=severity,  # type: ignore[arg-type]
                title=title,
                description=description,
                raw_data={
                    # Store the full detail record so the redemption-window
                    # UI can read redemptionExpirationDate directly, and so
                    # nothing the API returns is lost.
                    "saleRecordNumber": record_no,
                    "dateOfSale": r.get("dateOfSale"),
                    "typeOfSale": r.get("typeOfSale"),
                    "address": address,
                    "city": city,
                    "mortgagors": r.get("mortgagors"),
                    "mortgagee": r.get("mortgagee"),
                    "toWhomSold": r.get("toWhomSold"),
                    "finalBidAmount": r.get("finalBidAmount"),
                    "redemptionExpirationDate": r.get(
                        "redemptionExpirationDate"
                    ),
                    "lawFirm": r.get("lawFirm"),
                    "mortgageDocumentNumber": r.get(
                        "mortgageDocumentNumber"
                    ),
                    "comments": r.get("comments"),
                    "noticeOfIntent": r.get("noticeOfIntent"),
                    "_source": self.source_name,
                },
                observed_at=datetime.now(timezone.utc),
            ))

        return signals

    # ---- Write (mirror Anoka: resolve parcels + dedup events) ----

    async def write(
        self, signals: list[DistressEventInsert]
    ) -> tuple[int, int, int]:
        if not signals:
            return 0, 0, 0

        unique_parcels: dict[str, ParcelUpsert] = {}
        for ev in signals:
            if ev.parcel_id in unique_parcels:
                continue
            raw = ev.raw_data or {}
            address = _safe_str(raw.get("address"))
            city = _safe_str(raw.get("city"))

            unique_parcels[ev.parcel_id] = ParcelUpsert(
                parcel_id=ev.parcel_id,
                county_code=self.county_code,
                state="MN",
                address=address,
                city=city,
                zip=None,
                raw_data={
                    "hennepin_foreclosure": raw,
                    "_source": self.source_name,
                },
                data_sources=[self.source_name],
                last_observed_at=datetime.now(timezone.utc),
            )

        parcels_failed = 0
        for payload in unique_parcels.values():
            if resolve_parcel(payload) is None:
                parcels_failed += 1

        new_events, failed_events = write_events_dedup(signals)
        logger.info(
            "Hennepin write complete",
            source=self.source_name,
            parcels=len(unique_parcels),
            events_new=new_events,
            failed=failed_events + parcels_failed,
        )
        return new_events, 0, failed_events + parcels_failed


__all__ = ["HennepinSheriffScraper"]
