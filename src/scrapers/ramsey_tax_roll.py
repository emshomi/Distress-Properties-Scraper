"""
Ramsey County Tax-Roll miner (special-assessment burden on absentee owners).

Source: NOT an external website — this is a DATABASE MINING job, the Ramsey
        analogue of hennepin_tax_roll. It reads the Ramsey parcels already
        loaded in core.parcels (by the ramsey_parcels scraper) and derives a
        tax-distress signal from their attribute data.

=== WHY THIS SIGNAL (and the honesty around it) ===
Ramsey's attributed parcel data does NOT expose a tax-delinquency-year field
or a tax-forfeit flag the way Hennepin's does (verified against the live
layer). Ramsey's forfeit listing (xnet TFL) is published only during active
auctions and is empty between them. So the Hennepin forfeit/delinquent rules
do not transfer.

What Ramsey parcels DO carry on every row is SpecialAssessmentDue — unpaid
municipal special assessments (sidewalks, sewer, nuisance abatement, etc.).
A special assessment by itself is NOT distress: ~66% of the county carries
some assessment. But a LARGE unpaid assessment on an ABSENTEE-owned parcel is
a defensible motivated-seller indicator: an out-of-area owner carrying a
multi-thousand-dollar municipal charge.

This is an INFERRED indicator, softer than a recorded forfeiture or
delinquency. It is surfaced and titled honestly as an assessment burden, not
as a legal forfeit/delinquent status.

=== THE RULE (verified against live data) ===
  Emit one event per parcel where BOTH:
    - SpecialAssessmentDue >= $2,000
    - absentee owner: taxpayer mailing street (TaxAddress1) differs from the
      site address (SiteAddress)
  Verified subset: 1,673 of 163,880 Ramsey parcels.
  -> event_type = 'tax_assessment', subtype 'special_assessment_burden'

=== VERIFIED Ramsey raw_data field names (from live core.parcels rows) ===
  SpecialAssessmentDue   numeric unpaid special assessment
  OwnerName              owner name
  EMVTotal               total estimated market value
  SiteAddress            property street address
  SiteCityName           property city
  SiteZIP5               property zip
  TaxName1               taxpayer (mailing) name
  TaxAddress1            taxpayer mailing street (drives absentee)
  TaxCityStateZIP        taxpayer mailing city/state/zip
  TotalTax               annual property tax

=== ARCHITECTURE (mirrors hennepin_tax_roll) ===
  fetch():  page through Ramsey parcels in core.parcels (cursor by parcel_id).
            Ramsey raw_data has no JSON index, so the assessment+absentee test
            is applied in Python over the paged rows (~164 pages of 1000).
  parse():  emit a DistressEventInsert for each qualifying parcel.
  write():  write_events_dedup (idempotent — re-mining is safe; unchanged
            parcels produce new=0).

Severity scales with the size of the unpaid assessment:
    >= $10,000  -> high
    >= $5,000   -> medium
    >= $2,000   -> low
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, ClassVar

from src.db.supabase_client import core_table
from src.models.signal import DistressEventInsert
from src.scrapers.base_scraper import BaseScraper
from src.services.event_writer import write_events_dedup
from src.utils.errors import SourceUnavailableError
from src.utils.logger import logger


# Verified Ramsey parcel attribute names.
_ASSESSMENT_FIELD = "SpecialAssessmentDue"
_OWNER_FIELD = "OwnerName"
_MKT_VAL_FIELD = "EMVTotal"
_SITE_ADDR_FIELD = "SiteAddress"
_SITE_CITY_FIELD = "SiteCityName"
_SITE_ZIP_FIELD = "SiteZIP5"
_TAX_NAME_FIELD = "TaxName1"
_MAIL_STREET_FIELD = "TaxAddress1"
_MAIL_CSZ_FIELD = "TaxCityStateZIP"
_TOTAL_TAX_FIELD = "TotalTax"

# The qualifying thresholds (verified: >=2000 AND absentee -> 1,673 parcels).
_MIN_ASSESSMENT = Decimal("2000")
_SEVERITY_MEDIUM_AT = Decimal("5000")
_SEVERITY_HIGH_AT = Decimal("10000")

# Stable event_date. The assessment signal has no natural date in the parcel
# data, and event_date is part of the dedup key, so it MUST be constant across
# runs for re-mining to be idempotent. A fixed sentinel guarantees that.
_ASSESSMENT_SENTINEL_DATE = date(2000, 1, 1)

# Read paging. We page the full Ramsey parcel set (~163,880) and filter in
# Python, because Ramsey raw_data has no JSON index to filter server-side.
_READ_PAGE_SIZE = 1000
_MAX_PAGES = 300  # 163,880 / 1000 ~= 164 pages; headroom.

_TITLE = "Significant unpaid special assessment (absentee owner)"
_DESC = (
    "Parcel carries an unpaid municipal special assessment of {amount} and is "
    "owned by an absentee party (mailing address differs from the property). "
    "An indicator of owner burden and potential sale motivation, not a "
    "recorded tax forfeiture or delinquency."
)


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
    try:
        d = Decimal(str(value))
        return d if d >= 0 else None
    except (InvalidOperation, ValueError, TypeError):
        return None


def _compose_owner_mailing(raw: dict[str, Any]) -> str | None:
    """Join the taxpayer mailing lines (TaxAddress1 + TaxCityStateZIP)."""
    line1 = _safe_str(raw.get(_MAIL_STREET_FIELD))
    line2 = _safe_str(raw.get(_MAIL_CSZ_FIELD))
    parts = [p for p in (line1, line2) if p]
    return ", ".join(parts) if parts else None


def _is_absentee(site_addr: str | None, mailing_street: str | None) -> bool:
    """Absentee owner = taxpayer mailing street differs from site street.

    Conservative: only True when we have both and they clearly differ after
    normalizing (uppercase, collapsed whitespace).
    """
    if not site_addr or not mailing_street:
        return False
    s = " ".join(site_addr.upper().split())
    m = " ".join(mailing_street.upper().split())
    return s != m


def _format_money(amount: Decimal) -> str:
    """Human-readable dollar string for the description, e.g. $3,203."""
    return f"${amount:,.0f}"


class RamseyTaxRollScraper(BaseScraper[dict[str, Any], DistressEventInsert]):
    """Ramsey tax-roll miner: significant special assessment + absentee owner."""

    source_name: ClassVar[str] = "ramsey_tax_roll"
    signal_type: ClassVar[str] = "tax_roll"
    county_code: ClassVar[str] = "ramsey"

    # ---- Fetch: page through Ramsey parcels in core.parcels ----

    async def fetch(self, trigger: str) -> list[dict[str, Any]]:
        """
        Read Ramsey parcels loaded by the ramsey_parcels scraper.

        Unlike Hennepin (which has partial JSON indexes for forfeit/delq), the
        Ramsey assessment test (SpecialAssessmentDue >= 2000 AND absentee) is
        not cheaply expressible against an index, so we page the full Ramsey
        parcel set and apply the test in parse(). ~164 pages of 1000; the
        per-page query uses the parcel_id cursor and the county_code filter.
        """
        rows_out: list[dict[str, Any]] = []
        last_parcel_id = ""

        for page in range(_MAX_PAGES):
            try:
                resp = (
                    core_table("parcels")
                    .select(
                        "parcel_id, county_code, address, city, zip, "
                        "estimated_market_value, data_sources, raw_data"
                    )
                    .eq("county_code", self.county_code)
                    .contains("data_sources", ["ramsey_parcels"])
                    .gt("parcel_id", last_parcel_id)
                    .order("parcel_id")
                    .limit(_READ_PAGE_SIZE)
                    .execute()
                )
            except Exception as e:
                raise SourceUnavailableError(
                    f"Reading core.parcels (ramsey) for tax-roll mining "
                    f"failed: {type(e).__name__}: {e}",
                    source=self.source_name,
                    context={"page": page, "cursor": last_parcel_id},
                ) from e

            rows = resp.data or []
            if not rows:
                break

            rows_out.extend(rows)
            last_parcel_id = rows[-1]["parcel_id"]

            if (page + 1) % 20 == 0:
                logger.info(
                    "Ramsey tax-roll page read",
                    source=self.source_name,
                    page=page + 1,
                    cumulative=len(rows_out),
                )

            if len(rows) < _READ_PAGE_SIZE:
                break

        logger.info(
            "Ramsey tax-roll fetch complete",
            source=self.source_name,
            candidate_parcels=len(rows_out),
        )
        return rows_out

    # ---- Parse: emit an event per qualifying parcel ----

    async def parse(
        self, raw_records: list[dict[str, Any]]
    ) -> list[DistressEventInsert]:
        signals: list[DistressEventInsert] = []
        n_emitted = 0

        for row in raw_records:
            parcel_id = _safe_str(row.get("parcel_id"))
            if not parcel_id:
                continue

            raw = row.get("raw_data") or {}

            assessment = _safe_decimal(raw.get(_ASSESSMENT_FIELD))
            if assessment is None or assessment < _MIN_ASSESSMENT:
                continue  # below threshold — not a signal

            site_addr = (
                _safe_str(row.get("address"))
                or _safe_str(raw.get(_SITE_ADDR_FIELD))
            )
            mailing_street = _safe_str(raw.get(_MAIL_STREET_FIELD))
            if not _is_absentee(site_addr, mailing_street):
                continue  # owner-occupied — not the signal we ship

            # --- Qualifies. Build the enriched event. ---
            owner_name = _safe_str(raw.get(_OWNER_FIELD))
            municipality = (
                _safe_str(row.get("city")) or _safe_str(raw.get(_SITE_CITY_FIELD))
            )
            prop_zip = _safe_str(row.get("zip")) or _safe_str(raw.get(_SITE_ZIP_FIELD))
            market_value = (
                _safe_decimal(row.get("estimated_market_value"))
                or _safe_decimal(raw.get(_MKT_VAL_FIELD))
            )
            mv_str = str(market_value) if market_value is not None else "0"
            annual_tax = _safe_decimal(raw.get(_TOTAL_TAX_FIELD))
            tax_tot_str = str(annual_tax) if annual_tax is not None else None
            owner_mailing = _compose_owner_mailing(raw)

            if assessment >= _SEVERITY_HIGH_AT:
                severity = "high"
            elif assessment >= _SEVERITY_MEDIUM_AT:
                severity = "medium"
            else:
                severity = "low"

            signals.append(DistressEventInsert(
                parcel_id=parcel_id,
                event_type="tax_assessment",
                event_subtype="special_assessment_burden",
                # Stable sentinel — the assessment has no date in the parcel
                # data, and event_date is part of the dedup key, so it must be
                # constant across runs for re-mining to be idempotent.
                event_date=_ASSESSMENT_SENTINEL_DATE,
                event_value=assessment,
                source=self.source_name,
                source_id=parcel_id,
                severity=severity,  # type: ignore[arg-type]
                title=_TITLE,
                description=_DESC.format(amount=_format_money(assessment)),
                raw_data={
                    "owner_name": owner_name,
                    "market_value": mv_str,
                    "municipality": municipality,
                    # Property identification.
                    "property_address": site_addr,
                    "property_city": municipality,
                    "property_zip": prop_zip,
                    # Owner contact (mailing) + absentee signal (always True
                    # here — it's part of the qualifying rule).
                    "owner_mailing": owner_mailing,
                    "is_absentee": True,
                    "annual_tax": tax_tot_str,
                    # The signal-defining value, surfaced for display.
                    "special_assessment_due": str(assessment),
                    "_derived_from": "ramsey_parcels.raw_data",
                },
                observed_at=datetime.now(timezone.utc),
            ))
            n_emitted += 1

        logger.info(
            "Ramsey tax-roll parse complete",
            source=self.source_name,
            tax_assessment=n_emitted,
            total=len(signals),
            scanned=len(raw_records),
        )
        return signals

    # ---- Write: idempotent dedup upsert ----

    async def write(
        self, signals: list[DistressEventInsert]
    ) -> tuple[int, int, int]:
        if not signals:
            return 0, 0, 0

        new_events, failed_events = write_events_dedup(signals)
        logger.info(
            "Ramsey tax-roll write complete",
            source=self.source_name,
            events_new=new_events,
            failed=failed_events,
        )
        return new_events, 0, failed_events


__all__ = ["RamseyTaxRollScraper"]
