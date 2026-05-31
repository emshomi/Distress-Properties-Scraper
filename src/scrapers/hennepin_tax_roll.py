"""
Hennepin County Tax-Roll miner (delinquent + forfeited land).

Source: NOT an external website — this is a DATABASE MINING job. It reads the
        Hennepin parcels already loaded in core.parcels (by the
        hennepin_parcels scraper) and derives tax-distress signals from them.
        This is the "distress mining happens later via raw_data queries" step
        the hennepin_parcels docstring refers to.

=== THE TWO RULES (reverse-engineered + verified against the live data) ===
Every Hennepin parcel falls into at most one tax-distress bucket. The rules
were confirmed by reconciling against the original one-time load:

  1. TAX-FORFEIT (139 parcels):
        raw_data->>'FORFEIT_LAND_IND' == 'T'
     The redemption period expired; the parcel is forfeited to the State of
     Minnesota, now owned by "HENNEPIN FORFEITED LAND", awaiting auction.
     -> event_type = 'tax_forfeit', subtype 'state_forfeited_land'

  2. TAX-DELINQUENT (4,112 parcels):
        raw_data->>'EARLIEST_DELQ_YR' present AND non-empty
        AND NOT forfeited (the two sets are mutually exclusive — verified:
        delq_not_forfeit == has_delq_yr == 4112, forfeit == 139, sum 4251)
     The owner is behind on property taxes but the parcel has NOT yet
     forfeited. Early-stage distress.
     -> event_type = 'tax_delinquent', subtype 'property_tax_delinquent'

Verified counts: 139 forfeit + 4,112 delinquent = 4,251 — matches the
original load exactly, with zero overlap.

=== VERIFIED FIELD NAMES (from live core.parcels rows) ===
  FORFEIT_LAND_IND  'T' marks forfeited land
  EARLIEST_DELQ_YR  two-digit delinquency year, e.g. '25' = 2025
  OWNER_NM          owner name
  MKT_VAL_TOT       total market value
  MUNIC_NM          municipality

=== raw_data SHAPES (match the original rows so re-mining dedups cleanly) ===
  forfeit:    {owner_name, market_value, municipality,
               _derived_from, forfeit_land_ind:'T'}
  delinquent: {owner_name, market_value, municipality,
               _derived_from, earliest_delq_year:<int>, earliest_delq_yr_raw}

=== ARCHITECTURE ===
Unlike the web scrapers, fetch() queries the database instead of HTTP:
  fetch():  page through core.parcels for Hennepin, pulling any parcel that
            is either forfeited OR has a delinquency year.
  parse():  classify each parcel into tax_forfeit or tax_delinquent and emit
            the matching DistressEventInsert.
  write():  write_events_dedup (idempotent — re-mining is safe; unchanged
            parcels produce new=0).

Severity:
  tax_forfeit                     -> medium (already seized; awaiting auction)
  tax_delinquent, older year      -> higher distress (longer behind)
    delinquent >= 3 years         -> high
    delinquent 1-2 years          -> medium
    delinquent current year       -> low
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


# Verified Hennepin parcel attribute names.
_FORFEIT_FIELD = "FORFEIT_LAND_IND"
_FORFEIT_TRUE = "T"
_DELQ_YR_FIELD = "EARLIEST_DELQ_YR"
_OWNER_FIELD = "OWNER_NM"
_MKT_VAL_FIELD = "MKT_VAL_TOT"
_MUNIC_FIELD = "MUNIC_NM"

# Stable event_date for forfeit rows (no date in the parcel data). The dedup
# key includes event_date, so this MUST be constant across runs or re-mining
# would insert duplicates every time. A fixed sentinel guarantees idempotency.
_FORFEIT_SENTINEL_DATE = date(2000, 1, 1)

# Read paging. We pull the union of (forfeited OR has delinquency year),
# which is ~4,251 of 448K — small, but page defensively.
_READ_PAGE_SIZE = 1000
_MAX_PAGES = 200

_FORFEIT_TITLE = "Tax-forfeited land (state)"
_FORFEIT_DESC = (
    "Parcel forfeited to the State of Minnesota for unpaid property taxes. "
    "Subject to county liquidation under the post-Tyler v. Hennepin reforms."
)
_DELQ_TITLE = "Tax-delinquent property"
_DELQ_DESC = (
    "Property is behind on Hennepin County property taxes (delinquent since "
    "{year}). Unresolved delinquency can proceed toward tax forfeiture."
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


def _expand_delq_year(raw_yr: str | None) -> int | None:
    """Convert Hennepin's 2-digit delinquency year ('25') to 2025.

    All observed values are recent 2-digit years. We map '00'-'99' to
    2000-2099, which is correct for the delinquency horizon this data covers.
    """
    s = _safe_str(raw_yr)
    if s is None:
        return None
    digits = s.zfill(2)[-2:]
    if not digits.isdigit():
        return None
    return 2000 + int(digits)


class HennepinTaxRollScraper(BaseScraper[dict[str, Any], DistressEventInsert]):
    """Hennepin tax-roll miner: forfeited land + delinquent properties."""

    source_name: ClassVar[str] = "hennepin_tax_roll"
    signal_type: ClassVar[str] = "tax_roll"
    county_code: ClassVar[str] = "hennepin"

    # ---- Fetch: query core.parcels for forfeited OR delinquent parcels ----

    async def fetch(self, trigger: str) -> list[dict[str, Any]]:
        """
        Cursor-paginate Hennepin parcels that are forfeited OR delinquent.

        We page by parcel_id cursor (WHERE parcel_id > last_seen) rather than
        offset .range(), because the matching parcels are sparse (~4,251 of
        448K) and offset pagination re-scans from the top every page, which
        blows past the DB statement timeout. Cursor pagination never re-scans,
        so each page picks up where the last ended.

        Relies on partial expression indexes on
        (raw_data->>'FORFEIT_LAND_IND') and (raw_data->>'EARLIEST_DELQ_YR')
        for Hennepin parcels to keep the OR filter cheap (see migration).
        """
        all_rows: list[dict[str, Any]] = []

        # OR filter: forfeit flag == 'T'  OR  delinquency-year is not null.
        or_filter = (
            f"raw_data->>{_FORFEIT_FIELD}.eq.{_FORFEIT_TRUE},"
            f"raw_data->>{_DELQ_YR_FIELD}.not.is.null"
        )

        last_parcel_id = ""  # cursor; parcel_ids sort lexicographically
        for page in range(_MAX_PAGES):
            try:
                resp = (
                    core_table("parcels")
                    .select("parcel_id, county_code, address, city, raw_data")
                    .eq("county_code", self.county_code)
                    .or_(or_filter)
                    .gt("parcel_id", last_parcel_id)
                    .order("parcel_id")
                    .limit(_READ_PAGE_SIZE)
                    .execute()
                )
            except Exception as e:
                raise SourceUnavailableError(
                    f"Reading core.parcels for tax-roll mining failed: "
                    f"{type(e).__name__}: {e}",
                    source=self.source_name,
                    context={"page": page, "cursor": last_parcel_id},
                ) from e

            rows = resp.data or []
            if not rows:
                break

            all_rows.extend(rows)
            last_parcel_id = rows[-1]["parcel_id"]  # advance cursor

            logger.info(
                "Hennepin tax-roll parcels page read",
                source=self.source_name,
                page=page + 1,
                rows=len(rows),
                cumulative=len(all_rows),
                cursor=last_parcel_id,
            )

            if len(rows) < _READ_PAGE_SIZE:
                break

        logger.info(
            "Hennepin tax-roll fetch complete",
            source=self.source_name,
            candidate_parcels=len(all_rows),
        )
        return all_rows

    # ---- Parse: classify each parcel into forfeit or delinquent ----

    async def parse(
        self, raw_records: list[dict[str, Any]]
    ) -> list[DistressEventInsert]:
        signals: list[DistressEventInsert] = []
        today = date.today()
        n_forfeit = 0
        n_delq = 0

        for row in raw_records:
            parcel_id = _safe_str(row.get("parcel_id"))
            if not parcel_id:
                continue

            raw = row.get("raw_data") or {}
            owner_name = _safe_str(raw.get(_OWNER_FIELD))
            municipality = (
                _safe_str(row.get("city")) or _safe_str(raw.get(_MUNIC_FIELD))
            )
            market_value = _safe_decimal(raw.get(_MKT_VAL_FIELD))
            mv_str = str(market_value) if market_value is not None else "0"

            forfeit_flag = _safe_str(raw.get(_FORFEIT_FIELD))
            delq_raw = _safe_str(raw.get(_DELQ_YR_FIELD))

            # --- Rule 1: forfeited (takes precedence; mutually exclusive) ---
            if forfeit_flag is not None and forfeit_flag.upper() == _FORFEIT_TRUE:
                signals.append(DistressEventInsert(
                    parcel_id=parcel_id,
                    event_type="tax_forfeit",
                    event_subtype="state_forfeited_land",
                    # Stable sentinel — forfeiture has no date in parcel data,
                    # and event_date is part of the dedup key, so it must be
                    # constant across runs for re-mining to be idempotent.
                    event_date=_FORFEIT_SENTINEL_DATE,
                    event_value=market_value,
                    source=self.source_name,
                    source_id=parcel_id,
                    severity="medium",  # type: ignore[arg-type]
                    title=_FORFEIT_TITLE,
                    description=_FORFEIT_DESC,
                    raw_data={
                        "owner_name": owner_name or "HENNEPIN FORFEITED LAND",
                        "market_value": mv_str,
                        "municipality": municipality,
                        "_derived_from": "hennepin_parcels.raw_data",
                        "forfeit_land_ind": _FORFEIT_TRUE,
                    },
                    observed_at=datetime.now(timezone.utc),
                ))
                n_forfeit += 1
                continue

            # --- Rule 2: delinquent (has a delinquency year, not forfeited) ---
            if delq_raw is not None:
                delq_year = _expand_delq_year(delq_raw)
                # Severity scales with how long the parcel has been behind.
                if delq_year is not None:
                    years_behind = today.year - delq_year
                else:
                    years_behind = 0
                if years_behind >= 3:
                    severity = "high"
                elif years_behind >= 1:
                    severity = "medium"
                else:
                    severity = "low"

                desc = _DELQ_DESC.format(
                    year=delq_year if delq_year is not None else "unknown"
                )

                # Stable, meaningful event_date: Jan 1 of the delinquency year.
                # Same value every run (idempotent dedup) AND it represents
                # when the delinquency began. Fallback to the sentinel only if
                # the year somehow won't parse (verified: 0 such rows).
                delq_event_date = (
                    date(delq_year, 1, 1)
                    if delq_year is not None
                    else _FORFEIT_SENTINEL_DATE
                )

                signals.append(DistressEventInsert(
                    parcel_id=parcel_id,
                    event_type="tax_delinquent",
                    event_subtype="property_tax_delinquent",
                    event_date=delq_event_date,
                    event_value=market_value,
                    source=self.source_name,
                    source_id=parcel_id,
                    severity=severity,  # type: ignore[arg-type]
                    title=_DELQ_TITLE,
                    description=desc,
                    raw_data={
                        "owner_name": owner_name,
                        "market_value": mv_str,
                        "municipality": municipality,
                        "_derived_from": "hennepin_parcels.raw_data",
                        "earliest_delq_year": delq_year,
                        "earliest_delq_yr_raw": delq_raw,
                    },
                    observed_at=datetime.now(timezone.utc),
                ))
                n_delq += 1

        logger.info(
            "Hennepin tax-roll parse complete",
            source=self.source_name,
            tax_forfeit=n_forfeit,
            tax_delinquent=n_delq,
            total=len(signals),
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
            "Hennepin tax-roll write complete",
            source=self.source_name,
            events_new=new_events,
            failed=failed_events,
        )
        return new_events, 0, failed_events


__all__ = ["HennepinTaxRollScraper"]
