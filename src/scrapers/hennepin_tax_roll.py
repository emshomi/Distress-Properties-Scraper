"""
Hennepin County Tax-Forfeited Land miner.

Source: NOT an external website — this is a DATABASE MINING job. It reads the
        Hennepin parcels already loaded in core.parcels (by the
        hennepin_parcels scraper) and derives tax-forfeiture distress signals
        from them. This is the "distress mining happens later via raw_data
        queries" step the hennepin_parcels docstring refers to.

=== WHY THIS EXISTS / HOW IT WAS DISCOVERED ===
The hennepin_parcels scraper loads all ~448K parcels into core.parcels with
their full Hennepin attribute set in raw_data, but writes NO distress signals.
Hennepin's parcel attributes include `FORFEIT_LAND_IND` — a Y/N-style flag
("T" = tax-forfeited land owned by the State). This miner selects those
parcels and emits one `hennepin_tax_roll` distress event per forfeited parcel.

The original one-time load (May 2026) produced these rows but left no
repeatable job. This module makes the mining a first-class, schedulable
scraper so the tax-forfeit signal stays current as parcels enter/leave
forfeiture (a slow, multi-year process under the post-Tyler liquidation
window — monthly re-mining is plenty).

=== THE MINING RULE (verified from existing rows) ===
A parcel is tax-forfeited when:
    raw_data->>'FORFEIT_LAND_IND' == 'T'   (case-insensitive)
    AND county_code == 'hennepin'
Carried into the signal's raw_data: owner_name, market_value, municipality,
plus _derived_from + forfeit_land_ind provenance markers (matching the shape
the properties.py extractor already reads).

=== ARCHITECTURE ===
Unlike the web scrapers, fetch() queries the database instead of HTTP:
  fetch():  page through core.parcels WHERE raw_data->>FORFEIT_LAND_IND='T'
  parse():  one DistressEventInsert per forfeited parcel
  write():  write_events_dedup (idempotent — re-mining is safe, dedups on
            source + source_id, so unchanged parcels produce new=0)

Severity: forfeited land in the active liquidation window is a real
acquisition opportunity → medium (it's not time-boxed like a redemption
clock, so not high; not stale, so not low).
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, ClassVar

from src.db.supabase_client import core_table
from src.models.signal import DistressEventInsert
from src.scrapers.base_scraper import BaseScraper
from src.services.event_writer import write_events_dedup
from src.utils.errors import SourceUnavailableError
from src.utils.logger import logger


# Hennepin parcel attribute that flags tax-forfeited (state-owned) land.
_FORFEIT_FIELD = "FORFEIT_LAND_IND"
_FORFEIT_TRUE = "T"

# JSON-path filter key for the Supabase query (PostgREST ->> text accessor).
_FORFEIT_JSONPATH = f"raw_data->>{_FORFEIT_FIELD}"

# Page size for reading core.parcels. Forfeited parcels are a small subset
# (~4K of 448K), but we page defensively.
_READ_PAGE_SIZE = 1000
_MAX_PAGES = 200  # 200K forfeited parcels would be absurd; safety ceiling.

_TITLE = "Tax-forfeited land (state)"
_DESCRIPTION = (
    "Parcel forfeited to the State of Minnesota for unpaid property taxes. "
    "Subject to county liquidation under the post-Tyler v. Hennepin reforms."
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


class HennepinTaxRollScraper(BaseScraper[dict[str, Any], DistressEventInsert]):
    """Hennepin tax-forfeited land — derived from core.parcels (DB miner)."""

    source_name: ClassVar[str] = "hennepin_tax_roll"
    signal_type: ClassVar[str] = "tax_forfeit"
    county_code: ClassVar[str] = "hennepin"

    # ---- Fetch: query core.parcels for forfeited-land parcels ----

    async def fetch(self, trigger: str) -> list[dict[str, Any]]:
        """
        Page through core.parcels where FORFEIT_LAND_IND = 'T' for Hennepin.

        Returns the matching parcel rows (parcel_id + raw_data + a few cols),
        which parse() turns into tax-forfeit signals.
        """
        all_rows: list[dict[str, Any]] = []

        for page in range(_MAX_PAGES):
            start = page * _READ_PAGE_SIZE
            end = start + _READ_PAGE_SIZE - 1
            try:
                # Equality on the JSON text accessor. Hennepin stores 'T'.
                resp = (
                    core_table("parcels")
                    .select("parcel_id, county_code, address, city, raw_data")
                    .eq("county_code", self.county_code)
                    .eq(_FORFEIT_JSONPATH, _FORFEIT_TRUE)
                    .range(start, end)
                    .execute()
                )
            except Exception as e:
                raise SourceUnavailableError(
                    f"Reading core.parcels for forfeited land failed: "
                    f"{type(e).__name__}: {e}",
                    source=self.source_name,
                    context={"page": page, "start": start},
                ) from e

            rows = resp.data or []
            all_rows.extend(rows)

            logger.info(
                "Hennepin tax-roll parcels page read",
                source=self.source_name,
                page=page + 1,
                rows=len(rows),
                cumulative=len(all_rows),
            )

            if len(rows) < _READ_PAGE_SIZE:
                break

        logger.info(
            "Hennepin tax-roll fetch complete",
            source=self.source_name,
            forfeited_parcels=len(all_rows),
        )
        return all_rows

    # ---- Parse: one forfeited parcel → one tax_forfeit signal ----

    async def parse(
        self, raw_records: list[dict[str, Any]]
    ) -> list[DistressEventInsert]:
        signals: list[DistressEventInsert] = []

        for row in raw_records:
            parcel_id = _safe_str(row.get("parcel_id"))
            if not parcel_id:
                continue

            raw = row.get("raw_data") or {}

            # Defensive: confirm the flag really is 'T' (the query should
            # guarantee it, but raw_data casing can vary across loads).
            flag = _safe_str(raw.get(_FORFEIT_FIELD))
            if flag is None or flag.upper() != _FORFEIT_TRUE:
                continue

            owner_name = _safe_str(raw.get("OWNER_NM")) or "HENNEPIN FORFEITED LAND"
            municipality = (
                _safe_str(row.get("city"))
                or _safe_str(raw.get("MUNIC_NM"))
            )
            market_value = _safe_decimal(raw.get("MKT_VAL_TOT"))

            signals.append(DistressEventInsert(
                parcel_id=parcel_id,
                event_type="tax_forfeit",
                event_subtype="forfeited_to_state",
                # Forfeiture has no single "event date" in the parcel row;
                # leave event_date null (consistent with the original rows,
                # which had sale_date: null).
                event_date=None,
                event_value=market_value,
                source=self.source_name,
                source_id=parcel_id,
                severity="medium",  # type: ignore[arg-type]
                title=_TITLE,
                description=_DESCRIPTION,
                raw_data={
                    "owner_name": owner_name,
                    "market_value": (
                        str(market_value) if market_value is not None else "0"
                    ),
                    "municipality": municipality,
                    "_derived_from": "hennepin_parcels.raw_data",
                    "forfeit_land_ind": _FORFEIT_TRUE,
                },
                observed_at=datetime.now(timezone.utc),
            ))

        logger.info(
            "Hennepin tax-roll parse complete",
            source=self.source_name,
            signals=len(signals),
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
