"""
Hennepin foreclosure enrichment — owner / market value / homestead by address.

The Hennepin sheriff API (hennepin_sheriff) gives clean foreclosure data —
sale date, mortgagor, amount, exact redemption date — but NO parcel ID and no
owner-of-record / market value / homestead. Unlike Anoka (whose notices carry a
tax_parcel_no we matched to the county GIS PIN2), Hennepin foreclosure rows have
only an address as a join key.

Fortunately we ALREADY hold the full Hennepin parcel roll in core.parcels
(~448K parcels loaded by the hennepin parcel ingest; ~425K with a real market
value). So this is a PURE INTERNAL JOIN — no external server, no network call.
We match each foreclosure event's address to a real assessor parcel and copy
its owner / market value / taxpayer-mailing / homestead onto the event.

=== VERIFIED AGAINST LIVE DATA (2026-05-31) ===
Of 465 hennepin_sheriff events, a normalized-address join to real assessor
parcels (county=hennepin, real PID, excluding the foreclosures' own
HENNEPIN-FC-* placeholder parcels) yields:
    332 unique matches (71%)  -> enrich (exactly one parcel)
    130 no match      (28%)   -> leave blank (condos w/ #unit, "&" multi-
                                 address rows, format differences)
      3 multi-match    (1%)   -> SKIP (ambiguous; never guess which parcel)
Matched rows returned real, correct data (e.g. "ALISA STEWART" / $319,900 /
homestead H; bank/agency owners on REO properties), cross-checked sane.

Real Hennepin parcel raw_data keys (ALL-CAPS assessor schema):
    OWNER_NM      owner of record
    TAXPAYER_NM   taxpayer name (mailing party)  -> mailing/absentee context
    MKT_VAL_TOT   total market value
    HMSTD_CD1     homestead code: 'H' = homestead (owner-occupied),
                  'N' = non-homestead (absentee/investor), null = unknown
                  Distribution: H 328k / N 115k / null 4.8k.

=== ABSENTEE RULE ===
HMSTD_CD1 == 'H'  -> is_absentee = False (owner-occupied)
HMSTD_CD1 == 'N'  -> is_absentee = True  (not owner-occupied)
otherwise         -> is_absentee = None  (unknown)

=== WHY THIS IS AN UPDATE JOB, NOT A SCRAPER ===
The hennepin_sheriff write path uses write_events_dedup (ON CONFLICT DO
NOTHING), so re-inserting can't refresh existing rows' raw_data. This job
therefore UPDATEs the existing distress_events rows in place, writing the
enrichment under raw_data.detail.gis_* (the same keys the Anoka enrichment
uses and the backend extractor already reads). Running it again refreshes
the values (e.g. after a parcel-roll update) — idempotent by design.

Soft by construction: a parcel read failure aborts with a clear error before
any writes; per-row update failures are counted and logged, never fatal.
"""

from __future__ import annotations

import asyncio
from typing import Any

from src.db.supabase_client import core_table, signals_table
from src.utils.logger import logger


_COUNTY = "hennepin"
_SOURCE = "hennepin_sheriff"

# Cursor-paged reads (mirrors ramsey_tax_roll convention).
_READ_PAGE_SIZE = 1000
_MAX_PAGES = 600  # ~448K parcels / 1000 = ~449 pages; headroom.


def _norm_addr(s: Any) -> str:
    """Normalize an address for matching: upper, collapse whitespace, strip.

    Deliberately conservative — we only collapse case and internal spaces.
    We do NOT try to rewrite 'Ave'/'AVE', strip unit numbers, etc.; the
    verified 71% unique-match rate is on this exact normalization, and any
    looser rewriting risks wrong matches. Addresses that don't match cleanly
    stay un-enriched (honest blank) rather than risk a bad join.
    """
    if s is None:
        return ""
    return " ".join(str(s).strip().upper().split())


def _absentee_from_homestead(hmstd: Any) -> bool | None:
    code = (str(hmstd).strip().upper() if hmstd is not None else "")
    if code == "H":
        return False
    if code == "N":
        return True
    return None


def _mkt_value(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    # Treat 0 / negative as "no real value" (forfeited land etc.) -> null.
    return f if f > 0 else None


async def _load_foreclosure_events() -> list[dict[str, Any]]:
    """Read all hennepin_sheriff events we might enrich."""
    rows: list[dict[str, Any]] = []
    last_id = ""
    for page in range(_MAX_PAGES):
        resp = (
            signals_table("distress_events")
            .select("id, source_id, parcel_id, raw_data")
            .eq("source", _SOURCE)
            .gt("source_id", last_id)
            .order("source_id")
            .limit(_READ_PAGE_SIZE)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        rows.extend(batch)
        last_id = batch[-1]["source_id"]
        if len(batch) < _READ_PAGE_SIZE:
            break
    logger.info("Hennepin enrichment: foreclosure events loaded", count=len(rows))
    return rows


async def _build_address_index() -> dict[str, list[dict[str, Any]]]:
    """Read real Hennepin assessor parcels and index them by normalized
    address. Returns {normalized_address: [parcel_enrichment, ...]}.

    Excludes the foreclosures' own HENNEPIN-FC-* placeholder parcels and any
    parcel without a real PID. Addresses mapping to >1 parcel are kept as a
    list so the caller can SKIP ambiguous (multi-parcel) matches.
    """
    index: dict[str, list[dict[str, Any]]] = {}
    last_id = ""
    total = 0
    for page in range(_MAX_PAGES):
        resp = (
            core_table("parcels")
            .select("parcel_id, address, raw_data")
            .eq("county_code", _COUNTY)
            .gt("parcel_id", last_id)
            .order("parcel_id")
            .limit(_READ_PAGE_SIZE)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        for p in batch:
            pid = p.get("parcel_id") or ""
            if pid.startswith("HENNEPIN-FC-"):
                continue  # skip foreclosure placeholder parcels
            raw = p.get("raw_data") or {}
            if not raw.get("PID"):
                continue  # only real assessor parcels
            addr = _norm_addr(p.get("address"))
            if not addr:
                continue
            owner = raw.get("OWNER_NM")
            taxpayer = raw.get("TAXPAYER_NM")
            enrichment = {
                "gis_owner": (str(owner).strip() if owner else None),
                "gis_owner_mailing": (str(taxpayer).strip() if taxpayer else None),
                "gis_is_absentee": _absentee_from_homestead(raw.get("HMSTD_CD1")),
                "gis_market_value": _mkt_value(raw.get("MKT_VAL_TOT")),
                "gis_homestead": (str(raw.get("HMSTD_CD1")).strip()
                                  if raw.get("HMSTD_CD1") else None),
                "gis_pid": str(raw.get("PID")),
            }
            index.setdefault(addr, []).append(enrichment)
            total += 1
        last_id = batch[-1]["parcel_id"]
        if (page + 1) % 50 == 0:
            logger.info(
                "Hennepin enrichment: parcel index building",
                page=page + 1,
                indexed=total,
            )
        if len(batch) < _READ_PAGE_SIZE:
            break
    logger.info(
        "Hennepin enrichment: parcel index built",
        distinct_addresses=len(index),
        indexed_parcels=total,
    )
    return index


def _merge_enrichment_into_raw(raw_data: dict[str, Any],
                               enrichment: dict[str, Any]) -> dict[str, Any]:
    """Return a new raw_data with enrichment merged under detail.gis_*.

    Mirrors where the Anoka enrichment lives (raw_data.detail.gis_*), which
    the backend foreclosure extractor already reads. Hennepin's raw_data is
    flat (no 'detail' sub-object), so we create one to carry the gis_* fields
    without disturbing the existing top-level fields the extractor uses.
    """
    new_raw = dict(raw_data or {})
    detail = dict(new_raw.get("detail") or {})
    detail.update({
        "gis_owner": enrichment.get("gis_owner"),
        "gis_owner_mailing": enrichment.get("gis_owner_mailing"),
        "gis_is_absentee": enrichment.get("gis_is_absentee"),
        "gis_market_value": enrichment.get("gis_market_value"),
        "gis_homestead": enrichment.get("gis_homestead"),
        "gis_pid": enrichment.get("gis_pid"),
    })
    new_raw["detail"] = detail
    return new_raw


async def run_hennepin_foreclosure_enrichment() -> dict[str, int]:
    """Enrich hennepin_sheriff events with parcel owner/value/homestead by a
    unique normalized-address match to core.parcels. UPDATES events in place.

    Returns a small stats dict. Raises on a parcel-read failure (nothing has
    been written at that point); per-row update failures are counted, not
    fatal.
    """
    logger.info("Hennepin foreclosure enrichment starting")

    events = await _load_foreclosure_events()
    if not events:
        logger.info("Hennepin enrichment: no foreclosure events; nothing to do")
        return {"events": 0, "enriched": 0, "no_match": 0,
                "multi_match": 0, "failed": 0}

    index = await _build_address_index()

    enriched = 0
    no_match = 0
    multi_match = 0
    failed = 0

    for ev in events:
        raw = ev.get("raw_data") or {}
        addr = _norm_addr(raw.get("address"))
        matches = index.get(addr, []) if addr else []

        if len(matches) == 0:
            no_match += 1
            continue
        if len(matches) > 1:
            # Ambiguous — multiple parcels share this address (duplex/condo).
            # Never guess; leave un-enriched.
            multi_match += 1
            continue

        enrichment = matches[0]
        new_raw = _merge_enrichment_into_raw(raw, enrichment)

        try:
            (
                signals_table("distress_events")
                .update({"raw_data": new_raw})
                .eq("id", ev["id"])
                .execute()
            )
            enriched += 1
        except Exception as e:
            failed += 1
            logger.warning(
                "Hennepin enrichment: row update failed",
                event_id=ev.get("id"),
                source_id=ev.get("source_id"),
                error_type=type(e).__name__,
                error_repr=repr(e),
            )

    stats = {
        "events": len(events),
        "enriched": enriched,
        "no_match": no_match,
        "multi_match": multi_match,
        "failed": failed,
    }
    logger.info("Hennepin foreclosure enrichment complete", **stats)
    return stats


__all__ = ["run_hennepin_foreclosure_enrichment"]
