"""
Washington foreclosure enrichment — owner / market value / homestead by PID.

The Washington sheriff feed (washington_sheriff) gives clean completed-sale
data — sale date, sale amount, document number — but no owner, no market value,
no mailing address. Each sheriff event DOES carry the parcel's unformatted PID,
stored as the event's parcel_id in the form "WASHINGTON-FC-{pid}".

We hold the full Washington tax-parcel roll in core.parcels (loaded by
washington_parcels from the county TaxParcel FeatureServer — PIN, owner,
mailing, market value, homestead). So this is a PURE INTERNAL JOIN — no external
server, no network call.

=== WHY THIS IS A CLEAN PID JOIN (not an address match like Dakota) ===
Unlike Dakota (whose foreclosure feed carried only a messy GeoAddress, forcing a
suffix-normalized address match with multi-unit ambiguity), the Washington
sheriff file carries the unformatted PID directly — the SAME value as the
TaxParcel PIN. A pre-build verification confirmed 110 of 111 foreclosure stubs
match a real parcel by exact PID. So we join on PID, with NO address
normalization and NO multi-match guessing: a PID maps to exactly one parcel or
to none. The one unmatched stub (a split/combined/retired parcel) honestly stays
blank ("—") rather than getting wrong data.

Washington parcel raw_data keys (from the TaxParcel layer):
    PIN              real parcel id      -> the join key
    OWNER_NAME       owner of record
    OWN_ADD_L1/L2/L3 owner mailing address (3 lines)
    EMV_TOTAL        total estimated market value
    HOMESTEAD        homestead flag (free text)
    SITUS_ADDRESS    site (property) address
    CITY             site city
    ZIP              site zip

=== ABSENTEE / HOMESTEAD RULE ===
HOMESTEAD contains "NON"        -> is_absentee = True  (non-homestead) -> "N"
HOMESTEAD contains "HOMESTEAD"  -> is_absentee = False (homestead)     -> "H"
blank / unknown                 -> is_absentee = None                  -> None
(Same convention the Dakota/Anoka/Hennepin enrichment use; the backend
extractor and frontend already read these gis_* keys uniformly.)

=== WHY AN UPDATE JOB, NOT A SCRAPER ===
Same reason as Dakota/Hennepin: washington_sheriff writes via write_events_dedup
(ON CONFLICT DO NOTHING), so re-inserting can't refresh existing rows. This job
UPDATEs the existing distress_events rows in place, writing enrichment under
raw_data.detail.gis_* (the keys the backend foreclosure extractor reads).
Idempotent — safe to re-run.

Soft by construction: a parcel read failure aborts with a clear error before any
writes; per-row update failures are counted and logged, never fatal.
"""

from __future__ import annotations

from typing import Any

from src.db.supabase_client import core_table, signals_table
from src.utils.logger import logger


_COUNTY = "washington"
_SOURCE = "washington_sheriff"
_FC_PREFIX = "WASHINGTON-FC-"

# Cursor-paged reads.
_READ_PAGE_SIZE = 1000
_MAX_PAGES = 400  # ~118K parcels / 1000 = ~119 pages; headroom.


def _absentee_from_homestead(homestead: Any) -> bool | None:
    """Map Washington's HOMESTEAD flag to an absentee boolean.

    Washington uses single-letter codes: 'Y' = homestead, 'N' = non-homestead.
    We also keep a word-based fallback ('NON' / 'HOMESTEAD') so the function
    stays correct if the format ever changes or matches other counties.
    'Y' / homestead -> absentee False; 'N' / non-homestead -> absentee True;
    blank/unknown -> None."""
    if homestead is None:
        return None
    text = str(homestead).strip().upper()
    if not text:
        return None
    # Washington's single-letter codes.
    if text == "Y":
        return False  # homestead -> owner-occupied (not absentee)
    if text == "N":
        return True   # non-homestead -> absentee
    # Word-based fallback (other formats).
    if "NON" in text:
        return True
    if "HOMESTEAD" in text:
        return False
    return None

def _homestead_label(homestead: Any) -> str | None:
    """Surface the homestead flag as a compact code (H / N) matching the other
    counties, so the backend extractor and frontend treat all counties the
    same. homestead -> 'H', non-homestead -> 'N', else None."""
    ab = _absentee_from_homestead(homestead)
    if ab is True:
        return "N"
    if ab is False:
        return "H"
    return None


def _mkt_value(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f > 0 else None


def _owner_mailing(raw: dict[str, Any]) -> str | None:
    """Join the 3 owner-address lines into one mailing string, skipping
    blanks."""
    parts = []
    for key in ("OWN_ADD_L1", "OWN_ADD_L2", "OWN_ADD_L3"):
        v = raw.get(key)
        if v is not None:
            s = str(v).strip()
            if s:
                parts.append(s)
    return ", ".join(parts) if parts else None


def _owner_name(raw: dict[str, Any]) -> str | None:
    """Washington carries a single OWNER_NAME field."""
    v = raw.get("OWNER_NAME")
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _site_address(raw: dict[str, Any], fallback: Any) -> str | None:
    """Prefer SITUS_ADDRESS from raw_data; fall back to the stored address."""
    v = raw.get("SITUS_ADDRESS") or fallback
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _load_foreclosure_events() -> list[dict[str, Any]]:
    """Read all washington_sheriff events we might enrich."""
    rows: list[dict[str, Any]] = []
    last_id = ""
    for _page in range(_MAX_PAGES):
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
    logger.info("Washington enrichment: foreclosure events loaded", count=len(rows))
    return rows


def _build_pid_index() -> dict[str, dict[str, Any]]:
    """Read real Washington parcels and index by PIN.
    Returns {pin: parcel_enrichment}.

    Only includes parcels with a real PIN. PID is a primary key in the roll, so
    each PIN maps to exactly one parcel — no list / ambiguity handling needed
    (unlike Dakota's address index). Foreclosure stub parcels are skipped.
    """
    index: dict[str, dict[str, Any]] = {}
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
            if pid.startswith(_FC_PREFIX):
                continue  # skip foreclosure placeholder parcels
            raw = p.get("raw_data") or {}
            pin = raw.get("PIN")
            if not pin:
                continue  # only real assessor parcels
            pin_key = "".join(str(pin).split())
            if not pin_key:
                continue
            enrichment = {
                "gis_owner": _owner_name(raw),
                "gis_owner_mailing": _owner_mailing(raw),
                "gis_is_absentee": _absentee_from_homestead(raw.get("HOMESTEAD")),
                "gis_market_value": _mkt_value(raw.get("EMV_TOTAL")),
                "gis_homestead": _homestead_label(raw.get("HOMESTEAD")),
                "gis_site_address": _site_address(raw, p.get("address")),
                "gis_city": (str(raw.get("CITY")).strip()
                             if raw.get("CITY") else None),
                "gis_zip": (str(raw.get("ZIP")).strip()
                            if raw.get("ZIP") else None),
                "gis_pid": str(pin),
            }
            # If a PIN somehow appears twice, last write wins — harmless, since
            # both rows describe the same parcel.
            index[pin_key] = enrichment
            total += 1
        last_id = batch[-1]["parcel_id"]
        if (page + 1) % 25 == 0:
            logger.info(
                "Washington enrichment: parcel index building",
                page=page + 1,
                indexed=total,
            )
        if len(batch) < _READ_PAGE_SIZE:
            break
    logger.info(
        "Washington enrichment: parcel index built",
        distinct_pins=len(index),
        indexed_parcels=total,
    )
    return index


def _pid_from_event(ev: dict[str, Any]) -> str | None:
    """Extract the bare PIN from a sheriff event's parcel_id
    ('WASHINGTON-FC-{pid}' -> '{pid}'), normalized the same way the index keys
    are (whitespace removed)."""
    parcel_id = ev.get("parcel_id") or ""
    if not parcel_id.startswith(_FC_PREFIX):
        return None
    pid = parcel_id[len(_FC_PREFIX):]
    pid = "".join(str(pid).split())
    return pid or None


def _merge_enrichment_into_raw(raw_data: dict[str, Any],
                               enrichment: dict[str, Any]) -> dict[str, Any]:
    """Return a new raw_data with enrichment merged under detail.gis_*.

    Mirrors where the Dakota/Anoka/Hennepin enrichment lives
    (raw_data.detail.gis_*), which the backend foreclosure extractor reads. The
    washington_sheriff raw_data already has a 'sale' sub-object; we add/merge a
    'detail' object without disturbing it.
    """
    new_raw = dict(raw_data or {})
    detail = dict(new_raw.get("detail") or {})
    detail.update({
        "gis_owner": enrichment.get("gis_owner"),
        "gis_owner_mailing": enrichment.get("gis_owner_mailing"),
        "gis_is_absentee": enrichment.get("gis_is_absentee"),
        "gis_market_value": enrichment.get("gis_market_value"),
        "gis_homestead": enrichment.get("gis_homestead"),
        "gis_site_address": enrichment.get("gis_site_address"),
        "gis_city": enrichment.get("gis_city"),
        "gis_zip": enrichment.get("gis_zip"),
        "gis_pid": enrichment.get("gis_pid"),
    })
    new_raw["detail"] = detail
    return new_raw


def run_washington_foreclosure_enrichment() -> dict[str, int]:
    """Enrich washington_sheriff events with parcel owner/value/homestead by an
    exact PID match to core.parcels. UPDATES events in place.

    Returns a small stats dict. Raises on a parcel-read failure (nothing has
    been written at that point); per-row update failures are counted, not fatal.
    """
    logger.info("Washington foreclosure enrichment starting")

    events = _load_foreclosure_events()
    if not events:
        logger.info("Washington enrichment: no foreclosure events; nothing to do")
        return {"events": 0, "enriched": 0, "no_match": 0, "failed": 0}

    index = _build_pid_index()

    enriched = 0
    no_match = 0
    failed = 0

    for ev in events:
        pid = _pid_from_event(ev)
        enrichment = index.get(pid) if pid else None

        if enrichment is None:
            no_match += 1
            continue

        raw = ev.get("raw_data") or {}
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
                "Washington enrichment: row update failed",
                event_id=ev.get("id"),
                source_id=ev.get("source_id"),
                error_type=type(e).__name__,
                error_repr=repr(e),
            )

    stats = {
        "events": len(events),
        "enriched": enriched,
        "no_match": no_match,
        "failed": failed,
    }
    logger.info("Washington foreclosure enrichment complete", **stats)
    return stats


__all__ = ["run_washington_foreclosure_enrichment"]
