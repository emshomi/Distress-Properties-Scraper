"""
Dakota foreclosure enrichment — owner / market value / homestead by address.

The Dakota sheriff feed (dakota_sheriff) gives clean foreclosure data — sale
date, address, sale amount — but its ArcGIS attributes carry NO owner, NO
market value, and NO PIN (only GeoAddress / GeoCity). So, like Hennepin,
the only join key is the address.

We hold the full Dakota tax-parcel roll in core.parcels (loaded by
dakota_parcels from the county's DCGIS_OL_PropertyInformation Tax Parcels layer
71 — owner, mailing, market value, homestead). So this is a PURE INTERNAL JOIN
— no external server, no network call. We match each foreclosure event's
GeoAddress to a real parcel's SITEADDRESS and copy owner / market value /
mailing / homestead onto the event.

=== THE DAKOTA ADDRESS WRINKLE (verified against live data, 2026-06) ===
Unlike Hennepin (where foreclosure and parcel addresses shared formatting),
Dakota's two layers DISAGREE on street-type spelling:
    foreclosure GeoAddress : "15527 CORNELL TRAIL",  "2007 122ND ST E"
    parcel SITEADDRESS      : "15527 CORNELL TRL",    "2007 122ND ST E #B-1"
So a Hennepin-style upper+collapse normalization would FAIL on TRAIL vs TRL,
AVENUE vs AVE, etc. We therefore normalize BOTH sides through the same suffix
map (TRAIL->TRL, AVENUE->AVE, ...) and strip any unit suffix (#B-1) before
comparing. This is the minimum normalization needed for a real match rate;
it is applied identically to both sides so it can't introduce a wrong match.

Many parcels (condos, vacant land, common areas) have an EMPTY SITEADDRESS or
share a building address across many units (the "#B-1..#B-24" case). When a
foreclosure address maps to MORE THAN ONE parcel we SKIP it (never guess which
unit). When it maps to zero we leave it blank. Only an EXACTLY-ONE match is
enriched — same honest rule as Hennepin.

Dakota parcel raw_data keys (from layer 71):
    FULLNAME_PUBLIC      owner of record (county's public-display name)
    JOINT_OWNER_PUBLIC   second owner, if any (public-display)
    OWN_ADD_L1/L2/L3     owner mailing address (3 lines)
    TOTALVAL             total estimated market value
    HOMESTEAD            "FULL HOMESTEAD" / "NON HOMESTEAD" / blank
    SITEADDRESS          site (property) address  -> the join key
    PIN                  real parcel id

=== ABSENTEE RULE ===
HOMESTEAD contains "NON"        -> is_absentee = True  (non-homestead)
HOMESTEAD contains "HOMESTEAD"  -> is_absentee = False (homestead)
blank / unknown                 -> is_absentee = None
(The frontend renders these as "Non-homestead" / "Homestead" / "—".)

=== WHY AN UPDATE JOB, NOT A SCRAPER ===
Same reason as Hennepin: dakota_sheriff writes via write_events_dedup (ON
CONFLICT DO NOTHING), so re-inserting can't refresh existing rows. This job
UPDATEs the existing distress_events rows in place, writing enrichment under
raw_data.detail.gis_* (the same keys the Anoka/Hennepin enrichment use and the
backend extractor already reads). Idempotent — safe to re-run.

Soft by construction: a parcel read failure aborts with a clear error before
any writes; per-row update failures are counted and logged, never fatal.
"""

from __future__ import annotations

from typing import Any

from src.db.supabase_client import core_table, signals_table
from src.utils.logger import logger


_COUNTY = "dakota"
_SOURCE = "dakota_sheriff"

# Cursor-paged reads.
_READ_PAGE_SIZE = 1000
_MAX_PAGES = 400  # ~150K parcels / 1000 = ~150 pages; headroom.

# Street-type suffix normalization. We map the SPELLED-OUT form (which the
# foreclosure feed uses) to the ABBREVIATION (which the parcel layer uses), so
# both sides converge. We never expand abbreviations (ambiguous), only collapse
# long->short. Applied as whole-word replacements after upper+split.
_SUFFIX_MAP: dict[str, str] = {
    "TRAIL": "TRL",
    "AVENUE": "AVE",
    "STREET": "ST",
    "DRIVE": "DR",
    "ROAD": "RD",
    "LANE": "LN",
    "COURT": "CT",
    "CIRCLE": "CIR",
    "BOULEVARD": "BLVD",
    "PLACE": "PL",
    "PARKWAY": "PKWY",
    "HIGHWAY": "HWY",
    "TERRACE": "TER",
    "CROSSING": "XING",
    "HEIGHTS": "HTS",
    "POINT": "PT",
    "SQUARE": "SQ",
}


# Directional normalization. The foreclosure feed spells directionals out
# ("SOUTH", "EAST"); the parcel layer abbreviates ("S", "E"). Like the suffix
# map, we collapse long->short and apply it identically to BOTH sides, so it
# can never introduce a one-sided (wrong) match. Compound directionals
# (NORTHEAST) are included alongside the single forms.
_DIRECTIONAL_MAP: dict[str, str] = {
    "NORTHEAST": "NE",
    "NORTHWEST": "NW",
    "SOUTHEAST": "SE",
    "SOUTHWEST": "SW",
    "NORTH": "N",
    "SOUTH": "S",
    "EAST": "E",
    "WEST": "W",
}

def _norm_addr(s: Any) -> str:
    """Normalize an address for matching across Dakota's two layers.

    Steps: upper -> drop any unit suffix (everything from the first '#') ->
    split into tokens -> map each spelled-out street type to its abbreviation
    -> rejoin. Applied IDENTICALLY to both the foreclosure GeoAddress and the
    parcel SITEADDRESS, so the mapping can never create a one-sided mismatch.

    Examples:
        "15527 CORNELL TRAIL"      -> "15527 CORNELL TRL"
        "15527 CORNELL TRL"        -> "15527 CORNELL TRL"   (already short)
        "2007 122ND ST E #B-1"     -> "2007 122ND ST E"     (unit stripped)
        "350 18TH AVENUE SOUTH"    -> "350 18TH AVE S"
    """
    if s is None:
        return ""
    text = str(s).strip().upper()
    if not text:
        return ""
    # Drop unit suffix: anything from the first '#' onward (e.g. " #B-1").
    hash_idx = text.find("#")
    if hash_idx != -1:
        text = text[:hash_idx]
    tokens = text.split()
    # Map each token through BOTH the street-suffix and directional maps
    # (a token is only ever in one of them), collapsing spelled-out forms to
    # the abbreviation the parcel layer uses. Applied identically to both
    # sides, so it cannot create a one-sided mismatch.
    mapped = []
    for tok in tokens:
        if tok in _SUFFIX_MAP:
            mapped.append(_SUFFIX_MAP[tok])
        elif tok in _DIRECTIONAL_MAP:
            mapped.append(_DIRECTIONAL_MAP[tok])
        else:
            mapped.append(tok)
    return " ".join(mapped)


def _absentee_from_homestead(homestead: Any) -> bool | None:
    """Dakota HOMESTEAD is free text: 'FULL HOMESTEAD' / 'NON HOMESTEAD' / ''.
    'NON' -> non-homestead (absentee True); 'HOMESTEAD' present without 'NON'
    -> homestead (absentee False); blank/unknown -> None."""
    if homestead is None:
        return None
    text = str(homestead).strip().upper()
    if not text:
        return None
    if "NON" in text:
        return True
    if "HOMESTEAD" in text:
        return False
    return None


def _homestead_label(homestead: Any) -> str | None:
    """Surface the homestead flag as a compact code mirroring Hennepin's
    H / N convention, so the backend extractor and frontend treat all counties
    uniformly. 'FULL HOMESTEAD' -> 'H', 'NON HOMESTEAD' -> 'N', else None."""
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
    """Join the 3 owner-address lines into a single mailing string, skipping
    blanks. e.g. L1='15527 CORNELL TRL', L2='', L3='ROSEMOUNT MN 55068'
    -> '15527 CORNELL TRL, ROSEMOUNT MN 55068'."""
    parts = []
    for key in ("OWN_ADD_L1", "OWN_ADD_L2", "OWN_ADD_L3"):
        v = raw.get(key)
        if v is not None:
            s = str(v).strip()
            if s:
                parts.append(s)
    return ", ".join(parts) if parts else None


def _owner_name(raw: dict[str, Any]) -> str | None:
    """Prefer FULLNAME_PUBLIC; append JOINT_OWNER_PUBLIC if present. Use the
    county's _PUBLIC display names (not the raw FULLNAME) to respect Dakota's
    own public-display redaction choice."""
    primary = raw.get("FULLNAME_PUBLIC")
    primary = str(primary).strip() if primary else ""
    joint = raw.get("JOINT_OWNER_PUBLIC")
    joint = str(joint).strip() if joint else ""
    if primary and joint:
        return f"{primary} & {joint}"
    return primary or joint or None


def _load_foreclosure_events() -> list[dict[str, Any]]:
    """Read all dakota_sheriff events we might enrich."""
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
    logger.info("Dakota enrichment: foreclosure events loaded", count=len(rows))
    return rows


def _build_address_index() -> dict[str, list[dict[str, Any]]]:
    """Read real Dakota parcels and index by normalized SITEADDRESS.
    Returns {normalized_address: [parcel_enrichment, ...]}.

    Only includes parcels with a real PIN and a non-empty SITEADDRESS.
    Addresses mapping to >1 parcel are kept as a list so the caller can SKIP
    ambiguous (multi-unit / shared-address) matches.
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
            if pid.startswith("DAKOTA-FC-"):
                continue  # skip foreclosure placeholder parcels
            raw = p.get("raw_data") or {}
            if not raw.get("PIN"):
                continue  # only real assessor parcels
            # Prefer SITEADDRESS from raw_data; fall back to the stored address.
            site = raw.get("SITEADDRESS") or p.get("address")
            addr = _norm_addr(site)
            if not addr:
                continue
            enrichment = {
                "gis_owner": _owner_name(raw),
                "gis_owner_mailing": _owner_mailing(raw),
                "gis_is_absentee": _absentee_from_homestead(raw.get("HOMESTEAD")),
                "gis_market_value": _mkt_value(raw.get("TOTALVAL")),
                "gis_homestead": _homestead_label(raw.get("HOMESTEAD")),
                "gis_pid": str(raw.get("PIN")),
            }
            index.setdefault(addr, []).append(enrichment)
            total += 1
        last_id = batch[-1]["parcel_id"]
        if (page + 1) % 25 == 0:
            logger.info(
                "Dakota enrichment: parcel index building",
                page=page + 1,
                indexed=total,
            )
        if len(batch) < _READ_PAGE_SIZE:
            break
    logger.info(
        "Dakota enrichment: parcel index built",
        distinct_addresses=len(index),
        indexed_parcels=total,
    )
    return index


def _merge_enrichment_into_raw(raw_data: dict[str, Any],
                               enrichment: dict[str, Any]) -> dict[str, Any]:
    """Return a new raw_data with enrichment merged under detail.gis_*.

    Mirrors where Anoka/Hennepin enrichment lives (raw_data.detail.gis_*),
    which the backend foreclosure extractor already reads. Dakota's raw_data
    has 'attributes'/'geometry' sub-objects; we add/merge a 'detail' object
    for the gis_* fields without disturbing the existing structure.
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


def run_dakota_foreclosure_enrichment() -> dict[str, int]:
    """Enrich dakota_sheriff events with parcel owner/value/homestead by a
    unique normalized-address match to core.parcels. UPDATES events in place.

    Returns a small stats dict. Raises on a parcel-read failure (nothing has
    been written at that point); per-row update failures are counted, not
    fatal.
    """
    logger.info("Dakota foreclosure enrichment starting")

    events = _load_foreclosure_events()
    if not events:
        logger.info("Dakota enrichment: no foreclosure events; nothing to do")
        return {"events": 0, "enriched": 0, "no_match": 0,
                "multi_match": 0, "failed": 0}

    index = _build_address_index()

    enriched = 0
    no_match = 0
    multi_match = 0
    failed = 0

    for ev in events:
        raw = ev.get("raw_data") or {}
        # Dakota foreclosure address lives at raw_data.attributes.GeoAddress.
        attrs = raw.get("attributes") or {}
        geo_address = attrs.get("GeoAddress")
        addr = _norm_addr(geo_address)
        matches = index.get(addr, []) if addr else []

        if len(matches) == 0:
            no_match += 1
            continue
        if len(matches) > 1:
            # Ambiguous — multiple parcels share this address (condo/duplex).
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
                "Dakota enrichment: row update failed",
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
    logger.info("Dakota foreclosure enrichment complete", **stats)
    return stats


__all__ = ["run_dakota_foreclosure_enrichment"]
