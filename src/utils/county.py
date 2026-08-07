"""
County code resolution — the ONE place a county slug is derived.

WHY THIS EXISTS
---------------
The rule for turning a county name into a core.counties slug was, as of
2026-08-07, implemented FOUR separate times: src/llm/foreclosure_promotion.py
(_county_slug), src/routes/properties.py (_county_slug), signals.distress_
with_parcel (an inline three-regex chain, since replaced), and core.county_slug
in SQL. Each free to drift from the others.

A divergence here is not cosmetic. It re-creates the duplicate-core.counties-
key bug that a naive slugify produced on 2026-07-28 — 'washington county'
became 'washington_county' alongside the real 'washington', and four counties
each held one orphaned parcel.

New code derives county slugs HERE. The existing copies should migrate to it.

THE RULE (must match core.county_slug in SQL exactly):
  1. strip a trailing "County" / "Counties"
  2. lowercase, collapse non-alphanumerics to single underscores, trim
  3. alias saint_louis -> st_louis
Verified equivalent to the SQL function on every dirty value present in the
data: 'st. louis' / 'saint louis' / 'st. louis county' all -> st_louis;
'washington county' -> washington; 'le sueur' -> le_sueur.
"""

from __future__ import annotations

import re
import time
from typing import Any

from src.db.supabase_client import core_table
from src.utils.logger import logger

_COUNTY_SUFFIX_RE = re.compile(r"\s*\bcount(?:y|ies)\b\s*$", re.IGNORECASE)

# St. Louis is the only MN county with a Saint/St. variant, so a one-entry
# alias map is sufficient. A generic slugify would produce 'st._louis'.
_COUNTY_SLUG_ALIASES = {
    "saint_louis": "st_louis",
    "st_louis": "st_louis",
}

# core.source_county_map is 18 rows and changes when a source is onboarded,
# i.e. almost never. Cached for the life of a scraper run rather than read
# per batch.
_SOURCE_MAP_CACHE: dict[str, Any] = {"data": None, "at": 0.0}
_SOURCE_MAP_TTL_S = 600.0


def county_slug(county: str | None) -> str | None:
    """Fold a county NAME into its core.counties county_code slug.

    'St. Louis' -> 'st_louis', 'Saint Louis' -> 'st_louis',
    'Washington County' -> 'washington', 'Otter Tail' -> 'otter_tail'.
    Returns None when nothing usable remains.
    """
    if not county:
        return None
    bare = _COUNTY_SUFFIX_RE.sub("", str(county)).strip()
    if not bare:
        return None
    s = re.sub(r"[^a-z0-9]+", "_", bare.lower())
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        return None
    return _COUNTY_SLUG_ALIASES.get(s, s)


def _source_map() -> dict[str, str | None]:
    """{source: county_slug} from core.source_county_map, cached.

    A NULL county_slug means the source is a STATEWIDE publisher whose county
    varies per row (mnpublicnotice, startribune_legal) — the caller falls back
    to raw_data.detail.county for those.
    """
    now = time.monotonic()
    cached = _SOURCE_MAP_CACHE["data"]
    if cached is not None and now - _SOURCE_MAP_CACHE["at"] < _SOURCE_MAP_TTL_S:
        return cached

    try:
        result = core_table("source_county_map").select(
            "source, county_slug"
        ).execute()
        mapping = {
            row["source"]: row.get("county_slug")
            for row in (result.data or [])
            if row.get("source")
        }
    except Exception as e:
        logger.warning(
            "source_county_map read failed; county resolution degraded",
            error_type=type(e).__name__,
        )
        # Return the stale cache if we have one — better than nothing.
        return cached if cached is not None else {}

    _SOURCE_MAP_CACHE["data"] = mapping
    _SOURCE_MAP_CACHE["at"] = now
    return mapping


def resolve_county_code(
    source: str, raw_data: dict[str, Any] | None = None
) -> str | None:
    """County slug for a row, from its source and (if statewide) its raw_data.

    Mirrors the expression proven against the live data on 2026-08-07:

        COALESCE(m.county_slug,
                 core.county_slug(raw_data -> 'detail' ->> 'county'))

    which resolved 7,460 of 7,472 distress events. The remaining 12 are rows
    whose parcel does not exist in core.parcels at all.

    Returns None when the county cannot be determined — an honest NULL, which
    leaves the composite FK unenforced (MATCH SIMPLE) rather than inventing a
    county. Callers must NOT substitute a default.
    """
    mapping = _source_map()

    if source in mapping:
        slug = mapping[source]
        if slug:
            return slug
        # Present with a NULL slug = statewide publisher, resolve per row.

    detail = (raw_data or {}).get("detail") or {}
    return county_slug(detail.get("county"))
