"""
Anoka parcel enrichment — look up owner / value / homestead by PIN.

The Anoka sheriff foreclosure notices give us a tax parcel number
(`tax_parcel_no`, e.g. "36-31-22-43-0006") but no owner, market value,
or homestead status. Anoka County's attributed parcel layer DOES carry
those — and exposes the same dashed PIN in its `PIN2` field, so we can
join the two exactly with no normalization.

VERIFIED JOIN (2026-05-31, against live data):
    sheriff detail.tax_parcel_no = "36-31-22-43-0006"
        matched layer PIN2        = "36-31-22-43-0006"   (exact)
        layer PIN                 = "363122430006"        (12-digit, no dashes)
        layer OWNER               = "VOJTECH, JOHN WARREN"
            (cross-checks against the sheriff notice's owner_name
             "John Warren Vojtech" — confirms it's the right parcel)
    A raw digit-strip on PIN ("3631224300006", 13 digits) did NOT match
    the 12-digit PIN, so we deliberately join on PIN2, not PIN.

Source layer:
    https://gis.anokacountymn.gov/anoka_gis/rest/services/Parcels/MapServer/0
    Fields used: PIN2 (join key), OWNER, TPYRADDY/TPYRCITY/TPYRSTATE/TPYRZIP
    (taxpayer mailing → absentee), LOC_ADDR (site), MKT_VALUE, HOMESTEAD.

    NOTE — fields confirmed EMPTY in this layer (do not use):
      SPC_ASSESS (all zero), TOTAL_TAX (all null), USE_DESC (all null).
    So this enrichment provides owner / mailing / market value / homestead
    only — NOT tax amount, NOT special assessment, NOT use code.

This is a SOFT enrichment: it must never break the core sheriff scrape.
Any network/parse failure logs a warning and yields no enrichment for the
affected rows (they keep their sheriff data, just without owner/value).
"""

from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import quote

import httpx

from src.utils.logger import logger


_PARCELS_QUERY_URL = (
    "https://gis.anokacountymn.gov/anoka_gis/rest/services/"
    "Parcels/MapServer/0/query"
)

# How many PIN2 values to request per HTTP call. The layer's MaxRecordCount
# is 12500, far above any realistic foreclosure batch (~58), so one chunk
# normally suffices — but we chunk anyway to keep the WHERE/URL bounded and
# to stay polite if the foreclosure list ever grows.
_PIN_CHUNK = 100

_OUT_FIELDS = ",".join([
    "PIN2",        # join key (dashed format, matches sheriff tax_parcel_no)
    "OWNER",       # assessor owner of record
    "TPYRADDY",    # taxpayer mailing street  -> absentee test
    "TPYRCITY",
    "TPYRSTATE",
    "TPYRZIP",
    "LOC_ADDR",    # site street address       -> absentee test
    "LOC_CITY",
    "MKT_VALUE",   # estimated market value
    "HOMESTEAD",   # 'Y' = owner-occupied; else not (absentee signal)
])

_TIMEOUT = httpx.Timeout(connect=15.0, read=60.0, write=30.0, pool=30.0)
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
}


def _norm(s: Any) -> str:
    """Normalize a string for comparison: upper, collapse internal spaces."""
    if s is None:
        return ""
    return " ".join(str(s).strip().upper().split())


def _is_absentee(attrs: dict[str, Any]) -> bool | None:
    """Determine absentee status from the parcel attributes.

    Two independent signals, combined:
      1. HOMESTEAD flag: 'Y' means the county classifies it as
         owner-occupied (homesteaded) -> NOT absentee. Anything else
         (e.g. 'N', '', None) -> treat as not-homesteaded -> absentee.
      2. Taxpayer mailing street (TPYRADDY) differs from the property
         site street (LOC_ADDR) -> owner gets tax bills elsewhere ->
         absentee.

    Either signal alone flags absentee. If we have neither field
    populated, return None (unknown) rather than guessing.
    """
    homestead = _norm(attrs.get("HOMESTEAD"))
    tpyr = _norm(attrs.get("TPYRADDY"))
    site = _norm(attrs.get("LOC_ADDR"))

    have_homestead = homestead != ""
    have_addresses = tpyr != "" and site != ""

    if not have_homestead and not have_addresses:
        return None

    # Homestead 'Y' is a strong owner-occupied signal.
    if have_homestead and homestead == "Y":
        # Still allow the address mismatch to override (rare, but a
        # homesteaded-but-mail-elsewhere parcel is unusual; trust the
        # homestead flag here and call it not-absentee).
        return False

    # Not homesteaded, or homestead unknown:
    if have_addresses:
        return tpyr != site
    # Homestead present and != 'Y' (e.g. 'N'): not owner-occupied.
    return True


def _mailing(attrs: dict[str, Any]) -> str | None:
    """Compose a single-line taxpayer mailing address, or None."""
    parts = [
        attrs.get("TPYRADDY"),
        attrs.get("TPYRCITY"),
        attrs.get("TPYRSTATE"),
        attrs.get("TPYRZIP"),
    ]
    cleaned = [str(p).strip() for p in parts if p and str(p).strip()]
    if not cleaned:
        return None
    # "123 MAIN ST NW, COON RAPIDS MN 55433"
    street = cleaned[0]
    rest = " ".join(cleaned[1:])
    return f"{street}, {rest}".strip(", ").strip() or None


async def _query_chunk(
    client: httpx.AsyncClient, pins: list[str]
) -> dict[str, dict[str, Any]]:
    """Query the parcel layer for one chunk of PIN2 values.

    Returns {normalized_pin2: enrichment_dict}. On any failure, logs a
    warning and returns {} for this chunk (soft-fail).
    """
    # Build: PIN2 IN ('a','b',...). PIN2 values are dashed digit strings
    # (e.g. 36-31-22-43-0006) — safe to quote, but escape stray quotes.
    quoted = ",".join("'" + p.replace("'", "''") + "'" for p in pins)
    where = f"PIN2 IN ({quoted})"
    params = {
        "where": where,
        "outFields": _OUT_FIELDS,
        "returnGeometry": "false",
        "outSR": "4326",
        "f": "json",
    }
    url = f"{_PARCELS_QUERY_URL}?" + "&".join(
        f"{k}={quote(str(v), safe='(),= ')}" for k, v in params.items()
    )

    try:
        resp = await client.get(url)
    except httpx.HTTPError as e:
        logger.warning(
            "Anoka enrichment chunk request failed",
            error_type=type(e).__name__,
            error_repr=repr(e),
            chunk_size=len(pins),
        )
        return {}

    if resp.status_code != 200:
        logger.warning(
            "Anoka enrichment chunk non-200",
            status_code=resp.status_code,
            chunk_size=len(pins),
        )
        return {}

    try:
        data = resp.json()
    except ValueError:
        logger.warning("Anoka enrichment chunk returned non-JSON")
        return {}

    # ArcGIS sometimes returns HTTP 200 with an {"error": {...}} body.
    if isinstance(data, dict) and data.get("error"):
        logger.warning(
            "Anoka enrichment chunk ArcGIS error",
            arcgis_error=str(data.get("error"))[:300],
        )
        return {}

    out: dict[str, dict[str, Any]] = {}
    for feat in data.get("features", []):
        attrs = feat.get("attributes") or {}
        pin2 = _norm(attrs.get("PIN2"))
        if not pin2:
            continue
        mkt = attrs.get("MKT_VALUE")
        try:
            market_value = float(mkt) if mkt is not None else None
        except (TypeError, ValueError):
            market_value = None
        out[pin2] = {
            "gis_owner": (str(attrs.get("OWNER")).strip()
                          if attrs.get("OWNER") else None),
            "gis_owner_mailing": _mailing(attrs),
            "gis_is_absentee": _is_absentee(attrs),
            "gis_market_value": market_value,
            "gis_homestead": (str(attrs.get("HOMESTEAD")).strip()
                              if attrs.get("HOMESTEAD") else None),
            "gis_site_address": (str(attrs.get("LOC_ADDR")).strip()
                                 if attrs.get("LOC_ADDR") else None),
        }
    return out


async def fetch_parcel_enrichment(
    tax_parcel_nos: list[str],
) -> dict[str, dict[str, Any]]:
    """Given sheriff tax_parcel_no values, return enrichment keyed by the
    NORMALIZED parcel number (upper/space-collapsed) so callers can match
    regardless of incidental whitespace/case.

    Soft-fail: returns {} (or a partial dict) on errors; never raises.
    """
    # Dedupe + normalize, dropping blanks.
    norm_to_raw: dict[str, str] = {}
    for raw in tax_parcel_nos:
        n = _norm(raw)
        if n:
            norm_to_raw.setdefault(n, raw)
    pins = list(norm_to_raw.keys())
    if not pins:
        return {}

    logger.info(
        "Anoka enrichment starting",
        unique_pins=len(pins),
    )

    result: dict[str, dict[str, Any]] = {}
    async with httpx.AsyncClient(
        timeout=_TIMEOUT, headers=_HEADERS, follow_redirects=True
    ) as client:
        for i in range(0, len(pins), _PIN_CHUNK):
            chunk = pins[i:i + _PIN_CHUNK]
            chunk_result = await _query_chunk(client, chunk)
            result.update(chunk_result)
            # Brief politeness pause between chunks (usually only one chunk).
            if i + _PIN_CHUNK < len(pins):
                await asyncio.sleep(0.3)

    logger.info(
        "Anoka enrichment complete",
        requested=len(pins),
        matched=len(result),
    )
    return result


__all__ = ["fetch_parcel_enrichment"]
