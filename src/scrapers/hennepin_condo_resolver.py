"""
Hennepin condo resolver — turn a unit address into a real parcel id.

Run: python -m scripts.run_hennepin_condo_resolver

=== WHAT IS WRONG ===
A Hennepin sheriff notice publishes a UNIT address -- '1225 Lasalle Ave #604'.
core.parcels, loaded from the county GIS parcel roll, stores only the BUILDING
address: 138 rows all reading '1225 LASALLE AVE', no unit anywhere. So the
event cannot be matched to a parcel and stays on its
HENNEPIN-FC-<saleRecordNumber> placeholder.

A placeholder EXISTS in core.parcels, so every join succeeds and returns a row
with no lat, no emv_total, no owner. Nothing errors, nothing logs, and the
product renders em-dashes as though the county published nothing.

Measured 2026-08-15: 113 hennepin sheriff_sale events on placeholders whose
address carries a '#'.

=== WHY THIS IS NOT AN ADDRESS MATCH ===
Matching on address alone cannot work: 121 Washington Ave S resolves to 615
parcels, 1225 Lasalle Ave to 138. Any match would be a guess across units whose
values differ by 4x.

Matching on address + OWNER NAME was measured and rejected: 3 of 8 sampled.
The assessor's owner of record has often already changed to the bank or a new
buyer by the time a foreclosure publishes, so the mortgagor is simply absent
from the building's owner list.

=== THE COUNTY PUBLISHES THE ANSWER ===
Hennepin's Property Information Search takes house number + street + UNIT and
returns the parcel. Verified by hand on 2026-08-15:

    POST https://www16.co.hennepin.mn.us/pins/addrresult.jsp
    house=1225&street=Lasalle&condo=604&ps=20
    -> Property ID number: 27-029-24-24-0203, EMV $118,000

A plain form POST. No token, no CAPTCHA, no JavaScript. Works county-wide --
'4387 Wilshire Blvd' resolves in MOUND with no city field.

=== TWO PATHS, BECAUSE THE NOTICE'S UNIT FORMAT IS NOT THE COUNTY'S ===
1. POST with the unit. A hit returns ONE parcel: scrape the Property ID.
2. A miss re-POSTs WITHOUT the unit and gets every unit in the building, each
   with its own PID, and matches on trailing digits.

Path 2 exists because the notice writes '4387 Wilshire Blvd #D204' and the
county records '#204'. Searching '#D204' returns nothing; the building list
contains #204 with PID 19-117-23-13-0127. Guessing which unit is meant would be
wrong; reading the building's own list is not.

ps=100 on the fallback. The default page size is 20 and 4387 Wilshire Blvd has
36 units -- at ps=20 the last 16 are on a second page and a unit could be
'missing' when it is merely paginated.

=== IT ONLY RE-KEYS; IT NEVER CREATES A PARCEL ===
The resolved parcel is ALREADY in core.parcels with value and coordinates --
2702924240203 holds emv_total $118,000, lat/lng, year_built 1978. Nothing needs
fetching. This writes distress_events.parcel_id and nothing else, and refuses
to write a parcel id that does not already exist.

=== PACING ===
The site sits behind an F5 load balancer (f5_cspm cookies on the response).
MCRO is recorded as F5 Shape bot-defended, so this paces deliberately at 1.5s
between parcels -- the same rate the Tyler portal work settled on -- and one
client keeps the session cookie rather than reconnecting per request.
"""

from __future__ import annotations

import asyncio
import html
import os
import re
import sys
from typing import Any

import httpx

from src.db.supabase_client import core_table, signals_table
from src.utils.logger import logger


_COUNTY = "hennepin"
_SOURCE = "hennepin_sheriff"
_URL = "https://www16.co.hennepin.mn.us/pins/addrresult.jsp"

_PACE_SECONDS = 1.5
_MAX_RETRIES = 3
_LIST_PAGE_SIZE = 100

# BOTH PATTERNS ARE WRITTEN AGAINST CAPTURED MARKUP, NOT AGAINST SCREENSHOTS.
#
# The first version of this file guessed both from rendered pages and matched
# NOTHING: run #1 on 2026-08-15 returned resolved=0 of 113 candidates, 107 of
# them reported as 'unit_not_in_building' when the real cause was that neither
# regex could ever fire. The page source was then captured and both were
# rebuilt and tested against it.
#
# Single-result page. The label and the value are SEPARATE divs with newlines
# and tabs between them:
#     <div class="col">Property ID number:</div>
#     <div class="col">
#         <strong>27-029-24-24-0203</strong>
#     </div>
# The old pattern allowed one optional tag in that gap. The real gap is
# '</div>\n\t<div class="col">\n\t\t<strong>'.
_PID_RE = re.compile(
    r"Property\s+ID\s+number:\s*</div>.*?<strong>\s*"
    r"([\d]{2}-[\d]{3}-[\d]{2}-[\d]{2}-[\d]{4})\s*</strong>",
    re.IGNORECASE | re.DOTALL,
)

# Building list. Keyed on the LINK, not the displayed text, because the href
# already carries the 13-digit parcel id in core.parcels format -- no
# hyphen-stripping, no reformatting:
#     <a href="pidresult.jsp?pid=1911723130112">&nbsp;19-117-23-13-0112</a>
#     </td><td ...><p> &nbsp;4387
#        WILSHIRE BLVD #101 </p></td>
# The address SPANS A NEWLINE and is preceded by &nbsp;, so any pattern
# matching '#' within one line misses every row.
_ROW_RE = re.compile(
    r'href="pidresult\.jsp\?pid=(\d+)".*?</td>\s*<td[^>]*>(.*?)</td>',
    re.IGNORECASE | re.DOTALL,
)


def _clean_cell(text: str) -> str:
    """Strip tags, unescape entities, collapse whitespace across newlines."""
    t = re.sub(r"<[^>]+>", " ", text or "")
    t = html.unescape(t).replace("\xa0", " ")
    return " ".join(t.split())
# '1225 Lasalle Ave #604' -> ('1225', 'Lasalle Ave', '604')
_ADDR_UNIT_RE = re.compile(r"^(\d+)\s+(.+?)\s*#\s*(.+)$")

# '3500 Portland Ave S' -> ('3500', 'Portland Ave S'). No unit.
#
# ADDED 2026-08-15. The first version only accepted addresses containing '#',
# because condos were the known problem. Measured afterwards: of 69 hennepin
# events still on a placeholder, only 13 are unresolved condos -- 52 carry an
# ORDINARY address that the internal match still missed.
#
# The county's form resolves those too, and its street matching is FUZZIER
# than ours: it returns 27 CIRCLE WEST for a search of 'Circle W'. That is
# exactly why an exact normalised join inside the database could never find
# them and this endpoint can.
#
# Verified by hand 2026-08-15:
#     3500 Portland Ave S  -> 03-028-24-41-0135 (MINNEAPOLIS)
#     27 Circle W          -> 29-117-21-11-0021 (EDINA, recorded '27 CIRCLE WEST')
#     11115 Quantico La N  -> TWO parcels, left unresolved on purpose
_ADDR_PLAIN_RE = re.compile(r"^(\d+)\s+(.+)$")

# Sheriff placeholders that are not addresses at all. 'SALE CARD NOT USED' is
# published for a cancelled or unused sale record; posting it to the county
# would be a guaranteed miss and a wasted request.
_NOT_AN_ADDRESS_RE = re.compile(
    r"sale\s+card|not\s+used|address\s+(pending|unassigned)|^\s*$",
    re.IGNORECASE,
)


def _headers() -> dict[str, str]:
    return {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://www16.co.hennepin.mn.us",
        "Referer": "https://www16.co.hennepin.mn.us/pins/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ),
    }


def _digits(s: str) -> str:
    """Trailing digits of a unit label. 'D204' -> '204', '#A1620' -> '1620'.

    The notice's prefix letters are its own formatting, not the county's: the
    county records 4387 Wilshire Blvd #D204 as #204. Comparing on digits is
    what lets the building list resolve it.
    """
    m = re.search(r"(\d+)\s*$", (s or "").strip())
    return m.group(1) if m else ""


def _to_parcel_id(pid_display: str) -> str:
    """'27-029-24-24-0203' -> '2702924240203', the core.parcels format."""
    return re.sub(r"[^0-9]", "", pid_display or "")


async def _post(client: httpx.AsyncClient, data: dict[str, str]) -> str | None:
    """One form POST. Returns the HTML body, or None after retries."""
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            resp = await client.post(_URL, data=data)
            if resp.status_code == 200:
                return resp.text
            if resp.status_code in (429, 500, 502, 503, 504):
                await asyncio.sleep(2 ** attempt)
                continue
            logger.warning(
                "Hennepin condo: unexpected status",
                status=resp.status_code,
                data=data,
            )
            return None
        except httpx.HTTPError as e:
            if attempt == _MAX_RETRIES:
                logger.warning(
                    "Hennepin condo: request failed",
                    error_type=type(e).__name__,
                    data=data,
                )
                return None
            await asyncio.sleep(2 ** attempt)
    return None


async def _resolve_one(
    client: httpx.AsyncClient, house: str, street: str, unit: str
) -> tuple[str | None, str]:
    """Resolve one address to a 13-digit parcel id. `unit` may be empty.

    Returns (parcel_id, how) where `how` records which path produced it, so a
    later query can tell a direct hit from a list match without re-running.
    """
    # NO UNIT: one POST. A single result is the parcel; a LIST means the
    # address maps to more than one parcel and must NOT be guessed at.
    # 11115 Quantico La N returns two parcels (33-120-22-31-0003 and
    # 33-120-22-32-0027) -- a house on two tax parcels, with nothing on the
    # page saying which one the foreclosure is against.
    if not unit:
        body = await _post(client, {"house": house, "street": street,
                                    "condo": "", "ps": str(_LIST_PAGE_SIZE)})
        if not body:
            return None, "no_response"
        m = _PID_RE.search(body)
        if m:
            return _to_parcel_id(m.group(1)), "direct_no_unit"
        rows = _ROW_RE.findall(body)
        if len(rows) == 1:
            return rows[0][0], "list_single_no_unit"
        if len(rows) > 1:
            return None, "ambiguous_address"
        return None, "address_not_found"

    # Path 1: ask for the unit directly.
    body = await _post(client, {"house": house, "street": street,
                                "condo": unit, "ps": "20"})
    if body:
        m = _PID_RE.search(body)
        if m:
            return _to_parcel_id(m.group(1)), "direct"

    await asyncio.sleep(_PACE_SECONDS)

    # Path 2: the whole building, matched on trailing digits.
    body = await _post(client, {"house": house, "street": street,
                                "condo": "", "ps": str(_LIST_PAGE_SIZE)})
    if not body:
        return None, "no_response"

    want = _digits(unit)
    if not want:
        return None, "unit_has_no_digits"

    # pid from the href is ALREADY the 13-digit core.parcels format.
    matches = [
        pid
        for pid, addr in _ROW_RE.findall(body)
        if _digits(_clean_cell(addr).split("#")[-1]) == want
    ]
    # A single result page (no list) can also come back here.
    if not matches:
        m = _PID_RE.search(body)
        if m:
            return _to_parcel_id(m.group(1)), "list_single"
        return None, "unit_not_in_building"

    if len(matches) > 1:
        # Two units whose trailing digits agree. Never guess.
        return None, "ambiguous_in_building"

    return matches[0], "list"


async def run_hennepin_condo_resolver() -> dict[str, int]:
    """Re-key condo foreclosure events onto their real parcels."""
    max_events = int(os.environ.get("MAX_EVENTS", "0") or 0)
    logger.info("Hennepin condo resolver starting",
                max_events=max_events or "uncapped")

    # Events on a placeholder whose parcel address carries a unit number.
    resp = (
        signals_table("distress_events")
        .select("id, source_id, parcel_id, raw_data")
        .eq("source", _SOURCE)
        .like("parcel_id", "HENNEPIN-FC-%")
        .execute()
    )
    # Any event with a usable address, not only condos. See _ADDR_PLAIN_RE.
    events = []
    for e in (resp.data or []):
        a = ((e.get("raw_data") or {}).get("address") or "").strip()
        if a and not _NOT_AN_ADDRESS_RE.search(a):
            events.append(e)
    logger.info("Hennepin condo resolver: candidates", count=len(events))

    stats = {"candidates": len(events), "resolved": 0, "rekeyed": 0,
             "unparsed_address": 0, "not_found": 0, "ambiguous": 0,
             "parcel_missing": 0, "rekey_collision": 0, "failed": 0}
    if not events:
        return stats

    timeout = httpx.Timeout(connect=20.0, read=60.0, write=30.0, pool=30.0)
    attempted = 0
    async with httpx.AsyncClient(timeout=timeout, headers=_headers(),
                                 follow_redirects=True) as client:
        for ev in events:
            # The cap bounds ATTEMPTS as well as re-keys. Run #1 was dispatched
            # with max_events=10 and walked all 113 for ten minutes, because
            # nothing was re-keyed and the cap only counted successes. A capped
            # test that ignores its cap when things go wrong is not a test.
            if max_events and (stats["rekeyed"] >= max_events
                               or attempted >= max_events * 3):
                logger.info("Hennepin condo resolver: MAX_EVENTS reached",
                            cap=max_events, attempted=attempted,
                            rekeyed=stats["rekeyed"])
                break
            attempted += 1

            addr = ((ev.get("raw_data") or {}).get("address") or "").strip()
            m = _ADDR_UNIT_RE.match(addr)
            if m:
                house, street, unit = m.group(1), m.group(2), m.group(3)
            else:
                m = _ADDR_PLAIN_RE.match(addr)
                if not m:
                    stats["unparsed_address"] += 1
                    continue
                house, street, unit = m.group(1), m.group(2), ""

            parcel_id, how = await _resolve_one(client, house, street, unit)
            await asyncio.sleep(_PACE_SECONDS)

            if not parcel_id:
                if how in ("ambiguous_in_building", "ambiguous_address"):
                    stats["ambiguous"] += 1
                else:
                    stats["not_found"] += 1
                logger.info("Hennepin condo: unresolved",
                            source_id=ev.get("source_id"), address=addr,
                            reason=how)
                continue

            stats["resolved"] += 1

            # The parcel MUST already exist. This resolver never creates one:
            # a parcel_id pointing at nothing would break the composite FK and
            # would be a stub by another name.
            chk = (
                core_table("parcels")
                .select("parcel_id")
                .eq("county_code", _COUNTY)
                .eq("parcel_id", parcel_id)
                .limit(1)
                .execute()
            )
            if not (chk.data or []):
                stats["parcel_missing"] += 1
                logger.warning("Hennepin condo: resolved parcel not in spine",
                               source_id=ev.get("source_id"),
                               address=addr, parcel_id=parcel_id)
                continue

            raw = dict(ev.get("raw_data") or {})
            raw["_condo_resolve"] = {
                "from": ev.get("parcel_id"),
                "to": parcel_id,
                "address": addr,
                "how": how,
                "source": "hennepin PINS addrresult.jsp",
            }

            try:
                (
                    signals_table("distress_events")
                    .update({"parcel_id": parcel_id, "raw_data": raw})
                    .eq("id", ev["id"])
                    .execute()
                )
                stats["rekeyed"] += 1
            except Exception as e:
                # distress_events_source_identity_key is
                # (county_code, source, source_id, event_date) and does NOT
                # contain parcel_id, so a re-key should not collide. Counted
                # separately rather than hidden in failed, because a collision
                # here would mean that assumption is wrong.
                stats["rekey_collision"] += 1
                logger.warning("Hennepin condo: re-key failed",
                               source_id=ev.get("source_id"),
                               to_parcel=parcel_id,
                               error_type=type(e).__name__,
                               error_repr=repr(e)[:300])

    logger.info("Hennepin condo resolver complete", **stats)
    return stats


if __name__ == "__main__":
    sys.exit(0 if asyncio.run(run_hennepin_condo_resolver()) else 0)


__all__ = ["run_hennepin_condo_resolver"]
