"""
Shared spine-parcel resolver.

Answers ONE question for every caller that has a distress record and needs the
REAL core.parcels row behind it: given a county and whatever identifiers the
source published, which parcel is this?

=== WHY THIS MODULE EXISTS (2026-08-17) ===

The rule lived in src/routes/admin.py, reachable only from the HTTP approve
path. Every scraper therefore invented its own synthetic parcel_id and wrote
it straight into core.parcels. Measured 2026-08-17:

    hennepin_sheriff  11:18-11:21   381 stubs
    dakota_sheriff    13:06         129 stubs
    anoka_sheriff     12:46          17 stubs

527 stubs in one day. Against the live spine, 523 of those 527 (99.2%) resolve
to exactly one real parcel by address alone -- before directionals are even
applied, so that figure is a floor. Every one of them had a real parcel sitting
in the same table on the same composite key, and no scraper looked.

A stub carries no market value, no coordinates, no owner, no lot size. Those
are the em-dash rows. 760 of them were migrated onto real parcels on 2026-08-16
over a full session; 529 were minted back within 21 hours. Cleanup behind an
open tap is not a fix.

=== THE THREE-WAY DISTINCTION THAT MATTERS ===

Callers must be able to tell these apart, and returning None for all three is
what caused the 2026-08-17 08:54 incident:

    resolved      -> a Parcel row.  Use it.
    not resolved  -> None.          No match, or ambiguous. Mint a stub.
    unavailable   -> raises.        The database could not be asked.

The third case is why SpineLookupUnavailable exists. On 2026-08-17 a
PostgREST connection dropped mid-lookup; admin.py caught the exception,
returned None, and minted HENNEPIN-FC-3211821340028-2026-08-26 -- a stub named
after the very parcel the query had failed to fetch. A two-second network blip
became permanent bad data. A transport failure is NOT evidence that a parcel
does not exist, and this module refuses to let a caller treat it as such.

=== WHAT IS DELIBERATELY NOT HERE ===

No spatial resolver. core.parcels.geom is geography(Point,4326) -- centroids,
not boundaries -- so point-in-polygon is impossible. Confirmed 2026-08-17
against pg_attribute after ST_Contains(geography, geometry) failed to resolve.
Loading parcel polygons is a data-acquisition problem, not a query problem.
"""

from __future__ import annotations

import re
from typing import Any, Optional

import httpx

from src.db.supabase_client import core_table, get_client
from src.utils.logger import logger


class SpineLookupUnavailable(RuntimeError):
    """The spine could not be queried. NOT the same as 'no match'.

    Raised only after the retry below has also failed. A caller that catches
    this must NOT fall back to a synthetic parcel_id: it does not know whether
    a real parcel exists, and minting a stub on a transient failure is
    irreversible in practice (the stub acquires events, imagery and listings
    that must then be re-pointed by hand).
    """


# Transport-level failures worth one retry. httpx.TransportError is the base
# of ConnectError, ReadError, WriteError, CloseError, the timeout family, and
# ProtocolError -- which is the class that actually fired on 2026-08-17
# (RemoteProtocolError: Server disconnected, six times in 150 seconds across
# five distinct call sites). An APIError from PostgREST is NOT in this family
# and is not retried: a 42P10 or a 23505 will fail identically on the second
# attempt.
_RETRYABLE = (httpx.TransportError,)


# ============================================================
# ADDRESS NORMALISATION
#
# Copied verbatim from src/routes/admin.py so the live path and the 2026-08-16
# backfill agree. When this changes, the migration SQL must change with it or
# the two will resolve different sets and neither will be reproducible.
# ============================================================

# Address text that is a LIST rather than one property. A package notice's
# property_address is every address the notice covers -- twelve of them for
# washington 26-003536FC -- and resolving a member against that list would
# attach a foreclosure to somebody else's house. Detected, never resolved.
_ADDR_LIST_RE = re.compile(r";|\s&\s|\band\b", re.IGNORECASE)

# Leading house number. No house number, no address match: a street name alone
# is not an identity.
_HOUSE_NO_RE = re.compile(r"^\s*(\d+)\s")

# Street-type words, in BOTH the spelled-out and abbreviated form a notice or
# an assessor may use. Removed from BOTH sides before comparison, so
# 'Ridgeway Road' and 'RIDGEWAY RD' compare equal.
#
# \b cannot fire inside an ordinal: there is no word boundary between the
# digit and the letters of '1ST', so \bST\b never matches it. Verified, not
# assumed.
_STREET_TYPE_RE = re.compile(
    r"\b(?:ROAD|RD|STREET|ST|AVENUE|AVE|DRIVE|DR|LANE|LN|COURT|CT"
    r"|BOULEVARD|BLVD|PLACE|PL|CIRCLE|CIR|TERRACE|TER|PARKWAY|PKWY"
    r"|HIGHWAY|HWY|TRAIL|TRL|PATH|WAY)\b",
    re.IGNORECASE,
)

# Directionals are CANONICALISED, never removed. Mapping NORTHWEST -> NW
# preserves the distinction while reconciling the spelling; deletion would
# collapse '100 Main St N' and '100 Main St S' into exactly ONE match, the
# WRONG house, undetectable. Minnesota addressing leans on directionals
# (Minneapolis is built on NE/NW/SE/SW quadrants).
#
# ORDER MATTERS. Compounds are replaced BEFORE simples, or 'NORTHEAST' is
# mangled into 'N EAST' by the NORTH rule firing first. Python dicts preserve
# insertion order, so this literal is the substitution order.
_DIRECTIONALS = {
    "NORTHEAST": "NE",
    "NORTHWEST": "NW",
    "SOUTHEAST": "SE",
    "SOUTHWEST": "SW",
    "NORTH": "N",
    "SOUTH": "S",
    "EAST": "E",
    "WEST": "W",
}

_DIRECTIONAL_RE = re.compile(
    r"\b(" + "|".join(_DIRECTIONALS) + r")\b",
    re.IGNORECASE,
)

# Digits of a county PIN. Counties print the same PID in incompatible shapes
# (wright '155-154-004010', beltrami '83.00180.00', dakota '42-42800-01-071'),
# so the match is digits-only against the expression index
# idx_parcels_county_pid_digits.
_NON_DIGIT_RE = re.compile(r"\D")


def addr_key(value: Any) -> str:
    """Comparison key for an address: uppercase alphanumerics, street-type
    words removed, directionals canonicalised to their abbreviation.

    NOT THE LIVE PATH as of 2026-08-17. resolve_by_address() now compares in
    SQL via core.addr_key(), because the comparison has to run against EVERY
    candidate row and PostgREST cannot express the normalisation (see that
    function for why the 50-row cap was fatal). This stays as the executable
    reference definition: the five verified pairs below are checked against
    THIS function, and core.addr_key() must produce the same key for the same
    input. **A change to either MUST change both.** Two definitions of one
    rule in two languages is the minimum possible here -- SQL cannot call
    Python -- but it is still two, and they will drift silently if edited
    apart.

    '315 1st Street South, Brook Park, Minnesota 55007' and
    '315 1st Street South' produce the same key ON PURPOSE.

    THE TRUNCATION IS NOW INSIDE THE KEY (2026-08-19). It used to rely on
    every caller passing only the first comma segment -- resolve_by_address()
    does that on the INPUT side. That was never true of the STORED side, and
    Washington's parcel load appends the city to the address itself:

        9665 GLACIAL VALLEY RD, CITY OF WOODBURY

    keyed to '9665GLACIALVALLEYCITYOFWOODBURY', which no notice could ever
    produce. Measured 2026-08-19: 101,413 of Washington's 101,423 addresses
    carry a comma and 96,143 carry ', CITY OF ...', so EVERY address
    resolution against a tier-1 metro county had been failing since the county
    was loaded -- silently, because a miss is indistinguishable from a parcel
    that is not there. Every other county is at or near zero (hennepin 4,
    anoka 3, st_louis 1).

    Truncating inside the key makes it symmetric: the same address produces
    the same key whichever side it arrives from, and no caller has to remember
    to split first. Verified before shipping that this creates no empty keys
    -- 0 of 101,413 Washington rows and 0 elsewhere have nothing before the
    comma, and the two anoka rows that key to empty ("," from an empty-field
    concatenation) already did so under the old definition.

    core.addr_key() was changed in the same session. **A change to either
    MUST change both.**

    Verified pairs, run before shipping rather than reasoned about:
        '8344 Onigum Road Northwest' == '8344 ONIGUM RD NW'      -> match
        '5195 194TH ST W'            == '5195 194th Street West' -> match
        '1221 1st Avenue Northwest'  == '1221 1ST AVE NW'        -> match
        '9665 Glacial Valley Rd.'
             == '9665 GLACIAL VALLEY RD, CITY OF WOODBURY'       -> match
        '100 Main St N'              vs '100 Main St S'          -> DIFFER
        '100 Main St NE'             vs '100 Main St NW'         -> DIFFER
        '9665 Glacial Valley Rd.'    vs '9665 66th St S'         -> DIFFER

    Order of operations mirrors the SQL the measurement used: punctuation
    becomes a SPACE first (so 'ST.' is recognisable as a word), then street
    types are dropped, then directionals are canonicalised, then all
    whitespace is removed.
    """
    # First comma segment only -- see the docstring. Mirrors
    # split_part($1, ',', 1) in core.addr_key().
    head = str(value or "").split(",")[0]
    text = re.sub(r"[^A-Za-z0-9]", " ", head).upper()
    text = _STREET_TYPE_RE.sub(" ", text)
    text = _DIRECTIONAL_RE.sub(
        lambda m: " " + _DIRECTIONALS[m.group(1).upper()] + " ", text
    )
    return re.sub(r"\s+", "", text)


def pid_digits(value: Any) -> str:
    """Digits of a published parcel identifier, or '' if it carries none."""
    return _NON_DIGIT_RE.sub("", str(value or ""))


# ============================================================
# QUERY EXECUTION
# ============================================================


def _execute_with_retry(build_query, *, what: str, county_code: str):
    """Run a PostgREST query, retrying ONCE on a transport failure.

    `build_query` is a zero-argument callable that constructs and executes the
    query. It is a callable rather than a built query object because a
    postgrest builder is not guaranteed safe to re-execute after a failed
    send -- the second attempt builds a fresh one.

    Raises SpineLookupUnavailable if both attempts fail at the transport
    layer. Any other exception propagates unchanged: a PostgREST APIError is a
    real answer about a malformed query and must not be retried or swallowed.
    """
    try:
        return build_query()
    except _RETRYABLE as first:
        logger.warning(
            "spine lookup transport failure — retrying once",
            what=what,
            county=county_code,
            error_type=type(first).__name__,
        )
        try:
            return build_query()
        except _RETRYABLE as second:
            logger.error(
                "spine lookup unavailable after retry",
                what=what,
                county=county_code,
                error_type=type(second).__name__,
            )
            raise SpineLookupUnavailable(
                f"{what} lookup failed twice for county {county_code}: "
                f"{type(second).__name__}"
            ) from second


# ============================================================
# RESOLVERS
# ============================================================


def resolve_by_digits(
    county_code: str,
    published_pid: Any,
) -> Optional[dict[str, Any]]:
    """The spine row whose PID digits equal this notice's, or None.

    Measured 2026-08-10: exact-string match resolved 47 of 217 notices,
    digits-only resolved 185, and the lookup runs in 0.57ms on an index scan
    of idx_parcels_county_pid_digits.

    Returns None on no match OR on ambiguity. Ambiguity within one county is
    fatal and is never guessed past -- a wrong parcel attaches a foreclosure to
    someone else's property.

    Raises SpineLookupUnavailable if the spine could not be queried.
    """
    if not county_code:
        return None
    digits = pid_digits(published_pid)
    if not digits:
        return None

    hit = _execute_with_retry(
        lambda: (
            core_table("parcel_pid_lookup")
            .select("parcel_id, address, city")
            .eq("county_code", county_code)
            .eq("pid_digits", digits)
            .limit(2)
            .execute()
        ),
        what="digits",
        county_code=county_code,
    )

    rows = hit.data or []
    if len(rows) == 1:
        return rows[0]
    if len(rows) > 1:
        logger.info(
            "spine parcel AMBIGUOUS by digits — refusing to guess",
            county=county_code,
            published_pid=str(published_pid),
        )
    return None


def resolve_by_address(
    county_code: str,
    address: Any,
) -> Optional[dict[str, Any]]:
    """The spine row at this street address, or None.

    Digits-only matching CANNOT work in several counties because the notice
    and the spine use different identifier systems, not different formatting
    of one system. Measured live in dakota: the spine holds '1702822570082'
    for 1251 Macarthur Ave while the notice prints '42-334-00-01-040'. There
    is no transformation between those strings.

    === THE 50-ROW CAP WAS FATAL — FIXED 2026-08-17 ===

    This function used to fetch up to 50 candidate rows by house number and
    filter them in Python. The cap was defended in a comment reading "a house
    number shared by more than 50 streets in one county would be ambiguous
    anyway." That reasoning is wrong: fifty rows sharing a house NUMBER are
    fifty different STREETS, and exactly one of them is right. The cap
    truncated the candidate set BEFORE the discriminating comparison ran.

    Measured on the first live Hennepin run: 517 records, 202 unresolved, 95
    new stubs minted. **95 of 95 had more than 50 candidates** -- minimum 58,
    maximum 526, because Minneapolis's grid puts the same house number on
    dozens of parallel avenues. '2417 Colfax Ave S' has 84 candidates at that
    house number and exactly one Colfax; the row was in the other 34.

    Dakota did not expose this (128 of 170 resolved) because a suburban county
    has few parallel streets per house number. The defect was county-shaped,
    which is why the first county to run looked like a success.

    The comparison now runs in SQL, in core.resolve_parcel_by_address(),
    against every candidate with no cap. Re-measured: **95 of 95 resolve, 0
    ambiguous, 0 misses.**

    Still deliberately strict, and these checks stay in Python because they
    are cheap and save a round trip:
      * package/list addresses are refused outright (see _ADDR_LIST_RE)
      * a leading house number is required
      * more than one distinct parcel returns None -- a wrong parcel attaches
        a foreclosure to somebody else's house

    Raises SpineLookupUnavailable if the spine could not be queried.
    """
    if not county_code or not address:
        return None

    # Only the first comma segment: the rest is city/state/zip, which the
    # spine stores in its own columns.
    head = str(address).split(",")[0]
    if _ADDR_LIST_RE.search(head):
        # A list of addresses, not one address. Never resolve these.
        return None
    if not _HOUSE_NO_RE.match(head):
        return None

    hit = _execute_with_retry(
        lambda: (
            get_client()
            .schema("core")
            .rpc("resolve_parcel_by_address", {
                "p_county_code": county_code,
                "p_address": head,
            })
            .execute()
        ),
        what="address",
        county_code=county_code,
    )

    # The function applies the house-number narrowing AND the exact key
    # comparison, then caps at 3 -- a cap on genuine duplicates, not on
    # candidates. Ambiguity is still refused here rather than there, so the
    # refusal is logged with the address that caused it.
    rows = hit.data or []
    distinct = {r.get("parcel_id"): r for r in rows}
    if len(distinct) != 1:
        if len(distinct) > 1:
            logger.info(
                "spine parcel AMBIGUOUS by address — refusing to guess",
                county=county_code,
                address=head,
                candidates=len(distinct),
            )
        return None
    return next(iter(distinct.values()))


def resolve_spine_parcel(
    county_code: str,
    published_pid: Any = None,
    address: Any = None,
) -> Optional[dict[str, Any]]:
    """Digits first, then address. The row, or None.

    This is the ladder every caller should use unless it has a reason not to.
    Digits are tried first because they are exact and cheap; address is the
    fallback for the counties where the two identifier systems do not
    correspond at all.

    A MISS on digits falls through to address. AMBIGUITY on digits does NOT --
    a conflict is never guessed past, and trying a second rule after seeing a
    conflict is guessing.

    Raises SpineLookupUnavailable if the spine could not be queried. The
    caller must not mint a synthetic parcel_id in that case.
    """
    if not county_code:
        return None

    digits = pid_digits(published_pid)
    if digits:
        hit = _execute_with_retry(
            lambda: (
                core_table("parcel_pid_lookup")
                .select("parcel_id, address, city")
                .eq("county_code", county_code)
                .eq("pid_digits", digits)
                .limit(2)
                .execute()
            ),
            what="digits",
            county_code=county_code,
        )
        rows = hit.data or []
        if len(rows) == 1:
            logger.info(
                "spine parcel resolved by DIGITS",
                county=county_code,
                parcel_id=rows[0].get("parcel_id"),
                published_pid=str(published_pid),
            )
            return rows[0]
        if len(rows) > 1:
            # Ambiguous. Fatal -- do NOT try address after a conflict.
            logger.info(
                "spine parcel AMBIGUOUS by digits — refusing to guess",
                county=county_code,
                published_pid=str(published_pid),
            )
            return None

    resolved = resolve_by_address(county_code, address)
    if resolved is not None:
        logger.info(
            "spine parcel resolved by ADDRESS (digits missed)",
            county=county_code,
            parcel_id=resolved.get("parcel_id"),
            published_pid=str(published_pid) if published_pid else None,
        )
    return resolved


__all__ = [
    "SpineLookupUnavailable",
    "addr_key",
    "pid_digits",
    "resolve_by_digits",
    "resolve_by_address",
    "resolve_spine_parcel",
]
