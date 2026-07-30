"""
Address normalization for the Connect owner lookup.

An owner types their address the way they would write it on a letter. The
county wrote it the way a database stores it. Those are rarely the same
string, and every mismatch shows a homeowner "we do not have a record for
that address" for a property we hold perfectly well.

Measured against the real assessor value 'AVE N':

    what they type                    matches without this module?
    5331 Angeline                     yes
    5331 Angeline Avenue North        NO  — assessor abbreviates
    5331 Angeline Ave. N.             NO  — punctuation
    5331 angeline ave n, crystal      NO  — city is not in the address field
    5331 N Angeline Ave               NO  — directional placed first

The suffix and directional maps are the same ones
src/scrapers/dakota_foreclosure_enrichment.py already uses, and for the same
reason: Dakota's own two layers disagreed on street-type spelling, so the
enrichment had to normalize BOTH sides through one map. Applying it here is
the same problem seen from the owner's side.

DIRECTION OF MAPPING: always long -> short. We never expand abbreviations,
because expansion is ambiguous ('N' could be North or a street literally
named N) while contraction is not.
"""

from __future__ import annotations

import re
from typing import Optional


# Spelled-out street types -> the abbreviation county records use.
_SUFFIX_MAP: dict[str, str] = {
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
    "TRAIL": "TRL",
    "CROSSING": "XING",
    "HEIGHTS": "HTS",
    "POINT": "PT",
    "SQUARE": "SQ",
    "WAY": "WAY",
}

# Compound forms first — 'NORTHEAST' must not be read as 'NORTH' + 'EAST'.
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

_UNIT_WORDS = {"APT", "UNIT", "STE", "SUITE", "#"}


def normalize_address(raw: Optional[str]) -> str:
    """Upper-case, strip punctuation and unit designators, collapse
    spelled-out street types and directionals to their abbreviations.

    Applied identically to what the owner typed and to what the county
    stored, so it can never create a one-sided mismatch.
    """
    if not raw:
        return ""
    s = str(raw).upper()

    # Everything from a unit marker onward is noise for matching purposes.
    hash_at = s.find("#")
    if hash_at != -1:
        s = s[:hash_at]

    # Drop a trailing ", CRYSTAL MN 55429" — the county keeps city and zip in
    # separate columns, so anything after a comma cannot help and will only
    # prevent a match.
    if "," in s:
        s = s.split(",")[0]

    s = re.sub(r"[.\-_]", " ", s)          # 5331 Angeline Ave. N. -> AVE N
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    out: list[str] = []
    for tok in s.split():
        if tok in _UNIT_WORDS:
            break                          # unit designator: stop here
        out.append(_DIRECTIONAL_MAP.get(tok, _SUFFIX_MAP.get(tok, tok)))
    return " ".join(out)


def split_house_number(normalized: str) -> tuple[Optional[str], str]:
    """('5331 ANGELINE AVE N') -> ('5331', 'ANGELINE AVE N').

    The house number is matched with a LEADING anchor rather than a
    substring, so a search for 5331 does not also return 15331 Oak St or a
    property on Highway 5331. Across 1.1M parcels an unanchored number match
    returns noise from the whole metro, and the owner — who only sees masked
    addresses — cannot tell which if any is theirs.
    """
    if not normalized:
        return None, ""
    parts = normalized.split(" ", 1)
    if parts[0].isdigit():
        return parts[0], (parts[1] if len(parts) > 1 else "")
    return None, normalized


def is_searchable(raw: Optional[str]) -> bool:
    """True when there is enough to search on: a number AND a street word.

    A bare '5331' is not a search, it is a prefix, and answering it with ten
    masked addresses from ten different cities tells an owner nothing except
    that we appear not to have their home.
    """
    norm = normalize_address(raw)
    if len(norm) < 4:
        return False
    number, rest = split_house_number(norm)
    return bool(number and len(rest) >= 2)


__all__ = [
    "normalize_address",
    "split_house_number",
    "is_searchable",
]
