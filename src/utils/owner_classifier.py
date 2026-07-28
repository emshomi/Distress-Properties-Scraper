"""
Shared owner-name classifier.

Extracted 2026-07-28 from FIVE identical copies of _OWNER_TYPE_PATTERNS
living in ramsey_parcels, olmsted_parcels, hennepin_parcels,
fillmore_parcels and mngac_parcels. Five copies meant five places to fix
and a guarantee they would drift; this is the single definition.

Vocabulary matches signals.owner_distress_summary:
    government / bank_lender / llc_business / individual

=== WHY THE GOVERNMENT PATTERN GREW ===
The original matched 'STATE OF MINNESOTA' spelled out, so every abbreviated
form fell through to 'individual'. Measured live before the fix: 1,184
government-owned parcels classified as individual, plus 69 as llc_business
and 2 as bank_lender — roughly 40% of all government owners missed.
Biggest offenders: 'STATE OF MN TRUST EXEMPT' (277 parcels), 'MN STATE OF
DNR' (247), 'STATE OF MINN' (79), 'STATE OF MN - MNDOT' (70), 'UNITED
STATES OF AMERICA | % MARY STEFANSKI' (64 — the Upper Mississippi River
National Wildlife Refuge).

This matters beyond tidiness: government parcels are permanently off-market,
so misfiling them as individual owners puts refuge land, DNR forest and
state hunting grounds into 'absentee owner with large acreage' lead lists.
That is exactly what happened to the Wabasha bare-land shortlist.

=== TWO DELIBERATE JUDGEMENT CALLS ===
1. NO bare 'USA' pattern. It would catch 'HOME DEPOT USA INC',
   "MCDONALD'S USA LLC", 'FORESTAR (USA) REAL EST GRP' and
   'USA MID PRV/SOC/JS ST IGN TR' (a Jesuit trust) — 60+ parcels of false
   positives — while catching nothing real, since federal records spell
   out 'UNITED STATES OF AMERICA'.
2. NATURE CONSERVANCY and similar land trusts are classified 'government'
   though they are private nonprofits. They hold permanently conserved
   land that will never transact, which is the property of the same
   character. Adding a fifth category would mean teaching every consumer
   about it for no gain.

Verified against 33 real owner names from core.owners — 23 government,
10 that must not be — with zero misclassifications.
"""

from __future__ import annotations

import re


_OWNER_TYPE_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("government", re.compile(
        r"(SECRETARY OF|VETERANS AFFAIRS|\bHUD\b|HOUSING & URBAN|"
        r"HOUSING AND URBAN|COUNTY OF|STATE OF MINNESOTA|CITY OF|"
        # --- ADDED 2026-07-28 (see module docstring) ---
        r"^UNITED STATES\b|UNITED STATES OF AMERICA|"
        r"STATE OF MN\b|STATE OF MINN\b|MN STATE OF\b|MINNESOTA STATE OF\b|"
        r"\bDNR\b|DEPT OF NAT|DEPARTMENT OF NAT|\bMNDOT\b|"
        r"POSTAL SERV|ARMY CORPS|FOREST SERVICE|"
        r"BUREAU OF (LAND|INDIAN|RECLAMATION)|"
        r"NATURE CONSERVANCY|CONSERVATION FUND|TRUST FOR PUBLIC LAND|"
        r"WATERSHED DISTRICT|SCHOOL DISTRICT|\bISD\b|PORT AUTHORITY|"
        r"\bTOWN OF\b|TOWNSHIP OF|\bHRA\b|\bEDA\b)")),
    ("bank_lender", re.compile(
        r"(BANK|MORTGAGE|\bMTGE\b|\bMTG\b|LENDING|FINANCIAL|"
        r"CREDIT UNION|NATIONSTAR|FREDDIE|FANNIE|MIDFIRST|BANKUNITED|"
        r"FEDERAL HOME LOAN|FEDERAL NAT|SERVBANK|CITIMORTGAGE)")),
    ("bank_lender", re.compile(
        r"(\bLOAN\b|NATIONAL ASSOC|\bNA\b|\bN A\b|\bN\.A\.|TRUSTEE)")),
    ("llc_business", re.compile(
        r"(\bLLC\b|L\.?L\.?C|\bINC\b|\bLTD\b|HOLDINGS|VENTURES|"
        r"PROPERTIES|RENOVATION|REALTY|GROUP|COMPANY|\bCO\b)")),
]

_LENDER_TRUST = re.compile(
    r"(MORTGAGE|\bMTG\b|\bLOAN\b|PARTIC|POINT|FUNDING|CAPITAL|MASTER|"
    r"TITLE TRUST|TRUST [0-9])")


def classify_owner(name: str) -> str:
    """Return government / bank_lender / llc_business / individual.

    Order matters: government is tested first so 'STATE OF MN TRUST EXEMPT'
    does not fall through to the TRUST handling below.
    """
    up = (name or "").upper()
    for otype, pat in _OWNER_TYPE_PATTERNS:
        if pat.search(up):
            return otype
    if "TRUST" in up and _LENDER_TRUST.search(up):
        return "bank_lender"
    return "individual"


__all__ = ["classify_owner"]
