"""
Govire Connect — the public redemption calculator.

The free, open, no-login tool. An owner in a redemption window arrives here
(overwhelmingly from organic search) and gets four things:

  1. THEIR ACTUAL DEADLINE — a date and a countdown, not a generic timeline.
     Most owners are operating on rumour.
  2. WHAT IS AT STAKE — assessed value against estimated debt. Half of
     owners assume the equity is already gone and stop trying; this single
     fact changes behaviour more than advice does.
  3. WHAT HAPPENED TO PEOPLE LIKE THEM — deed-confirmed, from
     scoring.distress_multipliers. Evidence, not reassurance.
  4. THEIR OPTIONS, counselling first, selling last.

=== THIS IS DELIBERATELY NOT GATED ===
No auth, no tier, no redaction. `resolve_tier` and `redact_property` exist
to meter access to data owners never consented to share — a different
consent basis entirely. Gating this would also kill the SEO that is the
whole point: the calculator is the ranking asset, not the blog.

Nothing here leaks the paid product. An owner sees depth on ONE property
they already own, which is worthless to an investor. The paid product is
BREADTH — 714 live windows, sortable, with comps. That stays behind
/properties.

=== THE PRIVACY RULE THAT SHAPES THE API ===
"We can know things, but we only reveal them when the owner asks."

A bare address lookup that announces someone's foreclosure to whoever typed
it would be a violation, not a service — a family member, a neighbour, or a
wholesaler could be at the keyboard. So the flow is two steps:

  GET /connect/lookup    → does a record exist? Masked address only.
                           NO distress data, NO dates, NO owner name.
  GET /connect/status    → full detail, requires i_am_the_owner=true.

The confirmation is honour-system — we cannot verify ownership from a web
form, and pretending otherwise would be theatre. The point is that we never
*volunteer* the information. We also never send unsolicited "we noticed
you're in foreclosure" mail: that is the postcard this product exists to be
an alternative to.

=== CONFIDENCE IS SHOWN, NEVER ASSUMED ===
Minnesota redemption periods are 6 or 12 months (Minn. Stat. ch. 580) and
can be shortened to five weeks under 580.07. Only some counties publish the
period:

    hennepin    publishes redemptionExpirationDate — authoritative
    washington  mixed (44 stated / 118 assumed)
    dakota      publishes nothing
    anoka       publishes nothing

So every response carries `date_confidence` derived from the row's
`period_source`, and the front end MUST render the caveat when it is
'assumed'. Telling an owner a wrong deadline is worse than telling them
nothing. See docs/ECRV_RUNBOOK_AND_LESSONS.md Part 7.

Endpoints:
  GET /connect/lookup    — find a property, masked. Public.
  GET /connect/status    — full detail after owner confirmation. Public.
  GET /connect/outcomes  — county-level outcome bands. Public. Powers the
                           county SEO pages and needs no property at all.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Any, Optional

from fastapi import APIRouter, Query, status as http_status

from src.db.supabase_client import core_table, outcomes_table, scoring_table
from src.utils.errors import success_envelope
from src.utils.logger import logger


router = APIRouter(tags=["connect"])


# Channels we NEVER show an owner as a price. See runbook Part 8.
#   transfer_non_market — median 0.109. Quitclaims and nominal transfers.
#     Paper transactions, not sales. Would read as "homes sell for 11% of
#     value" and is simply false as a price signal.
#   seller_financed — median 1.029. Above assessed because contract-for-deed
#     carries a financing premium. Real, but not a cash number, so it would
#     mislead an owner comparing cash offers.
_HIDDEN_CHANNELS = {"transfer_non_market", "seller_financed"}

# Plain-English labels. The owner should never see a raw channel code.
_CHANNEL_LABELS = {
    "direct_standard": "Sold by the owner",
    "estate": "Sold through an estate",
    "reo_resale": "Resold by the lender after foreclosure",
    "other": "Other sale type",
}

# Minimum sample before a band is quoted at all. A ratio without an n is a
# guess; a ratio on n=3 is noise wearing a decimal point.
_MIN_SAMPLE = 5


def _norm_addr(s: Optional[str]) -> str:
    """Upper, collapse whitespace, drop unit suffixes. Deliberately loose:
    an owner typing their own address will not match the assessor's
    formatting, and a near-miss that finds nothing is a dead end for someone
    in crisis."""
    if not s:
        return ""
    t = re.sub(r"\s+", " ", str(s).strip().upper())
    hash_at = t.find("#")
    if hash_at != -1:
        t = t[:hash_at].strip()
    return t


def _mask_address(addr: Optional[str]) -> Optional[str]:
    """'5331 Angeline Ave N' -> '53XX Angeline Ave N'.

    Enough for the owner to recognise their own home, not enough for a
    stranger to identify a foreclosure from a fuzzy search. The house number
    is what makes an address actionable.
    """
    if not addr:
        return None
    return re.sub(
        r"^(\d)(\d+)",
        lambda m: m.group(1) + "X" * len(m.group(2)),
        str(addr).strip(),
        count=1,
    )


def _confidence(period_source: Optional[str]) -> dict[str, Any]:
    """Turn period_source into something the page can render honestly."""
    if period_source == "scraped":
        return {
            "level": "stated",
            "basis": "This date comes from the county's own record.",
            "verify": None,
        }
    return {
        "level": "assumed",
        "basis": (
            "Your county does not publish the redemption period, so this "
            "assumes the standard six months from the sheriff's sale."
        ),
        "verify": (
            "Minnesota redemption periods can be 6 or 12 months, and in some "
            "cases as short as five weeks. Your sheriff's sale notice states "
            "yours — please check it against this date."
        ),
    }


def _options(days_remaining: Optional[int]) -> list[dict[str, Any]]:
    """Options in deliberate order: reinstate, then free counselling, then
    selling. The counselling referral sits ABOVE the sell option and we earn
    nothing from it. That ordering is the entire trust argument, and no
    wholesaler can copy it without undermining themselves."""
    opts: list[dict[str, Any]] = [
        {
            "key": "reinstate",
            "title": "Pay off or reinstate the loan",
            "detail": (
                "Paying the amount owed plus costs before the deadline ends "
                "the foreclosure and you keep the home. Ask your lender for "
                "a written reinstatement quote — the figure changes daily."
            ),
            "govire_earns": None,
        },
        {
            "key": "counseling",
            "title": "Talk to a free HUD-approved housing counsellor",
            "detail": (
                "Free, confidential, and independent. They can review your "
                "whole situation, including options not listed here."
            ),
            # TODO: populate per-county agency contacts. Open decision #4 in
            # docs/GOVIRE_CONNECT_IMPLEMENTATION_PLAN.md — needs real calls
            # to HUD-approved agencies by county. Until then the front end
            # links to the HUD directory rather than inventing a referral.
            "agencies": [],
            "govire_earns": None,
        },
        {
            "key": "sell",
            "title": "Sell before the deadline",
            "detail": (
                "If selling is the right answer, the figures below show what "
                "properties in your position actually sold for. Getting more "
                "than one offer is the single biggest thing you can do."
            ),
            "govire_earns": None,
        },
    ]
    if days_remaining is not None and days_remaining < 30:
        opts.insert(0, {
            "key": "urgent",
            "title": f"You have {max(days_remaining, 0)} days",
            "detail": (
                "That is not long enough for a normal sale to close. Speak "
                "to a housing counsellor today — free, and they can tell you "
                "what is still possible in the time you have."
            ),
            "govire_earns": None,
        })
    return opts


def _outcome_bands(county_code: Optional[str]) -> dict[str, Any]:
    """Outcome bands from scoring.distress_multipliers.

    County-scoped where the sample supports it, statewide otherwise —
    mirroring how comp_ratios falls back city -> county -> metro. Sample
    size is ALWAYS returned; the front end must show it.
    """
    try:
        res = (
            scoring_table("distress_multipliers")
            .select("channel, county_slug, n, p25, median, p75, avg_days_to_exit")
            .execute()
        )
        rows = res.data or []
    except Exception as e:
        logger.warning(
            "connect: outcome bands unavailable",
            error_type=type(e).__name__,
        )
        return {"scope": None, "bands": [], "note": None}

    def usable(scope_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            r for r in scope_rows
            if r["channel"] not in _HIDDEN_CHANNELS
            and (r.get("n") or 0) >= _MIN_SAMPLE
        ]

    scope = county_code
    picked = usable([r for r in rows if r.get("county_slug") == county_code])
    if len(picked) < 2:          # need at least owner-sale vs REO to compare
        scope = "minnesota"
        picked = usable([r for r in rows if r.get("county_slug") is None])

    bands = sorted(
        (
            {
                "channel": r["channel"],
                "label": _CHANNEL_LABELS.get(r["channel"], r["channel"]),
                "sample_size": r["n"],
                "low": float(r["p25"]) if r.get("p25") is not None else None,
                "typical": float(r["median"]) if r.get("median") is not None else None,
                "high": float(r["p75"]) if r.get("p75") is not None else None,
                "avg_days_to_sale": r.get("avg_days_to_exit"),
            }
            for r in picked
        ),
        key=lambda b: -(b["typical"] or 0),
    )
    return {
        "scope": scope,
        "bands": bands,
        "note": (
            "Figures are the sale price as a multiple of assessed value, from "
            "recorded deeds. Each band shows how many sales it is based on."
        ) if bands else None,
    }


@router.get(
    "/connect/lookup",
    status_code=http_status.HTTP_200_OK,
    summary="Find a property — masked, no distress detail",
)
async def connect_lookup(
    address: str = Query(..., min_length=4, max_length=200),
    county: Optional[str] = Query(default=None, max_length=40),
) -> dict[str, Any]:
    """Step one. Returns whether we hold a record and a MASKED address only.

    Deliberately returns NO redemption date, NO owner name and NO distress
    information — see the privacy rule in the module docstring. A stranger
    fuzzing addresses learns nothing they did not already know.
    """
    want = _norm_addr(address)
    try:
        q = core_table("parcels").select("parcel_id, address, city, county_code")
        if county:
            q = q.eq("county_code", county.strip().lower())
        res = q.ilike("address", f"%{want[:40]}%").limit(10).execute()
        rows = res.data or []
    except Exception as e:
        logger.warning("connect lookup failed", error_type=type(e).__name__)
        rows = []

   # Collapse duplicates. A foreclosed property often appears TWICE: once as
    # the real assessor parcel and once as a synthetic '<COUNTY>-FC-*'
    # placeholder minted by the foreclosure path when it could not resolve a
    # parcel. Verified live: '5331 Angeline' returned both
    # '0911821120148' and 'HENNEPIN-FC-2606002'.
    #
    # Two identical-looking rows is confusing for anyone, and actively
    # harmful here — if the owner picks the synthetic one, /connect/status
    # finds no assessed value, because synthetic parcels carry none. So when
    # a real parcel exists for an address, the placeholder is dropped.
    best: dict[str, dict[str, Any]] = {}
    for r in rows:
        if not r.get("address"):
            continue
        key = _norm_addr(r["address"])
        is_synthetic = "-FC-" in (r["parcel_id"] or "")
        existing = best.get(key)
        if existing is None or (existing["_synthetic"] and not is_synthetic):
            best[key] = {
                "parcel_id": r["parcel_id"],
                "masked_address": _mask_address(r.get("address")),
                "city": r.get("city"),
                "county_code": r.get("county_code"),
                "_synthetic": is_synthetic,
            }
    matches = [
        {k: v for k, v in m.items() if not k.startswith("_")}
        for m in best.values()
    ]

    logger.info("connect lookup", county=county, matches=len(matches))
    return success_envelope({
        "query": address,
        "match_count": len(matches),
        "matches": matches,
        "next_step": (
            "If one of these is your property, confirm you are the owner to "
            "see your redemption deadline and what is at stake."
        ),
    })


@router.get(
    "/connect/status",
    status_code=http_status.HTTP_200_OK,
    summary="Redemption deadline, equity and outcomes for one property",
)
async def connect_status(
    parcel_id: str = Query(..., min_length=4, max_length=60),
    county: str = Query(..., min_length=2, max_length=40),
    i_am_the_owner: bool = Query(
        default=False,
        description=(
            "Must be true. Nothing is revealed otherwise — see the privacy "
            "rule in this module's docstring."
        ),
    ),
) -> dict[str, Any]:
    """Step two. The full picture, only after the caller says they own it."""
    county = county.strip().lower()

    if not i_am_the_owner:
        return success_envelope({
            "revealed": False,
            "reason": (
                "We only show a property's foreclosure details to its owner. "
                "Confirm you are the owner to continue."
            ),
        })

    # --- the deadline: ALWAYS read redemption_current, never the raw
    # tracker. A postponed sale creates a new tracker row and leaves the old
    # one 'pending'; 132 parcels are affected. Showing the superseded date
    # would tell an owner their window closed weeks early. Runbook Part 7.
    try:
        rc = (
            outcomes_table("redemption_current")
            .select(
                "parcel_id, county_code, anchor_date, redemption_expiry_date, "
                "redemption_period_months, period_source, outcome, "
                "days_remaining, prior_postponements"
            )
            .eq("parcel_id", parcel_id)
            .eq("county_code", county)
            .limit(1)
            .execute()
        )
        window = (rc.data or [None])[0]
    except Exception as e:
        logger.warning("connect status: window lookup failed",
                       error_type=type(e).__name__)
        window = None

    # --- the property
    try:
        # Read BOTH value columns. core.parcels has two: emv_total (written by
        # the MNGAC loaders — anoka, wabasha) and estimated_market_value
        # (written by the older county-direct loaders — hennepin, dakota,
        # ramsey, washington, olmsted, fillmore). Verified live 2026-07-29:
        # Hennepin had emv_total on 29,522 rows but
        # estimated_market_value on 443,610. Reading only emv_total made
        # at_stake null for ~95% of Hennepin owners — the single most
        # behaviour-changing fact on the page.
        pr = (
            core_table("parcels")
            .select("parcel_id, address, city, zip, emv_total, emv_land, "
                    "emv_building, estimated_market_value, year_built, "
                    "property_type")
            .eq("parcel_id", parcel_id)
            .eq("county_code", county)
            .limit(1)
            .execute()
        )
        parcel = (pr.data or [None])[0]
    except Exception as e:
        logger.warning("connect status: parcel lookup failed",
                       error_type=type(e).__name__)
        parcel = None

    if parcel is None and window is None:
        return success_envelope({
            "revealed": True,
            "found": False,
            "message": (
                "We do not have a record for that property. That may simply "
                "mean we do not yet cover your county — it does not mean "
                "anything about your situation."
            ),
        })

    days = window.get("days_remaining") if window else None
    expiry = window.get("redemption_expiry_date") if window else None
    emv = None
    if parcel:
        for col in ("emv_total", "estimated_market_value"):
            if parcel.get(col):
                emv = float(parcel[col])
                break

    deadline: Optional[dict[str, Any]] = None
    if window and expiry:
        deadline = {
            "sheriff_sale_date": window.get("anchor_date"),
            "redemption_deadline": expiry,
            "days_remaining": days,
            "window_open": (days is not None and days >= 0
                            and window.get("outcome") == "pending"),
            "period_months": window.get("redemption_period_months"),
            "date_confidence": _confidence(window.get("period_source")),
            "prior_postponements": window.get("prior_postponements") or 0,
        }

    at_stake: Optional[dict[str, Any]] = None
    if emv:
        at_stake = {
            "assessed_value": emv,
            "assessed_land": float(parcel["emv_land"]) if parcel.get("emv_land") else None,
            "assessed_building": (float(parcel["emv_building"])
                                  if parcel.get("emv_building") else None),
            "note": (
                "Assessed value is the county's figure for tax purposes, not "
                "a listing price. What you would actually get depends on "
                "condition and how you sell — see the figures below."
            ),
        }

    logger.info("connect status revealed", county=county,
                has_window=window is not None, has_parcel=parcel is not None)

    return success_envelope({
        "revealed": True,
        "found": True,
        "property": {
            "parcel_id": parcel_id,
            "address": parcel.get("address") if parcel else None,
            "city": parcel.get("city") if parcel else None,
            "county_code": county,
            "year_built": parcel.get("year_built") if parcel else None,
        },
        "deadline": deadline,
        "at_stake": at_stake,
        "outcomes": _outcome_bands(county),
        "options": _options(days),
    })


@router.get(
    "/connect/outcomes",
    status_code=http_status.HTTP_200_OK,
    summary="County-level foreclosure outcome bands",
)
async def connect_outcomes(
    county: Optional[str] = Query(default=None, max_length=40),
) -> dict[str, Any]:
    """Outcome bands with no property involved.

    This powers the county SEO pages — the content nobody else can write,
    because it is deed-confirmed from 322,728 eCRV sales rather than a
    generic 'Minnesota foreclosure timeline' article.
    """
    slug = county.strip().lower() if county else None
    bands = _outcome_bands(slug)

    live = None
    if slug:
        try:
            res = (
                outcomes_table("redemption_current")
                .select("parcel_id", count="exact")
                .eq("county_code", slug)
                .eq("outcome", "pending")
                .gte("redemption_expiry_date", date.today().isoformat())
                .execute()
            )
            live = res.count
        except Exception as e:
            logger.warning("connect outcomes: live count failed",
                           error_type=type(e).__name__)

    return success_envelope({
        "county_code": slug,
        "open_redemption_windows": live,
        "outcomes": bands,
    })


__all__ = ["router"]
