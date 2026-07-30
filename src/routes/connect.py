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

from fastapi import APIRouter, Body, Header, HTTPException, Query, status as http_status

from src.db.supabase_client import core_table, outcomes_table, scoring_table
from src.routes.connect_auth import (
    create_listing,
    owner_from_session,
    request_link,
    verify_link,
)
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
#
# RAISED 5 -> 15 on 2026-07-29. At 5, Hennepin's reo_resale band showed at
# n=9 — too thin for a figure an owner will make a decision on. At 15 that
# band falls back to the statewide rollup (n=22), which is honest: the
# statewide REO ratio is a better estimate than nine local sales.
_MIN_SAMPLE = 15


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
                "Free, confidential and independent. A HUD-certified "
                "counsellor will review your whole situation and tell you "
                "what your options are — including ones not listed here, and "
                "including telling you not to sell if that is the right "
                "answer. We earn nothing from this and we are not involved."
            ),
            # DELIBERATELY LINKS THE OFFICIAL DIRECTORIES rather than
            # embedding a list of agencies (2026-07-29).
            #
            # Minnesota has ~59 HUD-approved counselling agencies. Hardcoding
            # their names, addresses and phone numbers would go stale, and a
            # homeowner in crisis calling a disconnected number is a real
            # harm — the same failure mode as directing people to the NEDA
            # helpline after it was permanently disconnected.
            #
            # The Minnesota Homeownership Center maintains the authoritative
            # searchable directory and keeps it current. Sending people there
            # is both more reliable and more honest than duplicating it.
            "directories": [
                {
                    "name": "Minnesota Homeownership Center — find an advisor",
                    "url": "https://www.hocmn.org/find-an-advisor/",
                    "note": (
                        "Search free foreclosure-prevention counsellors by "
                        "city or ZIP. This is the state's own referral tool."
                    ),
                },
                {
                    "name": "HUD — approved housing counselling agencies",
                    "url": (
                        "https://www.hud.gov/topics/"
                        "avoiding_foreclosure/foreclosureprocess"
                    ),
                    "note": "The federal list and HUD's own foreclosure guide.",
                },
            ],
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

   # Scope selection is ALL-OR-NOTHING on the two channels that carry the
    # comparison. The owner's decision is "sell it myself" vs "let it go to
    # foreclosure", so direct_standard and reo_resale must BOTH clear the
    # sample floor or we fall back to statewide for every band.
    #
    # Filtering band-by-band was the earlier behaviour and it was worse than
    # a thin sample: Hennepin's reo_resale (n=9) dropped out and the page
    # showed "sold by the owner 1.023x" with nothing to compare it against.
    # A number with no counterfactual is not evidence, it is an assertion.
    _REQUIRED = ("direct_standard", "reo_resale")

    def scope_ok(scope_rows: list[dict[str, Any]]) -> bool:
        have = {r["channel"] for r in usable(scope_rows)}
        return all(ch in have for ch in _REQUIRED)

    county_rows = [r for r in rows if r.get("county_slug") == county_code]
    state_rows = [r for r in rows if r.get("county_slug") is None]

    if county_code and scope_ok(county_rows):
        scope, picked = county_code, usable(county_rows)
    elif scope_ok(state_rows):
        scope, picked = "minnesota", usable(state_rows)
    else:
        # Neither scope can support the comparison. Say nothing rather than
        # publish half of it.
        scope, picked = None, []

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
        # The land/building split is included only where the county populates
        # it, and omitted entirely otherwise rather than returned as null —
        # a null reads as missing data when in fact it is detail we do not
        # need. Coverage is uneven and deliberately not backfilled: Olmsted
        # and Fillmore are at 100%, Hennepin at ~7% (its LAND_MV1/BLDG_MV1
        # sit unmapped in raw_data), Dakota/Ramsey/Washington use different
        # field names again. An owner deciding whether to reinstate, sell or
        # call a counsellor needs the TOTAL; how the assessor divides it
        # between dirt and structure changes nothing.
        at_stake = {
            "assessed_value": emv,
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

# ============================================================
# OWNER AUTH + HAND-RAISE
# ============================================================
# Passwordless by design. See src/routes/connect_auth.py for why this does
# not use app_auth.users (investor-shaped: password, tier, subscription) or
# Supabase Auth (zero rows, never used).


@router.post(
    "/connect/request-link",
    status_code=http_status.HTTP_200_OK,
    summary="Email a one-time sign-in link to an owner",
)
async def connect_request_link(
    email: Optional[str] = Body(default=None),
    phone: Optional[str] = Body(default=None),
    next_path: Optional[str] = Body(default=None),
) -> dict[str, Any]:
    """Returns the SAME response whether or not the contact is known — a
    different answer would let someone probe addresses to discover who is in
    foreclosure."""
    # Only same-site paths — never an absolute URL. An open redirect in an
    # emailed link is a phishing primitive.
    safe_next = next_path if (next_path or "").startswith("/") else None
    result = await request_link(email=email, phone=phone, next_path=safe_next)
    if not result.get("sent"):
        if result.get("internal_error"):
            # A database or send failure — NOT the caller's fault. Reporting
            # this as "invalid email" sent us hunting a phantom escaping bug
            # on 2026-07-29 while the real error sat in the logs.
            raise HTTPException(
                status_code=http_status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={"message": (
                    "We could not send that link just now. Please try again "
                    "in a moment."
                )},
            )
        raise HTTPException(
            status_code=http_status.HTTP_400_BAD_REQUEST,
            detail={"message": "Enter a valid email address or phone number."},
        )
    return success_envelope({
        "sent": True,
        "message": (
            "Check your email for a secure link. It works once and expires "
            "in 30 minutes."
        ),
    })


@router.get(
    "/connect/verify",
    status_code=http_status.HTTP_200_OK,
    summary="Exchange a one-time link for a session",
)
async def connect_verify(
    token: str = Query(..., min_length=20, max_length=200),
) -> dict[str, Any]:
    session = verify_link(token)
    if session is None:
        raise HTTPException(
            status_code=http_status.HTTP_401_UNAUTHORIZED,
            detail={"message": (
                "That link has expired or has already been used. Request a "
                "new one — it takes a moment."
            )},
        )
    return success_envelope({
        "session_token": session["session_token"],
        "expires_in_days": 30,
    })


@router.post(
    "/connect/raise-hand",
    status_code=http_status.HTTP_201_CREATED,
    summary="Owner signals they would consider offers",
)
async def connect_raise_hand(
    x_connect_session: Optional[str] = Header(default=None, alias="X-Connect-Session"),
    parcel_id: str = Body(...),
    county: str = Body(...),
    # Needed to verify ownership at all. Without a name there is nothing to
    # compare against the assessor's owner of record, and the
    # ownership_verified column is decorative.
    owner_name: Optional[str] = Body(default=None),
    occupancy: Optional[str] = Body(default=None),
    condition: Optional[str] = Body(default=None),
    primary_need: Optional[str] = Body(default=None),
    leaseback_interest: Optional[bool] = Body(default=None),
    buyback_interest: Optional[bool] = Body(default=None),
    earliest_close_date: Optional[str] = Body(default=None),
    preferred_close_date: Optional[str] = Body(default=None),
    contact_preference: Optional[str] = Body(default=None),
    contact_restrictions: Optional[str] = Body(default=None),
) -> dict[str, Any]:
    """Create the listing.

    NOTE what is NOT accepted: asking_price and description. Pricing is
    exactly what a distressed owner does not know and exactly what gets them
    taken advantage of — the offers set the price. Photos likewise: nobody
    photographs a house they are ashamed of.

    `primary_need` is the field that matters most. Nobody asks it today, and
    it is where value is being destroyed: an owner who would take 15% less
    for a 60-day leaseback has no way to say so, and the buyer who would
    happily agree never hears it.
    """
    owner_id = owner_from_session(x_connect_session)
    if owner_id is None:
        raise HTTPException(
            status_code=http_status.HTTP_401_UNAUTHORIZED,
            detail={"message": "Sign in with your emailed link first."},
        )

    county = county.strip().lower()

    # The parcel must be real. marketplace.listings FKs to core.parcels, and
    # a synthetic '<COUNTY>-FC-*' placeholder would both fail the FK and
    # carry no assessed value.
    try:
        pr = (
            core_table("parcels")
            .select("parcel_id, address, city")
            .eq("parcel_id", parcel_id)
            .eq("county_code", county)
            .limit(1)
            .execute()
        )
        parcel = (pr.data or [None])[0]
    except Exception as e:
        logger.warning("raise-hand: parcel lookup failed",
                       error_type=type(e).__name__)
        parcel = None

    if parcel is None or "-FC-" in parcel_id:
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail={"message": "We do not have a record for that property."},
        )

    # Ownership check against the assessor roll. NOT a rejection gate —
    # trusts, estates and spouses not on title are common, and estates are
    # among the best opportunities. A miss goes to manual review, never to a
    # closed door.
    # Ownership check against the assessor roll.
    #
    # NEVER a rejection gate. Trusts, estates, spouses not on title and
    # recent transfers are all common, and estates are among the best
    # opportunities on the platform. A mismatch goes to manual review, never
    # to a closed door.
    #
    # The earlier version set 'unverified' when an owner record EXISTED and
    # 'manual_review' when none did — inverted from what those words mean,
    # and meaningless either way because no name was ever collected.
    verified = "unverified"
    if owner_name:
        try:
            ow = (
                core_table("owners")
                .select("owner_name")
                .eq("parcel_id", parcel_id)
                .limit(1)
                .execute()
            )
            rows = ow.data or []
            if rows and rows[0].get("owner_name"):
                # Surname match on uppercase tokens. Deliberately loose: the
                # assessor writes 'JOHN GOETTE', an owner may type 'John R.
                # Goette' or 'Goette, John'. Requiring an exact string would
                # fail almost everyone.
                of_record = set(
                    t for t in str(rows[0]["owner_name"]).upper().split()
                    if len(t) > 2
                )
                claimed = set(
                    t.strip(",.") for t in owner_name.upper().split()
                    if len(t.strip(",.")) > 2
                )
                verified = "verified" if (of_record & claimed) else "manual_review"
            else:
                verified = "manual_review"
        except Exception as e:
            logger.warning("raise-hand: owner lookup failed",
                           error_type=type(e).__name__)
            verified = "manual_review"

    row: dict[str, Any] = {
        "parcel_id": parcel_id,
        "user_id": owner_id,
        "status": "active",
        "occupancy": occupancy,
        "condition": condition,
        "primary_need": primary_need,
        "leaseback_interest": leaseback_interest,
        "buyback_interest": buyback_interest,
        "earliest_close_date": earliest_close_date,
        "preferred_close_date": preferred_close_date,
        "contact_preference": contact_preference,
        "contact_restrictions": contact_restrictions,
        "ownership_verified": verified,
    }
    row = {k: v for k, v in row.items() if v is not None}

    listing_id = create_listing(owner_id, row)
    if listing_id is None:
        raise HTTPException(
            status_code=http_status.HTTP_400_BAD_REQUEST,
            detail={"message": (
                "We could not save that. If you have already raised your "
                "hand on this property, it is on file."
            )},
        )

    logger.info("raise-hand created", county=county, verified=verified)
    return success_envelope({
        "listing_id": listing_id,
        "status": "active",
        "ownership_verified": verified,
        "message": (
            "On file. You are not committed to anything and you can withdraw "
            "at any time. We will show you any offers that come in — nobody "
            "gets your address or contact details until you choose to open "
            "one."
        ),
    })

__all__ = ["router"]
