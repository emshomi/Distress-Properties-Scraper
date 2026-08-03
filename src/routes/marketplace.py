"""
Govire marketplace — the INVESTOR side.

Separate from src/routes/connect.py on purpose. connect.py is the owner's
front door: every path is /connect/*, every write resolves an owner from a
magic-link session, and its job is to SHOW an owner their own numbers. This
file is the opposite side of the wall — app_auth JWT identity, and its job is
to WITHHOLD most of those numbers. A single file doing both is a file where
the wrong branch is one mistake away.

============================================================
THE REDACTION RULE, AND WHY IT IS NOT ABOUT PRIVACY
============================================================

It is about DISINTERMEDIATION. Govire sells parcel-level data to the same
subscribers who browse this marketplace. So any EXACT value we publish here
is a join key back into that database:

  * parcel_id           -> the owner's name and mailing address, one county
                           records lookup away.
  * exact assessed value + city -> a near-unique key in core.parcels.
  * exact redemption date -> sheriff sale dates are published per parcel.

A buyer who can identify the house can bypass the monitored channel, the
progressive disclosure, the contact restrictions and the 72-hour floor
entirely, and cold-call a distressed owner directly. The marketplace would
become a lead list for exactly the behaviour it exists to prevent.

Hiding parcel_id alone does NOT achieve that. Every number has to be BANDED
or the bands reconstruct the parcel. Hence:

  NEVER SENT:  parcel_id, user_id, owner_name_submitted, owner_name_on_record,
               contact_preference, contact_restrictions, street address,
               exact assessed value, exact redemption date, days_remaining,
               asking_price, description.
  BANDED:      assessed value (~20% wide), timing (days window).
  COUNTY ONLY: no city. Crystal is 23,000 people; city plus a value band plus
               "vacant, needs work" narrows to a handful. There is no
               population column to threshold on and a hardcoded city list is
               the kind of hidden constant this codebase keeps regretting.
  FULL:        everything QUALITATIVE the owner chose to say. It identifies
               nothing and it is the entire matching value.

The buyer's handle is the opaque listings.id.

KNOWN COST, accepted: offers are indicative, not firm. A buyer cannot comp or
inspect what they cannot find, so an offer means "I would pay in this range,
subject to seeing it", and an owner may see offers soften after disclosure in
the deal room. A firm offer from someone who has already bypassed us is worth
less than an indicative one inside the channel.

NEVER show a buyer scoring.distress_multipliers. The same 0.636 that WARNS an
owner what foreclosure costs them TEACHES a buyer what they can get away with.
Assessed value is public record; the distressed bands are our analysis and
exist to protect the seller.

RLS: marketplace.* has RLS enabled with ZERO policies, and the service role
bypasses it. Every restriction here is enforced in Python. There is no
database-level backstop — a missing WHERE clause is a data leak, not a
degraded query.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from fastapi import APIRouter, Body, HTTPException, Query, status as http_status

from src.middleware.investor import InvestorContext, InvestorResolved
from src.routes.connect_auth import pg, send_offer_notification
from src.utils.errors import success_envelope
from src.utils.logger import logger

router = APIRouter(tags=["marketplace"])


# ============================================================
# BANDING
# ============================================================

# ~20% wide. Wide enough that the band is not a lookup key, narrow enough
# that a buyer can tell a $250k house from a $600k one.
_VALUE_BAND_WIDTH = Decimal("0.20")

# Timing buckets in days. A WINDOW, never a date and never a countdown.
#
# "Must close within 60-90 days" is a constraint on a transaction. "43 days
# remaining", ticking, is a pressure instrument pointed at a person — and if
# we render it, we built it. Same fact; the framing is the whole difference
# between informing a buyer and arming one.
#
# outcomes.redemption_current exposes days_remaining pre-computed. It is the
# INPUT here and must never be passed through raw.
_TIMING_BUCKETS: list[tuple[int, str]] = [
    (30, "under 30 days"),
    (60, "30-60 days"),
    (90, "60-90 days"),
    (180, "3-6 months"),
    (365, "6-12 months"),
]
_TIMING_BEYOND = "more than a year"


def _band_value(amount: Any) -> Optional[dict[str, Any]]:
    """A ~20%-wide range around an assessed value, rounded to $5k.

    Returns None when there is nothing to band. Deliberately does NOT fall
    back to a live core.parcels lookup: assessed_value_at_listing is the
    figure captured AT LISTING TIME, and quietly substituting a different
    number the owner never saw would be worse than saying nothing.
    """
    if amount is None:
        return None
    try:
        val = Decimal(str(amount))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if val <= 0:
        return None

    half = val * _VALUE_BAND_WIDTH / 2
    step = Decimal("5000")
    low = int(((val - half) / step).to_integral_value(rounding="ROUND_FLOOR") * step)
    high = int(((val + half) / step).to_integral_value(rounding="ROUND_CEILING") * step)
    if low < 0:
        low = 0
    return {
        "low": low,
        "high": high,
        "label": f"${low:,} - ${high:,}",
    }


# Equity bands, as a SHARE of value. Boundaries are the real quartiles of 723
# sheriff-sale parcels measured on 2026-08-03, not invented thresholds:
# p25 = 19%, median = 33%, p75 = 60%. 8.2% of that population is underwater.
#
# A SHARE, never the dollar figure and never the percentage. The dollar spread
# is (value - debt), and a buyer who holds both it and the value band can
# solve for the debt — which is the AMOUNT DUE column on /data, and therefore
# a lookup key straight to the address, the owner's name and their mailing
# address. Govire sells that product to these same subscribers. The band label
# tells a buyer what they need to judge the deal and reverses into nothing.
_EQUITY_BANDS: list[tuple[Decimal, str, str]] = [
    (Decimal("0"), "underwater",
     "Owed more than the property is worth"),
    (Decimal("19"), "thin",
     "Little room between what is owed and what it is worth"),
    (Decimal("60"), "moderate",
     "Some room between what is owed and what it is worth"),
]
_EQUITY_BEYOND = ("wide", "Substantial room between what is owed and what it is worth")

# k-anonymity floor.
#
# County plus a $50k value bucket is ALREADY 36.6% unique by cell across 723
# sheriff-sale parcels — before equity is published at all. Measured by
# PARCEL rather than by cell the picture is far better: 81.1% sit in cells of
# 11 or more, and only 3.6% are genuinely unique. The exposure is a long tail
# of unusual values in small counties, not a general failure.
#
# So the fix is to suppress the tail, not to widen the bands: widening the
# bucket from $50k to $100k moved uniqueness by 0.1 points, because one $850k
# property in a small county is unique at any width. Below this floor BOTH the
# value band and the equity band are withheld and the card simply omits them.
# Costs 12.7% of listings their value band; protects the 8.3% that sit in
# cells of three or fewer.
_MIN_COMPARABLE_PARCELS = 5


def _band_equity(value: Any, owed: Any) -> Optional[dict[str, Any]]:
    """Equity as a band, from a snapshotted value and a live debt.

    Returns None when either side is missing — which is the common case, not
    an edge case: only ~210 of 726 sheriff-sale parcels carry a usable latest
    event_value. A caller must render None as ABSENT, never as "no equity".
    Those are opposite claims and only one of them is true.

    The value is assessed_value_at_listing (what the owner saw at listing
    time) and the debt is live. Deliberately mismatched vintages: an assessed
    value is annual, while a foreclosure payoff accrues interest and fees
    continuously, so a snapshotted debt would be stale within a week. Because
    debt only grows, the live figure understates equity relative to a
    same-vintage comparison — which is the safe direction to be wrong in.
    """
    if value is None or owed is None:
        return None
    try:
        val = Decimal(str(value))
        debt = Decimal(str(owed))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if val <= 0:
        return None

    pct = ((val - debt) / val) * Decimal("100")

    for ceiling, band, label in _EQUITY_BANDS:
        if pct <= ceiling:
            return {"band": band, "label": label}
    band, label = _EQUITY_BEYOND
    return {"band": band, "label": label}


def _band_timing(days_remaining: Any) -> Optional[dict[str, Any]]:
    """A timing WINDOW from days_remaining. Never the date, never the count."""
    if days_remaining is None:
        return None
    try:
        days = int(days_remaining)
    except (TypeError, ValueError):
        return None

    if days < 0:
        # Expired window. The listing is filtered out upstream, but if one
        # reaches here it must not render as "under 30 days".
        return None

    for ceiling, label in _TIMING_BUCKETS:
        if days <= ceiling:
            return {"label": label, "bucket": label}
    return {"label": _TIMING_BEYOND, "bucket": _TIMING_BEYOND}


# ============================================================
# BROWSE
# ============================================================

# Columns a buyer may see. Written as an explicit allow-list rather than a
# SELECT * with fields popped afterwards: owner_name_submitted and
# owner_name_on_record live on this table, and a redaction that works by
# REMOVING fields fails open when a column is added. This one fails closed.
_LISTING_PUBLIC_QUALITATIVE = (
    "occupancy",
    "condition",
    "primary_need",
    "leaseback_interest",
    "buyback_interest",
    "viewing_access",
    "ownership_verified",
)


@router.get(
    "/marketplace/listings",
    status_code=http_status.HTTP_200_OK,
    summary="Browse owner-listed properties — banded, no parcel identity",
)
async def marketplace_listings(
    _ctx: InvestorContext = InvestorResolved,
    county: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=100),
) -> dict[str, Any]:
    """Active listings, banded.

    buyer_types_allowed is enforced in the WHERE clause, not in response
    shaping. It is an owner's CONSENT about who may see their property, not a
    display preference — an owner who excluded investors must not have the
    listing rendered and then filtered client-side.
    """
    # Two CTEs, both fixed-cost.
    #
    # `cells` counts comparable distressed parcels per county + $50k bucket.
    # It is computed over the WHOLE sheriff-sale population, not over
    # marketplace.listings — counting listings would return 1 for every cell
    # while the marketplace is small and suppress everything. The question is
    # how many comparable properties exist to hide among, and that is a
    # property of the distress data.
    #
    # `debt` uses DISTINCT ON so a parcel carrying several signals yields
    # exactly ONE row. Structural rather than a LIMIT 1 in a correlated
    # subquery: a fan-out here would silently duplicate listings in the browse
    # response. Measured at 20ms total against a LATERAL's 16ms + 1.3ms per
    # listing, so it is slower for one listing and flat thereafter.
    sql = """
        WITH cells AS (
            SELECT p.county_code,
                   floor(p.estimated_market_value / 50000) AS value_bucket,
                   count(*) AS n
              FROM signals.distress_with_parcel s
              JOIN core.parcels p
                    ON p.parcel_id   = s.eff_parcel_id
                   AND p.county_code = s.county_slug
             WHERE s.event_type = 'sheriff_sale'
               AND p.estimated_market_value IS NOT NULL
             GROUP BY 1, 2
        ),
        debt AS (
            SELECT DISTINCT ON (s.eff_parcel_id, s.county_slug)
                   s.eff_parcel_id,
                   s.county_slug,
                   s.event_value
              FROM signals.distress_with_parcel s
             WHERE s.event_type = 'sheriff_sale'
               AND s.event_value IS NOT NULL
             ORDER BY s.eff_parcel_id, s.county_slug, s.event_date DESC
        )
        SELECT l.id,
               l.created_at,
               l.assessed_value_at_listing,
               l.assessed_value_source,
               l.earliest_close_date,
               l.preferred_close_date,
               l.occupancy,
               l.condition,
               l.primary_need,
               l.leaseback_interest,
               l.buyback_interest,
               l.viewing_access,
               l.ownership_verified,
               p.county_code,
               r.days_remaining,
               r.outcome,
               c.n            AS comparable_parcels,
               d.event_value  AS amount_owed,
               (SELECT COUNT(*) FROM marketplace.offers o
                 WHERE o.listing_id = l.id) AS offer_count
          FROM marketplace.listings l
          LEFT JOIN core.parcels p
                 ON p.parcel_id = l.parcel_id
          LEFT JOIN outcomes.redemption_current r
                 ON r.parcel_id = l.parcel_id
                AND r.county_code = p.county_code
          LEFT JOIN cells c
                 ON c.county_code  = p.county_code
                AND c.value_bucket = floor(l.assessed_value_at_listing / 50000)
          LEFT JOIN debt d
                 ON d.eff_parcel_id = l.parcel_id
                AND d.county_slug   = p.county_code
         WHERE l.status = 'active'
           -- Owner consent. 'investor' is in the default array; an owner who
           -- narrowed it to {retail} is excluded here.
           AND ('investor' = ANY(l.buyer_types_allowed)
                OR l.buyer_types_allowed IS NULL)
           -- A property that has already redeemed or sold is not collecting
           -- offers. NULL outcome means no redemption row at all, which is a
           -- legitimate listing (tax delinquency, no sheriff sale).
           AND (r.outcome IS NULL OR r.outcome = 'pending')
           AND (%(county)s IS NULL OR p.county_code = %(county)s)
         ORDER BY l.created_at DESC
         LIMIT %(limit)s
    """
    try:
        with pg() as cur:
            cur.execute(sql, {
                "county": county.strip().lower() if county else None,
                "limit": limit,
            })
            rows = cur.fetchall() or []
    except Exception as e:
        logger.error("marketplace: listing browse FAILED",
                     error_type=type(e).__name__, error=str(e)[:400])
        raise HTTPException(
            status_code=http_status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"message": "Listings are temporarily unavailable."},
        )

    items: list[dict[str, Any]] = []
    for row in rows:
        # k-anonymity gate. Below the floor NEITHER band is published, because
        # they compound: value alone drives 36.6% of cell uniqueness and
        # equity adds ~10 points on top. Suppressing equity while publishing
        # the value band that causes most of the exposure would protect the
        # wrong field.
        #
        # A NULL count means the listing has no assessed value, so there was
        # no cell to join to and nothing to suppress — _band_value returns
        # None for it anyway.
        comparable = row.get("comparable_parcels")
        dense_enough = (
            comparable is not None
            and int(comparable) >= _MIN_COMPARABLE_PARCELS
        )

        if dense_enough:
            value_band = _band_value(row["assessed_value_at_listing"])
            equity = _band_equity(
                row["assessed_value_at_listing"],
                row.get("amount_owed"),
            )
        else:
            # Withheld, not missing. The card omits the field entirely rather
            # than showing a placeholder — an owner in a thin cell is exactly
            # the one a placeholder would draw attention to.
            value_band = None
            equity = None

        item: dict[str, Any] = {
            # The buyer's ONLY handle. Opaque, and not resolvable to a parcel
            # without database access.
            "listing_id": str(row["id"]),
            "county": row["county_code"],
            "listed_at": row["created_at"],
            "offer_count": int(row["offer_count"] or 0),
            "value_band": value_band,
            "timing": _band_timing(row["days_remaining"]),
            # None on ~70% of sheriff-sale parcels, which have a value but no
            # usable latest event_value. The client MUST render None as
            # absent, never as "no equity" — opposite claims, one of them
            # false.
            "equity": equity,
        }
        for col in _LISTING_PUBLIC_QUALITATIVE:
            item[col] = row.get(col)
        items.append(item)

    return success_envelope({
        "listings": items,
        "count": len(items),
        "note": (
            "Properties are shown as ranges, not exact figures, and are not "
            "identified until an owner opens a conversation with you. Every "
            "listing here is a property whose owner has asked for offers."
        ),
    })


# ============================================================
# OFFERS
# ============================================================

# Nothing may expire in under 72 hours.
#
# expires_at is BUYER-SET, and an exploding offer is the oldest pressure
# tactic there is against a distressed seller: a 24-hour deadline against
# someone in foreclosure is designed to prevent them getting a second offer,
# which is the single most valuable thing they can do. The floor costs a
# legitimate buyer nothing.
_MIN_OFFER_HOURS = 72

_ALLOWED_FINANCING = {"cash", "conventional", "hard_money", "seller_financed",
                      "other"}


@router.post(
    "/marketplace/offers",
    status_code=http_status.HTTP_201_CREATED,
    summary="Make an offer on a listing",
)
async def marketplace_create_offer(
    _ctx: InvestorContext = InvestorResolved,
    listing_id: str = Body(...),
    offer_amount: float = Body(...),
    proposed_close_date: Optional[str] = Body(default=None),
    financing_type: Optional[str] = Body(default=None),
    is_preapproved: bool = Body(default=False),
    preapproval_lender: Optional[str] = Body(default=None),
    contingencies: Optional[list[str]] = Body(default=None),
    expires_at: Optional[str] = Body(default=None),
    notes: Optional[str] = Body(default=None),
) -> dict[str, Any]:
    """Create an offer. POST, not PATCH — CORS in src/main.py allows only
    GET/POST/HEAD/OPTIONS, and a PATCH would be blocked at preflight with no
    status code and no server-side log.
    """
    # Amount first: everything else is optional decoration around it.
    try:
        amount = Decimal(str(offer_amount))
    except (InvalidOperation, TypeError, ValueError):
        raise HTTPException(
            status_code=http_status.HTTP_400_BAD_REQUEST,
            detail={"message": "Enter a valid offer amount."},
        )
    if amount <= 0:
        raise HTTPException(
            status_code=http_status.HTTP_400_BAD_REQUEST,
            detail={"message": "Enter an offer amount greater than zero."},
        )

    if financing_type and financing_type not in _ALLOWED_FINANCING:
        raise HTTPException(
            status_code=http_status.HTTP_400_BAD_REQUEST,
            detail={"message": "That financing type is not recognised."},
        )

    # The 72-hour floor.
    expiry: Optional[datetime] = None
    if expires_at:
        try:
            expiry = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        except ValueError:
            raise HTTPException(
                status_code=http_status.HTTP_400_BAD_REQUEST,
                detail={"message": "Enter a valid expiry date and time."},
            )
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        floor = datetime.now(timezone.utc) + timedelta(hours=_MIN_OFFER_HOURS)
        if expiry < floor:
            raise HTTPException(
                status_code=http_status.HTTP_400_BAD_REQUEST,
                detail={"message": (
                    "An offer cannot expire in less than 72 hours. Owners "
                    "here are working to a deadline and need time to "
                    "consider more than one offer."
                )},
            )

    close_date: Optional[date] = None
    if proposed_close_date:
        try:
            close_date = date.fromisoformat(proposed_close_date)
        except ValueError:
            raise HTTPException(
                status_code=http_status.HTTP_400_BAD_REQUEST,
                detail={"message": "Enter a valid proposed closing date."},
            )

    # The listing must exist, be active, and allow investors. Re-checked here
    # rather than trusted from the browse response: the browse is a separate
    # request and the owner may have withdrawn in between. RLS will not catch
    # this — there are no policies.
    try:
        with pg() as cur:
            cur.execute(
                """
                SELECT id, status, buyer_types_allowed
                  FROM marketplace.listings
                 WHERE id = %s
                 LIMIT 1
                """,
                (listing_id,),
            )
            listing = cur.fetchone()
    except Exception as e:
        logger.error("marketplace: listing lookup FAILED",
                     error_type=type(e).__name__, error=str(e)[:400])
        raise HTTPException(
            status_code=http_status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"message": "We could not reach that listing just now."},
        )

    # Same message for "no such listing" and "withdrawn". A buyer probing ids
    # should not be able to tell the difference between one that never
    # existed and one an owner pulled.
    if not listing or listing["status"] != "active":
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail={"message": "That listing is no longer taking offers."},
        )

    allowed = listing["buyer_types_allowed"]
    if allowed is not None and "investor" not in allowed:
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail={"message": "That listing is no longer taking offers."},
        )

    try:
        with pg() as cur:
            cur.execute(
                """
                INSERT INTO marketplace.offers
                    (listing_id, buyer_user_id, offer_amount,
                     proposed_close_date, financing_type, is_preapproved,
                     preapproval_lender, contingencies, expires_at, notes)
                VALUES
                    (%(listing_id)s, %(buyer)s, %(amount)s,
                     %(close_date)s, %(financing)s, %(preapproved)s,
                     %(lender)s, %(contingencies)s, %(expires)s, %(notes)s)
                RETURNING id
                """,
                {
                    "listing_id": listing_id,
                    "buyer": _ctx.user_id,
                    "amount": amount,
                    "close_date": close_date,
                    "financing": financing_type,
                    "preapproved": bool(is_preapproved),
                    "lender": preapproval_lender,
                    "contingencies": contingencies,
                    "expires": expiry,
                    "notes": notes,
                },
            )
            created = cur.fetchone()
    except Exception as e:
        logger.error("marketplace: offer INSERT FAILED",
                     error_type=type(e).__name__, error=str(e)[:400])
        raise HTTPException(
            status_code=http_status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"message": "We could not record that offer. Try again."},
        )

    offer_id = str(created["id"])
    logger.info("marketplace: offer created", tier=_ctx.tier)

    # Notify the owner. AFTER the insert and unable to affect the response:
    # the offer is committed, and a buyer must not see an error because
    # Resend was slow. send_offer_notification never raises; this is belt and
    # braces for the same reason the raise-hand confirmation has it.
    try:
        await send_offer_notification(listing_id)
    except Exception as e:  # pragma: no cover - defensive
        logger.error("marketplace: offer email raised",
                     error_type=type(e).__name__, error=str(e)[:400])

    return success_envelope({
        "offer_id": offer_id,
        "status": "submitted",
        "message": (
            "Your offer has been sent. The owner decides whether to open a "
            "conversation — the property is not identified to you until they "
            "do."
        ),
    })


__all__ = ["router"]
