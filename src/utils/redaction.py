"""
Tier-aware redaction for property payloads.

Implements the govire four-tier spec (Free / Basic / Standard / Premium) plus
admin (full). Redaction happens SERVER-SIDE: fields a tier may not see are
physically removed from the payload (set to None) and flagged `<field>_locked`,
so the real value never ships to a client that shouldn't have it. The frontend
renders a lock wherever it sees a `_locked: true` flag.

This is the single source of truth for "what can each tier see". It is applied
inside _shape_property_row so EVERY property endpoint (list, detail, owner) is
covered by one chokepoint.

The lock hierarchy (from the spec):
  - Tier 1 (sacred): exact equity / market_value / amount owed — the enrichment
    value that the county does NOT publish. Locked until BASIC.
  - Tier 2 (locators): address, city, parcel, owner, exact sale/redemption dates.
    Locked until STANDARD.
  - Tier 3 (leverage): owner portfolio, multi-signal overlay, redemption exact
    date. PREMIUM.
  - County is shown density-aware: a county is only revealed to lower tiers when
    enough rows share it that it can't triangulate to one property.

Anonymous/Free callers never receive a value precise enough to find a property
at the county; they get bands + relative cues only.
"""

from __future__ import annotations

from typing import Any, Optional

# Tier ordering for "at least this tier" checks.
_TIER_RANK = {"free": 0, "basic": 1, "standard": 2, "premium": 3, "admin": 99}


def tier_rank(tier: Optional[str]) -> int:
    return _TIER_RANK.get((tier or "free").lower(), 0)


# ------------------------------------------------------------------
# Equity band — derive a coarse band from an exact equity/value figure
# so free/anonymous callers feel that value exists without seeing it.
# ------------------------------------------------------------------
def equity_band(value: Optional[float]) -> Optional[str]:
    """Coarse band for a dollar value. None -> None (honest em-dash)."""
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return None
    if v >= 100_000:
        return "high"
    if v >= 40_000:
        return "moderate"
    return "low"


def redemption_relative(state: Optional[str]) -> Optional[str]:
    """Map an exact redemption_state to a non-locating relative cue.
    'expiring_soon' -> 'ending_soon', 'in_redemption' -> 'active',
    'expired' -> 'expired'. None -> None."""
    if not state:
        return None
    return {
        "expiring_soon": "ending_soon",
        "in_redemption": "active",
        "expired": "expired",
    }.get(state, None)


# ------------------------------------------------------------------
# Field groups (match the keys produced by the per-source extractors
# and _redemption_fields / _shape_property_row in properties.py).
# ------------------------------------------------------------------

# Tier 1 — the sacred VALUE fields (locked below BASIC).
_VALUE_FIELDS = ("market_value", "amount", "original_principal")

# Tier 2 — LOCATOR fields (locked below STANDARD).
_LOCATOR_FIELDS = (
    "address", "city", "zip", "owner", "owner_mailing", "tax_parcel_no",
    "parcel_id", "municipality", "neighborhood", "lat", "lng",
)

# Tier 2 — exact dates are locators too (locked below STANDARD).
_DATE_FIELDS = ("sale_date", "sale_time", "redemption_ends_at", "registered_date")

# Tier 3 — LEVERAGE fields (locked below PREMIUM).
_LEVERAGE_FIELDS = ("owner_portfolio", "overlay")


def _lock(payload: dict[str, Any], field: str) -> None:
    """Null a field and flag it locked, only if the key is present."""
    if field in payload:
        payload[field] = None
        payload[f"{field}_locked"] = True


def redact_property(
    shaped: dict[str, Any],
    *,
    tier: str,
    county_is_dense: bool = True,
) -> dict[str, Any]:
    """Return a tier-redacted copy of a shaped property payload.

    Args:
        shaped: the dict built by _shape_property_row (full, unredacted).
        tier: one of free|basic|standard|premium|admin.
        county_is_dense: whether this row's county currently has enough rows
            in its category that revealing the county can't triangulate to one
            property. When False, county is rolled up for sub-standard tiers.

    Admin/premium see everything. Lower tiers get progressively redacted, with
    derived bands/relative cues added so the locked state is still informative.
    """
    rank = tier_rank(tier)

    # Admin and premium: full payload, nothing redacted.
    if rank >= _TIER_RANK["premium"]:
        return shaped

    p = dict(shaped)  # shallow copy; we only reassign top-level keys

    # ---- Always derive non-locating cues from the (still-present) values,
    #      BEFORE we lock anything, so free/basic keep something informative.
    equity_source = p.get("market_value")
    p.setdefault("equity_band", equity_band(equity_source))
    p.setdefault("redemption_relative", redemption_relative(p.get("redemption_state")))

    # ---- STANDARD and below: lock leverage (premium-only) ----
    # (rank < premium already true here)
    for f in _LEVERAGE_FIELDS:
        _lock(p, f)

    # ---- BELOW STANDARD: lock locators + exact dates ----
    if rank < _TIER_RANK["standard"]:
        for f in _LOCATOR_FIELDS:
            _lock(p, f)
        for f in _DATE_FIELDS:
            _lock(p, f)
        # Exact redemption day-count is a near-locator; keep only the relative
        # cue and the state, drop the precise countdown.
        if "redemption_days_left" in p:
            p["redemption_days_left"] = None
            p["redemption_days_left_locked"] = True
        # County: only reveal when dense enough not to triangulate.
        if not county_is_dense:
            if "county" in p and p["county"] is not None:
                # Roll up to a region-ish label without naming the county.
                p["county"] = None
                p["county_locked"] = True

    # ---- BELOW BASIC (i.e. FREE): lock the sacred VALUE fields ----
    if rank < _TIER_RANK["basic"]:
        for f in _VALUE_FIELDS:
            _lock(p, f)
        # equity_band stays (it's the non-locating cue), but in a THIN county
        # even the band can help triangulate, so drop it there.
        if not county_is_dense:
            p["equity_band"] = None
            p["equity_band_locked"] = True

    return p


# ------------------------------------------------------------------
# Detail-view extras. The /properties/{source}/{source_id} endpoint attaches
# the ENTIRE raw scraped record under `raw` and full parcel characteristics
# under `enrichment`, AFTER shaping. Those bypass the field-level redaction
# above and would leak the real address/owner/values to any tier. We must
# redact them explicitly.
#
# Policy:
#   - `raw`        : the unredacted source record. Contains address, owner,
#                    values, everything. Only PREMIUM/admin get it in full.
#                    Below premium it is removed entirely (the shaped+redacted
#                    fields are what those tiers see).
#   - `enrichment` : parcel characteristics (year built, lot size, school
#                    district, assessor values...). These are descriptive, not
#                    locating, EXCEPT assessor dollar values. We expose
#                    enrichment from STANDARD up; below standard it is removed.
# ------------------------------------------------------------------
def redact_detail_extras(p: dict[str, Any], *, tier: str) -> dict[str, Any]:
    """Redact the detail-only `raw` and `enrichment` fields in-place-ish.
    Returns the same dict (mutated copy semantics handled by caller)."""
    rank = tier_rank(tier)

    if rank >= _TIER_RANK["premium"]:
        return p  # premium/admin: full detail

    # Below premium: never ship the raw source record.
    if "raw" in p:
        p["raw"] = None
        p["raw_locked"] = True

    # Below standard: drop enrichment entirely (it carries assessor values and
    # fine-grained parcel attributes that aid locating/valuation).
    if rank < _TIER_RANK["standard"]:
        if "enrichment" in p:
            p["enrichment"] = None
            p["enrichment_locked"] = True

    return p


# ------------------------------------------------------------------
# Owner-portfolio browse (the /owners and /owners/{owner}/properties
# endpoints) is itself a PREMIUM leverage feature. Below premium the
# owner-resolution surface should not be served at all.
# ------------------------------------------------------------------
def owner_browse_allowed(tier: str) -> bool:
    return tier_rank(tier) >= _TIER_RANK["premium"]


__all__ = [
    "redact_property",
    "redact_detail_extras",
    "owner_browse_allowed",
    "equity_band",
    "redemption_relative",
    "tier_rank",
]
