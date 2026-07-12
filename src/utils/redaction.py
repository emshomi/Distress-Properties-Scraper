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
    'outcome_pending'/'expired' -> 'expired', 'resolved' -> 'resolved'.
    None -> None."""
    if not state:
        return None
    return {
        "expiring_soon": "ending_soon",
        "in_redemption": "active",
        "expired": "expired",
        "outcome_pending": "expired",
        "resolved": "resolved",
    }.get(state, None)


# Human labels for the category implied by event_type / source, used to build
# a SAFE generic title/description for sub-standard tiers. The scraper-written
# title/description embed owner names and dollar amounts verbatim, so they
# cannot be shown below Standard — we replace them with a generated string
# built only from non-locating fields (category + county).
_EVENT_LABELS = {
    "sheriff_sale": "Foreclosure (sheriff sale)",
    "foreclosure": "Foreclosure",
    "tax_forfeit": "Tax-forfeit property",
    "tax_delinquent": "Tax-delinquent property",
    "tax_assessment": "Tax assessment",
    "vacant": "Vacant/registered building",
}


def _safe_title(p: dict[str, Any]) -> str:
    """A generic, non-locating title from category + county only."""
    label = _EVENT_LABELS.get((p.get("event_type") or "").lower(), "Distressed property")
    county = p.get("county")
    return f"{label} — {county} County" if county else label


def _safe_description(p: dict[str, Any]) -> str:
    """A generic, non-locating description. Mentions only the band + relative
    redemption cue, never owner / address / dollar figures."""
    label = _EVENT_LABELS.get((p.get("event_type") or "").lower(), "Distressed property")
    band = p.get("equity_band")
    parts = [f"{label}."]
    if band:
        parts.append(f"Estimated equity band: {band}.")
    parts.append("Details locked — upgrade to view owner, address, and amounts.")
    return " ".join(parts)


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

# Tier 2 — parcel attributes patched from core.parcels (2026-07-09:
# forfeit-land surfacing — lot size + property-type name). Not strictly
# locators, but the enrichment policy in redact_detail_extras already
# treats fine-grained parcel attributes as STANDARD+ ("aids
# locating/valuation"), so these follow the same line. lat/lng are NOT
# here — they were already in _LOCATOR_FIELDS.
_PARCEL_ATTR_FIELDS = ("lot_sqft", "property_type_name")

# Tier 3 — LEVERAGE fields (locked below PREMIUM).
# The redemption OUTCOME group is the substance behind the tier table's
# "outcomes" lever: what actually happened after the redemption window
# (redeemed / REO / sold) and at what price. Premium-only by design; to
# loosen resale price to Standard later, move it out of this tuple.
_LEVERAGE_FIELDS = (
    "owner_portfolio", "overlay",
    "redemption_outcome", "redemption_outcome_label",
    "redemption_outcome_ambiguous",
    "redemption_resale_price", "redemption_resale_date",
    # Deal math (payoff floor / calibrated value / negotiation band) is the
    # sharpest leverage on the platform — premium only.
    "deal_math",
    # Vacancy cost estimates (cumulative VBR fees + PVE exposure) are the
    # motivated-seller leverage for vacant properties — premium only.
    # (vacancy_years itself stays visible: it derives from a public date.)
    "vacancy_est_fees_paid", "vacancy_est_pve_exposure", "vacancy_cost_basis",
)

# ------------------------------------------------------------------
# Tyler-portal tax-delinquency status (the nested `tax_status` block on
# olmsted_delq_list rows, from signals.tax_delinquency_status — 2026-07-12).
#
# Tier policy (per HANDOFF_2026-07-11 Priority 1a):
#   - redeemed_since_list       : EVERY tier, free included. It is the list-
#                                 hygiene hook (50.6% of the county's annual
#                                 list had already redeemed at scrape time) —
#                                 a boolean, non-locating, and the platform's
#                                 headline differentiator.
#   - clock + amounts           : STANDARD+. first/years delinquent, the
#                                 estimated judgment/forfeiture dates WITH
#                                 their basis (the date never ships without
#                                 the basis — it is a computed estimate,
#                                 never county-stated), totals, and the
#                                 statutory flags.
#   - owner mailing block       : PREMIUM only. The skip-trace value
#                                 (owner_name/_2, mailing address lines).
# ------------------------------------------------------------------

# Premium-only keys inside the tax_status block.
_TAX_STATUS_OWNER_KEYS = (
    "owner_name", "owner_name_2",
    "owner_mailing_address", "owner_mailing_city_state_zip",
)


def _redact_tax_status(p: dict[str, Any], rank: int) -> None:
    """Tier-redact the nested tax_status block in place on the COPIED payload.

    Only called below premium (redact_property returns early for
    premium/admin). The nested dict is re-copied before mutation because
    redact_property's dict(shaped) copy is shallow — mutating the nested
    block directly would corrupt the caller's unredacted original.
    """
    block = p.get("tax_status")
    if not isinstance(block, dict):
        return

    block = dict(block)  # never mutate the original nested dict
    p["tax_status"] = block

    # STANDARD and below: strip the premium owner/skip-trace keys.
    for k in _TAX_STATUS_OWNER_KEYS:
        if k in block:
            block[k] = None
    block["owner_locked"] = True

    # BELOW STANDARD: collapse to the hygiene hook alone.
    if rank < _TIER_RANK["standard"]:
        p["tax_status"] = {
            "redeemed_since_list": block.get("redeemed_since_list"),
            "locked": True,
        }


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

    # ---- Tyler tax-delinquency status block (nested, tiered internally:
    #      owner keys premium-only; below standard only the redeemed flag
    #      survives). No-op for rows without a tax_status block. ----
    _redact_tax_status(p, rank)

    # ---- BELOW STANDARD: lock locators + exact dates ----
    if rank < _TIER_RANK["standard"]:
        # title/description are scraper-written and embed owner names + dollar
        # amounts in plain text. They bypass field-level locks, so replace them
        # with generated, non-locating placeholders built from safe fields only.
        if "title" in p:
            p["title"] = _safe_title(p)
            p["title_locked"] = True
        if "description" in p:
            p["description"] = _safe_description(p)
            p["description_locked"] = True
        for f in _LOCATOR_FIELDS:
            _lock(p, f)
        for f in _DATE_FIELDS:
            _lock(p, f)
        for f in _PARCEL_ATTR_FIELDS:
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


# ------------------------------------------------------------------
# Filter / sort gating (the launch premium differentiator).
#
# Per GOVIRE_FILTER_GATING_SPEC.md:
#   "Reading is not hunting." Standard can READ every property in full, but
#   only PREMIUM can HUNT — slice the whole dataset down with the power
#   filters and sorts. The principle that makes the price gap honest.
#
# The deliberate INVERSION: filters are banned for STANDARD ONLY.
#   - free / basic : filters allowed, but rows are LOCKED, so filtering only
#                    previews the SHAPE of the data, never locates a property
#                    (a teaser that whets the appetite).
#   - standard     : filters BANNED. Standard sees full property detail; if it
#                    could also hunt, premium would have nothing left. Removing
#                    filters in the middle tier IS the upgrade lever.
#   - premium/admin: filters allowed on full data — the destination.
#
# Enforcement is SERVER-SIDE: a Standard token that requests a gated filter or
# sort has it neutralized here, so it cannot be bypassed in the browser. The
# frontend separately shows the controls as locked ("Upgrade to Premium").
# ------------------------------------------------------------------
def filtering_allowed(tier: str) -> bool:
    """Whether this tier may use the power filters/sorts ("hunting").

    Per the spec's deliberate inversion, filters are banned for STANDARD ONLY:
      - free / basic : filters allowed, but their rows are LOCKED, so filtering
                       only previews the data's shape (a teaser — can't locate).
      - standard     : filters BANNED. Standard reads full property detail; if
                       it could also hunt, premium would have nothing left.
                       This middle-tier removal IS the upgrade lever.
      - premium/admin: filters allowed on FULL data — the destination.
    """
    return (tier or "free").lower() != "standard"


# ------------------------------------------------------------------
# AI features (natural-language search, per-property summary).
#
# These call Claude on every use, so they cost real money per request. They are
# also pure "hunting"/"leverage" tools. Both reasons point the same way:
# PREMIUM-ONLY (and admin). Every tier below premium sees them locked; the
# backend rejects the request BEFORE any Claude call, so no cost is incurred
# for a non-premium caller and the gate cannot be bypassed in the browser.
#
# NOTE: this is a STRICTER gate than filtering_allowed(). Filters are banned for
# standard only (free/basic keep them on locked rows). AI features are banned
# for everyone below premium — because each call spends money.
# ------------------------------------------------------------------
def ai_features_allowed(tier: str) -> bool:
    """Premium/admin only — AI search and AI summary (each costs a Claude call)."""
    return tier_rank(tier) >= _TIER_RANK["premium"]


# Navigation filters every tier keeps — these scope the READING view without
# surfacing "the best deals", so they are not hunting:
#   - category : which signal tab (foreclosure / vacant / ...) — core nav
#   - county   : scope to your area so you don't scroll other counties
#   - status   : active vs postponed — a state of the same reading view
# Everything else (multi_signal, value/price bands, year built, sqft, lot,
# property_type, school_district, min_amount, sale-date range, redemption-state
# filter, and all non-default SORTS) is HUNTING → premium-only.
_NAVIGATION_FILTERS_KEPT = ("category", "county", "status")

# The default sort non-premium tiers are pinned to (matches the endpoint
# default). Any other requested sort is reset to this for sub-premium tiers.
_DEFAULT_SORT = "event_date"


def gate_filters_for_tier(tier: str, params: dict[str, Any]) -> dict[str, Any]:
    """Given the raw filter/sort params dict, return a copy with the power
    filters/sorts neutralized for the STANDARD tier (only).

    Keys expected (any subset): multi_signal, min_amount, year_built_min,
    year_built_max, sqft_min, lot_sqft_min, property_type, school_district,
    price_min, price_max, sale_date_from, sale_date_to, redemption, sort.

    free / basic / premium / admin: returned unchanged (free/basic filter on
    locked rows as a teaser; premium/admin hunt on full data).
    standard: every gated filter is set to None; `sort` is forced to the
    default. Navigation filters (category/county/status) are never touched.
    """
    if filtering_allowed(tier):
        return dict(params)

    gated = dict(params)
    _GATED_FILTER_KEYS = (
        "multi_signal", "min_amount",
        "year_built_min", "year_built_max",
        "sqft_min", "lot_sqft_min",
        "property_type", "school_district",
        "price_min", "price_max",
        "sale_date_from", "sale_date_to",
        "redemption",
        # Owner filters (2026-07-09): hunting by the current owner's
        # classification / absentee status — premium leverage.
        "owner_type", "absentee",
    )
    for k in _GATED_FILTER_KEYS:
        if k in gated:
            gated[k] = None
    # Force default sort (ignore any requested non-default sort).
    if "sort" in gated:
        gated["sort"] = _DEFAULT_SORT
    return gated


__all__ = [
    "redact_property",
    "redact_detail_extras",
    "owner_browse_allowed",
    "filtering_allowed",
    "gate_filters_for_tier",
    "ai_features_allowed",
    "equity_band",
    "redemption_relative",
    "tier_rank",
]
