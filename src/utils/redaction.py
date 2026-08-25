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
  - Tier 2 (locators): address, city, parcel, owner, exact sale/redemption
    dates, and IMAGERY (a Google pano id is a locator — one metadata call
    resolves it to coordinates; see _redact_imagery).
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


def _equity_from(
    market_value: Optional[float],
    amount_owed: Optional[float],
) -> Optional[float]:
    """Equity = assessed value minus the amount owed, or None.

    BOTH are required. A row with an assessment and no debt figure — every
    vacant registration, every probate notice — has no knowable equity, and
    None is the honest answer. Returning the market value there is exactly
    the defect this replaced: it made 'high' mean 'this building is worth
    something', on rows where nothing is owed at all.

    Mirrors signals.distress_with_parcel.equity_spread, which is
    `p.emv_total - de.event_value` and is null whenever either side is.
    Values arrive from PostgREST as strings or Decimals depending on the
    path, so both are coerced rather than assumed to be floats.
    """
    if market_value is None or amount_owed is None:
        return None
    try:
        return float(market_value) - float(amount_owed)
    except (TypeError, ValueError):
        return None


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
    # ADDED 2026-08-22. Without it the generated free-tier title fell through
    # to "Distressed property", which tells an anonymous visitor nothing about
    # what the row is — the one thing the free tier is supposed to convey.
    "probate_filing": "Probate estate",
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
_VALUE_FIELDS = (
    "market_value", "amount", "original_principal",
    # ADDED 2026-08-22, found by listing every non-null field an ANONYMOUS
    # caller receives per category rather than by re-reading these tuples.
    # Both are dollar figures that shipped free since their categories landed:
    #
    #   special_assessment_due  the assessment OWED. The spec's first lock is
    #                           "exact equity and amount owed... locked until
    #                           Basic" — this is an amount owed, and it is the
    #                           headline number of the whole tax_assessment
    #                           category. It was the one figure that category
    #                           exists to sell.
    #   annual_tax              the annual tax bill. Worse than it looks: with
    #                           the county's assessment ratio it back-solves to
    #                           an approximate market value, which is the exact
    #                           figure market_value sits in this tuple to
    #                           protect. A locked field is not locked if
    #                           another field reconstructs it.
    #
    # vacancy_years is deliberately NOT here — see the note in _LEVERAGE_FIELDS:
    # it derives from a public registry date and is a duration, not a value.
    "annual_tax", "special_assessment_due",
)

# Tier 2 — LOCATOR fields (locked below STANDARD).
#
# PROBATE FIELDS ADDED 2026-08-22. The probate category shipped on
# 2026-08-19/20 (payload fields on _extract_probate, then the tab) and NONE of
# its fields were ever added here, so `decedent`, `personal_representative`,
# `case_number` and `notice_url` were serving at EVERY tier — including
# anonymous. Measured on the live incognito view: a signed-out visitor read
# "Alan G. Ihde · Rebecca L. Ihde · 55-PR-26-5497" in full, while the
# foreclosure tab beside it correctly locked every owner name.
#
# These are the same KIND of field under different column names:
#   decedent                — the owner of record; `owner` by another name
#   personal_representative — the person to contact about the estate
#   case_number             — the court file, which opens the whole record
#   notice_url              — a direct link to the published notice, which
#                             carries the name, the address and the
#                             representative together. One click past every
#                             other lock on this list.
#
# `match_basis` deliberately stays visible at all tiers: it is the amber
# "Name match" confidence badge, not a locator, and hiding a caveat while
# showing what it qualifies is the same error as an equity spread that omits
# a senior mortgage.
#
# THE LESSON, since this is the third instance in one day: this tuple is a
# hand-written literal and nothing checks it against the payload. Adding a
# category means adding its fields HERE as well as to the six registration
# points, or the new fields default to fully public.
_LOCATOR_FIELDS = (
    "address", "city", "zip", "owner", "owner_mailing", "tax_parcel_no",
    "parcel_id", "municipality", "neighborhood", "lat", "lng",
    # PROBATE. These are the PAYLOAD keys, which carry a `probate_` prefix —
    # NOT the column headings ("Decedent", "Court file") and NOT the database
    # column names. A first attempt on 2026-08-22 listed them unprefixed and
    # matched nothing at all, so the lock silently did nothing and the names
    # kept shipping. Verified against a live anonymous payload before landing.
    "probate_decedent", "probate_representative", "probate_case_number",
    "probate_notice_url",
    # source_id and _eff_key both carry the parcel id in plain text on EVERY
    # category, next to a `parcel_id` that is null and flagged locked:
    #   probate  source_id "23-PR-25-425:250121010"  (case number + parcel)
    #   sheriff  source_id "2302821410197-4459268"   (parcel + sale record)
    #   _eff_key ["fillmore", "250121010"]           (county + parcel)
    # The lock on parcel_id has therefore never done anything. _eff_key has no
    # frontend reader at all; source_id survives only as the Premium
    # Summarize call, which is above this tier and keeps its value. Row keys
    # moved to `id` first (frontend 2026-08-22) so nulling this cannot collapse
    # every React key to "<source>-undefined".
    "source_id", "_eff_key",
)

# Tier 2 — exact dates are locators too (locked below STANDARD).
_DATE_FIELDS = (
    "sale_date", "sale_time", "redemption_ends_at", "registered_date",
    # ADDED 2026-08-22, one round after the other four probate fields — it was
    # missed because it is the only probate field whose payload key is a DATE
    # rather than a name, so it sat in a different tuple and survived the sweep.
    #
    # A hearing date is not a locator by itself. Combined with the county and
    # the court it is one: Olmsted probate, Sep 4 2026 pulls the day's calendar
    # and recovers the decedent, the representative and the address — the exact
    # three fields locked one tuple above. That is the slow path to the same
    # place, and the bypass rule exists for the slow paths.
    "probate_hearing_date",
)

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
    # Redemption rates (2026-08-24) — the first FORWARD-LOOKING number on the
    # platform. Everything else here describes what a property IS; this is
    # what is likely to HAPPEN to it, from 225 resolved redemption windows.
    #
    # Premium by the same reasoning as deal_math, and more so: it cannot be
    # read off the public record at any level of access. A Standard
    # subscriber sees every field in the source data and could in principle
    # reconstruct a market value; nobody can reconstruct "properties bought
    # at under 50% of assessed value redeem 72% of the time" without the
    # outcome history behind it.
    "redemption_rates",
    # Redemption timing (2026-08-25) — the survival curves. Premium for the
    # same reason as redemption_rates: it cannot be read off the public
    # record at any level of access. Nobody reconstructs "11.1% of tracked
    # windows reach a foreclosure sale within a year" without the outcome
    # history behind it.
    "redemption_timing",
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


def _redact_imagery(p: dict[str, Any], rank: int) -> None:
    """Tier-redact the nested imagery block in place on the COPIED payload.

    ADDED 2026-08-13. Same nested-dict discipline as _redact_tax_status above:
    the block is re-copied before mutation because redact_property's
    dict(shaped) copy is shallow, and mutating the nested dict directly would
    corrupt the caller's unredacted original.

    === WHY IMAGERY IS A LOCATOR ===
    A Google panorama id resolves to exact coordinates with ONE
    unauthenticated call to the Street View metadata endpoint. It is
    machine-readable and bulk-harvestable across every row on a page, which
    makes it a STRONGER locator than the address string, not a weaker one.
    lat/lng are already in _LOCATOR_FIELDS; pano_id belongs at the same tier
    by the same reasoning.

    The image itself is equally locating even without the id: Street View
    frames a house from the kerb, and the house number, the mailbox, the kerb
    stencil and the cross-street sign are routinely legible. Nothing needs to
    be reverse-image-searched — it can simply be read.

    === WHY _lock() IS NOT USED HERE ===
    _lock() nulls the whole value, which for a nested dict would blank
    `available` along with everything else. That boolean is the ONE imagery
    field safe at any tier: it says a picture exists, not where. It is the
    same non-locating-cue principle as equity_band and redemption_relative
    above, and it is what lets a locked tile say "Street View imagery
    available" without disclosing a thing.

    Blurring the image server-side was considered and REJECTED: it would cost
    a billable request per anonymous view, and storing the blurred derivative
    would breach Google's prohibition on caching or rehosting Maps Content.
    A CSS blur over a live URL is not a lock at all — the src is in devtools.

    Below STANDARD the client receives exactly:
        {"available": true|false, "locked": true}
    """
    block = p.get("imagery")
    if not isinstance(block, dict):
        return

    if rank >= _TIER_RANK["standard"]:
        return

    p["imagery"] = {
        "available": bool(block.get("available")),
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
    #
    # === THIS BANDS EQUITY, NOT VALUE (FIXED 2026-08-23) ===
    # This line read `equity_source = p.get("market_value")`, so the band
    # answered "is this property assessed above $100,000?" and the label —
    # and _safe_description's sentence "Estimated equity band: high" — called
    # the answer EQUITY. Almost every Minnesota property is assessed above
    # 100k, so almost every row banded "high" and the field carried no
    # information at all.
    #
    # Worse than uninformative: it was WRONG in the direction that matters.
    # Verified on the live anonymous API 2026-08-23, event 154768 — a Saint
    # Paul sheriff sale with $382,049.98 owed against a $331,300 assessment,
    # underwater by $50,749.98 — was published to anonymous visitors as
    # equity_band=high. Event 154762, genuinely +$341,301, returned the same
    # string. The band could not tell them apart.
    #
    # The thresholds were always equity-shaped (100k / 40k), and
    # equity_band() already handles both edge cases correctly: v <= 0 returns
    # None, so an underwater property gets an honest em-dash rather than a
    # reassuring word, and None returns None, so a row with no debt figure —
    # a vacant registration, most of the 2,024-row vacant category — says
    # nothing instead of claiming high equity. Only the SOURCE was wrong.
    #
    # This defect was latent until 2026-08-23. Ramsey held emv_total on 3.6%
    # of its parcels, so market_value was null on most rows and the band was
    # an honest em-dash by accident. Backfilling 158,006 assessments that
    # afternoon made every Ramsey row band "high", which is how it surfaced.
    #
    # `amount` is the shaped key for signals.distress_events.event_value; the
    # view computes the same figure as emv_total - event_value. Computed here
    # rather than read from the view because _shape_property_row does not
    # carry equity_spread into the payload at all.
    equity_source = _equity_from(p.get("market_value"), p.get("amount"))
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

    # ---- Imagery block (nested). Below standard it collapses to a single
    #      non-locating boolean; at standard and above it passes through
    #      whole. A pano id is a locator — one metadata call turns it into
    #      coordinates — so it gates with address and lat/lng, not below them.
    _redact_imagery(p, rank)

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
        # `status` is the same hazard as title/description and was missed:
        # scraper-written free text that bypasses every field-level lock. On
        # probate it read "Estate of Robert Brogan in probate" — the decedent's
        # name, in full, to an anonymous visitor, on a row whose owner column
        # was correctly locked beside it.
        #
        # It is NOT simply locked, because on other categories it carries the
        # relative cues the free tier is built on ("Expiring soon", "Sold").
        # Instead it is replaced with the category label whenever it names
        # someone. Only probate is known to embed a name today; the membership
        # test is on the generated label rather than a name-detection heuristic,
        # because guessing at what looks like a person's name is how this class
        # of leak gets half-fixed.
        if p.get("event_type") == "probate_filing" and p.get("status"):
            p["status"] = "In probate"
            p["status_locked"] = True
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
    price_min, price_max, equity_min, equity_max, sale_date_from,
    sale_date_to, redemption, sort.

    free / basic / premium / admin: returned unchanged (free/basic filter on
    locked rows as a teaser; premium/admin hunt on full data).
    standard: every gated filter is set to None; `sort` is forced to the
    default. Navigation filters (category/county/status) are never touched.
    """
    if filtering_allowed(tier):
        return dict(params)

    gated = dict(params)
    _GATED_FILTER_KEYS = (
        "multi_signal", "min_amount", "max_amount",
        "year_built_min", "year_built_max",
        "sqft_min", "lot_sqft_min", "lot_sqft_max",
        "property_type", "school_district",
        "price_min", "price_max",
        # Equity spread (2026-08-12): the single most valuable hunting filter
        # on the platform — "show me everything with $150k+ of spread" is the
        # investor's core query. Premium-only, same as every other power
        # filter. Standard still SEES the control (Decision 1) and gets the
        # upgrade prompt.
        "equity_min", "equity_max",
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
