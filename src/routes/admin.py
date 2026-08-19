"""
Admin endpoints for managing access requests to the gated /data page, plus
review/promotion of extracted foreclosure notices (Feature #5).

All endpoints require the admin key (X-Admin-Key header), reusing the same
AdminKeyRequired dependency as the trigger routes.

Routes:
    GET  /admin/requests            — list all access requests
    POST /admin/approve             — approve a request (generates key, emails)
    POST /admin/decline             — block a request
    GET  /admin/extractions         — list extracted foreclosure notices
    POST /admin/extractions/approve — promote an extraction to live tables
    POST /admin/extractions/reject  — reject an extraction
"""

from __future__ import annotations

import os
from datetime import date, datetime, timezone
from typing import Any, Optional

import asyncio

from fastapi import APIRouter, HTTPException, status as http_status
from pydantic import BaseModel

from src.middleware.auth import AdminKeyRequired
from src.db.supabase_client import (
    access_table,
    ai_table,
    signals_table,
    get_client,
)
from src.llm.foreclosure_promotion import (
    build_promotion_rows,
    split_pids,
    derive_source,
    derive_source_id,
    _county_slug as _fc_county_slug,
)
from src.services.spine_resolver import (
    SpineLookupUnavailable,
    resolve_spine_parcel,
)
from src.scrapers.startribune_legal import run_startribune_scrape
from src.scrapers.mnpublicnotice_probe import probe_mnpublicnotice
from src.scrapers.hennepin_sheriff_probe import probe_hennepin_sheriff
from src.scrapers.anoka_assessor_probe import probe_anoka_assessor
from src.scrapers.mnpublicnotice import run_mnpublicnotice_scrape
from src.utils.errors import success_envelope
from src.utils.logger import logger


router = APIRouter(prefix="/admin", tags=["admin"])

# The public site origin, used to build the access link in the email.
_SITE_ORIGIN = "https://govire.com"

# Every source label a promoted foreclosure row may carry.
#
# ADDED 2026-08-02. foreclosure_promotion.py used to stamp "startribune_legal"
# onto every promoted row regardless of which feed produced it. It now uses the
# extraction's real source_name (369 of 371 are mnpublicnotice), and the 237
# misattributed distress_events rows are being migrated.
#
# The lookups below match on EITHER label, deliberately, because the code and
# the data cannot change in the same instant. Matching only the new label would
# mean the idempotency guard finds nothing among the un-migrated rows and
# re-inserts every foreclosure as a DUPLICATE; matching only the old one breaks
# the moment a row is migrated. Accepting both removes the window entirely and
# makes the deploy order irrelevant.
#
# Safe to narrow to the real source once the migration is verified and no rows
# carry a mislabelled source — but there is no cost to leaving it, and a third
# feeder would slot in here rather than needing this reasoning rediscovered.
_PROMOTION_SOURCES = ("startribune_legal", "mnpublicnotice")


# ============================================================
# GET /admin/requests — list all requests
# ============================================================


@router.get(
    "/requests",
    status_code=http_status.HTTP_200_OK,
    summary="List all access requests.",
    dependencies=[AdminKeyRequired],
)
async def list_requests() -> dict[str, Any]:
    """Return every access request, newest first, for the admin page."""
    try:
        result = (
            access_table("access_requests")
            .select(
                "id, email, name, role, phone, company, how_heard, reason, "
                "status, requested_at, approved_at, last_seen_at"
            )
            .order("requested_at", desc=True)
            .limit(1000)
            .execute()
        )
        return success_envelope({"requests": result.data or []})
    except Exception as e:
        logger.exception(
            "admin list requests failed", error_type=type(e).__name__
        )
        raise HTTPException(status_code=500, detail="Failed to list requests.")


# ============================================================
# POST /admin/approve — approve a request + email the link
# ============================================================


class AdminActionIn(BaseModel):
    id: int


def _send_approval_email(to_email: str, name: Optional[str], key: str) -> bool:
    """Send the access-link email via Resend. Returns True on success.
    Best-effort: a send failure does NOT undo the approval — the owner can
    always resend the link manually."""
    api_key = os.environ.get("RESEND_API_KEY")
    from_addr = os.environ.get("RESEND_FROM", "noreply@govire.com")
    if not api_key:
        logger.warning("RESEND_API_KEY not set — skipping approval email")
        return False

    link = f"{_SITE_ORIGIN}/data?key={key}"
    greeting = f"Hi {name}," if name else "Hi,"
    html = (
        f"<p>{greeting}</p>"
        f"<p>You're approved to access the govire.com property data. "
        f"Use this private link to view it:</p>"
        f'<p><a href="{link}">{link}</a></p>'
        f"<p>Bookmark it — it's your personal access link. "
        f"Please don't share it.</p>"
        f"<p>— govire</p>"
    )
    try:
        import resend
        resend.api_key = api_key
        resend.Emails.send({
            "from": f"govire <{from_addr}>",
            "to": [to_email],
            "subject": "Your govire.com access link",
            "html": html,
        })
        return True
    except Exception as e:
        logger.exception("approval email send failed", error_type=type(e).__name__)
        return False


@router.post(
    "/approve",
    status_code=http_status.HTTP_200_OK,
    summary="Approve an access request and email the access link.",
    dependencies=[AdminKeyRequired],
)
async def approve_request(payload: AdminActionIn) -> dict[str, Any]:
    """Set a request to approved (the DB trigger generates access_key), then
    email the person their access link."""
    try:
        # Flip to approved. The BEFORE-UPDATE trigger fills access_key.
        access_table("access_requests").update(
            {"status": "approved"}
        ).eq("id", payload.id).execute()

        # Read back the row to get the generated key + email.
        row_result = (
            access_table("access_requests")
            .select("id, email, name, status, access_key")
            .eq("id", payload.id)
            .limit(1)
            .execute()
        )
        rows = row_result.data or []
        if not rows:
            raise HTTPException(status_code=404, detail="Request not found.")

        row = rows[0]
        email_sent = False
        if row.get("status") == "approved" and row.get("access_key"):
            email_sent = _send_approval_email(
                row["email"], row.get("name"), row["access_key"]
            )

        logger.info(
            "access request approved",
            request_id=payload.id,
            email_sent=email_sent,
        )
        return success_envelope({
            "id": row["id"],
            "status": row["status"],
            "email_sent": email_sent,
            "access_link": f"{_SITE_ORIGIN}/data?key={row['access_key']}"
            if row.get("access_key") else None,
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("admin approve failed", error_type=type(e).__name__)
        raise HTTPException(status_code=500, detail="Failed to approve request.")


# ============================================================
# POST /admin/decline — block a request
# ============================================================


@router.post(
    "/decline",
    status_code=http_status.HTTP_200_OK,
    summary="Decline (block) an access request.",
    dependencies=[AdminKeyRequired],
)
async def decline_request(payload: AdminActionIn) -> dict[str, Any]:
    """Set a request to blocked. Any existing key stops working immediately
    (the gate checks status == 'approved')."""
    try:
        access_table("access_requests").update(
            {"status": "blocked"}
        ).eq("id", payload.id).execute()
        logger.info("access request declined", request_id=payload.id)
        return success_envelope({"id": payload.id, "status": "blocked"})
    except Exception as e:
        logger.exception("admin decline failed", error_type=type(e).__name__)
        raise HTTPException(status_code=500, detail="Failed to decline request.")


# ============================================================
# Foreclosure-notice extraction review (Feature #5)
# ============================================================
# Admin-gated review of ai.extracted_foreclosures. Approving promotes the
# extracted notice into the live signals tables (distress_events +
# sheriff_sales) plus the core.parcels row their FKs depend on; rejecting
# marks it and never promotes.


@router.get(
    "/extractions",
    status_code=http_status.HTTP_200_OK,
    summary="List extracted foreclosure notices for review.",
    dependencies=[AdminKeyRequired],
)
async def list_extractions(status: str = "pending") -> dict[str, Any]:
    """Return extracted foreclosure notices filtered by review_status
    (default 'pending'), lowest-confidence first so the rows most needing a
    human look surface at the top."""
    try:
        result = (
            ai_table("extracted_foreclosures")
            .select(
                "id, source_url, source_name, fetched_at, mortgagor, mortgagee, "
                "property_address, city, county, parcel_id, original_principal, "
                "amount_due, sale_date, sale_time, sale_location, "
                "redemption_period, vacate_date, attorney_firm, attorney_file_no, "
                "confidence, extraction_notes, review_status, reviewed_at, "
                "promoted_at, model"
            )
            .eq("review_status", status)
            .order("confidence", desc=False)
            .limit(500)
            .execute()
        )
        return success_envelope({"extractions": result.data or []})
    except Exception as e:
        logger.exception("admin list extractions failed", error_type=type(e).__name__)
        raise HTTPException(status_code=500, detail="Failed to list extractions.")


def _resolve_spine_parcel(extracted: dict[str, Any]) -> Optional[dict[str, Any]]:
    """The REAL core.parcels row for this notice, or None.

    Returns the ROW ({parcel_id, address, city}) rather than the id alone. A
    PACKAGE notice's extracted['property_address'] is the notice's FULL LIST of
    addresses -- twelve of them for washington 26-003536FC -- so each member
    needs ITS OWN address, and only a DB lookup can supply it.
    build_promotion_rows stays pure.

    MOVED 2026-08-17 to src/services/spine_resolver.py. The rule used to live
    here, in an HTTP route module, reachable only from the approve path. Every
    sheriff scraper therefore invented its own synthetic parcel_id: measured
    2026-08-17, hennepin_sheriff minted 381 stub parcels, dakota_sheriff 129
    and anoka_sheriff 17 IN ONE DAY, of which 523 of 527 resolve to exactly one
    real parcel by address. The 760 stubs migrated onto real parcels on 08-16
    were 529 back within 21 hours. One copy of the rule, callable by
    everything -- when it changes, the migration SQL changes with it.

    THE EXCEPTION IS NO LONGER SWALLOWED HERE. The previous version caught
    every Exception and returned None, sending the caller to the synthetic
    path. On 2026-08-17 08:54 a PostgREST connection dropped mid-lookup three
    times (RemoteProtocolError, logged with error_type but read as a digits
    miss) and minted HENNEPIN-FC-3211821340028-2026-08-26 -- a stub named after
    the very parcel the failed query would have returned. It also defeated the
    idempotency guard below, which keys on the effective pid: the guard checked
    a stub, found no events, inserted, and hit 23505 two seconds later. A
    transport failure is NOT evidence that a parcel does not exist, so
    SpineLookupUnavailable propagates to the caller, which returns 503 and
    mints nothing.
    """
    county_code = _fc_county_slug(extracted.get("county"))
    if not county_code:
        return None
    return resolve_spine_parcel(
        county_code,
        published_pid=extracted.get("parcel_id"),
        address=extracted.get("property_address"),
    )



def _as_date(value: Any) -> Optional[date]:
    """Parse an ISO date out of whatever the API layer hands back.

    PostgREST returns dates as strings, build_promotion_rows carries through
    whatever the extractor produced. Returns None rather than raising: an
    unparseable date must mean "cannot compare", never "treat as earlier".
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    s = str(value).strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def _apply_postponement(existing_row: dict[str, Any],
                        built: dict[str, Any],
                        source_id: str) -> bool:
    """A notice arrived for a foreclosure already on file. If it moved the
    sale LATER, update the live rows. Returns True if anything changed.

    Returns False — changing nothing — whenever the comparison cannot be made
    safely: either date missing or unparseable, incoming date not strictly
    later. Silence is the correct outcome there. The overwhelmingly common
    case is the same notice re-scraped (one Golden Valley notice was
    re-extracted twice daily for eleven days), and re-stamping identical
    values on every one of those would be noise at best.

    Updates BOTH source tables. redemption_builder reads
    raw_data->>'dateOfSale' FIRST and falls back to event_date, so correcting
    only one of them leaves the builder deriving the old anchor. Verified
    2026-08-02: all 14 affected rows had event_date and raw_data.dateOfSale
    holding the same stale value.
    """
    new_event = built["distress_event"]
    new_sale = built["sheriff_sale"]

    old_date = _as_date(existing_row.get("event_date"))
    new_date = _as_date(new_event.get("event_date"))

    if old_date is None or new_date is None:
        logger.info("postponement check skipped — date missing",
                    source_id=source_id)
        return False
    if new_date <= old_date:
        return False

    # distress_events: the whole raw_data blob is replaced rather than one key
    # patched. It is rebuilt from the NEWER notice, so it also carries the
    # newer source_url, confidence and extraction flags — strictly better than
    # the superseded copy. title and description embed the sale date in prose
    # and would otherwise still read the old one back to a human.
    signals_table("distress_events").update({
        "event_date": new_event.get("event_date"),
        "event_value": new_event.get("event_value"),
        "raw_data": new_event.get("raw_data"),
        "title": new_event.get("title"),
        "description": new_event.get("description"),
    }).in_("source", list(_PROMOTION_SOURCES)).eq(
        "source_id", source_id).execute()

    # sheriff_sales has no source_id column; parcel_id is the handle, and the
    # synthetic PID is built from source_id so it identifies the same
    # foreclosure. postponement_count is read then incremented — it was
    # hardcoded to 0 at promotion and had never moved, so the fact that a sale
    # had been postponed at all was recorded nowhere.
    parcel_id = new_sale.get("parcel_id")
    # COMPOSITE KEY (2026-08-11). Both the read below and the UPDATE further
    # down filtered on parcel_id ALONE. Minnesota parcel IDs are not unique
    # across counties: measured 2026-08-11, 95,036 parcel_ids exist in more
    # than one county (315,575 rows involved, one PIN in TWENTY-FIVE
    # counties).
    #
    # The UPDATE was the dangerous half -- no county and no limit, so a
    # postponement here could rewrite sale_date, sale_time, sale_location and
    # postponement_count on another county's foreclosure entirely.
    #
    # sheriff_sales.county_code is set by build_promotion_rows, so the value
    # is on new_sale. If it is ever missing, both statements below are
    # SKIPPED rather than run unscoped: a lost postponement statistic is
    # recoverable, a rewritten sale date on someone else's property is not.
    county_code = new_sale.get("county_code")
    if not parcel_id or not county_code:
        logger.warning(
            "postponement skipped -- no county_code on the sheriff_sale row",
            source_id=source_id,
            parcel_id=parcel_id,
        )
        return True

    prior = 0
    try:
        found = (
            signals_table("sheriff_sales")
            .select("postponement_count")
            .eq("county_code", county_code)
            .eq("parcel_id", parcel_id)
            .limit(1)
            .execute()
        )
        if found.data:
            prior = int(found.data[0].get("postponement_count") or 0)
    except Exception as e:
        # Non-fatal: a missed increment is a lost statistic, while failing the
        # approval would leave the WRONG DATE live in front of an owner. The
        # date update above has already committed.
        logger.warning("postponement count read failed",
                       source_id=source_id, error_type=type(e).__name__)

    signals_table("sheriff_sales").update({
        "sale_date": new_sale.get("sale_date"),
        "sale_time": new_sale.get("sale_time"),
        "sale_location": new_sale.get("sale_location"),
        "postponement_count": prior + 1,
    }).eq("county_code", county_code).eq("parcel_id", parcel_id).execute()

    logger.info(
        "sheriff sale POSTPONED — live rows updated",
        source_id=source_id,
        old_sale_date=str(old_date),
        new_sale_date=str(new_date),
        days_later=(new_date - old_date).days,
    )
    return True


@router.post(
    "/extractions/approve",
    status_code=http_status.HTTP_200_OK,
    summary="Approve an extracted notice and promote it to the live tables.",
    dependencies=[AdminKeyRequired],
)
async def approve_extraction(payload: AdminActionIn) -> dict[str, Any]:
    """Promote one extraction into core.parcels + signals.distress_events +
    signals.sheriff_sales, then stamp it approved/promoted.

    Idempotent on (source, source_id). source_id is the attorney file number,
    which is stable PER FORECLOSURE, not per notice — so a POSTPONED sale
    republished as a new statutory notice arrives with the SAME source_id and
    a LATER sale_date.

    Until 2026-08-02 a match meant "skip the inserts entirely". That silently
    discarded every postponement. Measured live: 14 properties across 10
    counties were anchored to a superseded sale date, understating the
    redemption window by 30 to 84 days — 2500 Auburn Dr, Victoria was showing
    13 July when the sale had moved to 27 August. The failure is in the
    dangerous direction for an owner: they are told the door shut earlier than
    it did, and an owner who believes the deadline passed stops fighting.

    Now a match compares dates:
      * incoming sale_date LATER  -> UPDATE the existing rows (postponement)
      * incoming sale_date SAME or EARLIER -> skip, as before

    A postponement never moves a deadline backwards here. If a notice ever
    republished an earlier date we do not act on it: shortening someone's
    window on the strength of a re-published notice needs a human, not a rule.

    What this deliberately does NOT do is touch outcomes.redemption_tracker.
    redemption_builder.py owns the precedence ladder (county-published expiry
    > notice-stated period > statutory default) and rebuilds the tracker daily
    from these rows. Recomputing the expiry here would be a second copy of
    that logic — the owner-classifier-in-five-files mistake — and the two
    copies would drift. Correcting the SOURCE rows is what makes the fix
    durable: the builder derives the right anchor on its next run.

    SOURCE LABEL (resolved 2026-08-02): foreclosure_promotion.py no longer
    hardcodes "startribune_legal" — it reads the extraction's real source_name,
    of which 369 of 371 are mnpublicnotice. The lookups here match EITHER label
    via _PROMOTION_SOURCES so the code change and the data migration do not
    have to land in the same instant. See that constant for the reasoning."""
    try:
        row_result = (
            ai_table("extracted_foreclosures")
            .select("*")
            .eq("id", payload.id)
            .limit(1)
            .execute()
        )
        rows = row_result.data or []
        if not rows:
            raise HTTPException(status_code=404, detail="Extraction not found.")
        extracted = rows[0]

        if extracted.get("review_status") == "rejected":
            raise HTTPException(
                status_code=409,
                detail="This extraction was rejected; cannot approve.",
            )

        # === ONE NOTICE MAY COVER MANY PARCELS (added 2026-08-15) ===
        #
        # Measured on mnpublicnotice: washington 26-003536FC lists THIRTEEN
        # parcels -- twelve addresses on Keibler Ct and 211th St, Forest Lake --
        # under one bid of $261,140.77. Before this, the whole PID field went to
        # _pid_digits() as one string, matched nothing, and the notice landed on
        # a single synthetic stub. Thirteen distressed properties appeared to a
        # subscriber as ONE row, and twelve were invisible.
        #
        # Each parcel now becomes its own event, so all of them are searchable.
        # The bid is NOT divided or copied: build_promotion_rows sets
        # event_value NULL on a package member and records the package total in
        # raw_data._package. Copying one bid onto thirteen parcels would
        # fabricate thirteen equity spreads.
        #
        # A single-PID notice takes exactly the old path: pins has one element,
        # package stays None, and build_promotion_rows behaves as before.
        pins = split_pids(extracted.get("parcel_id"))
        if not pins:
            # No usable PID at all (a fragment like cass '45-118', or free text).
            # Keep the original value so the synthetic-stub path is unchanged.
            pins = [extracted.get("parcel_id")]
        package_base = (
            {"size": len(pins), "parcel_ids": pins} if len(pins) > 1 else None
        )
        if package_base:
            logger.info(
                "package sale: promoting one notice as several events",
                extraction_id=payload.id,
                parcels=len(pins),
            )

        promoted_count = 0
        duplicate_count = 0
        postponed_count = 0
        # Members that ended up WITH an event on a real parcel — either just
        # inserted by the RPC, or already present so the insert was correctly
        # skipped. Distinct from promoted_via_rpc, which is only the first
        # case. See the promoted_at stamp after the loop.
        members_with_event = 0
        source_id = None
        # True once signals.promote_extraction() has run for at least one
        # member. That function marks the extraction approved itself, inside
        # the same transaction as the inserts, so the standalone update below
        # is only needed when NOTHING promoted (every member a duplicate or a
        # postponement).
        promoted_via_rpc = False

        for _idx, _pin in enumerate(pins, start=1):
            # A per-member copy so real_pid, _synthetic_pid and
            # raw_data.detail.gis_pid are all this parcel's, with no signature
            # change to build_promotion_rows.
            extracted_member = {**extracted, "parcel_id": _pin}
            package = (
                {**package_base, "index": _idx} if package_base else None
            )

            # Resolve the notice's PID against the county spine BEFORE building.
            # build_promotion_rows stays pure (no DB), so the lookup happens here
            # and the answer is passed in. See _resolve_spine_parcel().
            resolved_row = _resolve_spine_parcel(extracted_member)
            resolved_pid = (resolved_row or {}).get("parcel_id")
            built = build_promotion_rows(
                extracted_member,
                resolved_parcel_id=resolved_pid,
                package=package,
                # Only a package member needs this; build_promotion_rows
                # ignores it otherwise. A member with no resolved parcel gets
                # an EMPTY address rather than the notice's whole list.
                member_address=(
                    {
                        "address": (resolved_row or {}).get("address"),
                        "city": (resolved_row or {}).get("city"),
                    }
                    if package
                    else None
                ),
            )
            source_id = built["source_id"]
            effective_pid = built["parcel_id"]

            # Idempotency guard: does the distress_events row already exist?
            #
            # CHANGED 2026-08-10: was keyed on source_id, which is NOT stable.
            # source_id falls back to `ef-{extraction id}` when a notice carries
            # no attorney file number, and Minnesota requires a foreclosure
            # notice to run SIX CONSECUTIVE WEEKS (Minn. Stat. 580.03) — so the
            # same sale is re-extracted weekly with a new id, a new source_id,
            # and this guard missed every time.
            #
            # Measured live: 289 mnpublicnotice sheriff_sale rows were 219
            # distinct properties — 68 duplicates, 24% inflation. 4318 Harvest
            # Court, Monticello appeared NINETEEN times, five of them on a
            # single day.
            #
            # The true identity of a foreclosure event is the PARCEL plus the
            # SALE DATE. Both are now stable: a resolved spine parcel, or a
            # synthetic keyed on (county, pid digits, sale date). Sale date is
            # deliberately NOT part of the match — a postponement keeps the same
            # parcel with a later date, and _apply_postponement below must still
            # see it as the same foreclosure moving, not a new one.
            existing = (
                signals_table("distress_events")
                .select("id, event_date, source")
                .in_("source", list(_PROMOTION_SOURCES))
                .eq("parcel_id", effective_pid)
                .eq("event_type", "sheriff_sale")
                .limit(1)
                .execute()
            )
            existing_rows = existing.data or []

            # === FALLBACK: MATCH ON THE PUBLISHED PID (added 2026-08-19) ===
            #
            # The lookup above matches on effective_pid, which in a county with
            # NO spine is a SYNTHETIC key -- and the synthetic key is derived
            # from source_id, which is 'ef-{extraction id}' whenever a notice
            # carries no attorney file number. A new extraction row every week
            # means a new id, a new source_id, a new synthetic parcel, and a
            # guard that can never find the event it wrote last week.
            #
            # Measured live 2026-08-19: Pine 355039000 (315 1st Street South,
            # Brook Park) holds TWO events -- PINE-FC-ef-164 dated 2026-07-09
            # and PINE-FC-ef-426 dated 2026-08-13 -- one property, one
            # foreclosure, postponed. Both approvals then 500'd on
            # distress_events_source_identity_key because the guard missed and
            # the insert hit the unique index. Across all live mnpublicnotice
            # and startribune_legal events: 273 rows for 250 distinct
            # properties.
            #
            # The stable identity is what the COUNTY published: county plus the
            # parcel number printed on the notice, held in
            # raw_data.detail.gis_pid. That does not move when the sale date
            # moves, which is precisely what the guard needs.
            #
            # Filtering happens in Python rather than as a PostgREST JSON
            # filter: _PROMOTION_SOURCES covers 273 live rows STATEWIDE, so a
            # single county's unresolved sheriff sales are a handful of rows,
            # and this avoids depending on nested-JSON filter syntax.
            #
            # resolved_at IS NULL is required: a foreclosure that already
            # completed or cured must not block a genuinely new one on the
            # same parcel years later.
            if not existing_rows:
                _member_digits = "".join(ch for ch in str(_pin or "") if ch.isdigit())
                _event_county = (built["distress_event"] or {}).get("county_code")
                if _member_digits and _event_county:
                    _candidates = (
                        signals_table("distress_events")
                        .select("id, event_date, source, parcel_id, raw_data")
                        .in_("source", list(_PROMOTION_SOURCES))
                        .eq("county_code", _event_county)
                        .eq("event_type", "sheriff_sale")
                        .is_("resolved_at", "null")
                        .execute()
                    )
                    for _row in (_candidates.data or []):
                        _pub = (
                            ((_row.get("raw_data") or {}).get("detail") or {})
                            .get("gis_pid")
                        )
                        _pub_digits = "".join(
                            ch for ch in str(_pub or "") if ch.isdigit()
                        )
                        if _pub_digits and _pub_digits == _member_digits:
                            existing_rows = [_row]
                            logger.info(
                                "existing event found by PUBLISHED PID "
                                "(synthetic parcel key missed)",
                                extraction_id=payload.id,
                                county_code=_event_county,
                                published_pid=_pin,
                                stale_parcel_key=effective_pid,
                                matched_event_id=_row.get("id"),
                                matched_parcel_id=_row.get("parcel_id"),
                            )
                            break

            already = bool(existing_rows)
            postponed = False

            if already:
                postponed = _apply_postponement(
                    existing_row=existing_rows[0],
                    built=built,
                    source_id=source_id,
                )

            if not already:
                # === ONE TRANSACTION, NOT FOUR HTTP CALLS (2026-08-17) ===
                #
                # This block used to issue four independent PostgREST requests
                # -- county seed, parcel insert, distress_events insert,
                # sheriff_sales insert -- and the extraction status update ran
                # after the loop as a fifth. PostgREST has no multi-statement
                # transaction, so a failure part-way left the earlier writes
                # committed.
                #
                # Measured live 2026-08-17: two approvals failed at the
                # extraction-status update AFTER the event insert had
                # committed. The extraction stayed 'pending', the card
                # reappeared, the banner said "Approve failed. Try again." --
                # and the retry hit 23505 on
                # distress_events_source_identity_key, because the event it was
                # about to insert already existed. The UI was actively driving
                # the duplicate.
                #
                # signals.promote_extraction() does all of it inside one
                # plpgsql transaction: a failure anywhere rolls the whole
                # promotion back, so a retry starts from a clean state. It also
                # marks the extraction approved, which is why the standalone
                # update after this loop now only runs when NOTHING promoted.
                #
                # parcel_row is None when the PID resolved to a real spine
                # parcel: that row already exists and carries assessor data a
                # stub would overwrite. The function skips BOTH the county seed
                # and the parcel insert in that case -- a resolved parcel's own
                # FK already guarantees its county is seeded.
                parcel_row = built["parcel_row"]

                # Readable county name for the auto-seed, e.g. "Saint Louis"
                # -> "Saint Louis County". Statewide notices reference any of
                # MN's 87 counties, and an unseeded county FK-fails the parcel
                # insert -- the cause of the earlier approval 500s.
                _raw_county = (
                    extracted.get("county")
                    or (parcel_row or {}).get("county_code")
                    or ""
                ).strip()
                _county_name = (
                    (
                        _raw_county
                        if _raw_county.lower().endswith("county")
                        else f"{_raw_county} County"
                    )
                    if _raw_county
                    else None
                )

                rpc_result = (
                    get_client()
                    .schema("signals")
                    .rpc("promote_extraction", {
                        "p_extraction_id": payload.id,
                        "p_county_code": (parcel_row or {}).get("county_code"),
                        "p_county_name": _county_name,
                        "p_parcel_row": parcel_row,
                        "p_distress_event": built["distress_event"],
                        "p_sheriff_sale": built["sheriff_sale"],
                    })
                    .execute()
                )
                promoted_via_rpc = True
                logger.info(
                    "extraction member promoted in one transaction",
                    extraction_id=payload.id,
                    parcel_id=effective_pid,
                    resolved=resolved_pid is not None,
                    rpc=rpc_result.data,
                )


            # Tally per member. `already` and `postponed` are set by the body
            # above for THIS parcel.
            if already:
                members_with_event += 1
                if postponed:
                    postponed_count += 1
                else:
                    duplicate_count += 1
            else:
                members_with_event += 1
                promoted_count += 1

        already = promoted_count == 0
        postponed = postponed_count > 0

        

        # The RPC already stamped this row inside the promotion transaction.
        # Anything else reaches here unstamped, and what it gets depends on
        # whether an event actually exists.
        #
        # === promoted_at IS NOT SET WHEN NOTHING PROMOTED (2026-08-19) ===
        # This block used to stamp promoted_at unconditionally on `if not
        # promoted_via_rpc`. That flag is False in TWO cases that are nothing
        # alike:
        #
        #   every member was already there  -> the event EXISTS; the insert
        #                                      was correctly skipped
        #   no member resolved to a parcel  -> NOTHING was written anywhere
        #
        # Both were marked promoted. Measured 2026-08-19 across every approval
        # since 2026-06-07: 523 approvals, 493 with an event on the published
        # PID, its digits, or the address-resolved parcel -- and 30 with an
        # event on NONE of them, all stamped promoted_at, all looking done in
        # the admin UI. Nineteen are in counties holding between 1 and 67
        # parcels (martin 3, beltrami 1, roseau 3, todd 1, sibley 1, redwood 1,
        # dodge 1, blue_earth 1, le_sueur 11, pine 67) where no resolver could
        # place them; the rest are multi-address package notices whose whole
        # address list is passed as one address.
        #
        # None of that is fixable here, and none of it should be silent. An
        # approval that placed nothing now keeps promoted_at NULL, so
        # `promoted_at IS NULL AND review_status = 'approved'` is a queue that
        # can be counted, alerted on and worked -- instead of 30 rows that
        # claim success. Same failure shape as the billing service's
        # `if user_id and sub_id:` with no else: the guard was right, the
        # reporting was not.
        #
        # review_status still becomes 'approved' either way. You DID approve
        # it; the system just could not place it.
        if not promoted_via_rpc:
            ts = datetime.now(timezone.utc).isoformat()
            _update: dict[str, Any] = {
                "review_status": "approved",
                "reviewed_at": ts,
            }
            if members_with_event:
                _update["promoted_at"] = ts
            ai_table("extracted_foreclosures").update(_update).eq(
                "id", payload.id
            ).execute()
            if not members_with_event:
                logger.warning(
                    "extraction approved but NOTHING promoted — no member "
                    "resolved to a parcel with an event; promoted_at left "
                    "NULL so this stays visible as outstanding work",
                    extraction_id=payload.id,
                    county=extracted.get("county"),
                    published_pid=extracted.get("parcel_id"),
                    address=extracted.get("property_address"),
                    members=len(pins),
                )

        logger.info(
            "extraction approved + promoted",
            extraction_id=payload.id,
            source_id=source_id,
            already_existed=already,
            postponed=postponed,
        )
        return success_envelope({
            "id": payload.id,
            "status": "approved",
            # A package notice promotes SEVERAL parcels from one extraction.
            "parcels": len(pins),
            "promoted_count": promoted_count,
            "duplicate_count": duplicate_count,
            "postponed_count": postponed_count,
            "promoted": promoted_count > 0,
            # A postponement is neither a fresh promotion nor a no-op
            # duplicate. The review UI needs to tell them apart: "already on
            # file" and "sale date moved, deadline extended" are different
            # facts about a property.
            "duplicate": already and not postponed,
            "postponed": postponed,
            "source_id": source_id,
        })

    except HTTPException:
        raise
    except SpineLookupUnavailable as e:
        # The spine could not be QUERIED -- not "this parcel is not in the
        # spine". Nothing was written: _resolve_spine_parcel raises before
        # build_promotion_rows runs, so no synthetic parcel was minted and no
        # event was inserted.
        #
        # 503, not 500, and deliberately retryable. On 2026-08-17 the previous
        # code swallowed this exact failure, fell through to the synthetic
        # path, and minted HENNEPIN-FC-3211821340028-2026-08-26 -- a stub named
        # after the real parcel the dropped connection would have returned.
        # Six RemoteProtocolErrors hit five different call sites in 150
        # seconds; the resolver already retries once before giving up, so
        # reaching here means the database was genuinely unreachable twice.
        logger.error(
            "admin approve extraction unavailable — spine unreachable",
            extraction_id=payload.id,
            error=str(e),
        )
        raise HTTPException(
            status_code=503,
            detail="Parcel lookup is temporarily unavailable. Nothing was "
                   "written; please retry.",
        )
    except Exception as e:
        logger.exception("admin approve extraction failed", error_type=type(e).__name__)
        raise HTTPException(
            status_code=500,
            detail="Failed to approve extraction.",
        )


@router.post(
    "/extractions/reject",
    status_code=http_status.HTTP_200_OK,
    summary="Reject an extracted notice (never promoted).",
    dependencies=[AdminKeyRequired],
)
async def reject_extraction(payload: AdminActionIn) -> dict[str, Any]:
    """Mark an extraction rejected, and retire the event IT created (if any).

    CHANGED 2026-08-19. This route used to update review_status and nothing
    else, and its docstring claimed the extraction "is never promoted to the
    live tables". That is false for anything approved BEFORE it was rejected.

    Measured live: extraction 126 (St. Louis, file 26-120401) was promoted
    2026-06-14 and rejected 2026-06-29 -- fifteen days later. Event 61632 was
    still on the foreclosure tab on 2026-08-19, seven weeks after the
    rejection, because rejecting changed a status column and left the live row
    untouched.

    === WHY THE MATCH IS DELIBERATELY NARROW ===

    Retiring "the event for this parcel" would be actively wrong. Minnesota
    requires six weeks of publication (Minn. Stat. 580.03) and postponements
    are routine, so ONE foreclosure produces SEVERAL extraction rows over
    months, all on the same parcel. Rejecting a stale republication is the
    normal way to clear the queue -- and under a parcel-wide rule that would
    retire the CURRENT event.

    Concrete case from 2026-08-19: extraction 90 (Martin, 058273-F1, sale
    2026-06-26) was rejected as stale. Its parcel's live event 69252 had been
    moved to 2026-08-17 by a postponement and must survive.

    So the match is the identity of THIS NOTICE VERSION: same source_id (with
    the package '#parcel' suffix allowed) AND the same sale date. Under that
    rule extraction 90 finds nothing -- 058273-F1 matches, 2026-06-26 does not
    equal 2026-08-17 -- and 69252 stays live, which is correct.

    A NULL sale_date matches nothing and retires nothing. That is deliberate:
    an extraction with no sale date cannot identify a version, and doing
    nothing is the safe failure.
    """
    try:
        row_result = (
            ai_table("extracted_foreclosures")
            .select("id, source_name, attorney_file_no, sale_date")
            .eq("id", payload.id)
            .limit(1)
            .execute()
        )
        rows = row_result.data or []
        if not rows:
            raise HTTPException(status_code=404, detail="Extraction not found.")
        extracted = rows[0]

        retired_event_ids: list[int] = []
        _sale_date = extracted.get("sale_date")
        _source = derive_source(extracted)
        _source_id = derive_source_id(extracted)

        if _sale_date and _source_id:
            _candidates = (
                signals_table("distress_events")
                .select("id, source_id, event_date, parcel_id")
                .eq("source", _source)
                .eq("event_type", "sheriff_sale")
                .eq("event_date", _sale_date)
                .is_("resolved_at", "null")
                .execute()
            )
            for _row in (_candidates.data or []):
                _sid = str(_row.get("source_id") or "")
                # Exact match, or a package member of this same notice.
                if _sid == _source_id or _sid.startswith(f"{_source_id}#"):
                    retired_event_ids.append(_row["id"])

        if retired_event_ids:
            signals_table("distress_events").update({
                "resolved_at": datetime.now(timezone.utc).isoformat(),
                "resolution": "rejected",
            }).in_("id", retired_event_ids).execute()
            logger.info(
                "rejected extraction: live events retired",
                extraction_id=payload.id,
                source=_source,
                source_id=_source_id,
                sale_date=_sale_date,
                retired_event_ids=retired_event_ids,
            )

        ai_table("extracted_foreclosures").update({
            "review_status": "rejected",
            "reviewed_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", payload.id).execute()
        logger.info(
            "extraction rejected",
            extraction_id=payload.id,
            retired_events=len(retired_event_ids),
        )
        return success_envelope({
            "id": payload.id,
            "status": "rejected",
            "retired_event_ids": retired_event_ids,
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("admin reject extraction failed", error_type=type(e).__name__)
        raise HTTPException(status_code=500, detail="Failed to reject extraction.")

# ============================================================
# POST /admin/scrape/startribune — pull new foreclosure notices now
# ============================================================


class ScrapeIn(BaseModel):
    """Optional per-run cap on how many NEW notices to extract."""
    max_new: int = 25


@router.post(
    "/scrape/startribune",
    status_code=http_status.HTTP_200_OK,
    summary="Fetch the Star Tribune foreclosures listing and stage new notices.",
    dependencies=[AdminKeyRequired],
)
async def scrape_startribune(payload: ScrapeIn) -> dict[str, Any]:
    """Run the Star Tribune foreclosure-notice scraper on demand. It fetches
    the listing, extracts each NEW notice (not already staged), and inserts it
    into ai.extracted_foreclosures as 'pending' for review on the Notice-review
    tab. Returns a summary of what it found and staged.

    The scraper is synchronous and network-bound (one listing fetch + up to
    max_new detail fetches, paced ~1s apart), so a run can take a minute or two
    for a full batch. That's fine for a manual admin trigger."""
    try:
        # run_startribune_scrape is synchronous (httpx.Client + time.sleep);
        # run it in a worker thread so it doesn't block the event loop.
        import anyio
        result = await anyio.to_thread.run_sync(
            lambda: run_startribune_scrape(max_new=payload.max_new)
        )

        return success_envelope({
            "ok": result.ok,
            "listing_urls_found": result.listing_urls_found,
            "already_staged": result.already_staged,
            "newly_extracted": result.newly_extracted,
            "extraction_failed": result.extraction_failed,
            "stored_ids": result.stored_ids,
            "error": result.error,
            "notes": result.notes,
        })
    except Exception as e:
        logger.exception(
            "admin startribune scrape failed", error_type=type(e).__name__
        )
        raise HTTPException(
            status_code=500,
            detail="Failed to run the Star Tribune scrape.",
        )

# ============================================================
# POST /admin/probe/mnpublicnotice — verify server can use the site
# ============================================================


@router.post(
    "/probe/mnpublicnotice",
    status_code=http_status.HTTP_200_OK,
    summary="Diagnostic: can THIS server fetch + search mnpublicnotice.com?",
    dependencies=[AdminKeyRequired],
)
async def probe_mnpublicnotice_route() -> dict[str, Any]:
    """Run the mnpublicnotice.com source-verification probe from this server
    and return the diagnostic. Writes nothing — it only reports what our
    server receives, so we know whether a real scraper is viable before
    building one."""
    try:
        import anyio
        diag = await anyio.to_thread.run_sync(probe_mnpublicnotice)
        return success_envelope(diag)
    except Exception as e:
        logger.exception("admin mnpublicnotice probe failed", error_type=type(e).__name__)
        raise HTTPException(status_code=500, detail="Probe failed to run.")


@router.get(
    "/probe/anoka_assessor",
    status_code=http_status.HTTP_200_OK,
    summary="Diagnostic: does Anoka's Assessor_Sales service expose year_built / property_type?",
    dependencies=[AdminKeyRequired],
)
async def probe_anoka_assessor_route() -> dict[str, Any]:
    """Dump the Anoka Assessor_Sales service (layers, fields, one sample row
    each) so we can see whether year_built / property_type are available before
    deciding to ingest Anoka parcels into core.parcels. Writes nothing."""
    try:
        import anyio
        diag = await anyio.to_thread.run_sync(probe_anoka_assessor)
        return success_envelope(diag)
    except Exception as e:
        logger.exception("admin anoka assessor probe failed", error_type=type(e).__name__)
        raise HTTPException(status_code=500, detail="Probe failed to run.")
 

@router.post(
    "/probe/hennepin-sheriff",
    status_code=http_status.HTTP_200_OK,
    summary="Diagnostic: does the Hennepin Sheriff foreclosure API work from this server?",
    dependencies=[AdminKeyRequired],
)
async def probe_hennepin_sheriff_route() -> dict[str, Any]:
    """Run the Hennepin Sheriff API probe from this server. Confirms the Search
    API works here + tries to discover a detail endpoint. Writes nothing."""
    try:
        import anyio
        diag = await anyio.to_thread.run_sync(probe_hennepin_sheriff)
        return success_envelope(diag)
    except Exception as e:
        logger.exception("admin hennepin sheriff probe failed", error_type=type(e).__name__)
        raise HTTPException(status_code=500, detail="Probe failed to run.")

# ============================================================
# POST /admin/scrape/mnpublicnotice — statewide foreclosure notices
# ============================================================


class MnNoticeScrapeIn(BaseModel):
    """Optional per-run cap + recent-window size for the mnpublicnotice scrape."""
    max_new: int = 25
    window_days: int = 14


@router.post(
    "/scrape/mnpublicnotice",
    status_code=http_status.HTTP_200_OK,
    summary="Search mnpublicnotice.com for recent foreclosure notices and stage new ones.",
    dependencies=[AdminKeyRequired],
)
async def scrape_mnpublicnotice(payload: MnNoticeScrapeIn) -> dict[str, Any]:
    """Run the statewide mnpublicnotice foreclosure-notice scraper on demand.
    Searches the recent window, extracts each NEW notice (dedup by notice ID),
    and stages it in ai.extracted_foreclosures as 'pending' for review."""
    try:
        import anyio
        result = await anyio.to_thread.run_sync(
            lambda: run_mnpublicnotice_scrape(
                max_new=payload.max_new, window_days=payload.window_days
            )
        )
        return success_envelope({
            "ok": result.ok,
            "notices_on_results": result.notices_on_results,
            "new_ids": result.new_ids,
            "already_staged": result.already_staged,
            "newly_extracted": result.newly_extracted,
            "extraction_failed": result.extraction_failed,
            "stored_ids": result.stored_ids,
            "error": result.error,
            "notes": result.notes,
        })
    except Exception as e:
        logger.exception("admin mnpublicnotice scrape failed", error_type=type(e).__name__)
        raise HTTPException(status_code=500, detail="Failed to run the mnpublicnotice scrape.")

@router.post(
    # No "/admin" prefix here — the router already carries it (see the other
    # routes in this file, which are declared "/approve", "/decline" etc).
    # Writing it out produced /admin/admin/run-saved-search-alerts, which
    # answered correctly and looked like a typo to anyone reading it.
    "/run-saved-search-alerts",
    status_code=http_status.HTTP_200_OK,
    summary="Send any saved-search digests that are due (manual trigger).",
    dependencies=[AdminKeyRequired],
)
async def run_saved_search_alerts_now() -> dict[str, Any]:
    """Run the alert job once, on demand.

    ADDED 2026-08-11 so the digest can be watched working before it goes on
    a schedule. Sending on a cron that has never been observed sending is
    how a broken alert stays broken for a month.

    Runs on a WORKER THREAD for the reason cron.py's header sets out at
    length: the write path underneath is synchronous throughout, and awaiting
    it on the API's loop would pin uvicorn for the duration.

    Respects the same due-check as the scheduled run — a search alerted an
    hour ago will not be alerted again, so calling this repeatedly cannot
    spam anyone.
    """
    from src.services.saved_search_alerts import run_saved_search_alerts_blocking

    try:
        summary = await asyncio.to_thread(run_saved_search_alerts_blocking)
    except Exception as e:
        logger.exception(
            "manual saved-search alert run failed",
            error_type=type(e).__name__,
        )
        raise HTTPException(
            status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"message": f"Alert run failed: {type(e).__name__}: {e}"},
        ) from e

    return success_envelope(summary)


__all__ = ["router"]
