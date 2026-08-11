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

from fastapi import APIRouter, HTTPException, status as http_status
from pydantic import BaseModel

from src.middleware.auth import AdminKeyRequired
from src.db.supabase_client import (
    access_table,
    ai_table,
    signals_table,
    core_table,
)
from src.llm.foreclosure_promotion import (
    build_promotion_rows,
    _county_slug as _fc_county_slug,
    _pid_digits as _fc_pid_digits,
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


def _resolve_spine_parcel(extracted: dict[str, Any]) -> Optional[str]:
    """The REAL core.parcels.parcel_id for this notice, or None.

    ADDED 2026-08-10. Every mnpublicnotice foreclosure was hung off a
    SYNTHETIC parcel built from the notice id, never the actual parcel — so
    the rows carried no market value, no owner, no coordinates, no equity.
    That is why a Beltrami row renders as em-dashes while a Hennepin row
    shows $344,800: there is no parcel record behind it, only a stub named
    after the notice.

    Counties print the same PID in incompatible shapes (wright
    '155-154-004010', beltrami '83.00180.00', dakota '42-42800-01-071'), so
    the match is digits-only via core.parcel_pid_lookup, backed by the
    expression index idx_parcels_county_pid_digits. Measured 2026-08-10:
    exact match resolved 47 of 217 notices, digits-only resolved 185, and
    the lookup runs in 0.57ms on an index scan.

    Returns None on ANY doubt — no county, no usable PID, no hit, more than
    one hit, or an error. The caller then falls back to the synthetic path,
    which is exactly today's behaviour. A wrong parcel would attach a
    foreclosure to someone else's property, so silence is the only safe
    failure here.
    """
    county_code = _fc_county_slug(extracted.get("county"))
    if not county_code:
        return None
    digits = _fc_pid_digits(extracted.get("parcel_id"))
    if not digits:
        return None
    try:
        hit = (
            core_table("parcel_pid_lookup")
            .select("parcel_id")
            .eq("county_code", county_code)
            .eq("pid_digits", digits)
            .limit(2)
            .execute()
        )
    except Exception as e:
        logger.warning(
            "spine parcel lookup failed — falling back to synthetic PID",
            county=county_code,
            error_type=type(e).__name__,
        )
        return None
    rows = hit.data or []
    if len(rows) != 1:
        # 0 = not in the spine (beltrami and redwood have NO spine at all).
        # 2+ = ambiguous within one county; never guess.
        return None
    return rows[0].get("parcel_id")


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

        # Resolve the notice's PID against the county spine BEFORE building.
        # build_promotion_rows stays pure (no DB), so the lookup happens here
        # and the answer is passed in. See _resolve_spine_parcel().
        resolved_pid = _resolve_spine_parcel(extracted)
        built = build_promotion_rows(extracted, resolved_parcel_id=resolved_pid)
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
        already = bool(existing_rows)
        postponed = False

        if already:
            postponed = _apply_postponement(
                existing_row=existing_rows[0],
                built=built,
                source_id=source_id,
            )

        if not already:
            # FK chain: distress_events.parcel_id -> core.parcels.parcel_id,
            # and core.parcels.county_code -> core.counties.county_code. So both
            # the county AND the parcel must exist first.
            #
            # Ensure the county exists in core.counties BEFORE inserting the
            # parcel — statewide notices reference any of MN's 87 counties, and
            # a parcel insert for an unseeded county would FK-fail (the cause of
            # the approval 500s). Auto-seeding here means any new MN county
            # self-heals instead of needing a manual seed.
            county_code = built["parcel_row"].get("county_code")
            if county_code:
                county_exists = (
                    core_table("counties")
                    .select("county_code")
                    .eq("county_code", county_code)
                    .limit(1)
                    .execute()
                )
                if not county_exists.data:
                    # Build a readable name from the raw extraction county, e.g.
                    # "Saint Louis" -> "Saint Louis County". Slug is the FK value.
                    raw_county = (extracted.get("county") or county_code).strip()
                    county_name = (
                        raw_county
                        if raw_county.lower().endswith("county")
                        else f"{raw_county} County"
                    )
                    core_table("counties").insert({
                        "county_code": county_code,
                        "county_name": county_name,
                        "state": "MN",
                    }).execute()
                    logger.info(
                        "auto-seeded county for extraction promotion",
                        county_code=county_code,
                        county_name=county_name,
                    )

            # Parcel next (check-then-insert). parcel_row is None when the
            # PID resolved to a real spine parcel — that row already exists
            # and carries assessor data a stub would overwrite.
            if built["parcel_row"] is not None:
                pid = built["parcel_row"]["parcel_id"]
                # COMPOSITE key. Minnesota parcel IDs are NOT unique across
                # counties, so a parcel_id-only check can find another
                # county's row, skip the insert, and leave the FK unsatisfied.
                # Same defect class fixed the same day in
                # properties._apply_assessor_owners.
                parcel_exists = (
                    core_table("parcels")
                    .select("parcel_id")
                    .eq("county_code", built["parcel_row"].get("county_code"))
                    .eq("parcel_id", pid)
                    .limit(1)
                    .execute()
                )
                if not parcel_exists.data:
                    core_table("parcels").insert(built["parcel_row"]).execute()

            signals_table("distress_events").insert(built["distress_event"]).execute()
            signals_table("sheriff_sales").insert(built["sheriff_sale"]).execute()

        

        ts = datetime.now(timezone.utc).isoformat()
        ai_table("extracted_foreclosures").update({
            "review_status": "approved",
            "reviewed_at": ts,
            "promoted_at": ts,
        }).eq("id", payload.id).execute()

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
            "promoted": not already,
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
    """Mark an extraction rejected. It is never promoted to the live tables."""
    try:
        ai_table("extracted_foreclosures").update({
            "review_status": "rejected",
            "reviewed_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", payload.id).execute()
        logger.info("extraction rejected", extraction_id=payload.id)
        return success_envelope({"id": payload.id, "status": "rejected"})
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

__all__ = ["router"]
