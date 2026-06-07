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
from datetime import datetime, timezone
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
from src.llm.foreclosure_promotion import build_promotion_rows
from src.utils.errors import success_envelope
from src.utils.logger import logger


router = APIRouter(prefix="/admin", tags=["admin"])

# The public site origin, used to build the access link in the email.
_SITE_ORIGIN = "https://govire.com"


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


@router.post(
    "/extractions/approve",
    status_code=http_status.HTTP_200_OK,
    summary="Approve an extracted notice and promote it to the live tables.",
    dependencies=[AdminKeyRequired],
)
async def approve_extraction(payload: AdminActionIn) -> dict[str, Any]:
    """Promote one extraction into core.parcels + signals.distress_events +
    signals.sheriff_sales, then stamp it approved/promoted. Idempotent: if a
    distress_events row with the same (source, source_id) already exists, we
    skip the inserts but still mark the extraction approved."""
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

        built = build_promotion_rows(extracted)
        source_id = built["source_id"]

        # Idempotency guard: does the distress_events row already exist?
        existing = (
            signals_table("distress_events")
            .select("id")
            .eq("source", "startribune_legal")
            .eq("source_id", source_id)
            .limit(1)
            .execute()
        )
        already = bool(existing.data)

        if not already:
            # FK chain: distress_events.parcel_id -> core.parcels.parcel_id,
            # and core.parcels.county_code -> core.counties.county_code. So the
            # parcel must exist first. Check-then-insert (plain insert, so any
            # error surfaces rather than silently no-op'ing).
            pid = built["parcel_row"]["parcel_id"]
            parcel_exists = (
                core_table("parcels")
                .select("parcel_id")
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
        )
        return success_envelope({
            "id": payload.id,
            "status": "approved",
            "promoted": not already,
            "duplicate": already,
            "source_id": source_id,
        })

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("admin approve extraction failed", error_type=type(e).__name__)
        raise HTTPException(
            status_code=500,
            detail=f"approve failed: {type(e).__name__}: {e}",
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


__all__ = ["router"]
