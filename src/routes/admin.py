"""
Admin endpoints for managing access requests to the gated /data page.

All endpoints require the admin key (X-Admin-Key header), reusing the same
AdminKeyRequired dependency as the trigger routes.

Routes:
    GET  /admin/requests   — list all access requests (pending/approved/blocked)
    POST /admin/approve    — approve a request (generates key, emails the link)
    POST /admin/decline    — block a request
"""

from __future__ import annotations

import os
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, status as http_status
from pydantic import BaseModel

from src.middleware.auth import AdminKeyRequired
from src.db.supabase_client import access_table, ai_table, signals_table
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
            # Return the link so the admin UI can show/copy it even if email failed.
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


__all__ = ["router"]
