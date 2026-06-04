"""
Access-gate endpoints for the gated /data page.

The /data page on the frontend is gated: a visitor must hold a valid
access key to view the property data. Keys live in access.access_requests
(Supabase). Flow:

    1. Visitor submits the request form  -> POST /access/request
       -> writes a 'pending' row. Owner reviews in Supabase and sets
          status='approved', which auto-generates access_key (DB trigger).
    2. Owner sends them govire.com/data?key=<access_key>.
    3. Frontend calls GET /access/check?key=... on load to confirm the key
       is valid (status='approved'); the data endpoints also re-check it.

Routes:
    POST /access/request   — submit an access request (public)
    GET  /access/check     — validate an access key (public)
"""

from __future__ import annotations

import re
from typing import Any, Optional

from fastapi import APIRouter, status as http_status
from fastapi import Query
from pydantic import BaseModel, Field, field_validator

from src.db.supabase_client import access_table
from src.utils.errors import success_envelope
from src.utils.logger import logger


router = APIRouter(prefix="/access", tags=["access"])

# Simple, permissive email shape check (avoids a dependency on
# pydantic's EmailStr / email-validator package).
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


# ============================================================
# Request model — mirrors the frontend form fields
# ============================================================


class AccessRequestIn(BaseModel):
    """Payload from the /data request form. Only email is required."""

    email: str = Field(..., max_length=320)
    name: Optional[str] = Field(default=None, max_length=200)
    role: Optional[str] = Field(default=None, max_length=50)
    phone: Optional[str] = Field(default=None, max_length=50)
    company: Optional[str] = Field(default=None, max_length=200)
    how_heard: Optional[str] = Field(default=None, max_length=500)
    reason: Optional[str] = Field(default=None, max_length=2000)

    @field_validator("email")
    @classmethod
    def _validate_email(cls, v: str) -> str:
        v = (v or "").strip()
        if not _EMAIL_RE.match(v):
            raise ValueError("invalid email address")
        return v


# ============================================================
# POST /access/request — submit an access request
# ============================================================


@router.post(
    "/request",
    status_code=http_status.HTTP_200_OK,
    summary="Submit a request for access to the data page.",
)
async def submit_access_request(payload: AccessRequestIn) -> dict[str, Any]:
    """Write a pending access request. Idempotent on email: if the email
    already has a row, leave it untouched (don't reset an approved user back
    to pending) and report its current status."""
    email = payload.email.strip().lower()

    try:
        existing = (
            access_table("access_requests")
            .select("id, status")
            .eq("email", email)
            .limit(1)
            .execute()
        )
        if existing.data:
            current = existing.data[0].get("status")
            return success_envelope({
                "received": True,
                "already_existed": True,
                "status": current,
            })

        access_table("access_requests").insert({
            "email": email,
            "name": payload.name,
            "role": payload.role,
            "phone": payload.phone,
            "company": payload.company,
            "how_heard": payload.how_heard,
            "reason": payload.reason,
        }).execute()

        logger.info("access request submitted", email=email)
        return success_envelope({
            "received": True,
            "already_existed": False,
            "status": "pending",
        })

    except Exception as e:
        logger.exception(
            "access request submission failed",
            error_type=type(e).__name__,
        )
        return success_envelope({
            "received": False,
            "error": "Could not submit request. Please try again.",
        })


# ============================================================
# GET /access/check — validate an access key
# ============================================================


@router.get(
    "/check",
    status_code=http_status.HTTP_200_OK,
    summary="Check whether an access key is valid (approved).",
)
async def check_access_key(
    key: str = Query(..., min_length=1, max_length=200),
) -> dict[str, Any]:
    """Return {valid: true} if the key belongs to an approved row, else
    {valid: false}. On a valid check, stamp last_seen_at (activity)."""
    try:
        result = (
            access_table("access_requests")
            .select("id, status")
            .eq("access_key", key)
            .limit(1)
            .execute()
        )
        rows = result.data or []
        valid = bool(rows) and rows[0].get("status") == "approved"

        if valid:
            from datetime import datetime, timezone
            try:
                access_table("access_requests").update(
                    {"last_seen_at": datetime.now(timezone.utc).isoformat()}
                ).eq("id", rows[0]["id"]).execute()
            except Exception as e:
                logger.warning(
                    "last_seen_at stamp failed",
                    error_type=type(e).__name__,
                )

        return success_envelope({"valid": valid})

    except Exception as e:
        logger.exception(
            "access key check failed",
            error_type=type(e).__name__,
        )
        return success_envelope({"valid": False})


__all__ = ["router"]
