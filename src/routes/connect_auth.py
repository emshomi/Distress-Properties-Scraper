"""
Magic-link authentication for Govire Connect owners.

Deliberately separate from app_auth, and deliberately passwordless.

=== WHY NOT app_auth.users ===
That table is investor-shaped: password_hash NOT NULL, tier NOT NULL,
subscription_status NOT NULL, plus failed_login_count and locked_until.
Three problems for a homeowner in foreclosure:

  * A required password is friction at exactly the wrong moment. Owners
    vanish for weeks and come back; a forgotten-password wall between them
    and offers waiting for them loses them entirely.
  * Assigning them a `tier` and `subscription_status` invites something
    downstream to gate them by it. They are the supply side, not a customer.
  * Login-lockout fields are built for repeated attempts on a paid account.
    Wrong model for someone who visits twice.

marketplace.owners is the identity instead: email or phone, a hashed
one-time token, nothing else required.

=== WHY NOT SUPABASE AUTH ===
auth.users has ZERO rows — Govire has never used it. marketplace.listings
originally FK'd to it, which is why that table could never hold a row. Every
RLS policy on the marketplace schema keyed on auth.uid(), which returns NULL
for us, so those policies matched nothing while appearing to protect owner
data. They were dropped 2026-07-29; access is enforced here instead, the
same way properties.py enforces tier in Python.

=== TOKEN DESIGN ===
  * 32 bytes from secrets.token_urlsafe — not uuid4, not random.random.
  * Stored as sha256. The plaintext exists only in the email or SMS, so a
    database read cannot impersonate an owner.
  * 30-minute expiry. Long enough for someone to find the email on a phone
    in a difficult moment; short enough to matter.
  * Single use: consumed on verification.
  * Session token issued on success, 30 days, same hashing rules. Owners
    should not have to re-authenticate to check on offers.

=== ENUMERATION ===
request_link() returns the same response whether or not the address is
known. Someone probing addresses to discover who is in foreclosure learns
nothing.
"""

from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import httpx

from src.config import settings
from src.db.supabase_client import get_client
from src.utils.logger import logger


_TOKEN_BYTES = 32
_LINK_TTL_MINUTES = 30
_SESSION_TTL_DAYS = 30

_RESEND_URL = "https://api.resend.com/emails"


def marketplace_table(table_name: str) -> Any:
    """Table handle in the marketplace schema.

    Not in supabase_client alongside core_table/outcomes_table because the
    marketplace is Connect-only and its access rules differ: RLS is enabled
    with NO policies, so the service role is the sole reader and writer, and
    every authorization decision happens in Python.
    """
    return get_client().schema("marketplace").table(table_name)


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_email(email: Optional[str]) -> Optional[str]:
    if not email:
        return None
    e = email.strip().lower()
    return e if "@" in e and len(e) >= 5 else None


def _normalize_phone(phone: Optional[str]) -> Optional[str]:
    """Digits only, then require 10 or 11 (US). Owners type phone numbers
    every imaginable way and a rejected number is a lost person."""
    if not phone:
        return None
    digits = "".join(c for c in str(phone) if c.isdigit())
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits if len(digits) == 10 else None


async def _send_magic_link(to_email: str, token: str) -> bool:
    """Email the link via Resend. Mirrors scripts/health_alert.py, but async
    and NON-FATAL: a send failure returns an error to the caller rather than
    killing the process."""
    api_key = getattr(settings, "resend_api_key", None)
    from_addr = getattr(settings, "alert_email_from", None)
    if api_key is None or not from_addr:
        logger.warning("connect: RESEND_API_KEY or ALERT_EMAIL_FROM unset; "
                       "cannot send magic link")
        return False
    if hasattr(api_key, "get_secret_value"):
        api_key = api_key.get_secret_value()

    base = str(getattr(settings, "frontend_origin", None) or "https://govire.com").rstrip("/")
    link = f"{base}/connect/verify?token={token}"

    body = (
        "Here is your secure link to see your property's redemption "
        "deadline and any offers waiting for you:\n\n"
        f"{link}\n\n"
        "The link works once and expires in 30 minutes. If you did not "
        "request it, you can ignore this email — nothing has been shared "
        "with anyone.\n\n"
        "Govire does not buy properties and takes no part of any sale."
    )

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(
                _RESEND_URL,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "from": from_addr,
                    "to": [to_email],
                    "subject": "Your Govire link",
                    "text": body,
                },
            )
    except httpx.HTTPError as e:
        logger.warning("connect: Resend unreachable", error_type=type(e).__name__)
        return False

    if 200 <= resp.status_code < 300:
        return True
    logger.warning("connect: Resend rejected send", status=resp.status_code)
    return False


async def request_link(
    email: Optional[str] = None,
    phone: Optional[str] = None,
) -> dict[str, Any]:
    """Create or find an owner, mint a one-time token, and email it.

    ALWAYS returns the same shape whether or not the contact is known.
    Revealing that an address exists would let someone probe for who is in
    foreclosure.
    """
    norm_email = _normalize_email(email)
    norm_phone = _normalize_phone(phone)
    if not norm_email and not norm_phone:
        return {"sent": False, "reason": "invalid_contact"}

    token = secrets.token_urlsafe(_TOKEN_BYTES)
    expires = _now() + timedelta(minutes=_LINK_TTL_MINUTES)

    try:
        q = marketplace_table("owners").select("id, email, phone")
        if norm_email:
            q = q.ilike("email", norm_email)
        else:
            q = q.eq("phone", norm_phone)
        existing = (q.limit(1).execute().data or [None])[0]

        if existing:
            owner_id = existing["id"]
            marketplace_table("owners").update({
                "login_token_hash": _hash(token),
                "login_token_expires_at": expires.isoformat(),
                "last_seen_at": _now().isoformat(),
            }).eq("id", owner_id).execute()
        else:
            row = {
                "email": norm_email,
                "phone": norm_phone,
                "login_token_hash": _hash(token),
                "login_token_expires_at": expires.isoformat(),
                "last_seen_at": _now().isoformat(),
            }
            created = marketplace_table("owners").insert(row).execute()
            owner_id = (created.data or [{}])[0].get("id")
    except Exception as e:
        logger.warning("connect: owner upsert failed",
                       error_type=type(e).__name__)
        # Same response as success — never leak that something went wrong
        # for a specific address.
        return {"sent": True, "channel": "email" if norm_email else "sms"}

    if norm_email:
        ok = await _send_magic_link(norm_email, token)
        logger.info("connect: magic link requested", delivered=ok)
    else:
        # SMS not wired yet. The token exists and the owner row is created,
        # so adding a sender later needs no schema change.
        logger.info("connect: magic link requested for phone; SMS not wired")

    return {"sent": True, "channel": "email" if norm_email else "sms"}


def verify_link(token: str) -> Optional[dict[str, Any]]:
    """Consume a magic-link token. Returns {owner_id, session_token} or None.

    Single use: the login token is cleared on success, so a link in an email
    that is later forwarded or leaked cannot be replayed.
    """
    if not token or len(token) < 20:
        return None
    try:
        res = (
            marketplace_table("owners")
            .select("id, login_token_expires_at")
            .eq("login_token_hash", _hash(token))
            .limit(1)
            .execute()
        )
        row = (res.data or [None])[0]
    except Exception as e:
        logger.warning("connect: token lookup failed",
                       error_type=type(e).__name__)
        return None

    if not row:
        return None

    expires_raw = row.get("login_token_expires_at")
    if not expires_raw:
        return None
    try:
        expires = datetime.fromisoformat(str(expires_raw).replace("Z", "+00:00"))
    except ValueError:
        return None
    if expires < _now():
        logger.info("connect: magic link expired")
        return None

    session = secrets.token_urlsafe(_TOKEN_BYTES)
    try:
        marketplace_table("owners").update({
            "login_token_hash": None,          # single use
            "login_token_expires_at": None,
            "session_token_hash": _hash(session),
            "session_expires_at": (_now() + timedelta(days=_SESSION_TTL_DAYS)).isoformat(),
            "email_verified": True,
            "last_seen_at": _now().isoformat(),
        }).eq("id", row["id"]).execute()
    except Exception as e:
        logger.warning("connect: session issue failed",
                       error_type=type(e).__name__)
        return None

    return {"owner_id": row["id"], "session_token": session}


def owner_from_session(session_token: Optional[str]) -> Optional[str]:
    """Resolve a session token to an owner id, or None.

    This is the authorization check for every owner-side write. It replaces
    what RLS would have done via auth.uid() — see the module docstring.
    """
    if not session_token or len(session_token) < 20:
        return None
    try:
        res = (
            marketplace_table("owners")
            .select("id, session_expires_at")
            .eq("session_token_hash", _hash(session_token))
            .limit(1)
            .execute()
        )
        row = (res.data or [None])[0]
    except Exception as e:
        logger.warning("connect: session lookup failed",
                       error_type=type(e).__name__)
        return None
    if not row or not row.get("session_expires_at"):
        return None
    try:
        expires = datetime.fromisoformat(
            str(row["session_expires_at"]).replace("Z", "+00:00"))
    except ValueError:
        return None
    return row["id"] if expires >= _now() else None


__all__ = [
    "marketplace_table",
    "request_link",
    "verify_link",
    "owner_from_session",
]
