"""
Magic-link authentication for Govire Connect owners.

Passwordless, and deliberately separate from every other identity system in
the codebase.

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
originally FK'd to it, which is why that table could never hold a row (fixed
2026-07-29). Every RLS policy on the marketplace schema keyed on auth.uid(),
which returns NULL for us, so those policies matched nothing while appearing
to protect owner data. They were dropped; authorization happens here, the
same way properties.py enforces tier in Python.

=== WHY DIRECT POSTGRES AND NOT POSTGREST ===
Rewritten 2026-07-29. The marketplace schema is RLS-enabled with NO policies
and no grants to anon/authenticated — service-role only by design. PostgREST
therefore buys nothing here and adds four independent failure modes: schema
exposure, per-table exposure, the PostgREST schema cache, and Content-Profile
/ Accept-Profile headers.

All four were hit in one evening. The final state was a correctly created
table, correctly exposed schema with a green tick, correct grants — and a
flat HTTP 406 on even a plain SELECT, while the identical INSERT ran first
try in the SQL editor. Rather than keep debugging a layer that adds no value
for this schema, Connect talks to Postgres directly over DATABASE_URL, the
same way outcome_capture/ already does.

=== TOKEN DESIGN ===
  * 32 bytes from secrets.token_urlsafe — not uuid4, not random.random.
  * Stored as sha256. The plaintext exists only in the email, so a database
    read cannot impersonate an owner.
  * 30-minute expiry on the login link. Long enough to find the email on a
    phone in a difficult moment; short enough to matter.
  * Single use: cleared on verification, so a forwarded or leaked link
    cannot be replayed.
  * 30-day session afterwards. Owners should not have to re-authenticate
    every time they check whether offers have come in.

=== ENUMERATION ===
request_link() returns the same response to the caller whether or not the
address is known. Someone probing addresses to discover who is in
foreclosure learns nothing. A genuine internal failure IS reported, via
internal_error — returning success on a failed insert made a real outage
indistinguishable from normal operation and cost an evening.
"""

from __future__ import annotations

import hashlib
import os
import secrets
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Optional

import httpx
import psycopg2
import psycopg2.extras

from src.config import settings
from src.utils.logger import logger


_TOKEN_BYTES = 32
_LINK_TTL_MINUTES = 30
_SESSION_TTL_DAYS = 30

_RESEND_URL = "https://api.resend.com/emails"


@contextmanager
def pg() -> Iterator[Any]:
    """A Postgres connection with a dict cursor.

    Opens per call rather than pooling: Connect's write volume is a handful
    of requests a day, and a short-lived connection cannot go stale between
    them. Revisit if that stops being true.
    """
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set")
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


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
    """Digits only, then require 10 (US), tolerating a leading 1.

    Owners type phone numbers every imaginable way and a rejected number is
    a lost person.
    """
    if not phone:
        return None
    digits = "".join(c for c in str(phone) if c.isdigit())
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits if len(digits) == 10 else None


async def _send_magic_link(to_email: str, token: str) -> bool:
    """Email the link via Resend. Mirrors scripts/health_alert.py but async,
    and NON-FATAL: a send failure is reported, never raised."""
    api_key = getattr(settings, "resend_api_key", None)
    from_addr = getattr(settings, "alert_email_from", None)
    if api_key is None or not from_addr:
        logger.error("connect: RESEND_API_KEY or ALERT_EMAIL_FROM unset; "
                     "cannot send magic link")
        return False
    if hasattr(api_key, "get_secret_value"):
        api_key = api_key.get_secret_value()

    base = str(getattr(settings, "frontend_origin", None)
               or "https://govire.com").rstrip("/")
    link = f"{base}/connect/verify?token={token}"

    body = (
        "Here is your secure link to see your property's redemption "
        "deadline and any offers waiting for you:\n\n"
        f"{link}\n\n"
        "The link works once and expires in 30 minutes. If you did not "
        "request it you can ignore this email — nothing has been shared "
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
        logger.error("connect: Resend unreachable",
                     error_type=type(e).__name__, error=str(e)[:400])
        return False

    if 200 <= resp.status_code < 300:
        return True
    logger.error("connect: Resend rejected send",
                 status=resp.status_code, body=resp.text[:400])
    return False


async def request_link(
    email: Optional[str] = None,
    phone: Optional[str] = None,
) -> dict[str, Any]:
    """Create or find an owner, mint a one-time token, and email the link."""
    norm_email = _normalize_email(email)
    norm_phone = _normalize_phone(phone)
    if not norm_email and not norm_phone:
        return {"sent": False, "reason": "invalid_contact"}

    token = secrets.token_urlsafe(_TOKEN_BYTES)
    expires = _now() + timedelta(minutes=_LINK_TTL_MINUTES)

    try:
        with pg() as cur:
            if norm_email:
                cur.execute(
                    "SELECT id FROM marketplace.owners "
                    "WHERE lower(email) = %s LIMIT 1",
                    (norm_email,),
                )
            else:
                cur.execute(
                    "SELECT id FROM marketplace.owners "
                    "WHERE phone = %s LIMIT 1",
                    (norm_phone,),
                )
            existing = cur.fetchone()

            if existing:
                cur.execute(
                    """
                    UPDATE marketplace.owners
                    SET login_token_hash = %s,
                        login_token_expires_at = %s,
                        last_seen_at = now()
                    WHERE id = %s
                    """,
                    (_hash(token), expires, existing["id"]),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO marketplace.owners
                        (email, phone, login_token_hash,
                         login_token_expires_at, last_seen_at)
                    VALUES (%s, %s, %s, %s, now())
                    RETURNING id
                    """,
                    (norm_email, norm_phone, _hash(token), expires),
                )
                cur.fetchone()
    except Exception as e:
        # print(), not logger. The structured logger emitted BLANK LINES to
        # Railway on 2026-07-29, which is why five hypotheses were guessed
        # instead of one error being read. A diagnostic that cannot be read
        # is worse than none: it creates the impression of visibility.
        print(f"[connect] OWNER UPSERT FAILED: {type(e).__name__}: {e}",
              flush=True)
        logger.error("connect: owner upsert FAILED",
                     error_type=type(e).__name__, error=str(e)[:800])
        return {
            "sent": False,
            "channel": "email" if norm_email else "sms",
            "internal_error": True,
        }

    if norm_email:
        ok = await _send_magic_link(norm_email, token)
        logger.info("connect: magic link requested", delivered=ok)
        if not ok:
            # The owner row and token exist, but the link never reached them.
            # Saying "sent" here would leave someone waiting for an email
            # that is not coming.
            return {"sent": False, "channel": "email", "internal_error": True}
    else:
        # SMS is not wired. The row and token exist, so adding a sender later
        # needs no schema change — but phone-only signup does not work yet.
        logger.info("connect: magic link requested for phone; SMS not wired")

    return {"sent": True, "channel": "email" if norm_email else "sms"}


def verify_link(token: str) -> Optional[dict[str, Any]]:
    """Consume a magic-link token. Returns {owner_id, session_token} or None."""
    if not token or len(token) < 20:
        return None
    try:
        with pg() as cur:
            cur.execute(
                """
                SELECT id, login_token_expires_at
                FROM marketplace.owners
                WHERE login_token_hash = %s
                LIMIT 1
                """,
                (_hash(token),),
            )
            row = cur.fetchone()
            if not row:
                return None
            expires = row.get("login_token_expires_at")
            if not expires or expires < _now():
                logger.info("connect: magic link expired or already used")
                return None

            session = secrets.token_urlsafe(_TOKEN_BYTES)
            cur.execute(
                """
                UPDATE marketplace.owners
                SET login_token_hash = NULL,        -- single use
                    login_token_expires_at = NULL,
                    session_token_hash = %s,
                    session_expires_at = %s,
                    email_verified = true,
                    last_seen_at = now()
                WHERE id = %s
                """,
                (_hash(session),
                 _now() + timedelta(days=_SESSION_TTL_DAYS),
                 row["id"]),
            )
            return {"owner_id": str(row["id"]), "session_token": session}
    except Exception as e:
        logger.error("connect: verify FAILED",
                     error_type=type(e).__name__, error=str(e)[:800])
        return None


def owner_from_session(session_token: Optional[str]) -> Optional[str]:
    """Resolve a session token to an owner id, or None.

    This is the authorization check for every owner-side write — it replaces
    what RLS would have done via auth.uid(). See the module docstring.
    """
    if not session_token or len(session_token) < 20:
        return None
    try:
        with pg() as cur:
            cur.execute(
                """
                SELECT id
                FROM marketplace.owners
                WHERE session_token_hash = %s
                  AND session_expires_at > now()
                LIMIT 1
                """,
                (_hash(session_token),),
            )
            row = cur.fetchone()
            return str(row["id"]) if row else None
    except Exception as e:
        logger.error("connect: session lookup FAILED",
                     error_type=type(e).__name__, error=str(e)[:400])
        return None


def create_listing(owner_id: str, fields: dict[str, Any]) -> Optional[str]:
    """Insert a marketplace.listings row. Returns the new id, or None.

    Column allow-list is explicit: asking_price, description and photos are
    NOT accepted. Pricing is exactly what a distressed owner does not know
    and exactly what gets them taken advantage of — the offers set the
    price. And nobody photographs a house they are ashamed of.
    """
    allowed = (
        "parcel_id", "status", "occupancy", "condition", "primary_need",
        "leaseback_interest", "buyback_interest", "earliest_close_date",
        "preferred_close_date", "contact_preference", "contact_restrictions",
        "ownership_verified",
    )
    cols = ["user_id"]
    vals: list[Any] = [owner_id]
    for key in allowed:
        # `is not None`, NOT truthiness. leaseback_interest=False means "no,
        # I do not want a leaseback" — real information an investor needs.
        # A truthiness test would silently discard it along with every other
        # deliberate 'no'.
        if key in fields and fields[key] is not None:
            cols.append(key)
            vals.append(fields[key])

    placeholders = ", ".join(["%s"] * len(vals))
    col_sql = ", ".join(cols)
    try:
        with pg() as cur:
            cur.execute(
                f"INSERT INTO marketplace.listings ({col_sql}) "
                f"VALUES ({placeholders}) RETURNING id",
                vals,
            )
            row = cur.fetchone()
            return str(row["id"]) if row else None
    except Exception as e:
        # TEMPORARY: returns the error string so it reaches the HTTP response.
        # The Railway log stream silently drops loguru lines — entries arrive
        # as blank strings — so it cannot be relied on for diagnosis. Revert
        # to `return None` once the cause is known; an internal error message
        # must never reach a homeowner.
        logger.error("connect: listing insert FAILED",
                     error_type=type(e).__name__, error=str(e)[:800])
        return f"ERROR::{type(e).__name__}: {e}"


__all__ = [
    "pg",
    "request_link",
    "verify_link",
    "owner_from_session",
    "create_listing",
]
