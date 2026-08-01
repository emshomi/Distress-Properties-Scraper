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
from urllib.parse import quote
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


async def _resend_send(to_email: str, subject: str, text: str,
                       context: str) -> bool:
    """POST one email to Resend. Returns True on a 2xx, False on anything else.

    NEVER raises. Every caller runs after the thing the owner actually asked
    for has already succeeded — a link row is written, a listing is saved —
    so a mail failure must degrade to a logged False, not an exception that
    turns a completed action into an error the owner sees.

    Shared rather than copied. The owner classifier lived in five files and
    had drifted from the requirement in all of them; this is the second
    caller and there will be a third, so it gets factored now while there is
    still only one correct version to preserve.

    `context` appears in the logs so a failure can be traced to which kind of
    email failed — Railway drops loguru lines intermittently and a generic
    "Resend rejected" line would be unattributable.
    """
    api_key = getattr(settings, "resend_api_key", None)
    from_addr = getattr(settings, "alert_email_from", None)
    if api_key is None or not from_addr:
        logger.error("connect: RESEND_API_KEY or ALERT_EMAIL_FROM unset; "
                     "cannot send email", context=context)
        return False
    if hasattr(api_key, "get_secret_value"):
        api_key = api_key.get_secret_value()

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
                    "subject": subject,
                    "text": text,
                },
            )
    except httpx.HTTPError as e:
        logger.error("connect: Resend unreachable", context=context,
                     error_type=type(e).__name__, error=str(e)[:400])
        return False

    if 200 <= resp.status_code < 300:
        return True
    logger.error("connect: Resend rejected send", context=context,
                 status=resp.status_code, body=resp.text[:400])
    return False


async def _send_magic_link(
    to_email: str, token: str, next_path: Optional[str] = None
) -> bool:
    """Email the link via Resend. NON-FATAL: a send failure is reported,
    never raised."""
    base = str(getattr(settings, "frontend_origin", None)
               or "https://govire.com").rstrip("/")
    # `next` carries the owner back to where they were. Without it the verify
    # page defaults to /connect and someone who had just filled in the offers
    # form landed on the calculator instead — their answers intact in
    # sessionStorage but invisible, with no obvious way back.
    nxt = f"&next={quote(next_path, safe='/')}" if next_path else ""
    link = f"{base}/connect/verify?token={token}{nxt}"

    body = (
        "Here is your secure link to see your property's redemption "
        "deadline and any offers waiting for you:\n\n"
        f"{link}\n\n"
        "The link works once and expires in 30 minutes. If you did not "
        "request it you can ignore this email — nothing has been shared "
        "with anyone.\n\n"
        "Govire does not buy properties and takes no part of any sale."
    )

    return await _resend_send(to_email, "Your Govire link", body,
                              context="magic_link")


async def send_listing_confirmation(
    owner_id: str,
    address: Optional[str] = None,
    city: Optional[str] = None,
) -> bool:
    """Tell an owner their property is on file. Returns True if sent.

    NON-FATAL and silent on absence. By the time this runs create_listing has
    already returned an id, so the listing exists whatever happens here. An
    owner with no email address is a SKIP, not an error: `email` is nullable
    and phone signup writes a row that SMS cannot yet deliver to, so a
    phone-only owner is reachable by nothing and must not raise.

    Deliberately does NOT say "we will email you when an offer arrives".
    Nothing sends that yet. /connect/me already makes that promise on screen
    and it is the next thing to build — repeating it here would double the
    number of places we owe someone a message we cannot send.

    The subject line names no distress. An email subject is visible on a lock
    screen to whoever is standing there, and an owner in foreclosure has not
    necessarily told the people around them.
    """
    try:
        with pg() as cur:
            cur.execute(
                "SELECT email FROM marketplace.owners WHERE id = %s LIMIT 1",
                (owner_id,),
            )
            row = cur.fetchone()
    except Exception as e:
        logger.error("connect: confirmation email — owner lookup FAILED",
                     error_type=type(e).__name__, error=str(e)[:400])
        return False

    to_email = _normalize_email(row["email"]) if row else None
    if not to_email:
        # Not an error worth alarming on: this is the phone-only case.
        logger.info("connect: confirmation email skipped, no address on owner")
        return False

    base = str(getattr(settings, "frontend_origin", None)
               or "https://govire.com").rstrip("/")

    # Worded to be true whether this was a first submission or an edit. The
    # route upserts and cannot cheaply tell the difference, and guessing
    # wrong in an email is worse than in a heading — the owner cannot see the
    # screen that would have corrected it.
    where = " · ".join(p for p in (address, city) if p)
    property_line = f"Property: {where}\n\n" if where else ""

    body = (
        "Your property is on file with Govire.\n\n"
        f"{property_line}"
        "What this means: verified buyers can see that a property is "
        "available and make an offer on it. Nobody has your address, your "
        "name or your contact details, and nobody gets them unless you "
        "choose to open an offer.\n\n"
        "You are not committed to anything. You can change your answers or "
        "withdraw at any time here:\n\n"
        f"{base}/connect/me\n\n"
        "Govire does not buy properties, is not your agent, and takes no "
        "part of any sale."
    )

    return await _resend_send(
        to_email, "Your property is on file with Govire", body,
        context="listing_confirmation",
    )


async def request_link(
    email: Optional[str] = None,
    phone: Optional[str] = None,
    next_path: Optional[str] = None,
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
        ok = await _send_magic_link(norm_email, token, next_path)
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
    """Create OR UPDATE the owner's active listing on a parcel. Returns the
    id, or None on failure.

    UPSERT, not a plain insert. There is no separate edit screen: an owner
    whose circumstances change — the tenant moves out, they decide a
    leaseback would work after all — has the offers form and nothing else.
    Before this, resubmitting produced a SECOND active listing that
    contradicted the first, and an investor had no way to tell which was
    current. Two such rows existed on parcel 0911821120148 on 2026-07-30.

    The partial unique index `listings_one_active_per_owner_parcel`
    (parcel_id, user_id) WHERE status = 'active' is what makes the conflict
    target work. The WHERE clause below must match that predicate exactly or
    Postgres cannot infer the index and raises. Partial by design: a
    withdrawn listing from six months ago must not block an owner from
    raising their hand again.

    Only the columns actually supplied are updated. An omitted field keeps
    its previous value rather than being nulled — otherwise a half-completed
    resubmission would silently erase answers the owner gave earlier.

    Column allow-list is explicit: asking_price, description and photos are
    NOT accepted. Pricing is exactly what a distressed owner does not know
    and exactly what gets them taken advantage of — the offers set the
    price. And nobody photographs a house they are ashamed of.
    """
    allowed = (
        "parcel_id", "status", "occupancy", "condition", "primary_need",
        "leaseback_interest", "buyback_interest", "earliest_close_date",
        "preferred_close_date", "contact_preference", "contact_restrictions",
        # Whether a buyer may see the house, and on what terms. Investors
        # who cannot view a property either discount heavily or walk, so an
        # owner willing to allow access is worth materially more to them —
        # and had no way to say so. Kept separate from occupancy: a vacant
        # house can take a lockbox, a tenant-occupied one cannot guarantee
        # entry, and an owner living there may need notice.
        "viewing_access",
        # The assessed value AS IT STOOD when the owner raised their hand,
        # plus which of core.parcels' two value columns it came from and when
        # it was read. Captured rather than joined at read time: a listing is
        # a record of what was represented, so a later reassessment or loader
        # fix must not silently restate it, and an outstate listing must not
        # spontaneously acquire a valuation the day its county is onboarded.
        "assessed_value_at_listing",
        "assessed_value_source",
        "assessed_value_captured_at",
        # The evidence behind the ownership verdict: what the owner typed,
        # what it was compared against, the basis of the comparison, and when.
        # A single mutating flag collapsed five different situations into
        # 'manual_review' — most importantly it could not distinguish "this
        # name did not match" from "this county publishes no owner data at
        # all", which is true of Dakota and Washington (286,074 parcels, zero
        # rows in core.owners) and of every outstate synthetic.
        "owner_name_submitted",
        "owner_name_on_record",
        "ownership_check_basis",
        "ownership_checked_at",
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

    # The conflict target columns are never self-assigned in the SET clause.
    updatable = [c for c in cols if c not in ("user_id", "parcel_id")]
    set_sql = ", ".join(f"{c} = EXCLUDED.{c}" for c in updatable)
    set_sql = f"{set_sql}, updated_at = now()" if set_sql else "updated_at = now()"

    placeholders = ", ".join(["%s"] * len(vals))
    col_sql = ", ".join(cols)
    try:
        with pg() as cur:
            cur.execute(
                f"INSERT INTO marketplace.listings ({col_sql}) "
                f"VALUES ({placeholders}) "
                f"ON CONFLICT (parcel_id, user_id) WHERE status = 'active' "
                f"DO UPDATE SET {set_sql} "
                f"RETURNING id",
                vals,
            )
            row = cur.fetchone()
            return str(row["id"]) if row else None
    except Exception as e:
        # print(), not just logger. The structured logger emitted BLANK LINES
        # to Railway on 2026-07-29, so it cannot be the only diagnostic path.
        print(f"[connect] LISTING UPSERT FAILED: {type(e).__name__}: {e}",
              flush=True)
        logger.error("connect: listing upsert FAILED",
                     error_type=type(e).__name__, error=str(e)[:800])
        # Returns None, never the exception text. An earlier version returned
        # f"ERROR::{...}" so the reason would reach the browser while Railway
        # logs were unreadable — but the caller only tests `is None`, so that
        # string passed as a valid listing_id and the owner was told "You are
        # on file" when nothing had been saved.
        return None


def get_active_listing(owner_id: str, parcel_id: str) -> Optional[dict[str, Any]]:
    """The owner's current active listing on a parcel, or None.

    Feeds the pre-filled offers form. An owner returning weeks later should
    see what buyers are currently being told about their home and change
    what they want to change — not face a blank form whose submission
    silently overwrites answers they no longer remember giving.

    Deliberately scoped to (owner_id, parcel_id): an owner can only ever
    read back their own listing, so a guessed parcel_id reveals nothing.
    """
    try:
        with pg() as cur:
            cur.execute(
                """
                SELECT id, parcel_id, occupancy, condition, primary_need,
                       leaseback_interest, buyback_interest,
                       earliest_close_date, preferred_close_date,
                       contact_preference, contact_restrictions,
                       viewing_access,
                       assessed_value_at_listing, assessed_value_source,
                       assessed_value_captured_at,
                       owner_name_submitted, owner_name_on_record,
                       ownership_check_basis, ownership_checked_at,
                       ownership_verified, status, created_at, updated_at
                FROM marketplace.listings
                WHERE user_id = %s AND parcel_id = %s AND status = 'active'
                LIMIT 1
                """,
                (owner_id, parcel_id),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    except Exception as e:
        print(f"[connect] ACTIVE LISTING READ FAILED: {type(e).__name__}: {e}",
              flush=True)
        logger.error("connect: active listing read FAILED",
                     error_type=type(e).__name__, error=str(e)[:800])
        return None


def get_owner_dashboard(owner_id: str) -> dict[str, Any]:
    """Everything an owner can see about their own properties.

    This is what makes the hand-raise more than a dead letterbox. Until this
    existed an owner submitted a listing and had no way back in — no page
    showing where things stood, no way to know whether an offer had arrived.
    Someone months from losing their home will not keep checking a website on
    faith.

    PLURAL by design. One listing each is the common case today, but nothing
    stops an owner having two distressed properties, and a page built around
    a single row would silently hide the second.

    Withdrawn and expired listings are returned too, not just active ones. An
    owner who withdrew needs to see that it happened and that nothing is
    being shown to buyers on their behalf.

    The offer count is returned even when it is zero: 'no offers yet' is
    information, and hiding the section entirely reads as a broken page.

    A single round trip. Three sequential queries on a page an anxious person
    reloads is a page that feels broken.
    """
    try:
        with pg() as cur:
            cur.execute(
                """
                SELECT l.id,
                       l.parcel_id,
                       l.status,
                       l.occupancy,
                       l.condition,
                       l.primary_need,
                       l.leaseback_interest,
                       l.buyback_interest,
                       l.viewing_access,
                       l.contact_restrictions,
                       l.ownership_verified,
                       l.assessed_value_at_listing,
                       l.assessed_value_source,
                       l.assessed_value_captured_at,
                       l.ownership_check_basis,
                       l.ownership_checked_at,
                       l.created_at,
                       l.updated_at,
                       p.address,
                       p.city,
                       p.county_code,
                       r.redemption_expiry_date,
                       r.days_remaining,
                       r.period_source,
                       r.anchor_date,
                       r.anchor_type,
                       r.outcome,
                       (SELECT COUNT(*) FROM marketplace.offers o
                         WHERE o.listing_id = l.id) AS offer_count
                FROM marketplace.listings l
                LEFT JOIN core.parcels p
                       ON p.parcel_id = l.parcel_id
                LEFT JOIN outcomes.redemption_current r
                       ON r.parcel_id = l.parcel_id
                      AND r.county_code = p.county_code
                WHERE l.user_id = %s
                ORDER BY l.created_at DESC
                """,
                (owner_id,),
            )
            rows = [dict(r) for r in (cur.fetchall() or [])]
    except Exception as e:
        # print(), not just logger — the structured logger emitted BLANK LINES
        # to Railway on 2026-07-29, so it cannot be the only diagnostic path.
        print(f"[connect] OWNER DASHBOARD READ FAILED: {type(e).__name__}: {e}",
              flush=True)
        logger.error("connect: owner dashboard read FAILED",
                     error_type=type(e).__name__, error=str(e)[:800])
        # Raise rather than returning an empty list. An owner shown "you have
        # no properties" when the query failed would reasonably conclude their
        # listing had been deleted.
        raise

    return {"listings": rows, "count": len(rows)}


def withdraw_listing(owner_id: str, listing_id: str) -> Optional[dict[str, Any]]:
    """Stop showing a listing to buyers. Returns the updated row, or None.

    The offers form has always told owners they can withdraw at any time, and
    until now nothing anywhere could do it — a promise the product did not
    keep. For someone who has been contacted by people wanting to buy their
    house cheaply, being unable to take their own information back is exactly
    the trap they were afraid of.

    Scoped to owner_id AND listing_id, so a guessed or copied id cannot
    withdraw someone else's property.

    Only acts on rows that are currently 'active'. Withdrawing something
    already withdrawn returns None rather than pretending to do work, and the
    endpoint reads that as "nothing to do" rather than an error.

    'withdrawn' rather than deletion: the record of what was submitted and
    when it stopped being shown is worth keeping, and the partial unique index
    listings_one_active_per_owner_parcel only covers status = 'active', so a
    withdrawn row does not block the same owner raising their hand on that
    property again later.
    """
    try:
        with pg() as cur:
            cur.execute(
                """
                UPDATE marketplace.listings
                SET status = 'withdrawn',
                    status_changed_at = now(),
                    updated_at = now()
                WHERE id = %s
                  AND user_id = %s
                  AND status = 'active'
                RETURNING id, parcel_id, status, status_changed_at
                """,
                (listing_id, owner_id),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    except Exception as e:
        # print(), not just logger — the structured logger emitted BLANK LINES
        # to Railway on 2026-07-29, so it cannot be the only diagnostic path.
        print(f"[connect] WITHDRAW FAILED: {type(e).__name__}: {e}", flush=True)
        logger.error("connect: withdraw FAILED",
                     error_type=type(e).__name__, error=str(e)[:800])
        # Raise rather than returning None. None means "nothing to withdraw",
        # and an owner told their listing was withdrawn when it is still live
        # would be a worse failure than an error message.
        raise


__all__ = [
    "pg",
    "request_link",
    "verify_link",
    "owner_from_session",
    "create_listing",
    "get_active_listing",
    "get_owner_dashboard",
    "withdraw_listing",
    "send_listing_confirmation",
]
