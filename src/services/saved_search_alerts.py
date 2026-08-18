"""
Saved-search alerts — the daily digest that makes a buy box worth having.

=== WHY THIS IS THE PRODUCT ===
A saved search that you have to remember to open is a bookmark. A saved
search that emails you the morning a property starts matching is a reason to
subscribe. Our scrapers run daily; the competing products ship a monthly
export, so "three new properties matched your buy box overnight" is a
sentence they structurally cannot write.

=== last_alerted_at IS NOT last_viewed_at ===
Two separate marks, deliberately:

  last_viewed_at   moved when the USER opens the search. Drives the "N new"
                   badge in the UI.
  last_alerted_at  moved when WE send an email. Drives what goes in the next
                   digest.

Sending must never clear the badge. Someone who gets a 7am digest and opens
the app at noon should still see what was new — if the email moved
last_viewed_at, the badge would be gone before they ever looked.

=== THE COUNT COMES FROM /properties ===
Same reasoning as the badge (see saved_searches.py): the digest calls
list_properties() rather than reimplementing the filters, so an email can
never claim a number the page then contradicts.

=== FAILURE IS PER-SEARCH, NEVER PER-RUN ===
One malformed filter set must not stop every other subscriber's digest. Each
search is counted and sent inside its own try, and a failure is logged and
skipped.

=== _process_one RETURNS A REASON, NOT A BOOL (2026-08-18) ===
It used to return True/False, so THREE different outcomes reported
identically as sent=0, failed=0:

    no_email      the joined user row carries no address
    no_matches    nothing new since last_alerted_at -- correct, no email
    send_failed   Resend rejected it

Measured 2026-08-18: the 14:00 run logged considered=2 due=2 sent=0
failed=0, and establishing which of the three had happened took a query
against signals.distress_events to prove no Hennepin sheriff_sale event
existed after 2026-08-15. The instrument could not answer its own question.

send_failed is now counted in `failed`, not merely absent from `sent`. It is
a soft failure -- resend_send returns falsy rather than raising, and
last_alerted_at deliberately does NOT advance so the matches are retried --
but a digest silently failing to send for a week while matches accumulate is
the exact failure this file exists to prevent. The retry means a transient
rejection self-heals, so a count that briefly reads 1 costs a second glance.
Same trade as UNHEALTHY_THRESHOLD = 1 in source_health_tracker.py.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from src.config import settings
from src.db.supabase_client import saved_table
from src.middleware.tier import TierContext
from src.routes.saved_searches import _count_matches
from src.services.email import resend_send
from src.utils.logger import logger


# A weekly digest should not fire six days early because the job happens to
# run daily. These are minimum ages, checked against last_alerted_at.
_MIN_INTERVAL = {
    "daily": timedelta(hours=20),
    "weekly": timedelta(days=6, hours=20),
}


def _is_due(frequency: str, last_alerted_at: Optional[str]) -> bool:
    """Has enough time passed since the last digest for this search?

    A search that has NEVER been alerted is due immediately — but see the
    caller: a brand-new search with no new matches sends nothing anyway,
    because the digest is about what CHANGED.
    """
    interval = _MIN_INTERVAL.get(frequency)
    if interval is None:          # 'never', or an unknown value
        return False
    if not last_alerted_at:
        return True
    try:
        prior = datetime.fromisoformat(
            str(last_alerted_at).replace("Z", "+00:00")
        )
    except ValueError:
        # An unparseable timestamp must not wedge a search permanently
        # unalerted; treat it as due and let the send overwrite it.
        return True
    if prior.tzinfo is None:
        prior = prior.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - prior >= interval


def _digest_body(name: str, count: int, base_url: str) -> str:
    """Plain text. Short on purpose.

    The email's whole job is to get someone to open the app; it is not the
    place to reproduce the table. It says what changed, for which buy box,
    and links to it.
    """
    noun = "property" if count == 1 else "properties"
    verb = "matches" if count == 1 else "match"
    return (
        f"{count} new {noun} {verb} your saved search \"{name}\".\n\n"
        f"See them: {base_url}/data\n\n"
        "---\n"
        "You are getting this because you asked govire to tell you when new "
        "properties match this search. Change or turn off alerts from the "
        "search itself on the data page.\n"
    )


async def _process_one(row: dict[str, Any], base_url: str) -> str:
    """Count, send and mark one saved search.

    Returns the OUTCOME, not a bool: one of "sent", "no_email",
    "no_matches", "send_failed". The caller counts each separately --
    see the module docstring for why a bare bool was not enough.

    Never raises — the caller runs this for every subscriber and one bad row
    must not end the run.
    """
    search_id = row.get("id")
    name = (row.get("name") or "your search").strip()
    email = (row.get("email") or "").strip()
    if not email:
        # No address on the joined user row. Nothing to do, and nothing worth
        # alarming about: the account exists, it simply cannot be mailed.
        return "no_email"

    # A digest is about what changed SINCE THE LAST DIGEST, so the baseline is
    # last_alerted_at — not last_viewed_at, which belongs to the badge.
    since = row.get("last_alerted_at") or row.get("created_at")

    # The alert is composed for the OWNER of the search, so the count must be
    # computed at their tier — a free-tier user must not be told about rows
    # they cannot see. user_id is set so anything downstream that needs an
    # identity has one.
    ctx = TierContext(
        tier=(row.get("tier") or "free"),
        is_admin=False,
        user_id=row.get("user_id"),
    )

    new_count = await _count_matches(
        ctx,
        row.get("filters") or {},
        row.get("categories") or [],
        since,
    )
    if not new_count:
        # None (count failed) or 0 (nothing new). Neither is worth an email,
        # and an empty digest is how a useful alert becomes spam.
        return "no_matches"

    sent = await resend_send(
        email,
        f"{new_count} new {'match' if new_count == 1 else 'matches'} — {name}",
        _digest_body(name, new_count, base_url),
        context="saved_search_digest",
    )
    if not sent:
        # Do NOT advance last_alerted_at on a failed send, or these matches
        # are never mentioned again -- they are retried on the next run.
        return "send_failed"

    try:
        saved_table("searches").update({
            "last_alerted_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", search_id).execute()
    except Exception as e:
        # The email went. Failing to record that means the next run sends a
        # duplicate, which is annoying but not harmful — and far better than
        # raising after a successful send.
        logger.warning(
            "saved search alert sent but mark failed",
            search_id=search_id,
            error_type=type(e).__name__,
        )
    return "sent"


async def run_saved_search_alerts() -> dict[str, int]:
    """Send every digest that is due. Returns a small summary for the logs."""
    base_url = str(
        getattr(settings, "frontend_origin", None) or "https://govire.com"
    ).rstrip("/")

    # Reads saved.searches_for_alerts, a VIEW that joins app_auth.users
    # server-side. Deliberate: app_auth is NOT in PostgREST's exposed schemas
    # and should not be — a view keeps the account table unreachable from the
    # API while still giving this job the address to mail.
    #
    # The view also enforces is_active AND email_verified, so the job cannot
    # forget to. Measured 2026-08-11: 4 of 6 accounts are verified, so the
    # flag IS wired up and the condition is meaningful rather than a filter
    # that would silently block every send.
    try:
        result = (
            saved_table("searches_for_alerts")
            .select("*")
            .execute()
        )
        rows = result.data or []
    except Exception as e:
        logger.error(
            "saved search alerts: could not load searches",
            error_type=type(e).__name__,
            error=str(e)[:300],
        )
        return {"considered": 0, "due": 0, "sent": 0, "failed": 0}

    considered = len(rows)
    due = 0
    # Every due search lands in exactly one of these, so they sum to `due`
    # and no outcome is invisible. A bucket that does not sum is how
    # ramsey_parcels reported 57,500 failures the digest could not reach
    # (see source_health_tracker.record_partial) -- the same shape.
    outcomes = {"sent": 0, "no_email": 0, "no_matches": 0,
                "send_failed": 0, "error": 0}

    for row in rows:
        if not _is_due(
            row.get("alert_frequency") or "never", row.get("last_alerted_at")
        ):
            continue
        due += 1
        try:
            outcome = await _process_one(row, base_url)
        except Exception as e:
            outcome = "error"
            logger.warning(
                "saved search alert failed",
                search_id=row.get("id"),
                error_type=type(e).__name__,
                error=str(e)[:300],
            )
        outcomes[outcome] = outcomes.get(outcome, 0) + 1

    # `failed` keeps its name and its place in the log line -- cron.py reads
    # it -- but now means "a digest that SHOULD have gone and did not",
    # which is send_failed plus a raised exception. no_matches is not a
    # failure; it is the correct outcome when nothing changed.
    failed = outcomes["send_failed"] + outcomes["error"]

    summary = {
        "considered": considered,
        "due": due,
        "sent": outcomes["sent"],
        "failed": failed,
        "no_matches": outcomes["no_matches"],
        "no_email": outcomes["no_email"],
        "send_failed": outcomes["send_failed"],
        "errored": outcomes["error"],
    }

    # Self-check: if these ever stop summing, a new return path was added to
    # _process_one and is landing in no bucket at all.
    counted = sum(outcomes.values())
    if counted != due:
        logger.error(
            "saved search alert outcomes do not sum to due",
            due=due,
            counted=counted,
            **outcomes,
        )

    logger.info("saved search alerts complete", **summary)
    return summary


def run_saved_search_alerts_blocking() -> dict[str, int]:
    """Entry point for the scheduler's worker thread.

    Own event loop via asyncio.run, for the reason cron.py's header explains
    at length: awaiting this on the API's loop would pin uvicorn for the
    duration, and the write path underneath is synchronous throughout.
    """
    return asyncio.run(run_saved_search_alerts())


__all__ = ["run_saved_search_alerts", "run_saved_search_alerts_blocking"]
