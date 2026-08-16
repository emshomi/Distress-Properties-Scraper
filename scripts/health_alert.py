"""
health_alert.py
================================================================================
Daily source-health digest for the Govire cloud scrapers.

Reads audit.source_health and emails a digest via Resend: which sources are
broken, which are stale, and an all-clear count when everything is fine.

WHY THIS EXISTS / WHY IT DOESN'T TRUST is_healthy
-------------------------------------------------
The is_healthy flag in source_health is currently unreliable -- sources that
404 or fail every write can still show is_healthy=true, because failures inside
fetch/parse/write get caught locally and never reach record_failure(). So this
alert deliberately IGNORES is_healthy and judges each source on RAW fields we
can trust:

  BROKEN if ANY of:
    - last_successful_run_at IS NULL            (never once succeeded)
    - consecutive_failures > 0                  (actively failing)
    - notes contains an error signature         (404, failed, error, timeout,
      unavailable, unexpected status, invalid)
    - last success older than its own cadence   (expected_interval_days * 1.5)

  HEALTHY otherwise.

STALE REQUIRES A CADENCE (2026-08-16)
-------------------------------------
A source with no expected_interval_days is not late — it has nothing to be
late against. 52 of the 79 rows are MnGeo county parcel loaders, and
.github/workflows/mngeo-parcels.yml is workflow_dispatch ONLY, deliberately:
"A schedule would start re-loading counties that have not been verified
once." They run when dispatched. NULL is correct data, not missing data.

Judging them against a flat 3-day default produced 57 of the 58 STALE lines
in the 2026-08-15 digest, all reading "no cadence set; default 3d". A digest
that cries wolf 57 times a morning is a digest nobody reads — which is how
parcel_enrich_mngeo hung three nights running while listed as HEALTHY.

Those rows are now reported as UNSCHEDULED: counted, named, and excluded
from staleness. They are NOT hidden. Suppressing them silently would repeat
the ramsey_sheriff failure, where a source nobody looked at sat wrong for
ten weeks.

THE RISK THIS CREATES, AND THE GUARD FOR IT
A NEW scraper with a real cron but no expected_interval_days would land in
UNSCHEDULED and never be watched. So any unscheduled source whose last
success is older than _UNSCHEDULED_REVIEW_DAYS is listed separately with
its age. A dispatch-only loader drifting past a quarter is worth a look;
a scheduled source that landed here by mistake shows up the same way.

This is intentionally conservative: it would rather flag a borderline source
than let a silent failure hide (the exact thing that let scrapers rot for weeks).

SELF-CONTAINED: talks to Supabase over the REST API with httpx and to Resend
over its HTTP API. No app code imported, so a bug in the app can't hide a
failure here. Reads config from environment variables only.

ENV VARS (set as GitHub Actions repo secrets)
  SUPABASE_URL                 https://<ref>.supabase.co
  SUPABASE_SERVICE_ROLE_KEY    service_role key (bypasses RLS for audit schema)
  RESEND_API_KEY               Resend API key
  ALERT_EMAIL_TO               where the digest is sent
  ALERT_EMAIL_FROM             verified Resend sender (e.g. noreply@govire.com)

HEALTH_STALE_DAYS is NO LONGER READ (2026-08-16). It was the flat fallback
for a missing cadence, and there is no longer a fallback: staleness comes
from the row's own expected_interval_days or it is not assessed. The
variable is still set in .github/workflows/health-alert.yml and can be
removed from there; leaving it does nothing.

Exit code is always 0 on a completed run (a broken *scraper* is not a failure
of this *alert*). It exits non-zero only if it cannot reach Supabase or Resend,
so the Actions run itself goes red and you notice the monitor is down.
================================================================================
"""

from __future__ import annotations

import os
import re
import sys
from datetime import datetime, timezone

import httpx


# Substrings that, if present in notes, indicate a real problem even when the
# source is flagged healthy. Lowercased comparison. Deliberately does NOT
# include the bare word "failed" -- a note like "500 of 163880 records failed"
# is a SUCCESSFUL run with a tiny fractional drop, not a break. Total write
# failures ("all N ... failed") ARE caught, via the explicit "all " check in
# _classify below.
_ERROR_SIGNATURES = (
    "404", "not found", "timeout", "unavailable",
    "unexpected status", "invalid url", "status 302", "returned status",
    "connecttimeout", "sourceunavailable",
)

# If notes match "<N> of <M> records failed" and N is at or below this fraction
# of M, treat it as a healthy run with minor drops rather than a break.
_MINOR_DROP_FRACTION = 0.05  # 5%

# An UNSCHEDULED source (no expected_interval_days) is never called stale —
# it runs on dispatch and has no cadence to miss. But one that has not run in
# this long is worth an eye, and this is the guard against a genuinely
# scheduled source landing in that bucket by mistake and going unwatched.
# 90 days: longer than the longest real cadence in the fleet (hennepin_parcels
# and anoka_parcels at 92-day quarterly, whose thresholds are 138), so a
# quarterly loader cannot trip it just by being quarterly.
_UNSCHEDULED_REVIEW_DAYS = 90.0

# ============================================================
# DATA FRESHNESS — added 2026-08-16
# ============================================================
# Everything above answers "did the SCRAPER run?". None of it answers "is the
# SOURCE still producing?" — and on 2026-08-16 eight sources were found
# frozen while every one reported HEALTHY here, three of them for over a
# year. dakota_sheriff had been silent 80 days with 15 successful runs in 30.
#
# src/scrapers/base_scraper.py now records, on EVERY run, the newest
# event_date present in what the source actually served, into
# audit.scraper_runs.metadata.source_max_date. That number is computed from
# the fetched payload and never read back from our tables, so no migration or
# re-key of ours can contaminate it — three earlier metrics failed exactly
# that way (event_date gap analysis, records_new, observed_at).
#
# A frozen source is one whose source_max_date STOPS MOVING. This section
# compares it against the source's own cadence.
#
# TWO SOURCES ARE EXEMPT, AND BOTH ARE DECLARED IN DATA, NOT NAMED HERE:
#   date_semantics = 'semantic'  event_date encodes something other than
#                                publication, so its max cannot indicate
#                                staleness. hennepin_tax_roll is the
#                                confirmed case: event_date is 1 January of
#                                the delinquency YEAR, so its max sits at
#                                2025-01-01 until parcels with
#                                EARLIEST_DELQ_YR='26' appear.
#   date_semantics = 'none'      the source carries no dates at all.
#
# Declared on audit.source_health rather than name-matched here, because a
# hardcoded list drifts and monitoring behaviour hidden in a string is how
# the SHELVED-prefix trap bit on 2026-08-16.
#
# Sources reporting NO dates are listed separately, never silently dropped.
# A freshness monitor blind to part of the fleet is the failure it exists to
# prevent — the same shape as 57 stale lines hiding one real one.

# How far past its own cadence a source's DATA may fall before it is frozen.
# Wider than the staleness multiplier (1.5) because publishers are lumpier
# than schedulers.
_FRESHNESS_MULTIPLIER = 3.0

# Minimum window for a source with a PUBLICATION cadence recorded. Set from
# measured behaviour: Dakota's 16 months of history show every month
# populated (5-17 sales, median ~11) with a worst observed gap of 33 days.
# 60 clears that comfortably and still catches the real stoppage at 80.
_FRESHNESS_MIN_DAYS = 60.0

# WHY publication_interval_days EXISTS SEPARATELY FROM expected_interval_days
#
# expected_interval_days is how often the SCRAPER RUNS. It is not how often
# the PUBLISHER PUBLISHES, and using it for freshness is a category error
# that produced two false positives in test on 2026-08-16:
#
#   ramsey_tfl        scraper weekly (7d); Ramsey publishes tax-forfeit
#                     auction lists ~2x/year. Flagged frozen at 157 days
#                     while behaving exactly as designed.
#   fillmore_probate  scraper weekly (7d); Fillmore is a small county whose
#                     probate volume is lumpy — 11 notices in one month, 2
#                     in another. Flagged frozen at 112 days.
#
# Both were diagnosed HEALTHY hours earlier. Shipping this would have put
# two known-false lines in the daily email, which is precisely how 57 false
# stale lines came to hide one real failure for ten weeks.
#
# So freshness is judged on publication_interval_days when it is declared,
# and a source without one is reported as UNJUDGED rather than guessed at.
# An honest "cannot judge" beats a confident wrong answer.


# Forward-dated sources (anoka_sheriff publishes to 2027-02-15, mnpublicnotice
# and postbulletin_legal carry future sale dates) would otherwise always look
# fresh. Freshness is judged on the newest date that has ACTUALLY OCCURRED.


def _env(name: str, required: bool = True, default: str | None = None) -> str | None:
    val = os.environ.get(name, default)
    if required and not val:
        print(f"FATAL: missing required env var {name}", flush=True)
        sys.exit(2)
    return val


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    # Supabase returns e.g. '2026-07-05T07:00:01.199533+00:00'
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _fetch_health(supabase_url: str, service_key: str) -> list[dict]:
    """Fetch all source_health rows via the Supabase REST API (audit schema)."""
    url = f"{supabase_url}/rest/v1/source_health"
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        # audit is a non-public schema; PostgREST needs it named explicitly.
        "Accept-Profile": "audit",
    }
    params = {"select": "*"}
    try:
        resp = httpx.get(url, headers=headers, params=params, timeout=30)
    except httpx.HTTPError as e:
        print(f"FATAL: could not reach Supabase: {type(e).__name__}: {e}", flush=True)
        sys.exit(2)
    if resp.status_code != 200:
        print(f"FATAL: Supabase returned {resp.status_code}: {resp.text[:300]}",
              flush=True)
        sys.exit(2)
    return resp.json()


def _fetch_source_max_dates(supabase_url: str, service_key: str) -> dict[str, str]:
    """Newest source_max_date each scraper has reported, from audit.scraper_runs.

    Same self-contained REST approach as _fetch_health: no app code imported,
    so a bug in the app cannot hide a failure in the monitor.

    PostgREST cannot order by a nested JSON key portably, so this pulls the
    recent run rows and reduces in Python. Bounded by a row cap rather than a
    date window: a source that runs monthly would fall outside a 30-day
    window and vanish from the report, which is precisely the source most
    worth watching.
    """
    url = f"{supabase_url}/rest/v1/scraper_runs"
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Accept-Profile": "audit",
    }
    params = {
        "select": "scraper_name,started_at,metadata",
        "order": "started_at.desc",
        "limit": "3000",
    }
    try:
        resp = httpx.get(url, headers=headers, params=params, timeout=60)
    except httpx.HTTPError as e:
        print(f"WARNING: could not read scraper_runs for freshness: "
              f"{type(e).__name__}: {e}", flush=True)
        return {}
    if resp.status_code != 200:
        print(f"WARNING: scraper_runs returned {resp.status_code} — "
              f"freshness section omitted", flush=True)
        return {}

    newest: dict[str, str] = {}
    for row in resp.json():
        name = row.get("scraper_name")
        meta = row.get("metadata") or {}
        smd = meta.get("source_max_date")
        if not name or not smd:
            continue
        # Rows arrive newest-first, but a source's most RECENT run is not
        # necessarily the one that saw the newest data — a partial fetch can
        # report an older max. Keep the highest value seen.
        if name not in newest or smd > newest[name]:
            newest[name] = smd
    return newest


def _freshness_state(row: dict, source_max_date: str | None) -> tuple[str, str]:
    """Return (state, reason) for one source's DATA freshness.

    States: 'fresh' | 'frozen' | 'exempt' | 'unjudged' | 'unknown'.
    """
    semantics = (row.get("date_semantics") or "").strip().lower()
    if semantics in ("semantic", "none"):
        return "exempt", f"date_semantics={semantics}"

    if not source_max_date:
        # Either it has not run since freshness capture shipped, or it
        # genuinely produces no dates. Both mean "cannot judge", and both
        # get said out loud rather than assumed healthy.
        return "unknown", "no source_max_date reported yet"

    # source_max_date is a bare YYYY-MM-DD (base_scraper._to_date_str), so
    # _parse_ts returns a NAIVE datetime and comparing it to an aware now()
    # raises TypeError. Caught in test 2026-08-16: in production that
    # exception would have been swallowed and the whole freshness section
    # would have silently disappeared — the exact blindness this section
    # exists to prevent. Normalise to UTC instead of catching.
    d = _parse_ts(source_max_date)
    if d is None:
        return "unknown", f"unparseable source_max_date {source_max_date!r}"
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)

    now = _now()
    if d > now:
        # Forward-dated: scheduled sales, published notices. Cannot be stale.
        return "fresh", f"newest data {source_max_date} (future-dated)"

    age_days = (now - d).total_seconds() / 86400.0

    # PUBLICATION cadence, never the scraper's. See the constants above.
    interval = row.get("publication_interval_days")
    if not (isinstance(interval, (int, float)) and interval > 0):
        return "unjudged", (
            f"newest data {source_max_date}, "
            f"{age_days:.0f} days old — no publication_interval_days declared"
        )

    threshold = max(_FRESHNESS_MIN_DAYS, float(interval) * _FRESHNESS_MULTIPLIER)
    cadence = f"publishes every ~{int(interval)}d"

    if age_days > threshold:
        return "frozen", (
            f"newest data {source_max_date}, {age_days:.0f} days old "
            f"({cadence}, threshold {threshold:.0f}d)"
        )
    return "fresh", f"newest data {source_max_date} ({age_days:.0f}d)"


def _classify(row: dict) -> tuple[str, str]:
    """Return (state, reason).

    state is one of: 'broken' | 'stale' | 'check' | 'shelved'
                   | 'unscheduled' | 'unscheduled_review' | 'healthy'.

    Key subtlety: the `notes` field is NOT cleared on a successful run -- it
    holds whatever message was last written, success or failure. So a stale
    error message can linger on a row that has since recovered. We therefore
    only trust notes as a failure signal when the row is ACTUALLY in a failed
    state: last failure newer than last success, or consecutive_failures > 0.
    Otherwise the source recovered and the note is just history.
    """
    name = row.get("source_name", "?")
    last_ok = _parse_ts(row.get("last_successful_run_at"))
    last_fail = _parse_ts(row.get("last_failed_run_at"))
    consec = row.get("consecutive_failures") or 0
    notes = (row.get("notes") or "").strip()
    notes_lc = notes.lower()

    # Shelved sources: intentionally retired (source removed upstream, no API,
    # etc.). Marked by a note beginning "SHELVED". These are excluded from the
    # digest entirely so a deliberately-disabled source doesn't nag as "broken"
    # (its last_successful_run_at may be null, which would otherwise trip the
    # never-succeeded rule below).
    if notes_lc.startswith("shelved"):
        return "shelved", notes[:100]

    # Never succeeded.
    if last_ok is None:
        return "broken", "never succeeded"

    # Actively failing (the counter is authoritative and IS reset on success).
    if consec and consec > 0:
        return "broken", f"{consec} consecutive failure(s)"

    # Is the row currently in a failed state? Only then do notes/failure text
    # count. If the last success is newer than the last failure, it recovered.
    currently_failed = last_fail is not None and last_fail > last_ok

    if currently_failed:
        # Total write failure.
        if notes_lc.startswith("all ") and "failed" in notes_lc:
            return "broken", f"total write failure: {notes[:100]}"
        # Fractional drop -- only broken if a large fraction failed.
        #
        # THE SUB-THRESHOLD EXIT IS LOAD-BEARING (2026-08-16). Before it, a
        # small drop fell past this test, past the error signatures (which
        # deliberately exclude the bare word "failed"), and landed on the
        # catch-all `return "broken"` at the bottom of this branch. That was
        # harmless only because record_partial() stamped a partial run as a
        # clean SUCCESS, so no partial ever reached here at all.
        #
        # Once record_partial stops claiming success -- the next change, and
        # the reason this one ships first -- every partial DOES reach here.
        # parcel_enrich_mngeo has 10 partial runs at 0.2% (15 of 6,223); a
        # daily scraper would have gone BROKEN on a fifteen-record drop.
        #
        # A minor drop is real and worth seeing, so it reports as "check"
        # rather than vanishing into "healthy". One line, not an alarm.
        m = re.search(r"(\d[\d,]*)\s+of\s+(\d[\d,]*)\s+records failed", notes_lc)
        if m:
            n = int(m.group(1).replace(",", ""))
            total = int(m.group(2).replace(",", "")) or 1
            frac = n / total
            if frac > _MINOR_DROP_FRACTION:
                return "broken", f"{n}/{total} records failed ({frac:.0%})"
            return "check", (
                f"last run dropped {n} of {total} records ({frac:.1%}) — "
                f"under the {_MINOR_DROP_FRACTION:.0%} threshold"
            )
        # Error signatures.
        for sig in _ERROR_SIGNATURES:
            if sig in notes_lc:
                return "broken", f"error in notes: {notes[:100]}"
        if "validationerror" in notes_lc or "literal_error" in notes_lc:
            return "broken", f"validation error dropping records: {notes[:100]}"
        # Failed state but no recognized signature -- still report it.
        return "broken", f"last run failed: {notes[:100] or 'no detail'}"

    # Row is in a SUCCESS state (last success newer than last failure, or no
    # failure recorded). Historically notes were NOT cleared on success, so a
    # recovered source can still carry an old "writes failed" / "404" message.
    # We cannot reliably tell a stale note from a current one using timestamps
    # (the tracker bumps updated_at on every success either way). The real fix
    # is record_success() clearing notes -- after which healthy rows carry an
    # empty note and there is nothing to misread. Until each source's next
    # successful run clears its note, we surface a lingering failure-note on a
    # healthy row as a soft "check" (not a hard break), so it neither hides a
    # real issue nor cries wolf on a recovered one.
    lingering = (
        (notes_lc.startswith("all ") and "failed" in notes_lc)
        or "validationerror" in notes_lc
        or "literal_error" in notes_lc
    )
    if not lingering:
        m = re.search(r"(\d[\d,]*)\s+of\s+(\d[\d,]*)\s+records failed", notes_lc)
        if m:
            n = int(m.group(1).replace(",", ""))
            total = int(m.group(2).replace(",", "")) or 1
            if (n / total) > _MINOR_DROP_FRACTION:
                lingering = True
    if lingering:
        return "check", f"healthy now, but carries a failure note: {notes[:90]}"

    # Stale: succeeded, recovered, but not recently — judged against THIS
    # source's expected cadence (daily scrapers within ~2 days, weekly within
    # ~10, monthly within ~46, quarterly loads within ~138).
    #
    # NO CADENCE = NOT STALE. There is no flat fallback any more; see the
    # module docstring. A dispatch-only source cannot be late.
    age_days = (_now() - last_ok).total_seconds() / 86400.0
    interval = row.get("expected_interval_days")

    if not isinstance(interval, (int, float)) or interval <= 0:
        if age_days > _UNSCHEDULED_REVIEW_DAYS:
            return "unscheduled_review", (
                f"no cadence recorded and last success {age_days:.0f} days ago"
            )
        return "unscheduled", "dispatch-only; no cadence recorded"

    threshold = max(2.0, float(interval) * 1.5)
    if age_days > threshold:
        return "stale", (
            f"last success {age_days:.1f} days ago "
            f"(expected every {int(interval)}d)"
        )

    return "healthy", "ok"


def _build_digest(
    rows: list[dict],
    source_max_dates: dict[str, str] | None = None,
) -> tuple[str, str, bool]:
    """Return (subject, body, any_problem)."""
    broken: list[tuple[str, str]] = []
    stale: list[tuple[str, str]] = []
    check: list[tuple[str, str]] = []
    shelved: list[str] = []
    unscheduled: list[str] = []
    unscheduled_review: list[tuple[str, str]] = []
    healthy: list[str] = []
    frozen: list[tuple[str, str]] = []
    undated: list[str] = []
    smd = source_max_dates or {}

    for row in sorted(rows, key=lambda r: r.get("source_name", "")):
        state, reason = _classify(row)
        name = row.get("source_name", "?")

        # Freshness is INDEPENDENT of run health: a source can run perfectly
        # every day and serve data that stopped moving a year ago. That is
        # exactly the case this section exists for, so it is judged for every
        # source that is not shelved, including the ones reporting HEALTHY.
        if state != "shelved":
            f_state, f_reason = _freshness_state(row, smd.get(name))
            if f_state == "frozen":
                frozen.append((name, f_reason))
            elif f_state in ("unknown", "unjudged"):
                undated.append(name)
        if state == "broken":
            broken.append((name, reason))
        elif state == "stale":
            stale.append((name, reason))
        elif state == "check":
            check.append((name, reason))
        elif state == "shelved":
            shelved.append(name)
        elif state == "unscheduled":
            unscheduled.append(name)
        elif state == "unscheduled_review":
            unscheduled_review.append((name, reason))
        else:
            healthy.append(name)

    # Shelved sources are excluded from the active total -- they're
    # intentionally retired, not part of the live fleet being monitored.
    # UNSCHEDULED sources ARE part of the fleet and stay in the total:
    # they are live, they just have no cadence to be late against.
    total = len(rows) - len(shelved)
    # frozen counts as a problem: a source serving year-old data is a defect
    # in the product even when its scraper is green.
    any_problem = bool(broken or stale or unscheduled_review or frozen)

    lines: list[str] = []
    lines.append(f"Govire scraper health digest -- "
                 f"{_now().strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("")
    lines.append(f"Total sources: {total}   "
                 f"Broken: {len(broken)}   "
                 f"Stale: {len(stale)}   "
                 f"Check: {len(check)}   "
                 f"Healthy: {len(healthy)}   "
                 f"Unscheduled: {len(unscheduled) + len(unscheduled_review)}   "
                 f"Frozen: {len(frozen)}")
    lines.append("")

    if frozen:
        lines.append("FROZEN (the scraper runs; the SOURCE has stopped "
                     "producing — start looking for a replacement):")
        for name, reason in frozen:
            lines.append(f"  - {name}: {reason}")
        lines.append("")

    if broken:
        lines.append("BROKEN (needs attention):")
        for name, reason in broken:
            lines.append(f"  - {name}: {reason}")
        lines.append("")

    if stale:
        lines.append("STALE (past its expected cadence):")
        for name, reason in stale:
            lines.append(f"  - {name}: {reason}")
        lines.append("")

    if check:
        lines.append("CHECK (running fine now; carries an old failure note "
                     "that clears on its next run):")
        for name, reason in check:
            lines.append(f"  - {name}: {reason}")
        lines.append("")

    if unscheduled_review:
        lines.append("UNSCHEDULED - REVIEW (no cadence recorded, and quiet "
                     "for a long time; check whether it SHOULD have one):")
        for name, reason in unscheduled_review:
            lines.append(f"  - {name}: {reason}")
        lines.append("")

    if healthy:
        lines.append(f"HEALTHY ({len(healthy)}): " + ", ".join(healthy))
        lines.append("")

    if undated:
        lines.append(
            f"NO FRESHNESS SIGNAL ({len(undated)}) - not yet reporting a "
            f"source_max_date, so data freshness cannot be judged: "
            + ", ".join(undated)
        )
        lines.append("")

    if unscheduled:
        lines.append(
            f"UNSCHEDULED ({len(unscheduled)}) - dispatch-only, not judged "
            f"for staleness: " + ", ".join(unscheduled)
        )
        lines.append("")

    if shelved:
        lines.append(f"Shelved (not monitored): " + ", ".join(shelved))
        lines.append("")

    if not any_problem:
        lines.append("All sources healthy. Nothing to do.")

    body = "\n".join(lines)

    if frozen:
        subject = f"[Govire health] {len(frozen)} FROZEN, {len(broken)} broken " \
                  f"({len(healthy)}/{total} healthy)"
    elif broken:
        subject = f"[Govire health] {len(broken)} BROKEN, {len(stale)} stale " \
                  f"({len(healthy)}/{total} healthy)"
    elif stale:
        subject = f"[Govire health] {len(stale)} stale " \
                  f"({len(healthy)}/{total} healthy)"
    elif unscheduled_review:
        subject = f"[Govire health] {len(unscheduled_review)} unscheduled " \
                  f"needing review ({len(healthy)}/{total} healthy)"
    else:
        subject = f"[Govire health] All {total} sources healthy"

    return subject, body, any_problem


def _send_email(api_key: str, to_addr: str, from_addr: str,
                subject: str, body: str) -> None:
    try:
        resp = httpx.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "from": from_addr,
                "to": [to_addr],
                "subject": subject,
                "text": body,
            },
            timeout=30,
        )
    except httpx.HTTPError as e:
        print(f"FATAL: could not reach Resend: {type(e).__name__}: {e}", flush=True)
        sys.exit(2)
    if 200 <= resp.status_code < 300:
        print(f"Digest emailed to {to_addr} ({subject})", flush=True)
    else:
        print(f"FATAL: Resend returned {resp.status_code}: {resp.text[:300]}",
              flush=True)
        sys.exit(2)


def main() -> int:
    supabase_url = _env("SUPABASE_URL").rstrip("/")
    service_key = _env("SUPABASE_SERVICE_ROLE_KEY")
    resend_key = _env("RESEND_API_KEY")
    to_addr = _env("ALERT_EMAIL_TO")
    from_addr = _env("ALERT_EMAIL_FROM")
    rows = _fetch_health(supabase_url, service_key)
    if not rows:
        # No rows at all is itself suspicious -- report it rather than stay silent.
        subject = "[Govire health] WARNING: source_health is empty"
        body = ("source_health returned zero rows. Either no scrapers have run, "
                "or the health table/permissions changed. Investigate.")
        _send_email(resend_key, to_addr, from_addr, subject, body)
        return 0

    # Never fatal: a freshness read that fails degrades the section to
    # "unknown" rather than losing the whole digest.
    source_max_dates = _fetch_source_max_dates(supabase_url, service_key)

    subject, body, _ = _build_digest(rows, source_max_dates)
    print(body, flush=True)  # also visible in the Actions run log
    _send_email(resend_key, to_addr, from_addr, subject, body)
    return 0


if __name__ == "__main__":
    sys.exit(main())
