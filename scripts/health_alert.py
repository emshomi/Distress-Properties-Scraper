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
    - last_successful_run_at older than STALE_DAYS

  HEALTHY otherwise.

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

Optional:
  HEALTH_STALE_DAYS            days without success before "stale" (default 3)

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


def _classify(row: dict, stale_days: int) -> tuple[str, str]:
    """Return (state, reason). state is 'broken' | 'stale' | 'healthy'.

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
        m = re.search(r"(\d[\d,]*)\s+of\s+(\d[\d,]*)\s+records failed", notes_lc)
        if m:
            n = int(m.group(1).replace(",", ""))
            total = int(m.group(2).replace(",", "")) or 1
            frac = n / total
            if frac > _MINOR_DROP_FRACTION:
                return "broken", f"{n}/{total} records failed ({frac:.0%})"
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

    # Stale: succeeded, recovered, but not recently.
    age_days = (_now() - last_ok).total_seconds() / 86400.0
    if age_days > stale_days:
        return "stale", f"last success {age_days:.1f} days ago"

    return "healthy", "ok"


def _build_digest(rows: list[dict], stale_days: int) -> tuple[str, str, bool]:
    """Return (subject, body, any_problem)."""
    broken: list[tuple[str, str]] = []
    stale: list[tuple[str, str]] = []
    check: list[tuple[str, str]] = []
    shelved: list[str] = []
    healthy: list[str] = []

    for row in sorted(rows, key=lambda r: r.get("source_name", "")):
        state, reason = _classify(row, stale_days)
        name = row.get("source_name", "?")
        if state == "broken":
            broken.append((name, reason))
        elif state == "stale":
            stale.append((name, reason))
        elif state == "check":
            check.append((name, reason))
        elif state == "shelved":
            shelved.append(name)
        else:
            healthy.append(name)

    # Shelved sources are excluded from the active total -- they're
    # intentionally retired, not part of the live fleet being monitored.
    total = len(rows) - len(shelved)
    any_problem = bool(broken or stale)

    lines: list[str] = []
    lines.append(f"Govire scraper health digest -- "
                 f"{_now().strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("")
    lines.append(f"Total sources: {total}   "
                 f"Broken: {len(broken)}   "
                 f"Stale: {len(stale)}   "
                 f"Check: {len(check)}   "
                 f"Healthy: {len(healthy)}")
    lines.append("")

    if broken:
        lines.append("BROKEN (needs attention):")
        for name, reason in broken:
            lines.append(f"  - {name}: {reason}")
        lines.append("")

    if stale:
        lines.append(f"STALE (no success in > {stale_days} days):")
        for name, reason in stale:
            lines.append(f"  - {name}: {reason}")
        lines.append("")

    if check:
        lines.append("CHECK (running fine now; carries an old failure note "
                     "that clears on its next run):")
        for name, reason in check:
            lines.append(f"  - {name}: {reason}")
        lines.append("")

    if healthy:
        lines.append(f"HEALTHY ({len(healthy)}): " + ", ".join(healthy))
        lines.append("")

    if shelved:
        lines.append(f"Shelved (not monitored): " + ", ".join(shelved))
        lines.append("")

    if not any_problem:
        lines.append("All sources healthy. Nothing to do.")

    body = "\n".join(lines)

    if broken:
        subject = f"[Govire health] {len(broken)} BROKEN, {len(stale)} stale " \
                  f"({len(healthy)}/{total} healthy)"
    elif stale:
        subject = f"[Govire health] {len(stale)} stale " \
                  f"({len(healthy)}/{total} healthy)"
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
    stale_days = int(_env("HEALTH_STALE_DAYS", required=False, default="3"))

    rows = _fetch_health(supabase_url, service_key)
    if not rows:
        # No rows at all is itself suspicious -- report it rather than stay silent.
        subject = "[Govire health] WARNING: source_health is empty"
        body = ("source_health returned zero rows. Either no scrapers have run, "
                "or the health table/permissions changed. Investigate.")
        _send_email(resend_key, to_addr, from_addr, subject, body)
        return 0

    subject, body, _ = _build_digest(rows, stale_days)
    print(body, flush=True)  # also visible in the Actions run log
    _send_email(resend_key, to_addr, from_addr, subject, body)
    return 0


if __name__ == "__main__":
    sys.exit(main())
