"""
Nightly data-integrity alert.

Calls audit.run_integrity_checks() and emails what it finds. Written
2026-08-18 out of the 2026-08-17 session, in which four scrapers were
found minting ~534 synthetic parcel stubs A DAY against a spine where
almost all of them already existed. The accumulation had been running
for months and was invisible, because the health digest counts
records_new -- which counts the stubs themselves. A scraper writing 381
junk rows every morning reported as healthy.

That is the gap this fills. health_alert.py answers "did the scrapers
run?"; this answers "is what they wrote correct?".

=== ALERT-ONLY, WITH ONE EXCEPTION ===

Sends only when a check returns 'alert' severity. Silence means clean.

A daily "all zero" email is one you stop reading inside a week, and then
it is worthless on the day it matters. But "no email" and "the job died"
look identical, which is exactly how dakota_sheriff went 67 days without
writing an event while reporting healthy. So a summary goes out on
MONDAYS regardless -- if that Monday email stops arriving, the job is
dead, and the absence is itself the signal.

Set INTEGRITY_ALWAYS_EMAIL=1 to force a send on any run (useful the
first few times, or when testing a change).

=== SELF-CONTAINED, DELIBERATELY ===

Talks to Supabase over the REST API with httpx and to Resend over its
HTTP API. No app code imported, so a bug in the app cannot hide a
failure in the monitor. Same reasoning as health_alert.py, whose shape
this follows on purpose -- one email style, one set of secrets, one
thing to learn.

=== WHAT IT DOES NOT DO ===

It never deletes, re-points or repairs anything. On 2026-08-17 six stub
populations were migrated by hand and the correct action differed EVERY
TIME on something no rule could have known in advance: hennepin's 14
imagery rows were failure records to delete, dakota's 2 held real Street
View panos to keep, washington matched by PIN rather than address, and
beltrami's stub was CORRECT because that county has no parcel spine at
all. An automated cleaner would have destroyed the panos and "fixed"
beltrami by deleting a legitimate row -- silently, because
core.parcel_imagery is ON DELETE CASCADE.

Environment:
  SUPABASE_URL                 https://<ref>.supabase.co
  SUPABASE_SERVICE_ROLE_KEY    service-role key
  RESEND_API_KEY               Resend API key
  ALERT_EMAIL_TO               where the alert is sent
  ALERT_EMAIL_FROM             verified Resend sender (e.g. noreply@govire.com)
  INTEGRITY_ALWAYS_EMAIL       optional; '1' forces a send every run
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

import httpx


# Order checks appear in the email. Anything not listed sorts last, so a
# check added to the SQL without being added here still shows up.
_CHECK_ORDER = [
    "stubs_minted_today",
    "duplicate_events_same_parcel",
    "events_orphaned_from_parcels",
    "approved_never_promoted",
    "source_silent_days",
    "sheriff_sales_without_event",
    "stubs_resolvable",
    "imagery_failures_on_stubs",
]

# One line of context per check, so the email explains itself without a
# trip to the migration file.
_CHECK_NOTE = {
    "stubs_minted_today":
        "Synthetic parcel rows created today. Was ~534/day before 2026-08-17. "
        "Expect zero. A county with no parcel spine (beltrami) can only ever "
        "produce these -- watch the trend per writer, not the total.",
    "duplicate_events_same_parcel":
        "Two events on one parcel for one sale date AND one subtype. Causes "
        "seen: a county reissuing a notice under a new detail id, a "
        "cross-source pair, or a pre-2026-08-10 stub the approve guard "
        "cannot see. NOT the pending/completed lifecycle -- that is two real "
        "facts and event_subtype keeps them apart.",
    "events_orphaned_from_parcels":
        "Events pointing at a parcel that does not exist. The composite FK "
        "should make this impossible, but a NULL county_code leaves it "
        "unenforced. Never expected.",
    "approved_never_promoted":
        "Extractions marked approved with no promoted_at. Should be "
        "impossible since the approve path became one transaction.",
    "source_silent_days":
        "Days since this source last WROTE AN EVENT. Not the digest's "
        "freshness rule, which reaches 7 of 78 sources and has reported a "
        "live source as FROZEN. n = days silent.",
    "sheriff_sales_without_event":
        "A sheriff_sales row whose parcel carries no sheriff_sale event. "
        "Usually an event re-pointed to a real parcel while its sale stayed "
        "on the stub.",
    "stubs_resolvable":
        "Stubs whose address now matches exactly one real parcel -- "
        "migration candidates. A BASELINE, not a zero. A RISING number "
        "means something started minting resolvable stubs again.",
    "imagery_failures_on_stubs":
        "Imagery rows on stubs. A stub has no lat/lng, so these are "
        "'no_location' failures repeated daily. Waste, not corruption. "
        "NEVER auto-delete: dakota stubs carry real panos.",
}


def _env(name: str, required: bool = True, default: str | None = None) -> str | None:
    val = os.environ.get(name, default)
    if required and not val:
        print(f"FATAL: missing required env var {name}", flush=True)
        sys.exit(2)
    return val


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _run_checks(supabase_url: str, service_key: str) -> list[dict]:
    """Call audit.run_integrity_checks() via PostgREST RPC.

    The function WRITES to audit.integrity_findings and returns this
    run's rows, so the history accumulates whether or not an email goes
    out. A read-only failure here must be loud: a monitor that fails
    quietly is worse than none.
    """
    url = f"{supabase_url}/rest/v1/rpc/run_integrity_checks"
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        # audit is a non-public schema; PostgREST needs it named explicitly.
        "Content-Profile": "audit",
        "Accept-Profile": "audit",
        "Content-Type": "application/json",
    }
    try:
        # The checks scan several large tables; 180s is generous but a
        # timeout here should be reported, not silently retried.
        resp = httpx.post(url, headers=headers, json={}, timeout=180)
    except httpx.HTTPError as e:
        print(f"FATAL: could not reach Supabase: {type(e).__name__}: {e}", flush=True)
        sys.exit(2)
    if resp.status_code != 200:
        print(f"FATAL: Supabase returned {resp.status_code}: {resp.text[:300]}",
              flush=True)
        sys.exit(2)
    return resp.json() or []


def _build_alert(rows: list[dict], is_monday: bool) -> tuple[str, str, bool]:
    """Return (subject, body, should_send)."""
    by_check: dict[str, list[dict]] = {}
    for r in rows:
        by_check.setdefault(r.get("check_name") or "(unnamed)", []).append(r)

    alerts = [r for r in rows if r.get("severity") == "alert"]
    warns = [r for r in rows if r.get("severity") == "warn"]

    lines: list[str] = [
        f"Govire integrity checks -- {_now():%Y-%m-%d %H:%M} UTC",
        "",
        f"{len(alerts)} alert, {len(warns)} warn, "
        f"{len(rows) - len(alerts) - len(warns)} info "
        f"({len(rows)} findings across {len(by_check)} checks)",
        "",
    ]

    if not rows:
        lines.append("No findings. Every check returned clean.")

    def _sort_key(name: str) -> tuple[int, str]:
        try:
            return (_CHECK_ORDER.index(name), name)
        except ValueError:
            return (len(_CHECK_ORDER), name)

    for check in sorted(by_check, key=_sort_key):
        group = by_check[check]
        sev = ("ALERT" if any(g.get("severity") == "alert" for g in group)
               else "WARN" if any(g.get("severity") == "warn" for g in group)
               else "INFO")
        total = sum(int(g.get("n") or 0) for g in group)
        lines.append(f"{sev} -- {check} ({len(group)} rows, n total {total})")
        note = _CHECK_NOTE.get(check)
        if note:
            lines.append(f"  {note}")
        # Cap the per-check detail: 57 duplicate groups in one email is
        # unreadable, and the count is what prompts the investigation.
        for g in sorted(group, key=lambda x: -int(x.get("n") or 0))[:12]:
            note_txt = (g.get("note") or "").strip()
            lines.append(f"    - {note_txt or '(no label)'}: {g.get('n')}")
        if len(group) > 12:
            lines.append(f"    ... and {len(group) - 12} more")
        lines.append("")

    lines.append("Findings are stored in audit.integrity_findings. To see the "
                 "detail behind any row:")
    lines.append("")
    lines.append("  SELECT now() AS run_at, check_name, county_code, n, detail")
    lines.append("  FROM audit.integrity_findings")
    lines.append("  WHERE run_at = (SELECT MAX(run_at) FROM audit.integrity_findings)")
    lines.append("    AND severity = 'alert'")
    lines.append("  ORDER BY n DESC;")
    lines.append("")
    lines.append("Nothing here has been changed or repaired -- these are "
                 "findings only. See MIGRATION_integrity_findings_20260818.sql "
                 "for why cleanup is deliberately manual.")

    if alerts:
        counts: dict[str, int] = {}
        for a in alerts:
            counts[a.get("check_name") or "?"] = counts.get(a.get("check_name") or "?", 0) + 1
        headline = ", ".join(
            f"{n} {name.replace('_', ' ')}" for name, n in
            sorted(counts.items(), key=lambda kv: -kv[1])[:2])
        subject = f"[Govire integrity] {headline}"
    elif warns:
        subject = f"[Govire integrity] {len(warns)} warnings, no alerts"
    else:
        subject = "[Govire integrity] All checks clean"

    # Alert-only, plus the Monday heartbeat. See the module docstring for
    # why an all-clear every day would be worse than useless.
    should_send = bool(alerts) or is_monday or \
        os.environ.get("INTEGRITY_ALWAYS_EMAIL") == "1"
    return subject, "\n".join(lines), should_send


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
        print(f"Integrity alert emailed to {to_addr} ({subject})", flush=True)
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

    rows = _run_checks(supabase_url, service_key)

    # Monday = weekday 0. The heartbeat: if this stops arriving, the job
    # is dead, and the absence is the signal.
    is_monday = _now().weekday() == 0

    subject, body, should_send = _build_alert(rows, is_monday)

    # Always print, so the Actions log holds the full picture even on a
    # run that sends nothing.
    print(body, flush=True)

    if should_send:
        _send_email(resend_key, to_addr, from_addr, subject, body)
    else:
        print(f"No alerts and not Monday -- no email sent. ({subject})",
              flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
