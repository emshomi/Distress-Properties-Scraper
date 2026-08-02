"""Standalone runner for the Hennepin Sheriff scraper.

Used by the GitHub Actions workflow at .github/workflows/hennepin-scrape.yml.
Running from GitHub Actions keeps it consistent with the other sheriff
scrapers; Hennepin's API is clean JSON (no IP block, no browser needed), so
this could also run from Railway, but we keep all sheriff scrapers on the
same GitHub Actions cadence for uniform observability.

Usage:
    python -m scripts.run_hennepin_sheriff [trigger_name]

Exit code 0 on success or partial success, 1 on failure, so GitHub Actions
marks a genuinely broken run as failed.

=== WHY THIS CALLS run() INSTEAD OF fetch/parse/write (2026-08-02) ===
It used to drive the three lifecycle steps itself and call
source_health_tracker directly. That worked, and health was recorded
correctly — but ONLY BaseScraper._run_locked writes audit.scraper_runs, so
every Actions run of this scraper was INVISIBLE in the run log.

The cost was not theoretical. On 2026-08-02 audit.scraper_runs showed one
Hennepin run for the day: the 11:15 Railway cron, status 'failed'. It did not
show the Actions run that succeeded at 12:36. Reading the run log as the
record of what happened produced a chain of wrong conclusions — that the
sheriff feeds were dead, that the health digest was lying about Hennepin,
that anoka_sheriff had not run in 66 days (its Actions workflow is green
daily; 86 consecutive runs). Three separate false alarms, all from a log
that was missing half its entries.

run() does everything this file used to do by hand — the audit run row, the
health row, the per-class lock, and the success/partial/failed decision — in
ONE place. Duplicating that here is the owner-classifier-in-five-files
mistake: five runners, five copies, drifting apart. run_dakota_parcels and
the other parcels/legal runners already call run() and record their Actions
runs correctly, which is how we know nothing about the Actions environment
prevents it.

=== EXIT CODE ===
run() CATCHES exceptions and returns a RunResult with status='failed' rather
than raising. So the exit code must come from result.status. A try/except
around it would see nothing and exit 0 on a broken scrape, turning the
workflow green over a failure — the exact blindness this change is undoing.

'partial' exits 0 deliberately. Today's 11:15 run fetched 503 records and hit
ONE transient Supabase timeout on one parcel; the old rule (`failed > 0` ->
exit 1) marked the whole run failed and turned the workflow red. A red mark
for 1 record in 503 is how people learn to ignore red marks. The count is
printed and the run row records it as 'partial', which is the honest label.
"""
from __future__ import annotations

import asyncio
import sys

# Import is at top level so a missing env var / config error fails fast
# (before we waste time on imports inside main()).
from src.scrapers.hennepin_sheriff import HennepinSheriffScraper
from src.utils.logger import logger


async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"

    logger.info("Hennepin runner starting", trigger=trigger)
    print(f"[hennepin-runner] trigger={trigger}", flush=True)

    scraper = HennepinSheriffScraper()

    # trigger="manual" so a disabled scraper RAISES ScraperDisabledError
    # instead of returning status='skipped' silently. A workflow that runs
    # daily against a scraper someone turned off should say so, not go green.
    result = await scraper.run(
        trigger="manual",
        metadata={"trigger_source": "github_actions", "runner": "hennepin"},
    )

    print(
        f"[hennepin-runner] {result.status} — run_id={result.run_id} "
        f"fetched={result.records_fetched} new={result.records_new} "
        f"updated={result.records_updated} failed={result.records_failed} "
        f"({result.duration_seconds:.1f}s)",
        flush=True,
    )
    if result.error_message:
        print(f"[hennepin-runner] note: {result.error_message}", flush=True)

    if result.status in ("success", "partial"):
        return 0

    print(f"[hennepin-runner] FAILED — exit 1", flush=True)
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
