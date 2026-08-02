"""Standalone runner for the Washington Sheriff scraper.

Used by the GitHub Actions workflow at .github/workflows/washington-scrape.yml.

Washington County publishes completed sheriff's sales as monthly Excel files in
its Property Records & Taxpayer Services archive. This runner fetches the recent
monthly files, parses the per-property sale rows, and writes foreclosure events.

Usage:
    python -m scripts.run_washington_sheriff [trigger_name]

Exit code 0 on success or partial success, 1 on failure, so GitHub Actions
marks a genuinely broken run as failed.

=== WHY THIS CALLS run() INSTEAD OF fetch/parse/write (2026-08-02) ===
It used to drive the three lifecycle steps itself and call
source_health_tracker directly. ONLY BaseScraper._run_locked writes
audit.scraper_runs, so every Actions run of this scraper was INVISIBLE in the
run log — audit.scraper_runs has never held a single washington_sheriff row.

MIGRATION_cadence_fix_2026-07-13.sql records the downstream effect: no
source_health row exists for washington_sheriff either, and the note there
attributes it to record_success never firing. Worth being precise, because
that note is misleading: the old code DID call record_success on the success
path. The row is missing because the run is not reaching it — the last
washington_sheriff write to signals.distress_events was 2026-07-05. Whatever
is wrong is in the fetch or the workflow, not the health call, and going
through run() is what will finally record it either way: a failure now writes
a run row with a status and an error message instead of vanishing.

Washington is also the county Part 7 flags as MIXED on redemption periods —
44 stated, 118 assumed — so the calculator drives its confidence line off
period_source per row rather than a per-county rule. A silently dead feed
here degrades that quietly rather than obviously.

run() does everything this file used to do by hand — the audit run row, the
health row, the per-class lock, and the success/partial/failed decision — in
ONE place, instead of five runners each keeping their own copy.

=== EXIT CODE ===
run() CATCHES exceptions and returns a RunResult with status='failed' rather
than raising, so the exit code must come from result.status. A try/except
around it would see nothing and exit 0 on a broken scrape.

'partial' exits 0 deliberately: a red mark for a few failed rows out of many
is how people learn to ignore red marks. The count is printed and the run row
records 'partial', which is the honest label.
"""
from __future__ import annotations

import asyncio
import sys

# Import is at top level so a missing env var / config error fails fast
# (before we waste time on imports inside main()).
from src.scrapers.washington_sheriff import WashingtonSheriffScraper
from src.utils.logger import logger


async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"

    logger.info("Washington runner starting", trigger=trigger)
    print(f"[washington-runner] trigger={trigger}", flush=True)

    scraper = WashingtonSheriffScraper()

    # trigger="manual" so a disabled scraper RAISES ScraperDisabledError
    # instead of returning status='skipped' silently. Given this scraper has
    # no run history at all, a config flag turning it off is a live
    # possibility and must not present as a green workflow.
    result = await scraper.run(
        trigger="manual",
        metadata={"trigger_source": "github_actions", "runner": "washington"},
    )

    print(
        f"[washington-runner] {result.status} — run_id={result.run_id} "
        f"fetched={result.records_fetched} new={result.records_new} "
        f"updated={result.records_updated} failed={result.records_failed} "
        f"({result.duration_seconds:.1f}s)",
        flush=True,
    )
    if result.error_message:
        print(f"[washington-runner] note: {result.error_message}", flush=True)

    if result.status in ("success", "partial"):
        return 0

    print("[washington-runner] FAILED — exit 1", flush=True)
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
