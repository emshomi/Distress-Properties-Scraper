"""Standalone runner for the Anoka Sheriff scraper.

Used by the GitHub Actions workflow at .github/workflows/anoka-scrape.yml.
The point of running this from GitHub Actions (rather than Railway's scheduler)
is that GitHub's outbound IPs are different from Railway's, which lets us bypass
the block we hit when trying to scrape Anoka from Railway.

That IP constraint is REAL and load-bearing: this scraper cannot simply be
moved to the Railway cron alongside hennepin_sheriff. Actions is the only
place it runs.

Usage:
    python -m scripts.run_anoka_sheriff [trigger_name]

Exit code 0 on success or partial success, 1 on failure, so GitHub Actions
marks a genuinely broken run as failed.

=== WHY THIS CALLS run() INSTEAD OF fetch/parse/write (2026-08-02) ===
It used to drive the three lifecycle steps itself and call
source_health_tracker directly. Health was recorded correctly — but ONLY
BaseScraper._run_locked writes audit.scraper_runs, so every Actions run of
this scraper was INVISIBLE in the run log.

Anoka is the worst case of that blindness. The Actions workflow has 86
consecutive green runs, most recently this morning at 07:25 CDT. The run log
showed its last run as 2026-05-28 — 66 days earlier. On 2026-08-02 that gap
was read as "the Anoka sheriff feed is dead", which would have meant 147 live
redemption windows going unmaintained. The feed was fine the whole time. The
log was lying by omission, and only the GitHub Actions tab disproved it.

run() does everything this file used to do by hand — the audit run row, the
health row, the per-class lock, and the success/partial/failed decision — in
ONE place. Five runners each reimplementing that is the
owner-classifier-in-five-files mistake; the parcels and legal runners already
call run() and record their Actions runs correctly.

=== EXIT CODE ===
run() CATCHES exceptions and returns a RunResult with status='failed' rather
than raising, so the exit code must come from result.status. A try/except
around it would see nothing and exit 0 on a broken scrape.

'partial' exits 0 deliberately: a red workflow mark for a handful of failed
records out of hundreds is how people learn to ignore red marks. The count is
printed and the run row records 'partial', which is the honest label.

Note Anoka publishes pending (not-yet-held) sales in the same feed; those are
skipped downstream by redemption_builder, not here, so a run that fetches
many rows and writes few is normal rather than a symptom.
"""
from __future__ import annotations

import asyncio
import sys

# Import is at top level so a missing env var / config error fails fast
# (before we waste time on imports inside main()).
from src.scrapers.anoka_sheriff import AnokaSheriffScraper
from src.utils.logger import logger


async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"

    logger.info("Anoka runner starting", trigger=trigger)
    print(f"[anoka-runner] trigger={trigger}", flush=True)

    scraper = AnokaSheriffScraper()

    # trigger="manual" so a disabled scraper RAISES ScraperDisabledError
    # instead of returning status='skipped' silently. A workflow that runs
    # daily against a scraper someone turned off should say so, not go green.
    result = await scraper.run(
        trigger="manual",
        metadata={"trigger_source": "github_actions", "runner": "anoka"},
    )

    print(
        f"[anoka-runner] {result.status} — run_id={result.run_id} "
        f"fetched={result.records_fetched} new={result.records_new} "
        f"updated={result.records_updated} failed={result.records_failed} "
        f"({result.duration_seconds:.1f}s)",
        flush=True,
    )
    if result.error_message:
        print(f"[anoka-runner] note: {result.error_message}", flush=True)

    if result.status in ("success", "partial"):
        return 0

    print("[anoka-runner] FAILED — exit 1", flush=True)
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
