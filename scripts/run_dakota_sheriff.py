"""Standalone runner for the Dakota Sheriff scraper.

Used by the GitHub Actions workflow at .github/workflows/dakota-scrape.yml.
Dakota's source is the county ArcGIS server (clean JSON, no bot resistance),
so this runs pure-httpx like Hennepin — no Playwright needed. We keep all
sheriff scrapers on the same GitHub Actions cadence for uniform observability.

Usage:
    python -m scripts.run_dakota_sheriff [trigger_name]

Exit code 0 on success or partial success, 1 on failure, so GitHub Actions
marks a genuinely broken run as failed.

=== WHY THIS CALLS run() INSTEAD OF fetch/parse/write (2026-08-02) ===
The previous version carried this comment above its record_success call:

    "This is the call the standalone runners were missing -- without it,
     source_health froze at whenever the scraper last went through
     BaseScraper.run(), even though the daily Actions run works."

That diagnosis was RIGHT, and the fix was half of one. Health froze because
the runner bypassed BaseScraper.run(). Calling source_health_tracker directly
unfroze health — but _run_locked is also the only thing that writes
audit.scraper_runs, so the run log stayed blind while health looked fine. The
two halves then disagreed, and the run log is the one people read.

The damage from that split showed up on 2026-08-02. audit.scraper_runs said
dakota_sheriff last ran 2026-05-28 while audit.source_health said it succeeded
at 12:40 that same morning. Neither was lying; each was recording a different
subset of reality. Reading them together produced a false conclusion that
every sheriff feed in the system was dead, and roughly an hour went into
chasing it.

Going through run() fixes both halves at once: the audit run row, the health
row, the per-class lock and the success/partial/failed decision all happen in
ONE place, which is exactly what BaseScraper exists for. The parcels and legal
runners already do this and their Actions runs have always been recorded
correctly — dakota_parcels included, from the same repo, on the same runner.

=== EXIT CODE ===
run() CATCHES exceptions and returns a RunResult with status='failed' rather
than raising, so the exit code must come from result.status. A try/except
around it would see nothing and exit 0 on a broken scrape.

'partial' exits 0 deliberately: a red mark for a few failed rows out of many
is how people learn to ignore red marks. The count is printed and the run row
records 'partial', which is the honest label.

Note Part 7: Dakota publishes NO redemption period or expiry date — the feed
carries SaleDate, SaleAmount, GeoAddress, GeoCity and ids only. Windows for
this county are computed from the statutory default, which is a documented
limitation of the source rather than an extraction gap.
"""
from __future__ import annotations

import asyncio
import sys

# Import at top level so a missing env var / config error fails fast.
from src.scrapers.dakota_sheriff import DakotaSheriffScraper
from src.utils.logger import logger


async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"

    logger.info("Dakota runner starting", trigger=trigger)
    print(f"[dakota-runner] trigger={trigger}", flush=True)

    scraper = DakotaSheriffScraper()

    # trigger="manual" so a disabled scraper RAISES ScraperDisabledError
    # instead of returning status='skipped' silently. A workflow that runs
    # daily against a scraper someone turned off should say so, not go green.
    result = await scraper.run(
        trigger="manual",
        metadata={"trigger_source": "github_actions", "runner": "dakota"},
    )

    print(
        f"[dakota-runner] {result.status} — run_id={result.run_id} "
        f"fetched={result.records_fetched} new={result.records_new} "
        f"updated={result.records_updated} failed={result.records_failed} "
        f"({result.duration_seconds:.1f}s)",
        flush=True,
    )
    if result.error_message:
        print(f"[dakota-runner] note: {result.error_message}", flush=True)

    if result.status in ("success", "partial"):
        return 0

    print("[dakota-runner] FAILED — exit 1", flush=True)
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
