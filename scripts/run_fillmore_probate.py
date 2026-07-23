"""Standalone runner for the Fillmore probate-notice scraper.

Used by the GitHub Actions workflow at .github/workflows/fillmore-probate.yml.
Fetches 90 days of Legal Notice posts from the Fillmore County Journal's
WordPress REST API, parses PROBATE DIVISION notices (decedent, PRs, case
number), matches decedent names against the Fillmore parcel spine's owner
names (word-boundary + middle-initial rules), and writes probate_estate
events for MATCHED parcels only — the estate channel caught at inception
(2026-07-23).

Calls .run() (not fetch()/parse()/write() directly) so the feature flag
SCRAPER_FILLMORE_PROBATE_ENABLED is honored and behavior matches the API
trigger path exactly. Writes are dedup-idempotent — re-running refreshes.

Usage:
    python -m scripts.run_fillmore_probate [trigger_name]

Exits 0 on success, 1 on failure / disabled.
"""

from __future__ import annotations

import asyncio
import sys
import traceback

# Import at top level so a missing env var / config error fails fast.
from src.scrapers.fillmore_probate import FillmoreProbateScraper
from src.utils.logger import logger


async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"
    logger.info("Fillmore probate runner starting", trigger=trigger)
    print(f"[fillmore-probate-runner] trigger={trigger}", flush=True)

    scraper = FillmoreProbateScraper()

    try:
        print(
            "[fillmore-probate-runner] run: fetching Fillmore County Journal "
            "probate notices (90-day window) ...",
            flush=True,
        )
        # trigger="manual" => a disabled flag raises (loud) instead of
        # silently skipping, so a misconfigured flag fails the Actions run
        # visibly.
        result = await scraper.run(trigger="manual", metadata={"trigger_source": trigger})
    except Exception as e:
        print(
            f"[fillmore-probate-runner] run: FAILED — {type(e).__name__}: {e}",
            flush=True,
        )
        traceback.print_exc()
        return 1

    print(
        f"[fillmore-probate-runner] run: status={result.status} "
        f"fetched={result.records_fetched} new={result.records_new} "
        f"failed={result.records_failed}",
        flush=True,
    )

    if result.status in ("failed", "skipped"):
        print(
            f"[fillmore-probate-runner] non-success status "
            f"({result.status}) — exit 1: {result.error_message}",
            flush=True,
        )
        return 1

    print("[fillmore-probate-runner] done.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
