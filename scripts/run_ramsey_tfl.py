"""Standalone runner for the Ramsey Tax-Forfeited Lands (TFL) scraper.

Used by the GitHub Actions workflow at .github/workflows/ramsey-tfl.yml.
Scrapes the county's tax-forfeited public-sales pages (auction list +
over-the-counter list). Both lists thin out between sale cycles — an
empty result is HONEST STATE and exits 0.

Calls .run() (not fetch()/parse()/write() directly) so the feature flag
SCRAPER_RAMSEY_TFL_ENABLED is honored and behavior matches the API
trigger path exactly. The write is idempotent (write_events_dedup keyed
on parcel + list date), so re-running only adds genuinely new listings.

Usage:
    python -m scripts.run_ramsey_tfl [trigger_name]

Exits 0 on success, 1 on failure / disabled / partial.
"""

from __future__ import annotations

import asyncio
import sys
import traceback

# Import at top level so a missing env var / config error fails fast.
from src.scrapers.ramsey_tfl import RamseyTflScraper
from src.utils.logger import logger


async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"
    logger.info("Ramsey TFL runner starting", trigger=trigger)
    print(f"[ramsey-tfl-runner] trigger={trigger}", flush=True)

    scraper = RamseyTflScraper()

    try:
        print(
            "[ramsey-tfl-runner] run: fetching Ramsey tax-forfeited "
            "auction + OTC pages ...",
            flush=True,
        )
        # trigger="manual" => a disabled flag raises (loud) instead of
        # silently skipping, so a misconfigured flag fails the Actions run
        # visibly.
        result = await scraper.run(trigger="manual", metadata={"trigger_source": trigger})
    except Exception as e:
        print(
            f"[ramsey-tfl-runner] run: FAILED — {type(e).__name__}: {e}",
            flush=True,
        )
        traceback.print_exc()
        return 1

    print(
        f"[ramsey-tfl-runner] run: status={result.status} "
        f"fetched={result.records_fetched} new={result.records_new} "
        f"failed={result.records_failed}",
        flush=True,
    )

    if result.status in ("failed", "skipped"):
        print(
            f"[ramsey-tfl-runner] non-success status "
            f"({result.status}) — exit 1: {result.error_message}",
            flush=True,
        )
        return 1

    print("[ramsey-tfl-runner] done.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
