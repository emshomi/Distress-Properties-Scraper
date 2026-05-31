"""Standalone runner for the Ramsey Parcels foundation loader.

Used by the GitHub Actions workflow at .github/workflows/ramsey-tax.yml
(step 1 of 2). Ramsey's source is the county ArcGIS AttributedData server
(clean JSON, no bot resistance), so this runs pure-httpx — no Playwright.

IMPORTANT — why this calls .run() and not fetch()/parse()/write():
RamseyParcelsScraper OVERRIDES run() with a STREAMING loop
(fetch-page -> write-page -> discard) so it never holds all ~163K parcels in
memory at once. Calling fetch() directly (as the sheriff runners do) would
load the entire dataset into RAM — the exact problem the streaming run avoids.
So we invoke run(), which also means the scraper's feature flag
(SCRAPER_RAMSEY_PARCELS_ENABLED) must be enabled in the workflow env.

Usage:
    python -m scripts.run_ramsey_parcels [trigger_name]

Exits 0 on success, 1 on failure / disabled / partial, so GitHub Actions
marks the run correctly.
"""

from __future__ import annotations

import asyncio
import sys
import traceback

# Import at top level so a missing env var / config error fails fast.
from src.scrapers.ramsey_parcels import RamseyParcelsScraper
from src.utils.logger import logger


async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"
    logger.info("Ramsey parcels runner starting", trigger=trigger)
    print(f"[ramsey-parcels-runner] trigger={trigger}", flush=True)

    scraper = RamseyParcelsScraper()

    try:
        print(
            "[ramsey-parcels-runner] run: streaming load from "
            "maps.co.ramsey.mn.us ArcGIS ...",
            flush=True,
        )
        # trigger="manual" makes a disabled flag raise (loud failure) rather
        # than silently skip — we WANT the run to fail visibly if the flag
        # isn't set, instead of quietly doing nothing.
        result = await scraper.run(trigger="manual", metadata={"trigger_source": trigger})
    except Exception as e:
        print(
            f"[ramsey-parcels-runner] run: FAILED — {type(e).__name__}: {e}",
            flush=True,
        )
        traceback.print_exc()
        return 1

    print(
        f"[ramsey-parcels-runner] run: status={result.status} "
        f"fetched={result.records_fetched} new={result.records_new} "
        f"failed={result.records_failed}",
        flush=True,
    )

    if result.status in ("failed", "skipped"):
        print(
            f"[ramsey-parcels-runner] non-success status "
            f"({result.status}) — exit 1: {result.error_message}",
            flush=True,
        )
        return 1

    print("[ramsey-parcels-runner] done.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
