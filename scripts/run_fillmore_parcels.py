"""Standalone runner for the Fillmore County parcels loader.

Used by the GitHub Actions workflow at .github/workflows/fillmore-parcels.yml.
Streams the full Fillmore parcel spine (~20,877 rows, verified live
2026-07-23) from the county's FillmoreAll FeatureServer Parcels layer into
core.parcels, with the core.owners projection riding alongside — the
Chatfield-corridor expansion's foundation load.

Calls .run() (not fetch()/parse()/write() directly) so the feature flag
SCRAPER_FILLMORE_PARCELS_ENABLED is honored and behavior matches the API
trigger path exactly. Upserts are idempotent — re-running refreshes.

Usage:
    python -m scripts.run_fillmore_parcels [trigger_name]

Exits 0 on success, 1 on failure / disabled / partial.
"""

from __future__ import annotations

import asyncio
import sys
import traceback

# Import at top level so a missing env var / config error fails fast.
from src.scrapers.fillmore_parcels import FillmoreParcelsScraper
from src.utils.logger import logger


async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"
    logger.info("Fillmore parcels runner starting", trigger=trigger)
    print(f"[fillmore-parcels-runner] trigger={trigger}", flush=True)

    scraper = FillmoreParcelsScraper()

    try:
        print(
            "[fillmore-parcels-runner] run: streaming the Fillmore parcel "
            "spine (~20.9K rows, keyset-paged) ...",
            flush=True,
        )
        # trigger="manual" => a disabled flag raises (loud) instead of
        # silently skipping, so a misconfigured flag fails the Actions run
        # visibly.
        result = await scraper.run(trigger="manual", metadata={"trigger_source": trigger})
    except Exception as e:
        print(
            f"[fillmore-parcels-runner] run: FAILED — {type(e).__name__}: {e}",
            flush=True,
        )
        traceback.print_exc()
        return 1

    print(
        f"[fillmore-parcels-runner] run: status={result.status} "
        f"fetched={result.records_fetched} new={result.records_new} "
        f"failed={result.records_failed}",
        flush=True,
    )

    if result.status in ("failed", "skipped"):
        print(
            f"[fillmore-parcels-runner] non-success status "
            f"({result.status}) — exit 1: {result.error_message}",
            flush=True,
        )
        return 1

    print("[fillmore-parcels-runner] done.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
