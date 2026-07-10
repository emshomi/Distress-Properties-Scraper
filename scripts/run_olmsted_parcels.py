"""Standalone runner for the Olmsted County parcels loader.

Used by the GitHub Actions workflow at .github/workflows/olmsted-parcels.yml.
Streams the full Olmsted parcel spine (~75,600 rows, verified live
2026-07-09) from the county's General_Land_Info layer into core.parcels,
with the core.owners projection riding alongside — the Rochester/Mayo
pilot's foundation load.

Calls .run() (not fetch()/parse()/write() directly) so the feature flag
SCRAPER_OLMSTED_PARCELS_ENABLED is honored and behavior matches the API
trigger path exactly. Upserts are idempotent — re-running refreshes.

Usage:
    python -m scripts.run_olmsted_parcels [trigger_name]

Exits 0 on success, 1 on failure / disabled / partial.
"""

from __future__ import annotations

import asyncio
import sys
import traceback

# Import at top level so a missing env var / config error fails fast.
from src.scrapers.olmsted_parcels import OlmstedParcelsScraper
from src.utils.logger import logger


async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"
    logger.info("Olmsted parcels runner starting", trigger=trigger)
    print(f"[olmsted-parcels-runner] trigger={trigger}", flush=True)

    scraper = OlmstedParcelsScraper()

    try:
        print(
            "[olmsted-parcels-runner] run: streaming the Olmsted parcel "
            "spine (~75.6K rows, keyset-paged) ...",
            flush=True,
        )
        # trigger="manual" => a disabled flag raises (loud) instead of
        # silently skipping, so a misconfigured flag fails the Actions run
        # visibly.
        result = await scraper.run(trigger="manual", metadata={"trigger_source": trigger})
    except Exception as e:
        print(
            f"[olmsted-parcels-runner] run: FAILED — {type(e).__name__}: {e}",
            flush=True,
        )
        traceback.print_exc()
        return 1

    print(
        f"[olmsted-parcels-runner] run: status={result.status} "
        f"fetched={result.records_fetched} new={result.records_new} "
        f"failed={result.records_failed}",
        flush=True,
    )

    if result.status in ("failed", "skipped"):
        print(
            f"[olmsted-parcels-runner] non-success status "
            f"({result.status}) — exit 1: {result.error_message}",
            flush=True,
        )
        return 1

    print("[olmsted-parcels-runner] done.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
