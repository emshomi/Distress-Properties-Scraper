"""Standalone runner for the Washington Parcels foundation scraper.

Used by the GitHub Actions workflow at .github/workflows/washington-parcels.yml.

Loads the full Washington County tax-parcel roll (~118K records) into
core.parcels via the streaming run() (fetch-page -> parse -> write, one page at
a time). This is the spine the Washington foreclosure enrichment joins to.

Usage:
    python -m scripts.run_washington_parcels [trigger_name]

The trigger_name defaults to "github_actions" and is recorded in the scraper_runs
table for observability. Exits 0 on success/partial, 1 on a failed run.
"""
from __future__ import annotations
import asyncio
import sys
import traceback
from src.scrapers.washington_parcels import WashingtonParcelsScraper
from src.utils.logger import logger
async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"
    logger.info("Washington parcels runner starting", trigger=trigger)
    print(f"[washington-parcels-runner] trigger={trigger}", flush=True)
    scraper = WashingtonParcelsScraper()
    try:
        print("[washington-parcels-runner] run: streaming TaxParcel roll ...", flush=True)
        result = await scraper.run(trigger=trigger)
    except Exception as e:
        print(f"[washington-parcels-runner] run: FAILED — {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        return 1
    print(
        f"[washington-parcels-runner] run: {result.status} — "
        f"fetched={result.records_fetched} new={result.records_new} "
        f"failed={result.records_failed} "
        f"duration={round(result.duration_seconds, 1)}s",
        flush=True,
    )
    if result.status == "failed":
        print("[washington-parcels-runner] run failed — exit 1", flush=True)
        return 1
    print("[washington-parcels-runner] done.", flush=True)
    return 0
if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
