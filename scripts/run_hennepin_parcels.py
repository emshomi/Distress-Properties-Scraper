"""Standalone runner for the Hennepin Parcels foundation scraper.

Used by the GitHub Actions workflow at .github/workflows/hennepin-parcels.yml
(quarterly: 4th of Mar/Jun/Sep/Dec — the ~448K-parcel Hennepin roll is the
platform's geographic backbone; assessor values and owners change slowly).

HISTORY: the roll was loaded once on 2026-05-28 and never scheduled; the
health digest flagged it 40 days stale on 2026-07-07. This runner + workflow
+ the expected_interval_days=92 cadence row are that fix.

Usage:
    python -m scripts.run_hennepin_parcels [trigger_name]

The trigger_name defaults to "github_actions" and is recorded in the
scraper_runs table for observability. Exits 0 on success/partial, 1 on a
failed run.
"""
from __future__ import annotations
import asyncio
import sys
import traceback
from src.scrapers.hennepin_parcels import HennepinParcelsScraper
from src.utils.logger import logger
async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"
    logger.info("Hennepin parcels runner starting", trigger=trigger)
    print(f"[hennepin-parcels-runner] trigger={trigger}", flush=True)
    scraper = HennepinParcelsScraper()
    try:
        print("[hennepin-parcels-runner] run: streaming parcel roll ...", flush=True)
        result = await scraper.run(trigger=trigger)
    except Exception as e:
        print(f"[hennepin-parcels-runner] run: FAILED — {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        return 1
    print(
        f"[hennepin-parcels-runner] run: {result.status} — "
        f"fetched={result.records_fetched} new={result.records_new} "
        f"failed={result.records_failed} "
        f"duration={round(result.duration_seconds, 1)}s",
        flush=True,
    )
    if result.status == "failed":
        print("[hennepin-parcels-runner] run failed — exit 1", flush=True)
        return 1
    print("[hennepin-parcels-runner] done.", flush=True)
    return 0
if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
