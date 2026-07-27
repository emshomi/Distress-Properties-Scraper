"""Standalone runner for the Wabasha County parcels loader.

Used by the GitHub Actions workflow at .github/workflows/wabasha-parcels.yml.
Streams the full Wabasha parcel spine (17,323 rows, verified live
2026-07-27) from the MnGeo statewide MNGAC open-parcels layer — filtered to
co_code = '27157' — into core.parcels, with the core.owners projection
riding alongside.

The eighth county, and the FIRST onboarded through the statewide aggregate
rather than a county-operated GIS server. The loader itself
(src/scrapers/mngac_parcels.py) is generic: every additional
gac_open_approval='true' county is a subclass plus a config flag, not a new
scraper.

Calls .run() (not fetch()/parse()/write() directly) so the feature flag
SCRAPER_WABASHA_PARCELS_ENABLED is honored and behavior matches the API
trigger path exactly. Upserts are idempotent — re-running refreshes.

Usage:
    python -m scripts.run_wabasha_parcels [trigger_name]

Exits 0 on success, 1 on failure / disabled.
"""

from __future__ import annotations

import asyncio
import sys
import traceback

# Import at top level so a missing env var / config error fails fast.
from src.scrapers.mngac_parcels import WabashaParcelsScraper
from src.utils.logger import logger


async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"
    logger.info("Wabasha parcels runner starting", trigger=trigger)
    print(f"[wabasha-parcels-runner] trigger={trigger}", flush=True)

    scraper = WabashaParcelsScraper()

    try:
        print(
            "[wabasha-parcels-runner] run: streaming the Wabasha parcel "
            "spine (~17.3K rows, keyset-paged) from the MNGAC statewide "
            "layer ...",
            flush=True,
        )
        # trigger="manual" => a disabled flag raises (loud) instead of
        # silently skipping, so a misconfigured flag fails the Actions run
        # visibly.
        result = await scraper.run(trigger="manual", metadata={"trigger_source": trigger})
    except Exception as e:
        print(
            f"[wabasha-parcels-runner] run: FAILED — {type(e).__name__}: {e}",
            flush=True,
        )
        traceback.print_exc()
        return 1

    print(
        f"[wabasha-parcels-runner] run: status={result.status} "
        f"fetched={result.records_fetched} new={result.records_new} "
        f"failed={result.records_failed}",
        flush=True,
    )

    if result.status in ("failed", "skipped"):
        print(
            f"[wabasha-parcels-runner] non-success status "
            f"({result.status}) — exit 1: {result.error_message}",
            flush=True,
        )
        return 1

    print("[wabasha-parcels-runner] done.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
