"""Standalone runner for the Dakota Parcels foundation loader.

Used by the GitHub Actions workflow (step 1, before Dakota foreclosure
enrichment). Dakota's source is the county ArcGIS DCGIS_OL_PropertyInformation
server (clean JSON, the same server the working dakota_sheriff scraper already
pulls foreclosure layers from), so this runs pure-httpx — no Playwright.

IMPORTANT — why this calls .run() and not fetch()/parse()/write():
DakotaParcelsScraper OVERRIDES run() with a STREAMING loop
(fetch-page -> write-page -> discard) so it never holds all ~150K parcels in
memory at once. Calling fetch() directly would load the entire dataset into
RAM — the exact problem the streaming run avoids. So we invoke run(), which
also means the scraper's feature flag (SCRAPER_DAKOTA_PARCELS_ENABLED) must be
enabled in the workflow env.

Usage:
    python -m scripts.run_dakota_parcels [trigger_name]

Exits 0 on success, 1 on failure / disabled / partial, so GitHub Actions
marks the run correctly.
"""

from __future__ import annotations

import asyncio
import sys
import traceback

# Import at top level so a missing env var / config error fails fast.
from src.scrapers.dakota_parcels import DakotaParcelsScraper
from src.utils.logger import logger


async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"
    logger.info("Dakota parcels runner starting", trigger=trigger)
    print(f"[dakota-parcels-runner] trigger={trigger}", flush=True)

    scraper = DakotaParcelsScraper()

    try:
        print(
            "[dakota-parcels-runner] run: streaming load from "
            "gis2.co.dakota.mn.us ArcGIS (Tax Parcels layer 71) ...",
            flush=True,
        )
        # trigger="manual" makes a disabled flag raise (loud failure) rather
        # than silently skip — we WANT the run to fail visibly if the flag
        # isn't set, instead of quietly doing nothing.
        result = await scraper.run(trigger="manual", metadata={"trigger_source": trigger})
    except Exception as e:
        print(
            f"[dakota-parcels-runner] run: FAILED — {type(e).__name__}: {e}",
            flush=True,
        )
        traceback.print_exc()
        return 1

    print(
        f"[dakota-parcels-runner] run: status={result.status} "
        f"fetched={result.records_fetched} new={result.records_new} "
        f"failed={result.records_failed}",
        flush=True,
    )

    if result.status in ("failed", "skipped"):
        print(
            f"[dakota-parcels-runner] non-success status "
            f"({result.status}) — exit 1: {result.error_message}",
            flush=True,
        )
        return 1

    print("[dakota-parcels-runner] done.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
