"""Standalone runner for the Anoka County parcels loader.

Used by the GitHub Actions workflow at .github/workflows/anoka-parcels.yml.
Streams the full Anoka parcel spine (139,930 rows, verified live
2026-07-28) from the MnGeo statewide MNGAC open-parcels layer — filtered to
co_code = '27003' — into core.parcels, with the core.owners projection
riding alongside.

THE NINTH COUNTY. Before this loader, core.parcels held 192 Anoka rows,
every one a synthetic ANOKA-FC-* placeholder created when a foreclosure
signal could not resolve to a real parcel: a top-five metro foreclosure
county with no spine, no EMV, no equity math, and no eCRV outcome
confirmation. Anoka's MNGAC acqdate is 2026-04-27 — one day before the
compile, the freshest tier in the aggregate — so this is a legitimate
primary source, not an expansion compromise.

Calls .run() (not fetch()/parse()/write() directly) so the feature flag
SCRAPER_ANOKA_PARCELS_ENABLED is honored and behavior matches the API
trigger path exactly. Upserts are idempotent — re-running refreshes.

Usage:
    python -m scripts.run_anoka_parcels [trigger_name]

Exits 0 on success, 1 on failure / disabled.
"""

from __future__ import annotations

import asyncio
import sys
import traceback

# Import at top level so a missing env var / config error fails fast.
from src.scrapers.mngac_parcels import AnokaParcelsScraper
from src.utils.logger import logger


async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"
    logger.info("Anoka parcels runner starting", trigger=trigger)
    print(f"[anoka-parcels-runner] trigger={trigger}", flush=True)

    scraper = AnokaParcelsScraper()

    try:
        print(
            "[anoka-parcels-runner] run: streaming the Anoka parcel "
            "spine (~139.9K rows, keyset-paged) from the MNGAC statewide "
            "layer ...",
            flush=True,
        )
        # trigger="manual" => a disabled flag raises (loud) instead of
        # silently skipping, so a misconfigured flag fails the Actions run
        # visibly.
        result = await scraper.run(trigger="manual", metadata={"trigger_source": trigger})
    except Exception as e:
        print(
            f"[anoka-parcels-runner] run: FAILED — {type(e).__name__}: {e}",
            flush=True,
        )
        traceback.print_exc()
        return 1

    print(
        f"[anoka-parcels-runner] run: status={result.status} "
        f"fetched={result.records_fetched} new={result.records_new} "
        f"failed={result.records_failed}",
        flush=True,
    )

    if result.status in ("failed", "skipped"):
        print(
            f"[anoka-parcels-runner] non-success status "
            f"({result.status}) — exit 1: {result.error_message}",
            flush=True,
        )
        return 1

    print("[anoka-parcels-runner] done.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
