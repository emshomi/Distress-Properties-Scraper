"""Standalone runner for the Washington Parcels foundation scraper.

Used by the GitHub Actions workflow at .github/workflows/washington-parcels.yml.

Loads the full Washington County tax-parcel roll (~118K records) into
core.parcels via the streaming run() (fetch-page -> parse -> write, one page at
a time). This is the spine the Washington foreclosure enrichment joins to.

Usage:
    python -m scripts.run_washington_parcels [trigger_name] [max_records]

The trigger_name defaults to "github_actions" and is recorded in the scraper_runs
table for observability. Exits 0 on success/partial, 1 on a failed run.

max_records (optional, ADDED 2026-08-13) caps the run at N parcels. A full pass
is ~118K records and takes the better part of the workflow's timeout, which is
far too slow a feedback loop for verifying a change to this loader: if the
centroid handling is wrong, every one of those rows lands with lat=None and the
run still reports success. A 200-record run answers the same question in under
a minute, and the answer is read from the DATABASE, not the run status.
"""
from __future__ import annotations
import asyncio
import sys
import traceback
from src.scrapers.washington_parcels import WashingtonParcelsScraper
from src.utils.logger import logger
async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"

    # Optional record cap. Anything unparseable (including the empty string a
    # skipped workflow input produces) means NO cap — a test flag must never
    # silently truncate a production run.
    max_records: int | None = None
    if len(sys.argv) > 2:
        try:
            parsed = int(sys.argv[2])
            max_records = parsed if parsed > 0 else None
        except (TypeError, ValueError):
            max_records = None

    logger.info(
        "Washington parcels runner starting",
        trigger=trigger,
        max_records=max_records,
    )
    print(
        f"[washington-parcels-runner] trigger={trigger} "
        f"max_records={max_records or 'ALL'}",
        flush=True,
    )
    scraper = WashingtonParcelsScraper()
    if max_records is not None:
        # Set on the INSTANCE, not the class — BaseArcGISScraper documents it
        # that way, and a class-level set would leak into any other scraper
        # constructed in the same process.
        scraper._max_records_override = max_records
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
