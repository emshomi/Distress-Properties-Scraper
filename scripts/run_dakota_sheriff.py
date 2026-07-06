"""Standalone runner for the Dakota Sheriff scraper.

Used by the GitHub Actions workflow at .github/workflows/dakota-scrape.yml.
Dakota's source is the county ArcGIS server (clean JSON, no bot resistance),
so this runs pure-httpx like Hennepin — no Playwright needed. We keep all
sheriff scrapers on the same GitHub Actions cadence for uniform observability.

Usage:
    python -m scripts.run_dakota_sheriff [trigger_name]

The trigger_name defaults to "github_actions" and is recorded for
observability. The script exits with code 0 on success, 1 on any failure
during fetch/parse/write so GitHub Actions correctly marks the run as failed.
"""
from __future__ import annotations

import asyncio
import sys
import traceback

# Import at top level so a missing env var / config error fails fast.
from src.scrapers.dakota_sheriff import DakotaSheriffScraper
from src.services import source_health_tracker
from src.utils.logger import logger


async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"

    logger.info("Dakota runner starting", trigger=trigger)
    print(f"[dakota-runner] trigger={trigger}", flush=True)

    scraper = DakotaSheriffScraper()

    # --- Fetch ---
    try:
        print("[dakota-runner] fetch: contacting gis2.co.dakota.mn.us ...", flush=True)
        raw_records = await scraper.fetch(trigger)
        print(f"[dakota-runner] fetch: OK, got {len(raw_records)} raw features", flush=True)
    except Exception as e:
        print(f"[dakota-runner] fetch: FAILED — {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        source_health_tracker.record_failure(
            scraper.source_name, notes=f"fetch failed: {type(e).__name__}: {e}"[:500]
        )
        return 1

    # --- Parse ---
    try:
        signals = await scraper.parse(raw_records)
        print(f"[dakota-runner] parse: OK, produced {len(signals)} signals", flush=True)
    except Exception as e:
        print(f"[dakota-runner] parse: FAILED — {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        source_health_tracker.record_failure(
            scraper.source_name, notes=f"parse failed: {type(e).__name__}: {e}"[:500]
        )
        return 1

    # --- Write ---
    try:
        new, updated, failed = await scraper.write(signals)
        print(
            f"[dakota-runner] write: OK — new={new} updated={updated} failed={failed}",
            flush=True,
        )
    except Exception as e:
        print(f"[dakota-runner] write: FAILED — {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        source_health_tracker.record_failure(
            scraper.source_name, notes=f"write failed: {type(e).__name__}: {e}"[:500]
        )
        return 1

    if failed > 0:
        print(
            f"[dakota-runner] completed with {failed} failed events — exit 1",
            flush=True,
        )
        source_health_tracker.record_failure(
            scraper.source_name,
            notes=f"{failed} of {new + updated + failed} record writes failed",
        )
        return 1

    # Clean run: record success. This is the call the standalone runners were
    # missing -- without it, source_health froze at whenever the scraper last
    # went through BaseScraper.run(), even though the daily Actions run works.
    # record_success clears notes, so a prior stale failure note is cleared too.
    source_health_tracker.record_success(scraper.source_name)
    print(
        f"[dakota-runner] done. (health: success recorded, "
        f"new={new} updated={updated})",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
