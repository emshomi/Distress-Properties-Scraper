"""Standalone runner for the Hennepin tax-roll (forfeited land) miner.

Used by the GitHub Actions workflow at
.github/workflows/hennepin-tax-roll-scrape.yml.

This is a DATABASE MINING job, not a web scraper — it reads the Hennepin
parcels already in core.parcels (where FORFEIT_LAND_IND = 'T') and derives
hennepin_tax_roll tax-forfeit signals from them. So it depends on the
hennepin_parcels scraper having populated core.parcels first; if parcels are
stale, re-run hennepin_parcels before this.

Usage:
    python -m scripts.run_hennepin_tax_roll [trigger_name]

The trigger_name defaults to "github_actions" and is recorded for
observability. Exits 0 on success, 1 on any fetch/parse/write failure so
GitHub Actions marks the run failed.
"""
from __future__ import annotations

import asyncio
import sys
import traceback

# Import at top level so a missing env var / config error fails fast.
from src.scrapers.hennepin_tax_roll import HennepinTaxRollScraper
from src.services import source_health_tracker
from src.utils.logger import logger


async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"

    logger.info("Hennepin tax-roll runner starting", trigger=trigger)
    print(f"[hennepin-tax-roll-runner] trigger={trigger}", flush=True)

    scraper = HennepinTaxRollScraper()

    # --- Fetch (reads core.parcels) ---
    try:
        print(
            "[hennepin-tax-roll-runner] fetch: querying core.parcels for "
            "forfeited land (FORFEIT_LAND_IND='T') ...",
            flush=True,
        )
        raw_records = await scraper.fetch(trigger)
        print(
            f"[hennepin-tax-roll-runner] fetch: OK, "
            f"{len(raw_records)} forfeited parcels",
            flush=True,
        )
    except Exception as e:
        print(
            f"[hennepin-tax-roll-runner] fetch: FAILED — {type(e).__name__}: {e}",
            flush=True,
        )
        traceback.print_exc()
        source_health_tracker.record_failure(
            scraper.source_name, notes=f"fetch failed: {type(e).__name__}: {e}"[:500]
        )
        return 1

    # --- Parse ---
    try:
        signals = await scraper.parse(raw_records)
        print(
            f"[hennepin-tax-roll-runner] parse: OK, produced {len(signals)} signals",
            flush=True,
        )
    except Exception as e:
        print(
            f"[hennepin-tax-roll-runner] parse: FAILED — {type(e).__name__}: {e}",
            flush=True,
        )
        traceback.print_exc()
        source_health_tracker.record_failure(
            scraper.source_name, notes=f"parse failed: {type(e).__name__}: {e}"[:500]
        )
        return 1

    # --- Write ---
    try:
        new, updated, failed = await scraper.write(signals)
        print(
            f"[hennepin-tax-roll-runner] write: OK — "
            f"new={new} updated={updated} failed={failed}",
            flush=True,
        )
    except Exception as e:
        print(
            f"[hennepin-tax-roll-runner] write: FAILED — {type(e).__name__}: {e}",
            flush=True,
        )
        traceback.print_exc()
        source_health_tracker.record_failure(
            scraper.source_name, notes=f"write failed: {type(e).__name__}: {e}"[:500]
        )
        return 1

    if failed > 0:
        print(
            f"[hennepin-tax-roll-runner] completed with {failed} failed events — exit 1",
            flush=True,
        )
        source_health_tracker.record_failure(
            scraper.source_name,
            notes=f"{failed} of {new + updated + failed} record writes failed",
        )
        return 1

    source_health_tracker.record_success(scraper.source_name)
    print(
        f"[hennepin-tax-roll-runner] done. (health: success recorded, "
        f"new={new} updated={updated})",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
