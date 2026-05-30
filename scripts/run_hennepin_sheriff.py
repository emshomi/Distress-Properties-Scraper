"""Standalone runner for the Hennepin Sheriff scraper.

Used by the GitHub Actions workflow at .github/workflows/hennepin-scrape.yml.
Running from GitHub Actions keeps it consistent with the other sheriff
scrapers; Hennepin's API is clean JSON (no IP block, no browser needed), so
this could also run from Railway, but we keep all sheriff scrapers on the
same GitHub Actions cadence for uniform observability.

Usage:
    python -m scripts.run_hennepin_sheriff [trigger_name]

The trigger_name defaults to "github_actions" and is recorded for
observability. The script exits with code 0 on success, 1 on any failure
during fetch/parse/write so GitHub Actions correctly marks the run as failed.
"""
from __future__ import annotations

import asyncio
import sys
import traceback

# Import is at top level so a missing env var / config error fails fast
# (before we waste time on imports inside main()).
from src.scrapers.hennepin_sheriff import HennepinSheriffScraper
from src.utils.logger import logger


async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"

    logger.info("Hennepin runner starting", trigger=trigger)
    print(f"[hennepin-runner] trigger={trigger}", flush=True)

    scraper = HennepinSheriffScraper()

    # --- Fetch ---
    try:
        print("[hennepin-runner] fetch: contacting api.hennepincounty.gov ...", flush=True)
        raw_records = await scraper.fetch(trigger)
        print(f"[hennepin-runner] fetch: OK, got {len(raw_records)} raw rows", flush=True)
    except Exception as e:
        print(f"[hennepin-runner] fetch: FAILED — {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        return 1

    # --- Parse ---
    try:
        signals = await scraper.parse(raw_records)
        print(f"[hennepin-runner] parse: OK, produced {len(signals)} signals", flush=True)
    except Exception as e:
        print(f"[hennepin-runner] parse: FAILED — {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        return 1

    # --- Write ---
    try:
        new, updated, failed = await scraper.write(signals)
        print(
            f"[hennepin-runner] write: OK — new={new} updated={updated} failed={failed}",
            flush=True,
        )
    except Exception as e:
        print(f"[hennepin-runner] write: FAILED — {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        return 1

    if failed > 0:
        print(
            f"[hennepin-runner] completed with {failed} failed events — exit 1",
            flush=True,
        )
        return 1

    print("[hennepin-runner] done.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
