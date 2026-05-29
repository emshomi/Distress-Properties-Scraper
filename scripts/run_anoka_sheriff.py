"""Standalone runner for the Anoka Sheriff scraper.

Used by the GitHub Actions workflow at .github/workflows/anoka-scrape.yml.
The point of running this from GitHub Actions (rather than Railway's scheduler)
is that GitHub's outbound IPs are different from Railway's, which lets us bypass
the block we hit when trying to scrape Anoka from Railway.

Usage:
    python -m scripts.run_anoka_sheriff [trigger_name]

The trigger_name defaults to "github_actions" and is recorded in the scraper_runs
table for observability. The script exits with code 0 on success, 1 on any failure
during fetch/parse/write so GitHub Actions correctly marks the run as failed.
"""
from __future__ import annotations

import asyncio
import sys
import traceback

# Import is at top level so a missing env var / config error fails fast
# (before we waste time on imports inside main()).
from src.scrapers.anoka_sheriff import AnokaSheriffScraper
from src.utils.logger import logger


async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"

    logger.info("Anoka runner starting", trigger=trigger)
    print(f"[anoka-runner] trigger={trigger}", flush=True)

    scraper = AnokaSheriffScraper()

    # --- Fetch ---
    try:
        print("[anoka-runner] fetch: contacting foreclosures.co.anoka.mn.us ...", flush=True)
        raw_records = await scraper.fetch(trigger)
        print(f"[anoka-runner] fetch: OK, got {len(raw_records)} raw rows", flush=True)
    except Exception as e:
        print(f"[anoka-runner] fetch: FAILED — {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        return 1

    # --- Parse ---
    try:
        signals = await scraper.parse(raw_records)
        print(f"[anoka-runner] parse: OK, produced {len(signals)} signals", flush=True)
    except Exception as e:
        print(f"[anoka-runner] parse: FAILED — {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        return 1

    # --- Write ---
    try:
        new, updated, failed = await scraper.write(signals)
        print(
            f"[anoka-runner] write: OK — new={new} updated={updated} failed={failed}",
            flush=True,
        )
    except Exception as e:
        print(f"[anoka-runner] write: FAILED — {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        return 1

    if failed > 0:
        print(
            f"[anoka-runner] completed with {failed} failed events — exit 1",
            flush=True,
        )
        return 1

    print("[anoka-runner] done.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
