"""Standalone runner for the Washington Sheriff scraper.

Used by the GitHub Actions workflow at .github/workflows/washington-scrape.yml.

Washington County publishes completed sheriff's sales as monthly Excel files in
its Property Records & Taxpayer Services archive. This runner fetches the recent
monthly files, parses the per-property sale rows, and writes foreclosure events.

Usage:
    python -m scripts.run_washington_sheriff [trigger_name]

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
from src.scrapers.washington_sheriff import WashingtonSheriffScraper
from src.utils.logger import logger
async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"
    logger.info("Washington runner starting", trigger=trigger)
    print(f"[washington-runner] trigger={trigger}", flush=True)
    scraper = WashingtonSheriffScraper()
    # --- Fetch ---
    try:
        print("[washington-runner] fetch: contacting washingtoncountymn.gov archive ...", flush=True)
        raw_records = await scraper.fetch(trigger)
        print(f"[washington-runner] fetch: OK, got {len(raw_records)} raw rows", flush=True)
    except Exception as e:
        print(f"[washington-runner] fetch: FAILED — {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        return 1
    # --- Parse ---
    try:
        signals = await scraper.parse(raw_records)
        print(f"[washington-runner] parse: OK, produced {len(signals)} signals", flush=True)
    except Exception as e:
        print(f"[washington-runner] parse: FAILED — {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        return 1
    # --- Write ---
    try:
        new, updated, failed = await scraper.write(signals)
        print(
            f"[washington-runner] write: OK — new={new} updated={updated} failed={failed}",
            flush=True,
        )
    except Exception as e:
        print(f"[washington-runner] write: FAILED — {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        return 1
    if failed > 0:
        print(
            f"[washington-runner] completed with {failed} failed events — exit 1",
            flush=True,
        )
        return 1
    print("[washington-runner] done.", flush=True)
    return 0
if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
