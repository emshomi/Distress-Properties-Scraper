"""Standalone runner for the Fillmore County Journal legal-notices scraper.

Used by the GitHub Actions workflow at .github/workflows/fillmore-legal.yml.
Fetches the last 45 days of "Legal Notice" posts from the Journal's
WordPress REST API, parses foreclosure/sheriff-sale notices (Fillmore-
gated, PIN-required), and writes scheduled sheriff_sale events — the
Chatfield-corridor expansion's first signal source (2026-07-23).

Calls .run() (not fetch()/parse()/write() directly) so the feature flag
SCRAPER_FILLMORE_LEGAL_ENABLED is honored and behavior matches the API
trigger path exactly. Writes are dedup-idempotent — re-running refreshes.

Usage:
    python -m scripts.run_fillmore_legal [trigger_name]

Exits 0 on success, 1 on failure / disabled.
"""

from __future__ import annotations

import asyncio
import sys
import traceback

# Import at top level so a missing env var / config error fails fast.
from src.scrapers.fillmore_legal import FillmoreLegalScraper
from src.utils.logger import logger


async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"
    logger.info("Fillmore legal runner starting", trigger=trigger)
    print(f"[fillmore-legal-runner] trigger={trigger}", flush=True)

    scraper = FillmoreLegalScraper()

    try:
        print(
            "[fillmore-legal-runner] run: fetching Fillmore County Journal "
            "legal notices (45-day window) ...",
            flush=True,
        )
        # trigger="manual" => a disabled flag raises (loud) instead of
        # silently skipping, so a misconfigured flag fails the Actions run
        # visibly.
        result = await scraper.run(trigger="manual", metadata={"trigger_source": trigger})
    except Exception as e:
        print(
            f"[fillmore-legal-runner] run: FAILED — {type(e).__name__}: {e}",
            flush=True,
        )
        traceback.print_exc()
        return 1

    print(
        f"[fillmore-legal-runner] run: status={result.status} "
        f"fetched={result.records_fetched} new={result.records_new} "
        f"failed={result.records_failed}",
        flush=True,
    )

    if result.status in ("failed", "skipped"):
        print(
            f"[fillmore-legal-runner] non-success status "
            f"({result.status}) — exit 1: {result.error_message}",
            flush=True,
        )
        return 1

    print("[fillmore-legal-runner] done.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
