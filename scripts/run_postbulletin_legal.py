"""Standalone runner for the Post Bulletin legal-notices scraper.

Used by the GitHub Actions workflow at
.github/workflows/postbulletin-legal.yml. Fetches Rochester Post Bulletin
foreclosure notices from the Column public-notices API and writes
scheduled Olmsted sheriff-sale events — the Olmsted pilot's first live
signal source. Zero notices in the window is honest state (small county)
and exits 0. The write is idempotent (write_events_dedup), so re-running
only adds genuinely new notices.

Usage:
    python -m scripts.run_postbulletin_legal [trigger_name]

Exits 0 on success, 1 on failure / disabled / partial.
"""

from __future__ import annotations

import asyncio
import sys
import traceback

# Import at top level so a missing env var / config error fails fast.
from src.scrapers.postbulletin_legal import PostBulletinLegalScraper
from src.utils.logger import logger


async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"
    logger.info("Post Bulletin legal runner starting", trigger=trigger)
    print(f"[postbulletin-legal-runner] trigger={trigger}", flush=True)

    scraper = PostBulletinLegalScraper()

    try:
        print(
            "[postbulletin-legal-runner] run: fetching Post Bulletin "
            "foreclosure notices (Column API) ...",
            flush=True,
        )
        # trigger="manual" => a disabled flag raises (loud) instead of
        # silently skipping, so a misconfigured flag fails the Actions run
        # visibly.
        result = await scraper.run(trigger="manual", metadata={"trigger_source": trigger})
    except Exception as e:
        print(
            f"[postbulletin-legal-runner] run: FAILED — {type(e).__name__}: {e}",
            flush=True,
        )
        traceback.print_exc()
        return 1

    print(
        f"[postbulletin-legal-runner] run: status={result.status} "
        f"fetched={result.records_fetched} new={result.records_new} "
        f"failed={result.records_failed}",
        flush=True,
    )

    if result.status in ("failed", "skipped"):
        print(
            f"[postbulletin-legal-runner] non-success status "
            f"({result.status}) — exit 1: {result.error_message}",
            flush=True,
        )
        return 1

    print("[postbulletin-legal-runner] done.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
