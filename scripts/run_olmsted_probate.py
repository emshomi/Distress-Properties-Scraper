"""Standalone runner for the Olmsted probate-notice scraper.

Used by the GitHub Actions workflow at .github/workflows/olmsted-probate.yml.
Fetches 365 days of Post Bulletin notices from the Column public-notices
API, classifies PROBATE DIVISION filings on TEXT rather than on Column's
noticetype (two real estates in the last year carried "Notice to
Creditors" and "" respectively), parses decedent / personal
representatives / case number, matches the decedent against core.owners
for Olmsted (word-boundary + middle-initial rules), and writes
probate_filing events for MATCHED parcels only.

Same shape as run_fillmore_probate.py, three differences worth knowing:

  - SOURCE is Column, not WordPress. Fillmore's paper runs WordPress;
    the Post Bulletin publishes through Column, the same endpoint
    postbulletin_legal already uses.
  - MATCHING reads core.owners. fillmore_probate reads
    core.parcels.raw_data->>'OWNERNAME', which is NULL on all 75,039
    Olmsted parcels (measured 2026-08-19). core.owners has one row per
    Olmsted parcel, 100% coverage, all joining on (county_code,
    parcel_id).
  - WINDOW is 365 days, not 90. An estate administers over a year and
    the dedup key makes a wide window free.

Calls .run() (not fetch()/parse()/write() directly) so the feature flag
SCRAPER_OLMSTED_PROBATE_ENABLED is honored and behavior matches the API
trigger path exactly. Writes are dedup-idempotent — re-running refreshes.

Usage:
    python -m scripts.run_olmsted_probate [trigger_name]

Exits 0 on success, 1 on failure / disabled.
"""

from __future__ import annotations

import asyncio
import sys
import traceback

# Import at top level so a missing env var / config error fails fast.
from src.scrapers.olmsted_probate import OlmstedProbateScraper
from src.utils.logger import logger


async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"
    logger.info("Olmsted probate runner starting", trigger=trigger)
    print(f"[olmsted-probate-runner] trigger={trigger}", flush=True)

    scraper = OlmstedProbateScraper()

    try:
        print(
            "[olmsted-probate-runner] run: fetching Post Bulletin notices "
            "via Column (365-day window) ...",
            flush=True,
        )
        # trigger="manual" => a disabled flag raises (loud) instead of
        # silently skipping, so a misconfigured flag fails the Actions run
        # visibly.
        result = await scraper.run(
            trigger="manual", metadata={"trigger_source": trigger}
        )
    except Exception as e:
        print(
            f"[olmsted-probate-runner] run: FAILED — {type(e).__name__}: {e}",
            flush=True,
        )
        traceback.print_exc()
        return 1

    print(
        f"[olmsted-probate-runner] run: status={result.status} "
        f"fetched={result.records_fetched} new={result.records_new} "
        f"failed={result.records_failed}",
        flush=True,
    )

    # records_fetched counts NOTICES SEEN, not estates matched. The
    # scraper's own parse log carries estates_seen / estates_no_match /
    # skipped_not_probate, and those are the numbers that say whether a
    # run was useful: ~1,191 Post Bulletin notices a year, of which
    # probate is maybe 80-120, of which only the ones whose decedent owns
    # an Olmsted parcel produce an event. A run that fetches 1,000 and
    # writes 3 is working correctly.
    if result.records_fetched and not result.records_new:
        print(
            "[olmsted-probate-runner] note: notices fetched but no new "
            "events — either every matched estate is already recorded "
            "(dedup) or no decedent in the window owns an Olmsted parcel. "
            "See estates_seen / estates_no_match in the run log.",
            flush=True,
        )

    if result.status in ("failed", "skipped"):
        print(
            f"[olmsted-probate-runner] non-success status "
            f"({result.status}) — exit 1: {result.error_message}",
            flush=True,
        )
        return 1

    print("[olmsted-probate-runner] done.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
