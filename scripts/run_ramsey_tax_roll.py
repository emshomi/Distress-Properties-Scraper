"""Standalone runner for the Ramsey Tax-Roll miner.

Used by the GitHub Actions workflow at .github/workflows/ramsey-tax.yml
(step 2 of 2, AFTER run_ramsey_parcels refreshes the spine). This is a
DATABASE MINING job — it reads the Ramsey parcels in core.parcels and writes
special-assessment-burden distress events. No external HTTP source.

Calls .run() (not fetch()/parse()/write() directly) so the feature flag
SCRAPER_RAMSEY_TAX_ROLL_ENABLED is honored and behavior matches the API
trigger path exactly. The write is idempotent (write_events_dedup with a
stable sentinel event_date), so re-running each month only adds genuinely
new qualifying parcels.

Usage:
    python -m scripts.run_ramsey_tax_roll [trigger_name]

Exits 0 on success, 1 on failure / disabled / partial.
"""

from __future__ import annotations

import asyncio
import sys
import traceback

# Import at top level so a missing env var / config error fails fast.
from src.scrapers.ramsey_tax_roll import RamseyTaxRollScraper
from src.utils.logger import logger


async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"
    logger.info("Ramsey tax-roll runner starting", trigger=trigger)
    print(f"[ramsey-tax-roll-runner] trigger={trigger}", flush=True)

    scraper = RamseyTaxRollScraper()

    try:
        print(
            "[ramsey-tax-roll-runner] run: mining core.parcels for Ramsey "
            "special-assessment burdens ...",
            flush=True,
        )
        # trigger="manual" => a disabled flag raises (loud) instead of silently
        # skipping, so a misconfigured flag fails the Actions run visibly.
        result = await scraper.run(trigger="manual", metadata={"trigger_source": trigger})
    except Exception as e:
        print(
            f"[ramsey-tax-roll-runner] run: FAILED — {type(e).__name__}: {e}",
            flush=True,
        )
        traceback.print_exc()
        return 1

    print(
        f"[ramsey-tax-roll-runner] run: status={result.status} "
        f"fetched={result.records_fetched} new={result.records_new} "
        f"failed={result.records_failed}",
        flush=True,
    )

    if result.status in ("failed", "skipped"):
        print(
            f"[ramsey-tax-roll-runner] non-success status "
            f"({result.status}) — exit 1: {result.error_message}",
            flush=True,
        )
        return 1

    print("[ramsey-tax-roll-runner] done.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
