"""Standalone runner for the Olmsted Tyler-portal tax-detail scraper.

Used by the GitHub Actions workflow at
.github/workflows/olmsted-tax-detail.yml. Visits the county's iasWorld
portal per parcel on the annual delinquent list and writes per-year
delinquency detail + computed forfeiture-clock status + owner mailing
addresses (see the scraper docstring for the full recon record).

Usage:
    python -m scripts.run_olmsted_tax_detail [trigger_name] [pins_csv]

    pins_csv (optional): comma-separated PARIDs — the test path. When
    given, ONLY those parcels are scraped (the 5-PIN verification run).
    When omitted, the full olmsted_delq_list set (~502 parcels) runs.

Exits 0 on success, 1 on failure / disabled. A 'partial' result (some
parcels not found in the portal) exits 0 but logs the count — the list
and the portal legitimately drift as owners redeem and parcels retire.
"""

from __future__ import annotations

import asyncio
import sys
import traceback

# Import at top level so a missing env var / config error fails fast.
from src.scrapers.olmsted_tax_detail import OlmstedTaxDetailScraper
from src.utils.logger import logger


async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"
    pins_csv = sys.argv[2] if len(sys.argv) > 2 else ""
    pins = [p.strip() for p in pins_csv.split(",") if p.strip()] or None

    logger.info(
        "Olmsted tax-detail runner starting", trigger=trigger,
        test_pins=len(pins) if pins else 0,
    )
    print(
        f"[olmsted-tax-detail-runner] trigger={trigger}"
        + (f" TEST MODE pins={len(pins)}" if pins else " full-list mode"),
        flush=True,
    )

    scraper = OlmstedTaxDetailScraper(pins=pins)

    try:
        print(
            "[olmsted-tax-detail-runner] run: scraping iasWorld portal "
            "(publicaccess.co.olmsted.mn.us) ...",
            flush=True,
        )
        # trigger="manual" => a disabled flag raises (loud) instead of
        # silently skipping, so a misconfigured flag fails the Actions
        # run visibly.
        result = await scraper.run(
            trigger="manual", metadata={"trigger_source": trigger}
        )
    except Exception as e:
        print(
            f"[olmsted-tax-detail-runner] run: FAILED — "
            f"{type(e).__name__}: {e}",
            flush=True,
        )
        traceback.print_exc()
        return 1

    print(
        f"[olmsted-tax-detail-runner] run: status={result.status} "
        f"fetched={result.records_fetched} new={result.records_new} "
        f"failed={result.records_failed}",
        flush=True,
    )

    if result.status in ("failed", "skipped"):
        print(
            f"[olmsted-tax-detail-runner] non-success status "
            f"({result.status}) — exit 1: {result.error_message}",
            flush=True,
        )
        return 1

    if result.status == "partial":
        print(
            "[olmsted-tax-detail-runner] PARTIAL: some parcels were not "
            "found in the portal or failed to write — see the log counts. "
            "Exiting 0 (list/portal drift is expected state).",
            flush=True,
        )

    print("[olmsted-tax-detail-runner] done.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
