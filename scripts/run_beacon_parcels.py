"""
Runner for the Beacon/Schneider parcel loader.

MUST RUN FROM A RESIDENTIAL IP. beacon.schneidercorp.com returns bot
detection to datacenter egress — verified both directions 2026-09-01, where
an automated fetch was blocked and the same URL rendered normally in a
browser minutes later. So this runs from the Windows box, like
govire_mnpn_browser.py, and NOT from Railway or GitHub Actions.

Usage:
    python scripts/run_beacon_parcels.py --county blue_earth --max 25
    python scripts/run_beacon_parcels.py --county blue_earth

--max caps the work list. Use it for the first run against any new county so
a parser fault costs 25 requests instead of 2,991.

trigger='manual' deliberately: BaseScraper.run() RAISES ScraperDisabledError
on a manual trigger and silently skips on a scheduled one. A disabled flag
should be loud here, not a green run that did nothing.
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from src.scrapers.beacon_parcels import BeaconParcelsScraper
from src.utils.logger import logger


async def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Beacon parcel loader")
    parser.add_argument(
        "--county",
        required=True,
        help="county_code of an ENABLED core.vendor_portals row (vendor='beacon')",
    )
    parser.add_argument(
        "--max",
        type=int,
        default=None,
        help="cap the work list; use for a first run against a new county",
    )
    args = parser.parse_args()

    portals = BeaconParcelsScraper.load_enabled_portals()
    row = next(
        (p for p in portals if p.get("county_code") == args.county), None
    )
    if row is None:
        # Never fall back to a default portal: a missing or disabled row must
        # not resolve to Blue Earth's AppID and scrape Blue Earth under
        # another county's name.
        logger.error(
            "No ENABLED beacon portal for this county",
            county_code=args.county,
            enabled_counties=[p.get("county_code") for p in portals],
        )
        return 2

    scraper = BeaconParcelsScraper.from_portal_row(row)
    scraper._max_parcels = args.max

    result = await scraper.run(trigger="manual")

    logger.info(
        "Beacon parcel run complete",
        county_code=args.county,
        source=result.scraper_name,
        status=result.status,
        run_id=result.run_id,
        duration_seconds=round(result.duration_seconds, 1),
        records_fetched=result.records_fetched,
        records_new=result.records_new,
        records_updated=result.records_updated,
        records_failed=result.records_failed,
        error=result.error_message,
    )
    return 0 if result.status in ("success", "partial") else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
