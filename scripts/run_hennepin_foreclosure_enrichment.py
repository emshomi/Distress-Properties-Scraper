"""Standalone runner for the Hennepin foreclosure enrichment job.

Used by .github/workflows/hennepin-foreclosure-enrichment.yml. This is a
DATABASE job — it reads hennepin_sheriff events + the Hennepin parcel roll
already in core.parcels and UPDATES each event's raw_data with owner /
market value / homestead / absentee, matched by a unique normalized address.
No external HTTP source; pure internal join.

Usage:
    python -m scripts.run_hennepin_foreclosure_enrichment [trigger_name]

Exits 0 on success, 1 on failure.
"""

from __future__ import annotations

import asyncio
import sys
import traceback

from src.scrapers.hennepin_foreclosure_enrichment import (
    run_hennepin_foreclosure_enrichment,
)
from src.utils.logger import logger


async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"
    logger.info("Hennepin foreclosure enrichment runner starting", trigger=trigger)
    print(f"[hennepin-fc-enrich-runner] trigger={trigger}", flush=True)

    try:
        stats = await run_hennepin_foreclosure_enrichment()
    except Exception as e:
        print(
            f"[hennepin-fc-enrich-runner] FAILED — {type(e).__name__}: {e}",
            flush=True,
        )
        traceback.print_exc()
        return 1

    print(
        f"[hennepin-fc-enrich-runner] done — events={stats['events']} "
        f"enriched={stats['enriched']} no_match={stats['no_match']} "
        f"multi_match={stats['multi_match']} failed={stats['failed']}",
        flush=True,
    )

    # A per-row update failure shouldn't fail the whole run unless everything
    # failed (which would signal a systemic problem worth a red X).
    if stats["events"] > 0 and stats["enriched"] == 0 and stats["failed"] > 0:
        print(
            "[hennepin-fc-enrich-runner] all updates failed — exit 1",
            flush=True,
        )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
