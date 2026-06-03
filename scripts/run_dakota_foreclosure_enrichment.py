"""Standalone runner for the Dakota foreclosure enrichment job.

Used by .github/workflows/dakota-enrichment.yml (step 2, after the Dakota
parcel loader). This is a DATABASE job — it reads dakota_sheriff events + the
Dakota parcel roll in core.parcels and UPDATEs each event's raw_data with
owner / market value / mailing / homestead, matched by a unique
suffix-normalized address. No external HTTP source; pure internal join.

NOTE: run_dakota_foreclosure_enrichment() is a synchronous function (the
Supabase client calls are sync and there is nothing to await), so unlike the
parcel-loader runner this does NOT use asyncio.

Usage:
    python -m scripts.run_dakota_foreclosure_enrichment [trigger_name]

Exits 0 on success, 1 on failure.
"""

from __future__ import annotations

import sys
import traceback

from src.scrapers.dakota_foreclosure_enrichment import (
    run_dakota_foreclosure_enrichment,
)
from src.utils.logger import logger


def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"
    logger.info("Dakota foreclosure enrichment runner starting", trigger=trigger)
    print(f"[dakota-fc-enrich-runner] trigger={trigger}", flush=True)

    try:
        stats = run_dakota_foreclosure_enrichment()
    except Exception as e:
        print(
            f"[dakota-fc-enrich-runner] FAILED — {type(e).__name__}: {e}",
            flush=True,
        )
        traceback.print_exc()
        return 1

    print(
        f"[dakota-fc-enrich-runner] done — events={stats['events']} "
        f"enriched={stats['enriched']} no_match={stats['no_match']} "
        f"multi_match={stats['multi_match']} failed={stats['failed']}",
        flush=True,
    )

    # A per-row update failure shouldn't fail the whole run unless everything
    # failed (which would signal a systemic problem worth a red X).
    if stats["events"] > 0 and stats["enriched"] == 0 and stats["failed"] > 0:
        print(
            "[dakota-fc-enrich-runner] all updates failed — exit 1",
            flush=True,
        )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
