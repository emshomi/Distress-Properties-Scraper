"""Standalone runner for the Washington foreclosure enrichment job.

Used by .github/workflows/washington-enrichment.yml (step after the Washington
parcel loader). This is a DATABASE job — it reads washington_sheriff events +
the Washington parcel roll in core.parcels and UPDATEs each event's raw_data
with owner / market value / mailing / homestead, matched by exact PID. No
external HTTP source; pure internal join.

NOTE: run_washington_foreclosure_enrichment() is a synchronous function (the
Supabase client calls are sync and there is nothing to await), so unlike the
parcel-loader runner this does NOT use asyncio.

Usage:
    python -m scripts.run_washington_foreclosure_enrichment [trigger_name]

Exits 0 on success, 1 on failure.
"""
from __future__ import annotations
import sys
import traceback
from src.scrapers.washington_foreclosure_enrichment import (
    run_washington_foreclosure_enrichment,
)
from src.utils.logger import logger
def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"
    logger.info("Washington foreclosure enrichment runner starting", trigger=trigger)
    print(f"[washington-fc-enrich-runner] trigger={trigger}", flush=True)
    try:
        stats = run_washington_foreclosure_enrichment()
    except Exception as e:
        print(
            f"[washington-fc-enrich-runner] FAILED — {type(e).__name__}: {e}",
            flush=True,
        )
        traceback.print_exc()
        return 1
    print(
        f"[washington-fc-enrich-runner] done — events={stats['events']} "
        f"enriched={stats['enriched']} no_match={stats['no_match']} "
        f"failed={stats['failed']}",
        flush=True,
    )
    # A per-row update failure shouldn't fail the whole run unless everything
    # failed (which would signal a systemic problem worth a red X).
    if stats["events"] > 0 and stats["enriched"] == 0 and stats["failed"] > 0:
        print(
            "[washington-fc-enrich-runner] all updates failed — exit 1",
            flush=True,
        )
        return 1
    return 0
if __name__ == "__main__":
    sys.exit(main())
