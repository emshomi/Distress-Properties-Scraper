"""Standalone runner for the Hennepin foreclosure enrichment job.

Used by .github/workflows/hennepin-foreclosure-enrichment.yml. This is a
DATABASE job — it reads hennepin_sheriff events + the Hennepin parcel roll
already in core.parcels and UPDATES each event's raw_data with owner /
market value / homestead / absentee, matched by a unique normalized address.
No external HTTP source; pure internal join.

Usage:
    python -m scripts.run_hennepin_foreclosure_enrichment [trigger_name]

Environment:
    MAX_EVENTS   cap on rows WRITTEN in one run; 0 or unset = uncapped.

=== IT ALSO RE-KEYS (2026-08-15) ===
This job no longer only writes raw_data. On a UNIQUE address match it now also
writes distress_events.parcel_id, moving the event off its
HENNEPIN-FC-<saleRecordNumber> placeholder and onto the real assessor parcel.

That placeholder is why a Hennepin foreclosure could open with no market value,
no deal math, no owner mailing, no homestead and no photo: the stub EXISTS in
core.parcels, so every join succeeds and returns a row carrying none of them.
Nothing errors and nothing logs. This job already resolved the real parcel and
stored its id under raw_data.detail.gis_pid -- and then never wrote it to
parcel_id.

So `rekeyed` is the number that matters on a run, not `enriched`. It is printed
below for that reason. `rekey_collision` counts events whose new key clashed
with an existing row: those keep their enrichment and stay on the stub, which
is correct but should not be silent.

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
        f"enriched={stats['enriched']} "
        f"REKEYED={stats.get('rekeyed', 0)} "
        f"rekey_collision={stats.get('rekey_collision', 0)} "
        f"no_match={stats['no_match']} "
        f"multi_match={stats['multi_match']} failed={stats['failed']}",
        flush=True,
    )
    print(
        "[hennepin-fc-enrich-runner] the log is not the measurement — verify "
        "with a query against signals.distress_events.",
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
