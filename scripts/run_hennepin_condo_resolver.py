"""Standalone runner for the Hennepin condo resolver.

Used by .github/workflows/hennepin-condo-resolver.yml.

This job takes hennepin_sheriff foreclosure events stuck on a
HENNEPIN-FC-<saleRecordNumber> placeholder whose address carries a unit number,
asks Hennepin's Property Information Search for that exact unit, and re-keys
the event onto the real parcel it returns.

Usage:
    python -m scripts.run_hennepin_condo_resolver [trigger_name]

Environment:
    MAX_EVENTS   cap on events RE-KEYED in one run; 0 or unset = uncapped.

=== WHY THIS EXISTS ===
The sheriff notice publishes '1225 Lasalle Ave #604'. core.parcels, loaded from
the county GIS parcel roll, stores only '1225 LASALLE AVE' -- 138 rows, no unit
number anywhere. So the event cannot be matched and stays on a placeholder.

A placeholder EXISTS in core.parcels, so every join succeeds and returns a row
with no lat, no emv_total, no owner. Nothing errors and nothing logs; the
product just renders em-dashes. 113 hennepin events are in that state.

Hennepin publishes the answer through a different endpoint than the parcel roll
we load. Verified by hand 2026-08-15: house + street + unit POSTed to
addrresult.jsp returns PID 27-029-24-24-0203 for 1225 Lasalle Ave #604, and
that parcel is ALREADY in core.parcels with emv_total $118,000 and coordinates.
Nothing needs fetching -- only the mapping was missing.

=== READ rekeyed, NOT resolved ===
`resolved` counts units the county identified. `rekeyed` counts events actually
moved onto a real parcel -- the number that changes what a subscriber sees.
They differ when a resolved parcel is not in our spine (`parcel_missing`).

`ambiguous` is a DELIBERATE non-action: two units in one building whose trailing
digits agree. Guessing between them would put a neighbour's market value on a
property, which is worse than the blank it replaces.

Exits 0 on success, 1 on failure.
"""

from __future__ import annotations

import asyncio
import sys
import traceback

from src.scrapers.hennepin_condo_resolver import run_hennepin_condo_resolver
from src.utils.logger import logger


async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"
    logger.info("Hennepin condo resolver runner starting", trigger=trigger)
    print(f"[hennepin-condo-runner] trigger={trigger}", flush=True)

    try:
        stats = await run_hennepin_condo_resolver()
    except Exception as e:
        print(
            f"[hennepin-condo-runner] FAILED — {type(e).__name__}: {e}",
            flush=True,
        )
        traceback.print_exc()
        return 1

    print(
        f"[hennepin-condo-runner] done — candidates={stats['candidates']} "
        f"resolved={stats['resolved']} REKEYED={stats['rekeyed']} "
        f"not_found={stats['not_found']} ambiguous={stats['ambiguous']} "
        f"parcel_missing={stats['parcel_missing']} "
        f"unparsed_address={stats['unparsed_address']} "
        f"rekey_collision={stats['rekey_collision']}",
        flush=True,
    )
    print(
        "[hennepin-condo-runner] the log is not the measurement — verify with "
        "a query against signals.distress_events.",
        flush=True,
    )

    # A county that resolves nothing at all is a broken instrument, not an
    # honest answer: 1225 Lasalle Ave #604 is known to resolve. Fail loudly
    # rather than report a clean zero -- the Washington loader reported
    # written=118,418 failed=0 with lat NULL on every row.
    if stats["candidates"] > 0 and stats["resolved"] == 0:
        print(
            "[hennepin-condo-runner] resolved 0 of "
            f"{stats['candidates']} candidates — treating as failure",
            flush=True,
        )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
