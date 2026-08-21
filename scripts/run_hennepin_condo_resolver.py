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
        f"unreachable={stats['unreachable']} "
        f"rekey_collision={stats['rekey_collision']}",
        flush=True,
    )
    print(
        "[hennepin-condo-runner] the log is not the measurement — verify with "
        "a query against signals.distress_events.",
        flush=True,
    )

    # === WHAT COUNTS AS FAILURE (rewritten 2026-08-20) ===
    #
    # This used to fail whenever `resolved` was 0, on the reasoning that
    # "a county that resolves nothing at all is a broken instrument, not an
    # honest answer: 1225 Lasalle Ave #604 is known to resolve."
    #
    # That canary has left the population. 1225 Lasalle Ave #604 resolved at
    # 03:40 on 2026-08-21 — in the SHERIFF run, because hennepin_sheriff now
    # asks PINS at mint time. It is no longer a candidate here, and neither
    # are the other 105 addresses that run resolved.
    #
    # What remains is the residue: 29 candidates, every one a case the county
    # ANSWERED and the answer was no. 14 ambiguous (two parcels for one unit —
    # a building deeds its parking stalls separately), 14 not found (multi-
    # property notices like '1215 & 1219 Knox Ave N', addresses with no unit),
    # 1 unparsed. Zero re-keys is the CORRECT result for that set, and a job
    # that goes red every night for a correct result trains everyone to
    # ignore it — so the one night it means something, nobody looks.
    #
    # The rule now tests what the old one was reaching for: CAN WE REACH THE
    # COUNTY. `unreachable` counts only 'no_response' and transport errors —
    # a timeout, a 5xx, a dropped connection. A refusal is an answer.
    if stats["unreachable"] > 0:
        print(
            f"[hennepin-condo-runner] {stats['unreachable']} of "
            f"{stats['candidates']} candidates got NO RESPONSE from the "
            "county — treating as failure",
            flush=True,
        )
        return 1

    if stats["candidates"] > 0 and stats["resolved"] == 0:
        print(
            f"[hennepin-condo-runner] resolved 0 of {stats['candidates']} "
            "— every candidate was answered and declined "
            f"({stats['ambiguous']} ambiguous, {stats['not_found']} not "
            f"found, {stats['unparsed_address']} unparsed). Not a failure.",
            flush=True,
        )

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
