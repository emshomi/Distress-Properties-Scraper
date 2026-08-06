"""Config-table-driven runner for the MnGeo statewide parcel spine.

Loads a county's parcel spine from the MnGeo MNGAC open-parcels layer into
core.parcels (with the core.owners projection riding alongside), driven by
core.mngeo_county_load rather than by a per-county subclass.

WHY THIS EXISTS RATHER THAN 51 MORE run_<county>_parcels.py FILES
-----------------------------------------------------------------
run_wabasha_parcels.py is the eighth county and the pattern it follows —
subclass + config flag + core.counties row + runner + workflow — is right
when counties arrive one at a time. At 51 it inverts into 255 artefacts, 51
cron schedules to offset and 51 audit.source_health rows.

Adding a county is now an INSERT into core.mngeo_county_load. Same move
already made twice for the same reason: source_county_map replaced a
hardcoded CASE, ecrv_county_map replaced a hardcoded county list.

WHAT IS DELIBERATELY NOT HERE
-----------------------------
The 8 counties already held by county-direct loaders (Hennepin, Ramsey,
Dakota, Anoka, Washington, Olmsted, Fillmore, Wabasha) have NO ROW in
core.mngeo_county_load, so this runner cannot touch them. That is a safety
property, not an omission: a MnGeo upsert over a held county would clobber
county-direct fields such as year_built and property_type, because
exclude_none=True does NOT stop PostgREST NULLing union-missing keys across
a mixed batch. Their held counts also disagree with MnGeo's (Dakota 167,494
held vs 153,848; Ramsey 163,883 vs 172,014), so a reload is not a no-op.

TWO GATES, BOTH MUST BE OPEN
----------------------------
  1. SCRAPER_MNGEO_PARCELS_ENABLED  — one master toggle for all 51.
  2. core.mngeo_county_load.enabled — per county, an UPDATE not a redeploy.

Both default to off. Loading Stearns is:
    UPDATE core.mngeo_county_load SET enabled = true
     WHERE county_code = 'stearns';

Usage:
    python -m scripts.run_mngeo_parcels <county_code> [trigger_name]
    python -m scripts.run_mngeo_parcels --enabled [trigger_name]

The second form runs every ENABLED county sequentially, largest first, so a
systematic failure surfaces on the biggest county rather than after fifty
small ones. Sequential on purpose: the loader holds a class-level lock and
concurrent county runs would serialise anyway, with a worse failure mode.

Exits 0 when every attempted county succeeded, 1 otherwise.
"""

from __future__ import annotations

import asyncio
import sys
import traceback
from datetime import datetime, timezone
from typing import Any

# Import at top level so a missing env var / config error fails fast.
from src.db.supabase_client import core_table
from src.scrapers.mngac_parcels import MNGACParcelsScraper
from src.utils.logger import logger

_TAG = "[mngeo-parcels-runner]"


def _load_rows(selector: str) -> list[dict[str, Any]]:
    """Fetch config rows: one county, or every enabled county largest-first.

    Selecting largest-first is the plan's own sequencing — a systematic
    failure (bad field name, auth, schema drift) shows up on St. Louis's
    186,455 rows immediately rather than after fifty small counties have
    written.
    """
    query = core_table("mngeo_county_load").select(
        "co_code, county_code, source_name, parcel_count, max_pages, "
        "enabled, load_status"
    )
    if selector == "--enabled":
        query = query.eq("enabled", True)
    else:
        query = query.eq("county_code", selector)
    result = query.order("parcel_count", desc=True).execute()
    return list(result.data or [])


def _mark(co_code: str, **fields: Any) -> None:
    """Stamp load_status / loaded_rows / last_loaded_at on the config row.

    Never raises: a bookkeeping failure must not turn a successful 73,149-row
    load into a failed run. Logged instead, so the discrepancy is visible.
    """
    try:
        core_table("mngeo_county_load").update(fields).eq(
            "co_code", co_code
        ).execute()
    except Exception as e:
        logger.warning(
            "mngeo_county_load status update failed (load unaffected)",
            co_code=co_code,
            fields=fields,
            error=str(e)[:300],
        )


async def _run_one(row: dict[str, Any], trigger: str) -> bool:
    """Run one county. Returns True on success."""
    co_code = str(row["co_code"])
    county = str(row["county_code"])
    expected = row.get("parcel_count")

    expected_note = f"~{expected:,} rows, " if expected else ""
    print(
        f"{_TAG} {county} ({co_code}): streaming {expected_note}"
        f"keyset-paged ...",
        flush=True,
    )

    scraper = MNGACParcelsScraper.from_config_row(row)
    _mark(co_code, load_status="running")

    try:
        # trigger="manual" => a closed master toggle RAISES rather than
        # silently skipping, so a misconfigured flag fails the run visibly.
        # Same choice as run_wabasha_parcels.py.
        result = await scraper.run(
            trigger="manual",
            metadata={"trigger_source": trigger, "driver": "config_table"},
        )
    except Exception as e:
        print(
            f"{_TAG} {county}: FAILED — {type(e).__name__}: {e}",
            flush=True,
        )
        traceback.print_exc()
        _mark(co_code, load_status="failed")
        return False

    print(
        f"{_TAG} {county}: status={result.status} "
        f"fetched={result.records_fetched} new={result.records_new} "
        f"failed={result.records_failed}",
        flush=True,
    )

    ok = result.status not in ("failed", "skipped")

    # A run that "succeeds" well short of the expected count is the silent
    # truncation the live-count preflight exists to prevent. Report it here
    # too — the runner is where a human looks first.
    if ok and expected and result.records_fetched < expected * 0.9:
        print(
            f"{_TAG} {county}: WARNING — fetched "
            f"{result.records_fetched:,} of ~{expected:,} expected "
            f"({result.records_fetched / expected:.0%}). Check max_pages "
            f"and the run log before trusting this county.",
            flush=True,
        )

    _mark(
        co_code,
        load_status="loaded" if ok else "failed",
        loaded_rows=result.records_new,
        last_loaded_at=datetime.now(timezone.utc).isoformat(),
    )

    if not ok:
        print(
            f"{_TAG} {county}: non-success status ({result.status}): "
            f"{result.error_message}",
            flush=True,
        )
    return ok


async def main() -> int:
    if len(sys.argv) < 2:
        print(
            f"{_TAG} usage: python -m scripts.run_mngeo_parcels "
            f"<county_code|--enabled> [trigger_name]",
            flush=True,
        )
        return 1

    selector = sys.argv[1]
    trigger = sys.argv[2] if len(sys.argv) > 2 else "github_actions"
    logger.info(
        "MnGeo parcels runner starting", selector=selector, trigger=trigger
    )
    print(f"{_TAG} selector={selector} trigger={trigger}", flush=True)

    try:
        rows = _load_rows(selector)
    except Exception as e:
        print(
            f"{_TAG} could not read core.mngeo_county_load — "
            f"{type(e).__name__}: {e}",
            flush=True,
        )
        traceback.print_exc()
        return 1

    if not rows:
        if selector == "--enabled":
            print(
                f"{_TAG} no counties are enabled. Enable one with:\n"
                f"    UPDATE core.mngeo_county_load SET enabled = true "
                f"WHERE county_code = 'stearns';",
                flush=True,
            )
        else:
            # Distinguish "not a MnGeo county" from "typo" — the 8 held
            # counties are absent BY DESIGN and that is worth saying.
            print(
                f"{_TAG} no core.mngeo_county_load row for "
                f"{selector!r}. Either the slug is wrong, the county is one "
                f"of the 28 without gac_open_approval, or it is one of the 8 "
                f"already held by a county-direct loader (excluded on "
                f"purpose — see this file's header).",
                flush=True,
            )
        return 1

    # Explicit single-county form refuses a disabled county rather than
    # skipping it, matching trigger="manual" semantics: an operator who named
    # a county expects it to run or to be told why not.
    if selector != "--enabled" and not rows[0].get("enabled"):
        print(
            f"{_TAG} {selector} is present but NOT enabled. Enable it with:\n"
            f"    UPDATE core.mngeo_county_load SET enabled = true "
            f"WHERE county_code = '{selector}';",
            flush=True,
        )
        return 1

    total = len(rows)
    print(
        f"{_TAG} {total} county(ies) to load, largest first: "
        f"{', '.join(str(r['county_code']) for r in rows[:5])}"
        f"{' ...' if total > 5 else ''}",
        flush=True,
    )

    failures: list[str] = []
    for index, row in enumerate(rows, start=1):
        print(f"{_TAG} --- {index}/{total} ---", flush=True)
        if not await _run_one(row, trigger):
            failures.append(str(row["county_code"]))

    if failures:
        print(
            f"{_TAG} {len(failures)} of {total} failed: "
            f"{', '.join(failures)} — exit 1",
            flush=True,
        )
        return 1

    print(f"{_TAG} done — {total} county(ies) loaded.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
