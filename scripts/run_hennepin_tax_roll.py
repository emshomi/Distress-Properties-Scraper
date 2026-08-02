"""Standalone runner for the Hennepin tax-roll (forfeited land) miner.

Used by the GitHub Actions workflow at
.github/workflows/hennepin-tax-roll-scrape.yml.

This is a DATABASE MINING job, not a web scraper — it reads the Hennepin
parcels already in core.parcels (where FORFEIT_LAND_IND = 'T') and derives
hennepin_tax_roll tax-forfeit signals from them. So it depends on the
hennepin_parcels scraper having populated core.parcels first; if parcels are
stale, re-run hennepin_parcels before this.

That dependency is worth watching: this job can run perfectly, report success
and produce nothing new simply because its INPUT is old. A green run here is
not evidence that Hennepin forfeiture data is current — only that the mining
step worked on whatever core.parcels happened to hold.

Usage:
    python -m scripts.run_hennepin_tax_roll [trigger_name]

Exit code 0 on success or partial success, 1 on failure, so GitHub Actions
marks a genuinely broken run as failed.

=== WHY THIS CALLS run() INSTEAD OF fetch/parse/write (2026-08-02) ===
It used to drive the three lifecycle steps itself and call
source_health_tracker directly. Health was recorded — but ONLY
BaseScraper._run_locked writes audit.scraper_runs, so every Actions run of
this job was INVISIBLE in the run log.

hennepin_tax_roll is a clear example of the resulting confusion: on
2026-08-02 audit.source_health showed a successful run at 2026-08-01 13:32
and audit.scraper_runs held NO row for it at all, ever. Its
expected_interval_days was also null, so the health digest could not judge
staleness either. A job that mines the forfeiture signal for Minnesota's
largest county was effectively unmonitored while appearing fine.

run() does everything this file used to do by hand — the audit run row, the
health row, the per-class lock, and the success/partial/failed decision — in
ONE place. This is the fifth and last runner converted; the parcels and legal
runners already worked this way.

=== EXIT CODE ===
run() CATCHES exceptions and returns a RunResult with status='failed' rather
than raising, so the exit code must come from result.status. A try/except
around it would see nothing and exit 0 on a broken run.

'partial' exits 0 deliberately: a red mark for a few failed rows out of
thousands is how people learn to ignore red marks. The count is printed and
the run row records 'partial', which is the honest label.
"""
from __future__ import annotations

import asyncio
import sys

# Import at top level so a missing env var / config error fails fast.
from src.scrapers.hennepin_tax_roll import HennepinTaxRollScraper
from src.utils.logger import logger


async def main() -> int:
    trigger = sys.argv[1] if len(sys.argv) > 1 else "github_actions"

    logger.info("Hennepin tax-roll runner starting", trigger=trigger)
    print(f"[hennepin-tax-roll-runner] trigger={trigger}", flush=True)

    scraper = HennepinTaxRollScraper()

    # trigger="manual" so a disabled scraper RAISES ScraperDisabledError
    # instead of returning status='skipped' silently. A workflow that runs
    # against a job someone turned off should say so, not go green.
    result = await scraper.run(
        trigger="manual",
        metadata={
            "trigger_source": "github_actions",
            "runner": "hennepin_tax_roll",
        },
    )

    print(
        f"[hennepin-tax-roll-runner] {result.status} — run_id={result.run_id} "
        f"fetched={result.records_fetched} new={result.records_new} "
        f"updated={result.records_updated} failed={result.records_failed} "
        f"({result.duration_seconds:.1f}s)",
        flush=True,
    )
    if result.error_message:
        print(f"[hennepin-tax-roll-runner] note: {result.error_message}", flush=True)

    if result.status in ("success", "partial"):
        return 0

    print("[hennepin-tax-roll-runner] FAILED — exit 1", flush=True)
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
