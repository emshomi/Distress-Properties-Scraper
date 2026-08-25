"""Standalone runner for the eCRV weekly-extract ingest.

UPLOAD-DRIVEN, not scheduled. The Minnesota Department of Revenue emails an
alert when a new Weekly Sales Extract is ready. Download the zip, upload it
to the private Supabase Storage bucket 'ecrv-extracts' via the dashboard,
then run this with the object name.

Idempotent on (crv_number_id, parcel_id_raw): re-running the same file
updates rather than duplicates, and a corrected certificate overwrites the
original. Weekly extracts overlap, so re-ingesting is expected and safe.

Usage:
    # from the storage bucket (what the GitHub Actions workflow calls)
    python -m scripts.run_ecrv_ingest 2026-07-27-02-10-41_eCRVExtract.zip

    # from a local path (developer convenience)
    python -m scripts.run_ecrv_ingest --local C:\\path\\to\\extract.zip

Exits 0 on success, 1 on failure.
"""

from __future__ import annotations

import os
import sys
import traceback

from src.scrapers.ecrv_extract import (
    STORAGE_BUCKET,
    ingest_from_storage,
    ingest_zip,
)
from src.utils.logger import logger


def report_unmapped_counties(source_file: str) -> None:
    """Print eCRV county codes in this file that have no map entry.

    === WHY THIS EXISTS ===
    The ingest reported `written=2927 failed=0` for the 2026-08-24 extract.
    Both numbers were true and both were silent about this: 490 of those
    rows carried a county_cde with NO row in outcomes.ecrv_county_map, so
    they joined to nothing in ecrv_buyer_activity, investor_activity,
    reo_seller_patterns, distressed_exit_sales or scoring.comp_ratios.
    Written, and invisible.

    SHERBURNE is why this is worth a function. 42,675 parcels loaded in
    core.parcels, no map entry, and every Sherburne sale in every weekly
    extract joined to nothing -- for months, with no log line anywhere
    saying so. Found 2026-08-25 only because someone went looking. Pope,
    12,888 parcels, was the same.

    A row that is written but unjoinable is worse than one that fails: a
    failure is counted and retried, this was neither.

    NON-FATAL BY DESIGN. An unmapped code is not a bad file. Most of the
    28 still unmapped are counties with no parcels loaded, which no map
    entry would fix -- the mapping is derived by joining eCRV parcel IDs
    against core.parcels, so a county with no parcels cannot be derived.
    This prints; it never changes the exit code. Any exception here is
    swallowed for the same reason: a broken warning must not break a
    working ingest.
    """
    try:
        from src.db.supabase_client import get_client

        client = get_client()

        # PAGINATED, and it must be. PostgREST caps a select at 1,000 rows
        # by default and returns the truncation SILENTLY. The 2026-08-24
        # extract held 2,927 rows, so an unpaginated read would have seen
        # 34% of the file and under-reported the unmapped count -- the same
        # silent-partial failure this function exists to catch.
        rows: list[dict] = []
        page = 0
        page_size = 1000
        while True:
            res = (
                client.table("ecrv_sales")
                .select("county_cde")
                .eq("source_file", source_file)
                .range(page * page_size, (page + 1) * page_size - 1)
                .execute()
            )
            batch = res.data or []
            rows.extend(batch)
            if len(batch) < page_size:
                break
            page += 1
            if page > 100:  # 100k rows; a weekly extract is ~3k
                print(
                    "[ecrv-ingest] unmapped-county check stopped at 100 "
                    "pages; counts below are a floor, not a total",
                    flush=True,
                )
                break

        if not rows:
            return

        mapped_res = (
            client.table("ecrv_county_map").select("county_cde").execute()
        )
        mapped = {r["county_cde"] for r in (mapped_res.data or [])}

        counts: dict[str, int] = {}
        for r in rows:
            code = r.get("county_cde")
            if code is not None and code not in mapped:
                counts[code] = counts.get(code, 0) + 1

        if not counts:
            print(
                "[ecrv-ingest] all county codes in this file are mapped",
                flush=True,
            )
            return

        # Largest first: the biggest unmapped code is the one most worth
        # deriving, and it is what SHERBURNE would have looked like.
        ordered = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
        detail = " ".join("%s(%d)" % (code, n) for code, n in ordered)
        print(
            "[ecrv-ingest] WARNING: %d row(s) in %d UNMAPPED county code(s) "
            "joined to nothing downstream: %s"
            % (sum(counts.values()), len(counts), detail),
            flush=True,
        )
        print(
            "[ecrv-ingest] a code with parcels loaded in core.parcels can be "
            "mapped by joining its digits-only parcel IDs against them; one "
            "with no parcels loaded cannot, and needs the parcel loader "
            "first",
            flush=True,
        )
    except Exception as e:
        print(
            "[ecrv-ingest] unmapped-county check skipped (%s: %s)"
            % (type(e).__name__, e),
            flush=True,
        )


def main() -> int:
    args = sys.argv[1:]
    if not args:
        print(
            "[ecrv-ingest] usage:\n"
            "  python -m scripts.run_ecrv_ingest <object-name-in-bucket>\n"
            "  python -m scripts.run_ecrv_ingest --local <path-to-zip>",
            flush=True,
        )
        return 1

    local_mode = args[0] == "--local"
    target = args[1] if local_mode else args[0]
    if local_mode and len(args) < 2:
        print("[ecrv-ingest] --local needs a file path", flush=True)
        return 1

    if local_mode:
        if not os.path.isfile(target):
            print(f"[ecrv-ingest] file not found: {target}", flush=True)
            return 1
        print(f"[ecrv-ingest] local file: {target}", flush=True)
    else:
        print(
            f"[ecrv-ingest] bucket={STORAGE_BUCKET} object={target}",
            flush=True,
        )

    logger.info("eCRV ingest runner starting", target=target,
                local_mode=local_mode)
    print("[ecrv-ingest] parsing certificates (one row per parcel) ...",
          flush=True)

    try:
        if local_mode:
            stats = ingest_zip(target)
        else:
            stats = ingest_from_storage(target)
    except Exception as e:
        print(
            f"[ecrv-ingest] FAILED — {type(e).__name__}: {e}",
            flush=True,
        )
        traceback.print_exc()
        return 1

    print(
        f"[ecrv-ingest] source_file={stats['source_file']} "
        f"certificates={stats['certificates']} "
        f"parcel_rows={stats['parcel_rows']} "
        f"written={stats['written']} failed={stats['failed']} "
        f"({stats.get('duration_seconds')}s)",
        flush=True,
    )

    if stats["parcel_rows"] == 0:
        print("[ecrv-ingest] no rows parsed — exit 1", flush=True)
        return 1
    if stats["failed"] > 0 and stats["written"] == 0:
        print("[ecrv-ingest] all writes failed — exit 1", flush=True)
        return 1

    # After the write, never before: the check reads back what this run
    # committed. Placed after the failure exits so it only runs on a file
    # that actually landed.
    report_unmapped_counties(stats["source_file"])

    print("[ecrv-ingest] done.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
