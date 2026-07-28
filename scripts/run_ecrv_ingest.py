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

    print("[ecrv-ingest] done.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
