"""Standalone runner for the eCRV weekly-extract ingest.

UPLOAD-DRIVEN, not scheduled. The Minnesota Department of Revenue emails an
alert when a new Weekly Sales Extract is available; you download the zip and
point this script at it. There is no fetchable URL and no cron — the eCRV
SOAP API requires county/city credentials Govire does not have.

Idempotent on (source, source_id): re-running the same zip updates rather
than duplicates, and a corrected certificate overwrites the original. Weekly
extracts overlap, so re-ingesting is expected and safe.

Usage:
    python -m scripts.run_ecrv_ingest /path/to/2026-07-27_eCRVExtract.zip

Exits 0 on success, 1 on failure.
"""

from __future__ import annotations

import os
import sys
import traceback

from src.scrapers.ecrv_extract import ingest_zip
from src.utils.logger import logger


def main() -> int:
    if len(sys.argv) < 2:
        print(
            "[ecrv-ingest] usage: python -m scripts.run_ecrv_ingest "
            "<path-to-eCRVExtract.zip>",
            flush=True,
        )
        return 1

    zip_path = sys.argv[1]
    if not os.path.isfile(zip_path):
        print(f"[ecrv-ingest] file not found: {zip_path}", flush=True)
        return 1

    logger.info("eCRV ingest runner starting", zip=zip_path)
    print(f"[ecrv-ingest] zip={zip_path}", flush=True)
    print("[ecrv-ingest] parsing + resolving parcels ...", flush=True)

    try:
        stats = ingest_zip(zip_path)
    except Exception as e:
        print(
            f"[ecrv-ingest] FAILED — {type(e).__name__}: {e}",
            flush=True,
        )
        traceback.print_exc()
        return 1

    print(
        f"[ecrv-ingest] records={stats['records']} "
        f"written={stats['written']} failed={stats['failed']} "
        f"parcel_matched={stats['matched']} "
        f"parcel_unmatched={stats['unmatched']} "
        f"({stats['duration_seconds']}s)",
        flush=True,
    )

    if stats["failed"] > 0 and stats["written"] == 0:
        print("[ecrv-ingest] all writes failed — exit 1", flush=True)
        return 1

    print("[ecrv-ingest] done.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
