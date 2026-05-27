"""
Event writer service.

Writes DistressEventInsert rows to signals.distress_events with
deduplication. The dedup key is (parcel_id, event_type, event_date, source).

Writes happen in batches of 500 to balance throughput vs. timeout risk.
"""

from __future__ import annotations

from typing import Any, Iterable

from src.db.supabase_client import signals_table
from src.models.signal import DistressEventInsert
from src.utils.logger import logger

# Batch size for bulk inserts
BATCH_SIZE: int = 500


def _chunked(iterable: list, size: int):
    """Yield successive chunks of `size` from `iterable`."""
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def write_events_dedup(events: Iterable[DistressEventInsert]) -> tuple[int, int]:
    """
    Insert events into signals.distress_events, skipping duplicates.

    Dedup uses the unique constraint on
        (parcel_id, event_type, event_date, source)
    in the underlying table. Postgres' ON CONFLICT DO NOTHING returns 0
    rows for the conflicting ones, which we count as duplicates.

    Args:
        events: Iterable of DistressEventInsert.

    Returns:
        (records_new, records_failed) tuple.
    """
    event_list = list(events)
    if not event_list:
        return 0, 0

    records_new = 0
    records_failed = 0

    for batch in _chunked(event_list, BATCH_SIZE):
        payload = [
            e.model_dump(mode="json", exclude_none=True) for e in batch
        ]

        try:
            # upsert with ignore_duplicates=True; PostgREST returns inserted rows
            result = (
                signals_table("distress_events")
                .upsert(
                    payload,
                    on_conflict="parcel_id,event_type,event_date,source",
                    ignore_duplicates=True,
                )
                .execute()
            )
            inserted = len(result.data or [])
            records_new += inserted
        except Exception as e:
            logger.warning(
                "Batch write to distress_events failed",
                batch_size=len(batch),
                error=str(e),
            )
            records_failed += len(batch)

    return records_new, records_failed


def write_typed_signals_dedup(
    table_name: str,
    rows: list[dict[str, Any]],
    *,
    on_conflict: str,
) -> tuple[int, int]:
    """
    Insert rows into a typed signals table (code_violations, sheriff_sales, etc.)
    with deduplication.

    Args:
        table_name: Table name in the `signals` schema (e.g., 'code_violations').
        rows: List of dicts ready for insertion.
        on_conflict: Comma-separated unique-constraint column list for dedup.

    Returns:
        (records_new, records_failed) tuple.
    """
    if not rows:
        return 0, 0

    records_new = 0
    records_failed = 0

    for batch in _chunked(rows, BATCH_SIZE):
        try:
            result = (
                signals_table(table_name)
                .upsert(batch, on_conflict=on_conflict, ignore_duplicates=False)
                .execute()
            )
            records_new += len(result.data or [])
        except Exception as e:
            logger.warning(
                f"Batch write to signals.{table_name} failed",
                batch_size=len(batch),
                error=str(e),
            )
            records_failed += len(batch)

    return records_new, records_failed


__all__ = [
    "BATCH_SIZE",
    "write_events_dedup",
    "write_typed_signals_dedup",
]
