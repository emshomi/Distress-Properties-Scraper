"""
Event writer service.

Writes DistressEventInsert rows to signals.distress_events with
deduplication. The dedup key is
(county_code, parcel_id, event_type, event_date, source).

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

    Dedup uses the unique index
        (county_code, parcel_id, event_type, event_date, source)
    in the underlying table. Postgres' ON CONFLICT DO NOTHING returns 0
    rows for the conflicting ones, which we count as duplicates.

    OUTSTANDING (2026-08-06): callers do not yet populate county_code on
    DistressEventInsert, so new rows land with it NULL. That is SAFE — the
    index is NULLS NOT DISTINCT so NULL-county rows still dedup against each
    other, and the composite FK is not enforced when a key column is NULL
    (MATCH SIMPLE). But it means a Hennepin event and a Ramsey event for the
    same PIN can no longer be told apart by this key, which is exactly the
    collapse the county-aware index was meant to prevent. Add county_code to
    DistressEventInsert and set it in each scraper to close this properly.

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
                    # county_code added 2026-08-06 to match the rebuilt
                    # distress_events_dedup_key. The index gained county_code
                    # when core.parcels moved to a composite PK, because two
                    # counties' events for the same PIN were collapsing into
                    # one — 51,662 nine-char PINs are shared across MN
                    # counties. A stale conflict target matches no unique
                    # index, so PostgREST rejects the whole batch; the except
                    # below swallows it into a warning and the run still
                    # reports counts, which is how this would have gone
                    # unnoticed on the DAILY sheriff feeds.
                    #
                    # The index is NULLS NOT DISTINCT, so rows that do not yet
                    # carry county_code still dedup correctly among themselves.
                    # See the docstring for why they should carry it.
                    on_conflict="county_code,parcel_id,event_type,event_date,source",
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
