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
from src.utils.county import resolve_county_code
from src.utils.logger import logger

# Batch size for bulk inserts
BATCH_SIZE: int = 500


def _chunked(iterable: list, size: int):
    """Yield successive chunks of `size` from `iterable`."""
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def _fill_county_codes(events: list[DistressEventInsert]) -> None:
    """Set county_code on any event that lacks one. Mutates in place.

    Derived centrally rather than in each of the fifteen scrapers: the rule is
    one expression (core.source_county_map, falling back to the per-row county
    for statewide publishers) and it resolved 7,460 of 7,472 live events when
    measured on 2026-08-07. Fifteen copies of it would drift.

    A county that cannot be resolved stays None. That is deliberate — see the
    field comment on DistressEventInsert. Never substitute a default.
    """
    for e in events:
        if e.county_code:
            continue
        try:
            e.county_code = resolve_county_code(e.source, e.raw_data)
        except Exception as exc:
            logger.warning(
                "county_code resolution failed; leaving NULL",
                source=e.source,
                error_type=type(exc).__name__,
            )


def write_events_dedup(events: Iterable[DistressEventInsert]) -> tuple[int, int]:
    """
    Insert events into signals.distress_events, skipping duplicates.

    Dedup uses the unique index
        (county_code, parcel_id, event_type, event_date, source)
    in the underlying table. Postgres' ON CONFLICT DO NOTHING returns 0
    rows for the conflicting ones, which we count as duplicates.

    county_code is DERIVED HERE when the caller has not set it, so no scraper
    needs changing. See _fill_county_codes.

    FIXED 2026-08-07. This docstring previously called a NULL county_code
    "SAFE". It was not. The index is NULLS NOT DISTINCT, so a NULL-county row
    NEVER matches the correctly-labelled row for the same event — every run
    re-inserted it. 1,451 duplicate events accumulated in roughly 24 hours,
    inflating the public signal count from 7,472 to 8,923 and breaking
    mpls_vbr and saint_paul_vacant outright.

    Args:
        events: Iterable of DistressEventInsert.

    Returns:
        (records_new, records_failed) tuple.
    """
    event_list = list(events)
    if not event_list:
        return 0, 0

    _fill_county_codes(event_list)

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
