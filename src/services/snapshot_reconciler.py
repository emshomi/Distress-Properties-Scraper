"""
Full-snapshot reconciliation.

Some sources publish their ENTIRE current state on every fetch: a city's
vacant-building registry, a county's delinquency list, the Minnesota Judicial
Branch's weekly bulk extracts. For those, a row that stops appearing carries
meaning — the building came off the registry, the taxes were paid, the court
record stopped being public.

Nothing in the upsert path can express that. `write_typed_signals_dedup`
inserts and updates; it has no concept of a row that should no longer be
there. So a stale row lives forever. Measured 2026-08-22:
signals.vacant_registrations held 400 active Saint Paul registrations against
the City's 392, and 8 buildings the City had already released were still being
published to subscribers — six of them Category 2 - Boarded, the
high-severity tier.

This module supplies the missing half: given the keys a run actually saw,
increment a miss counter on everything it did not, and deactivate a row once
it has been missing often enough.

=== WHY A MISS COUNTER AND NOT "ABSENT ONCE, RETIRE" ===
Absent-once is not evidence. Three real incidents say so:

  * mpls_vbr took a bare 403 from the City on 2026-08-16 and fetched zero
    rows. Under a one-miss rule that single bad morning retires all 311
    Minneapolis registrations.

  * When saint_paul_vacant repointed off a dead service onto PAULIE on
    2026-08-16, 8 rows the old service carried were absent from the new
    one. Six were boarded buildings registered as far back as 2022. Six
    long-standing boarded buildings do not leave a registry the same
    morning — that is a coverage difference between two datasets. Only a
    direct query against the City's own layer established that they really
    were gone.

  * parse() drops rows silently. parse_feature returns None for a missing
    identifier and a ParseError is logged and skipped, so the parsed list
    can be far shorter than the fetch while the run reports success. Those
    rows are still in the registry and would be counted absent.

The caller passes its own threshold, because the right number depends on
cadence. A daily source wants a small one — three daily runs means a genuine
removal reaches subscribers in three days, and no single anomaly or two-day
outage can retire anything. A weekly source wants the same count to buy three
weeks of margin, which is the correct direction to be wrong for court records
that disappear on expungement.

=== THE GUARD IS NOT OPTIONAL ===
Retirement is only sound on a COMPLETE fetch. Call snapshot_is_complete()
first and skip reconciliation when it returns False. Retiring on partial data
marks live rows inactive, which is worse than carrying a stale one: a stale
row is visibly wrong, a missing row is invisible.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable, Sequence

from src.db.supabase_client import get_client
from src.utils.logger import logger

# PostgREST puts .in_() lists in the query string, so a large scope has to be
# chunked or the URL overflows.
ID_CHUNK: int = 100

# Share of fetched rows that must survive parsing before reconciliation is
# allowed to run. Saint Paul parses 392 of 392, so this is inert in normal
# operation; it exists to stop a mass parse failure reading as a mass removal.
DEFAULT_MIN_PARSE_RATIO: float = 0.95


def normalize_key_value(value: Any) -> str | None:
    """Reduce one key component to a comparable string.

    PostgREST returns dates as ISO strings; an in-memory model carries
    datetime.date or None. Both sides must land on the same shape or every
    stored row looks absent and the whole scope retires.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        return None
    return text


def build_key(values: Iterable[Any]) -> tuple[str | None, ...]:
    """Build a comparable key tuple from raw column values."""
    return tuple(normalize_key_value(v) for v in values)


def snapshot_is_complete(
    *,
    fetched: int | None,
    parsed: int,
    record_cap: int | None = None,
    min_parse_ratio: float = DEFAULT_MIN_PARSE_RATIO,
) -> tuple[bool, str]:
    """Decide whether a run's data may drive retirement.

    Returns (ok, reason). The reason is for the caller to log when ok is
    False; it is written for a human reading a scraper log, not for parsing.

    Args:
        fetched: rows the source returned, or None if unknown.
        parsed: rows that survived parsing into signals.
        record_cap: a per-run limit if one was applied (a capped fetch is by
            definition not the whole snapshot).
        min_parse_ratio: minimum parsed/fetched share required.
    """
    if record_cap is not None:
        return False, "record cap set for this run"
    if not fetched:
        return False, "fetch returned no rows"
    if parsed < fetched * min_parse_ratio:
        return False, "too many rows dropped in parsing"
    return True, ""


def _chunk(ids: list[Any]) -> list[list[Any]]:
    return [ids[i : i + ID_CHUNK] for i in range(0, len(ids), ID_CHUNK)]


def reconcile_snapshot(
    schema: str,
    table_name: str,
    *,
    scope: dict[str, Any],
    key_columns: Sequence[str],
    seen_keys: set[tuple[str | None, ...]],
    threshold: int,
    id_column: str = "id",
    miss_column: str = "consecutive_misses",
    active_column: str = "is_active",
) -> tuple[int, int, int]:
    """
    Reconcile stored rows against the keys a complete fetch actually saw.

    Rows PRESENT in the fetch have their miss counter reset to zero and are
    reactivated. Reactivation matters: without it, one transient miss that
    reached the threshold would retire a row permanently even though the
    source is still publishing it.

    Rows ABSENT have their miss counter incremented, and are deactivated once
    it reaches `threshold`.

    Absent rows are grouped by their CURRENT miss count so each group is a
    single bulk update rather than one call per row.

    Args:
        schema: Postgres schema (e.g. 'signals').
        table_name: table within it (e.g. 'vacant_registrations').
        scope: equality filters bounding what this run owns, e.g.
            {'county_code': 'ramsey', 'city': 'Saint Paul'}. MUST be narrow
            enough that another source's rows can never be reached —
            reconciling an unscoped table would retire every other city.
        key_columns: columns forming the row identity, matching the table's
            dedup index.
        seen_keys: normalized key tuples this run observed.
        threshold: consecutive misses required before deactivation.

    Returns:
        (rows_reset, rows_missed, rows_retired)
    """
    if not scope:
        raise ValueError(
            "reconcile_snapshot requires a non-empty scope; an unscoped "
            "reconciliation would retire rows belonging to other sources"
        )

    select_cols = ",".join(
        [id_column, *key_columns, miss_column, active_column]
    )

    query = get_client().schema(schema).table(table_name).select(select_cols)
    for column, value in scope.items():
        query = query.eq(column, value)
    stored = query.execute()
    rows = stored.data or []

    present_ids: list[Any] = []
    absent_by_count: dict[int, list[Any]] = {}

    for row in rows:
        key = build_key(row.get(col) for col in key_columns)
        if key in seen_keys:
            # Only rewrite rows that actually need it.
            if row.get(miss_column) or not row.get(active_column):
                present_ids.append(row[id_column])
        else:
            current = row.get(miss_column) or 0
            absent_by_count.setdefault(current, []).append(row[id_column])

    table = get_client().schema(schema).table(table_name)

    rows_reset = 0
    rows_missed = 0
    rows_retired = 0

    for chunk in _chunk(present_ids):
        table.update({miss_column: 0, active_column: True}).in_(
            id_column, chunk
        ).execute()
        rows_reset += len(chunk)

    for current, ids in absent_by_count.items():
        new_count = current + 1
        still_active = new_count < threshold
        for chunk in _chunk(ids):
            table.update(
                {miss_column: new_count, active_column: still_active}
            ).in_(id_column, chunk).execute()
            rows_missed += len(chunk)
            if not still_active:
                rows_retired += len(chunk)

    logger.info(
        "Snapshot reconciled",
        table=f"{schema}.{table_name}",
        scope=scope,
        stored_rows=len(rows),
        seen_keys=len(seen_keys),
        rows_reset=rows_reset,
        rows_missed=rows_missed,
        rows_retired=rows_retired,
        threshold=threshold,
    )

    return rows_reset, rows_missed, rows_retired


__all__ = [
    "ID_CHUNK",
    "DEFAULT_MIN_PARSE_RATIO",
    "normalize_key_value",
    "build_key",
    "snapshot_is_complete",
    "reconcile_snapshot",
]
