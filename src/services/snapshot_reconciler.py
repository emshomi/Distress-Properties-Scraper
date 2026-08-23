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
increment a miss counter on everything it did not, deactivate a row once it
has been missing often enough, and CARRY THAT STATE THROUGH TO THE EVENT
PROJECTION so it reaches the product.

=== WHY THE EVENT PROJECTION IS PART OF THIS MODULE (2026-08-23) ===
The first version of this file stopped at the typed table. On 2026-08-23 the
threshold was reached for the first time and all 8 Saint Paul rows flipped to
is_active = false, exactly as designed — and all 8 were still served by the
API, anonymously, forty-five minutes later. Verified by GET on each event id:
eight SERVED, and the vacant category total did not move off 2,032.

The reason is that nothing subscriber-facing reads this table. The API reads
signals.distress_with_parcel, which filters on
signals.distress_events.resolved_at IS NULL. Retiring the registry row and
leaving the event unresolved changes a number nobody queries. The module
docstring described the harm as "still being published to subscribers" while
the code stopped one table short of publication.

So retirement now asserts BOTH halves, and it does so as a desired-state
assertion rather than an edge trigger:

    every INACTIVE registry row in scope  -> its event is resolved
    every ACTIVE registry row in scope    -> its event is not resolved

Edge triggers were rejected deliberately. A "resolve the event at the moment
the row flips" design would never have fired for the 8 rows that had already
flipped, and would have needed a separate one-off backfill — a second thing to
remember, which is how the mismatch happened in the first place. Asserting the
state repairs history on the next ordinary run and needs nothing remembered.

Both directions are filtered server-side so the assertion is cheap and
idempotent: the resolve pass touches only rows where resolved_at IS NULL, and
the restore pass touches only rows carrying THIS module's own resolution
value. A run that changes nothing writes nothing, and neither pass can clobber
a resolution set by another process — mnpublicnotice's 'superseded' and
hennepin_tax_roll's 'cured' are invisible to it.

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

That guard now protects more than it used to. Before the event projection, a
bad retirement wrote a boolean into a table nobody reads; now it withdraws
rows from the product. The blast radius of skipping the guard has grown, and
the guard has not moved — it is still the caller's job to call it.

=== RETIRE, NEVER DELETE ===
A retired event keeps its row, its resolved_at and its resolution, and stays
queryable. Models learn from change. Nothing here deletes anything, and the
restore pass exists precisely so that a row which reappears comes back rather
than being stranded.

The reconciled table can record the same pair (2026-08-23). Before that,
retirement on signals.vacant_registrations wrote a boolean and a counter and
nothing else: no when, and no why. observed_at was no help, because it
correctly means LAST SEEN IN THE PUBLISHER'S FEED — the 8 Saint Paul rows
retired on 2026-08-23 still read 2026-08-16 there, and should. The same
retirement was therefore fully documented on the event and undocumented on
the registry.

Pass resolved_column and resolution_column to close that. Both are opt-in, so
a table lacking the columns is unaffected, and both directions are handled:
crossing the threshold stamps them, reactivation clears them. The value is
the SAME timestamp written to the event, so the two rows join exactly.

This matters most for the Minnesota Judicial Branch extracts, where the
question "when did this record stop being public" is the one that has to be
answerable.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
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

# Schema and table the event projection writes to. Not parameterised: there is
# one unified feed and the API reads exactly one view over it.
EVENT_SCHEMA: str = "signals"
EVENT_TABLE: str = "distress_events"

# Value written to signals.distress_events.resolution when a row leaves its
# source's snapshot.
#
# NOT a new word. Read from the live column before choosing it (2026-08-23):
# 'cured' 955 and 'forfeited' 46 and 'source_removed' 5 from hennepin_tax_roll,
# 'superseded' 70 from anoka_sheriff and mnpublicnotice, 'rejected' 1. The
# publisher-stopped-listing-it case already had a name, and reusing it keeps
# one vocabulary across sources instead of two synonyms nobody can group by.
DEFAULT_EVENT_RESOLUTION: str = "source_removed"


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


def _project_event_state(
    *,
    event_source: str,
    county_code: str,
    resolve_keys: set[str],
    restore_keys: set[str],
    resolution: str,
    stamp: str,
) -> tuple[int, int]:
    """Assert the resolved state of the events projected from these rows.

    Args:
        event_source: signals.distress_events.source owned by this scraper.
        county_code: county the scope is bounded to. Applied as an equality
            filter on every statement — without it, a source_id collision
            across counties would reach another county's event. Minnesota
            PINs are not globally unique; 51,662 nine-character PINs are
            shared between counties.
        resolve_keys: source_id values whose registry row is now inactive.
        restore_keys: source_id values whose registry row is now active.
        resolution: value to write, and the only value the restore pass will
            clear.
        stamp: retirement timestamp for this run. Passed in rather than
            computed here so the registry row and its projected event carry
            the IDENTICAL value — that makes a retirement joinable across the
            two tables instead of matchable only within a time window.

    Returns:
        (events_resolved, events_restored) — rows actually changed, not rows
        addressed. Both statements are filtered server-side, and PostgREST
        returns the rows it touched, so len(result.data) is a real count here
        rather than the batch size. That distinction is the 2026-08-22 counter
        defect; it is safe in this direction precisely because the filters
        exclude rows already in the desired state.
    """
    table = get_client().schema(EVENT_SCHEMA).table(EVENT_TABLE)

    events_resolved = 0
    events_restored = 0

    # --- Inactive registry rows -> resolved events ---
    # .is_("resolved_at", "null") makes this idempotent: an already-resolved
    # event is not matched, so its resolved_at keeps the timestamp of the run
    # that actually retired it instead of being rewritten every morning.
    for chunk in _chunk(sorted(resolve_keys)):
        result = (
            table.update({"resolved_at": stamp, "resolution": resolution})
            .eq("source", event_source)
            .eq("county_code", county_code)
            .in_("source_id", chunk)
            .is_("resolved_at", "null")
            .execute()
        )
        events_resolved += len(result.data or [])

    # --- Active registry rows -> unresolved events ---
    # The symmetric half, and the one that is easy to leave out. Without it a
    # building that comes back onto the registry is reactivated in the typed
    # table and stays invisible in the product forever — worse than the bug
    # this module was written for, because nothing about it is visible.
    #
    # .eq("resolution", resolution) is the safety filter: this pass can only
    # clear a resolution THIS module wrote. A sheriff sale marked 'superseded'
    # or a tax event marked 'cured' on the same parcel is never touched.
    for chunk in _chunk(sorted(restore_keys)):
        result = (
            table.update({"resolved_at": None, "resolution": None})
            .eq("source", event_source)
            .eq("county_code", county_code)
            .in_("source_id", chunk)
            .eq("resolution", resolution)
            .execute()
        )
        events_restored += len(result.data or [])

    return events_resolved, events_restored


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
    event_source: str | None = None,
    event_key_column: str = "parcel_id",
    event_county_column: str = "county_code",
    event_resolution: str = DEFAULT_EVENT_RESOLUTION,
    resolved_column: str | None = None,
    resolution_column: str | None = None,
) -> tuple[int, int, int, int, int]:
    """
    Reconcile stored rows against the keys a complete fetch actually saw, and
    project the result onto signals.distress_events.

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
        event_source: distress_events.source this scraper owns. When None the
            event projection is SKIPPED entirely and only the typed table is
            reconciled — the pre-2026-08-23 behaviour, kept for a source whose
            events are not keyed on a single registry column.
        event_key_column: registry column whose value equals
            distress_events.source_id. Verified 2026-08-23 across both
            registry sources: saint_paul_vacant 400 of 400 and mpls_vbr 311 of
            311 have source_id = parcel_id, 1:1 in both directions, no parcel
            carrying two events.
        event_county_column: which scope key carries the county. Required to
            be present in `scope` when event_source is set.
        event_resolution: resolution string to write, and the only one the
            restore pass will clear.
        resolved_column: column on the RECONCILED table that records when a
            row was retired, or None to record nothing. Opt-in for the same
            reason event_source is: a table without the column would fail
            every update, and the caller is the only thing that knows its
            shape. Passing it also makes the reactivation branch clear the
            value, so a row that comes back does not keep a retirement date.
        resolution_column: column on the reconciled table recording WHY, using
            the same vocabulary as event_resolution.

    Returns:
        (rows_reset, rows_missed, rows_retired, events_resolved,
         events_restored)

        rows_missed counts every absent row, including those already retired
        on a previous run and skipped this time. rows_retired counts only the
        rows that CROSSED the threshold on this run.
    """
    if not scope:
        raise ValueError(
            "reconcile_snapshot requires a non-empty scope; an unscoped "
            "reconciliation would retire rows belonging to other sources"
        )

    # The event statements are bounded by county as well as source. If the
    # caller asks for the projection without telling us the county, fail here
    # rather than issuing a cross-county update.
    event_county_value: Any = None
    if event_source is not None:
        if event_county_column not in scope:
            raise ValueError(
                f"event projection requires {event_county_column!r} in scope; "
                "an event update bounded only by source_id could reach "
                "another county's rows"
            )
        event_county_value = scope[event_county_column]

    select_cols = ",".join(
        [id_column, *key_columns, miss_column, active_column]
    )
    # The event key is usually one of key_columns already; ask for it only if
    # it is not, so the select stays minimal and no column is requested twice.
    if event_source is not None and event_key_column not in (
        id_column,
        *key_columns,
        miss_column,
        active_column,
    ):
        select_cols = f"{select_cols},{event_key_column}"

    query = get_client().schema(schema).table(table_name).select(select_cols)
    for column, value in scope.items():
        query = query.eq(column, value)
    stored = query.execute()
    rows = stored.data or []

    present_ids: list[Any] = []
    absent_by_count: dict[int, list[Any]] = {}

    # Desired end state per event key, computed from every row in scope —
    # not just the rows this run changes. That is what makes the projection
    # self-healing: the 8 rows already retired on 2026-08-23 land in
    # resolve_keys even though their registry rows need no update at all.
    resolve_keys: set[str] = set()
    restore_keys: set[str] = set()

    rows_missed = 0
    rows_absent_already_retired = 0

    for row in rows:
        key = build_key(row.get(col) for col in key_columns)
        if key in seen_keys:
            final_active = True
            # Only rewrite rows that actually need it.
            if row.get(miss_column) or not row.get(active_column):
                present_ids.append(row[id_column])
        else:
            rows_missed += 1
            current = row.get(miss_column) or 0
            # A row that is already retired and still absent needs no further
            # write. Before this check the counter climbed 4, 5, 6... forever
            # and every one of those runs re-counted the row as retired, so
            # the daily log claimed 8 retirements a day for 8 rows retired
            # once. Skipping keeps consecutive_misses meaningful as "misses
            # that led to retirement" and stops a pointless daily UPDATE.
            if current >= threshold and not row.get(active_column):
                rows_absent_already_retired += 1
                final_active = False
            else:
                new_count = current + 1
                final_active = new_count < threshold
                absent_by_count.setdefault(current, []).append(row[id_column])

        if event_source is not None:
            event_key = normalize_key_value(row.get(event_key_column))
            if event_key is not None:
                if final_active:
                    restore_keys.add(event_key)
                else:
                    resolve_keys.add(event_key)

    table = get_client().schema(schema).table(table_name)

    # ONE timestamp for the whole run, shared with the event projection below.
    # A retirement is a single fact; recording it with two clock reads that
    # differ by milliseconds makes the registry row and its event look like
    # separate events to anyone joining them later.
    stamp = datetime.now(timezone.utc).isoformat()

    rows_reset = 0
    rows_retired = 0

    # Reactivation must CLEAR the retirement stamp, not merely flip the
    # boolean. Otherwise a building that comes back onto the registry is
    # active while still carrying the date it was retired — a row that
    # contradicts itself, and the kind of thing that is read as truth years
    # later because nothing about it looks wrong.
    reset_payload: dict[str, Any] = {miss_column: 0, active_column: True}
    if resolved_column is not None:
        reset_payload[resolved_column] = None
    if resolution_column is not None:
        reset_payload[resolution_column] = None

    for chunk in _chunk(present_ids):
        table.update(reset_payload).in_(id_column, chunk).execute()
        rows_reset += len(chunk)

    for current, ids in absent_by_count.items():
        new_count = current + 1
        still_active = new_count < threshold
        absent_payload: dict[str, Any] = {
            miss_column: new_count,
            active_column: still_active,
        }
        # Stamped only on the run that CROSSES the threshold. A row on its
        # first or second miss is still active and has not been retired, so
        # writing a retirement date there would be false.
        if not still_active:
            if resolved_column is not None:
                absent_payload[resolved_column] = stamp
            if resolution_column is not None:
                absent_payload[resolution_column] = event_resolution
        for chunk in _chunk(ids):
            table.update(absent_payload).in_(id_column, chunk).execute()
            if not still_active:
                rows_retired += len(chunk)

    # The event projection runs AFTER the typed table is settled, and its
    # failure must not roll back or obscure work that already succeeded — the
    # caller wraps this whole function in try/except and counts a failure
    # without failing the run.
    events_resolved = 0
    events_restored = 0
    if event_source is not None:
        events_resolved, events_restored = _project_event_state(
            event_source=event_source,
            county_code=event_county_value,
            resolve_keys=resolve_keys,
            restore_keys=restore_keys,
            resolution=event_resolution,
            stamp=stamp,
        )

    logger.info(
        "Snapshot reconciled",
        table=f"{schema}.{table_name}",
        scope=scope,
        stored_rows=len(rows),
        seen_keys=len(seen_keys),
        rows_reset=rows_reset,
        rows_missed=rows_missed,
        rows_absent_already_retired=rows_absent_already_retired,
        rows_retired=rows_retired,
        threshold=threshold,
        event_source=event_source,
        events_to_resolve=len(resolve_keys),
        events_to_restore=len(restore_keys),
        events_resolved=events_resolved,
        events_restored=events_restored,
    )

    return rows_reset, rows_missed, rows_retired, events_resolved, events_restored


__all__ = [
    "ID_CHUNK",
    "DEFAULT_MIN_PARSE_RATIO",
    "EVENT_SCHEMA",
    "EVENT_TABLE",
    "DEFAULT_EVENT_RESOLUTION",
    "normalize_key_value",
    "build_key",
    "snapshot_is_complete",
    "reconcile_snapshot",
]
