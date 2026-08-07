"""
Parcel resolver service.

Implements per-field merge semantics when upserting parcels:
  - Immutable fields: parcel_id, county_code, state (must match existing)
  - Overwrite fields: estimated_market_value, equity, mortgage_balance
    (always take new value — staler estimates are less useful)
  - Fill-in fields: everything else (only fill if existing is null/empty)
  - List fields: data_sources (set union, deduplicated)

This produces a parcel record that gets richer over time as more
scrapers contribute information.

=== core.parcels IS KEYED ON (county_code, parcel_id) — fixed 2026-08-07 ===
Minnesota county PINs are NOT globally unique: 51,662 nine-character PINs
are shared across counties (measured; 14 counties use 9-char PINs). Phase 5
of the composite-key migration moved core.parcels to
PRIMARY KEY (county_code, parcel_id). This file was missed — the migration
repaired seven parcel loaders and event_writer.py, but not the shared
resolver that EVERY scraper calls.

TWO defects, both fixed here. They are different in kind:

1. `on_conflict="parcel_id"` matched no unique index, so PostgREST raised
   42P10 and the upsert failed. LOUD. Measured 2026-08-07: hennepin_sheriff
   run 545 logged this 514 times and wrote nothing; anoka_sheriff failed 86
   of 172 and dakota_sheriff 170 of 340 — exactly half each, because the
   parcel leg failed while the distress_events leg succeeded.

2. The existing-row SELECT filtered on `parcel_id` alone with `.limit(1)`.
   SILENT, and far worse. A shared PIN matches rows in several counties;
   limit(1) returns an arbitrary one; _merge_parcel_payload then merges
   ANOTHER COUNTY'S address, market value and coordinates into this parcel.
   _IMMUTABLE_FIELDS keeps county_code correct, so the row would look
   properly labelled while carrying the wrong property's data — undetectable
   without comparing against the source. This is the mirror image of the
   original key defect that silently destroyed ~191,600 parcels across
   twenty counties.

Defect 1 was masking defect 2: nothing committed, so nothing was corrupted.
Fixing the conflict target ALONE would have unblocked the corruption.

NEVER query core.parcels on parcel_id alone. The correct filter is always
`.eq("parcel_id", ...).eq("county_code", ...)`, and the correct conflict
target is always "county_code,parcel_id".
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from src.db.supabase_client import core_table
from src.models.parcel import Parcel, ParcelUpsert
from src.utils.logger import logger


# Fields that should always be overwritten with newest value
_OVERWRITE_FIELDS: frozenset[str] = frozenset({
    "estimated_market_value",
    "estimated_equity",
    "estimated_mortgage_balance",
})

# Fields that cannot change once set
_IMMUTABLE_FIELDS: frozenset[str] = frozenset({
    "parcel_id",
    "county_code",
    "state",
})


def _merge_parcel_payload(
    existing: dict[str, Any] | None,
    incoming: ParcelUpsert,
) -> dict[str, Any]:
    """
    Merge incoming parcel data with existing row according to merge rules.

    Returns a dict ready for upsert into core.parcels.
    """
    incoming_dict = incoming.model_dump(mode="json", exclude_none=True)

    if existing is None:
        # First time we've seen this parcel — record first_observed_at
        incoming_dict["first_observed_at"] = datetime.now(timezone.utc).isoformat()
        incoming_dict["last_observed_at"] = incoming_dict.get(
            "last_observed_at"
        ) or incoming_dict["first_observed_at"]
        return incoming_dict

    merged: dict[str, Any] = dict(existing)

    # Always advance last_observed_at to now
    merged["last_observed_at"] = datetime.now(timezone.utc).isoformat()

    for field, new_value in incoming_dict.items():
        if field in _IMMUTABLE_FIELDS:
            # Don't modify immutable fields (existing values stay)
            continue
        if field == "data_sources":
            # Union of lists, deduped
            existing_sources = set(merged.get("data_sources") or [])
            new_sources = set(new_value or [])
            merged["data_sources"] = sorted(existing_sources | new_sources)
            continue
        if field in _OVERWRITE_FIELDS:
            # Overwrite with newer value
            merged[field] = new_value
            continue
        # Fill-in semantics: only set if existing is null/empty/zero
        existing_value = merged.get(field)
        if existing_value in (None, "", 0, 0.0):
            merged[field] = new_value

    return merged


def resolve_parcel(payload: ParcelUpsert) -> Parcel | None:
    """
    Upsert a parcel row, merging with existing data per field rules.

    Returns the resolved Parcel, or None if the operation failed.
    """
    parcel_id = payload.parcel_id
    county_code = payload.county_code

    # A parcel is identified by (county_code, parcel_id). Without a county we
    # cannot look one up unambiguously and MUST NOT guess: merging against an
    # arbitrary county's row would copy that property's address and market
    # value onto this one. Refuse instead.
    if not county_code:
        logger.error(
            "Cannot resolve parcel without county_code",
            parcel_id=parcel_id,
            hint="core.parcels is keyed on (county_code, parcel_id)",
        )
        return None

    # Fetch existing row (if any). BOTH key columns — see module docstring.
    existing: dict[str, Any] | None = None
    try:
        result = (
            core_table("parcels")
            .select("*")
            .eq("parcel_id", parcel_id)
            .eq("county_code", county_code)
            .limit(1)
            .execute()
        )
        if result.data and len(result.data) > 0:
            existing = result.data[0]
    except Exception as e:
        logger.warning(
            "Failed to fetch existing parcel for merge",
            parcel_id=parcel_id,
            county_code=county_code,
            error=str(e),
        )

    merged = _merge_parcel_payload(existing, payload)

    # Upsert the merged record. Conflict target must match the composite
    # PRIMARY KEY on core.parcels, in key order.
    try:
        result = (
            core_table("parcels")
            .upsert(merged, on_conflict="county_code,parcel_id")
            .execute()
        )
        if result.data and len(result.data) > 0:
            return Parcel.model_validate(result.data[0])
    except Exception as e:
        logger.error(
            "Failed to upsert parcel",
            parcel_id=parcel_id,
            county_code=county_code,
            error=str(e),
        )

    return None


def resolve_parcels_batch(payloads: list[ParcelUpsert]) -> tuple[int, int]:
    """
    Resolve many parcels. Returns (succeeded, failed).

    Each parcel is resolved independently — a failure on one doesn't stop
    the others.
    """
    succeeded = 0
    failed = 0
    for payload in payloads:
        if resolve_parcel(payload) is not None:
            succeeded += 1
        else:
            failed += 1
    return succeeded, failed


__all__ = [
    "resolve_parcel",
    "resolve_parcels_batch",
]
