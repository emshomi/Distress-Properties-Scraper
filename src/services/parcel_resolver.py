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

    # Fetch existing row (if any)
    existing: dict[str, Any] | None = None
    try:
        result = (
            core_table("parcels")
            .select("*")
            .eq("parcel_id", parcel_id)
            .limit(1)
            .execute()
        )
        if result.data and len(result.data) > 0:
            existing = result.data[0]
    except Exception as e:
        logger.warning(
            "Failed to fetch existing parcel for merge",
            parcel_id=parcel_id,
            error=str(e),
        )

    merged = _merge_parcel_payload(existing, payload)

    # Upsert the merged record
    try:
        result = (
            core_table("parcels")
            .upsert(merged, on_conflict="parcel_id")
            .execute()
        )
        if result.data and len(result.data) > 0:
            return Parcel.model_validate(result.data[0])
    except Exception as e:
        logger.error(
            "Failed to upsert parcel",
            parcel_id=parcel_id,
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
