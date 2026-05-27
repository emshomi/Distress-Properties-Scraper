"""
Owner resolver service.

Owners use an append-with-history pattern:
  - At most one row per parcel has is_current=True
  - When ownership changes, the previous row is closed (is_current=False,
    ownership_end_date set) and a new row inserted (is_current=True)
  - "Change" is detected by canonical owner key (name + mailing address)

Also detects absentee and out-of-state owners.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from typing import Any

from src.db.supabase_client import core_table
from src.models.parcel import Owner, OwnerUpsert
from src.utils.logger import logger


def _canonical_owner_key(payload: OwnerUpsert) -> str:
    """
    Compute a canonical key for the owner. Used to detect ownership changes.

    Lowercase + strip non-alphanumeric so cosmetic differences
    ("Smith, John" vs "SMITH JOHN") don't trigger spurious changes.
    """
    parts = [
        payload.owner_name or "",
        payload.mailing_address or "",
        payload.mailing_city or "",
        payload.mailing_state or "",
        payload.mailing_zip or "",
    ]
    raw = "|".join(parts).lower()
    return re.sub(r"[^a-z0-9|]", "", raw)


def _derive_absentee_flags(
    payload: OwnerUpsert,
    property_city: str | None,
    property_state: str | None,
) -> tuple[bool | None, bool | None]:
    """
    Derive (is_absentee, is_out_of_state) from owner + property data.

    - is_out_of_state: mailing state differs from property state
    - is_absentee: mailing city differs from property city (any state)
    """
    is_out_of_state: bool | None = None
    is_absentee: bool | None = None

    if payload.mailing_state and property_state:
        is_out_of_state = (
            payload.mailing_state.strip().upper() != property_state.strip().upper()
        )

    if payload.mailing_city and property_city:
        is_absentee = (
            payload.mailing_city.strip().lower() != property_city.strip().lower()
        )

    # If we determined out-of-state, that implies absentee too
    if is_out_of_state and is_absentee is None:
        is_absentee = True

    return is_absentee, is_out_of_state


def resolve_owner(
    payload: OwnerUpsert,
    *,
    property_city: str | None = None,
    property_state: str | None = None,
) -> Owner | None:
    """
    Resolve an owner record using append-with-history semantics.

    Args:
        payload: OwnerUpsert with the new owner details.
        property_city, property_state: Used for absentee detection.
    """
    # Derive absentee flags if caller didn't explicitly set them
    is_absentee, is_out_of_state = _derive_absentee_flags(
        payload, property_city, property_state
    )
    if payload.is_absentee is None:
        payload = payload.model_copy(update={"is_absentee": is_absentee})
    if payload.is_out_of_state is None:
        payload = payload.model_copy(update={"is_out_of_state": is_out_of_state})

    # Compute canonical key for change detection
    new_key = _canonical_owner_key(payload)
    if not new_key.replace("|", ""):
        # No identifying info — skip
        logger.debug("Skipping owner with empty key", parcel_id=payload.parcel_id)
        return None

    # Fetch current owner row
    try:
        result = (
            core_table("owners")
            .select("*")
            .eq("parcel_id", payload.parcel_id)
            .eq("is_current", True)
            .limit(1)
            .execute()
        )
        current = result.data[0] if result.data else None
    except Exception as e:
        logger.warning(
            "Failed to fetch current owner",
            parcel_id=payload.parcel_id,
            error=str(e),
        )
        current = None

    # If existing key matches new key, this is a no-op
    if current:
        existing_payload = OwnerUpsert(
            parcel_id=current["parcel_id"],
            owner_name=current.get("owner_name"),
            mailing_address=current.get("mailing_address"),
            mailing_city=current.get("mailing_city"),
            mailing_state=current.get("mailing_state"),
            mailing_zip=current.get("mailing_zip"),
        )
        existing_key = _canonical_owner_key(existing_payload)

        if existing_key == new_key:
            # Same owner, no change
            return Owner.model_validate(current)

        # Owner changed — close out the previous row
        try:
            core_table("owners").update(
                {
                    "is_current": False,
                    "ownership_end_date": (
                        payload.ownership_start_date.isoformat()
                        if payload.ownership_start_date
                        else date.today().isoformat()
                    ),
                }
            ).eq("id", current["id"]).execute()
        except Exception as e:
            logger.warning(
                "Failed to close previous owner row",
                parcel_id=payload.parcel_id,
                error=str(e),
            )

    # Insert the new owner row
    insert_payload: dict[str, Any] = payload.model_dump(
        mode="json", exclude_none=True
    )
    insert_payload["is_current"] = True
    insert_payload["observed_at"] = datetime.now(timezone.utc).isoformat()

    try:
        result = core_table("owners").insert(insert_payload).execute()
        if result.data and len(result.data) > 0:
            return Owner.model_validate(result.data[0])
    except Exception as e:
        logger.error(
            "Failed to insert new owner",
            parcel_id=payload.parcel_id,
            error=str(e),
        )

    return None


__all__ = ["resolve_owner"]
