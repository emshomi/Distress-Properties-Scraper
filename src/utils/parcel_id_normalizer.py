"""
Parcel ID normalization for Minnesota counties.

Each county uses a different parcel ID format. This module converts the
various source formats into the canonical form stored in core.parcels.

Canonical form for each county:
  - Hennepin: 13 digits, no separators (e.g., "0102924130001")
  - Ramsey:   12 digits, no separators (e.g., "012345678901")
  - Others:   strip non-alphanumeric, lowercase

Use safe_normalize_parcel_id() for use cases where you want to catch
malformed IDs without exceptions; use normalize_parcel_id() when an
invalid ID is a hard error.
"""

from __future__ import annotations

import re
from typing import Callable

# ============================================================
# COUNTY-SPECIFIC NORMALIZERS
# ============================================================


def _normalize_hennepin(raw: str) -> str:
    """
    Hennepin: 13-digit PID. Source data may have dashes, dots, spaces.

    LEADING-ZERO RECOVERY (2026-07-08): a 12-digit value is a 13-digit PID
    with its leading zero dropped — the classic spreadsheet/shapefile
    export defect (same class as the Dakota TAXPIN 12-vs-13 bridge).
    Hennepin PIDs legitimately begin with 0 (section numbers 01-09), so a
    12-digit input is unambiguous and is left-padded back to 13. Verified
    empirically on the mpls_vbr snapshot: 117/118 padded APNs join real
    assessor parcels in core.parcels.

    Examples:
        "01-029-24-13-0001" → "0102924130001"
        "0102924130001"     → "0102924130001"
        "01.029.24.13.0001" → "0102924130001"
        "902924220131"      → "0902924220131"  (dropped leading zero)
    """
    cleaned = re.sub(r"[^0-9]", "", raw)
    if len(cleaned) == 12:
        cleaned = cleaned.zfill(13)
    if len(cleaned) != 13:
        raise ValueError(
            f"Hennepin PID must be 13 digits after normalization; got {len(cleaned)} from {raw!r}"
        )
    return cleaned


def _normalize_ramsey(raw: str) -> str:
    """
    Ramsey: 12-digit PIN. Often dot-separated in source data.

    Examples:
        "01.23.45.6789.01" → "012345678901"
        "012345678901"     → "012345678901"
    """
    cleaned = re.sub(r"[^0-9]", "", raw)
    if len(cleaned) != 12:
        raise ValueError(
            f"Ramsey PIN must be 12 digits after normalization; got {len(cleaned)} from {raw!r}"
        )
    return cleaned


def _normalize_generic(raw: str) -> str:
    """
    Fallback for counties we haven't customized.

    Strips all non-alphanumeric chars, lowercases. Doesn't enforce length
    because formats vary widely across smaller MN counties.
    """
    cleaned = re.sub(r"[^a-zA-Z0-9]", "", raw).lower()
    if not cleaned:
        raise ValueError(f"Parcel ID is empty after normalization: {raw!r}")
    if len(cleaned) < 6:
        raise ValueError(
            f"Parcel ID too short ({len(cleaned)} chars) — likely malformed: {raw!r}"
        )
    return cleaned


# ============================================================
# REGISTRY
# ============================================================

_COUNTY_NORMALIZERS: dict[str, Callable[[str], str]] = {
    "hennepin": _normalize_hennepin,
    "ramsey": _normalize_ramsey,
    "dakota": _normalize_generic,
    "anoka": _normalize_generic,
    "washington": _normalize_generic,
    "scott": _normalize_generic,
    "carver": _normalize_generic,
    "wright": _normalize_generic,
    "st_louis": _normalize_generic,
    "olmsted": _normalize_generic,
    "stearns": _normalize_generic,
    "otter_tail": _normalize_generic,
    "cass": _normalize_generic,
    "chisago": _normalize_generic,
    "fillmore": _normalize_generic,  # Beacon/Schneider county; verify format against real IDs in Phase 1
}


# ============================================================
# PUBLIC API
# ============================================================


def normalize_parcel_id(county_code: str, raw_id: str) -> str:
    """
    Convert a raw parcel ID to canonical form for the given county.

    Args:
        county_code: Lowercase county identifier (e.g., 'hennepin').
        raw_id: The parcel ID as it appears in source data.

    Returns:
        Canonical parcel ID string.

    Raises:
        ValueError: If the county is unknown or the ID can't be normalized.
    """
    if not raw_id or not isinstance(raw_id, str):
        raise ValueError(f"Parcel ID must be a non-empty string; got {type(raw_id).__name__}")

    county_lower = county_code.strip().lower()
    normalizer = _COUNTY_NORMALIZERS.get(county_lower, _normalize_generic)
    return normalizer(raw_id)


def safe_normalize_parcel_id(
    county_code: str, raw_id: str
) -> tuple[str | None, str | None]:
    """
    Non-raising variant of normalize_parcel_id.

    Returns:
        (normalized_id, None) on success
        (None, error_message) on failure
    """
    try:
        return normalize_parcel_id(county_code, raw_id), None
    except (ValueError, TypeError) as e:
        return None, str(e)


__all__ = [
    "normalize_parcel_id",
    "safe_normalize_parcel_id",
]
