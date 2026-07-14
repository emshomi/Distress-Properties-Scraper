"""
Supabase client singleton + schema-scoped table helpers.

The client is created lazily on first access via get_client() to avoid
import-time failures when env vars aren't set yet (e.g., during testing).

Helper functions provide schema-scoped table access:
    core_table("parcels")        → schema=core, table=parcels
    signals_table("distress_events") → schema=signals
    audit_table("scraper_runs")  → schema=audit
    scoring_table("models")      → schema=scoring

Defensive omission: we deliberately do NOT provide helpers for marketplace,
users, or ai schemas — those are owned by the frontend, not this service.
"""

from __future__ import annotations

from typing import Any

from supabase import Client, create_client

from src.config import settings
from src.utils.errors import SupabaseUnreachableError
from src.utils.logger import logger


# ============================================================
# LAZY SINGLETON
# ============================================================

_client: Client | None = None


def get_client() -> Client:
    """
    Return the singleton Supabase client, creating it on first call.

    Raises:
        SupabaseUnreachableError: If SUPABASE_URL or service-role key is unset.
    """
    global _client

    if _client is not None:
        return _client

    if settings.supabase_url is None:
        raise SupabaseUnreachableError(
            "SUPABASE_URL is not configured",
            context={"hint": "Set SUPABASE_URL in environment"},
        )
    if settings.supabase_service_role_key is None:
        raise SupabaseUnreachableError(
            "SUPABASE_SERVICE_ROLE_KEY is not configured",
            context={"hint": "Set SUPABASE_SERVICE_ROLE_KEY in environment"},
        )

    url = str(settings.supabase_url).rstrip("/")
    key = settings.supabase_service_role_key.get_secret_value()

    try:
        _client = create_client(url, key)
    except Exception as e:
        raise SupabaseUnreachableError(
            f"Failed to create Supabase client: {e}",
            context={"exception": str(e)},
        ) from e

    logger.info("Supabase client created", url=url)
    return _client


# ============================================================
# SCHEMA-SCOPED TABLE HELPERS
# ============================================================


def core_table(table_name: str) -> Any:
    """Access a table in the `core` schema (parcels, owners, transactions)."""
    return get_client().schema("core").table(table_name)


def signals_table(table_name: str) -> Any:
    """Access a table in the `signals` schema (distress_events, sheriff_sales, etc.)."""
    return get_client().schema("signals").table(table_name)


def audit_table(table_name: str) -> Any:
    """Access a table in the `audit` schema (scraper_runs, scraper_errors, source_health)."""
    return get_client().schema("audit").table(table_name)

def access_table(table_name: str) -> Any:
    """Access a table in the `access` schema (access_requests — the /data gate)."""
    return get_client().schema("access").table(table_name)

def ai_table(table_name: str) -> Any:
    """Access a table in the `ai` schema (extracted_foreclosures — the
    document-extraction staging/review pipeline, owned by this service)."""
    return get_client().schema("ai").table(table_name)


def scoring_table(table_name: str) -> Any:
    """Access a table/materialized view in the `scoring` schema.

    Covers the ML side (models, parcel_features, parcel_scores) AND the
    deal-math calibration views (comp_ratios, distress_multipliers —
    refreshed weekly by pg_cron after the Monday eCRV load).

    NOTE (2026-07-13): this file briefly carried TWO definitions of this
    function with divergent docstrings (Python silently lets the second
    shadow the first). Merged into one — never redefine helpers here."""
    return get_client().schema("scoring").table(table_name)


def outcomes_table(table_name: str) -> Any:
    """Access a table in the `outcomes` schema (redemption_tracker,
    owner_checks, checker_runs, ecrv_sales — the outcome-capture system).
    NOTE: requires `outcomes` in Supabase's exposed schemas (Settings → API)."""
    return get_client().schema("outcomes").table(table_name)



# ============================================================
# CONNECTIVITY CHECK
# ============================================================


def ping_supabase() -> bool:
    """
    Validate Supabase connectivity by performing a minimal query.

    Used at startup (file 36) to log a clear error if config is wrong.
    Returns True on success; raises SupabaseUnreachableError on failure.
    """
    try:
        client = get_client()
        # Try a simple query against the audit schema (lowest-impact)
        client.schema("audit").table("source_health").select("source_name").limit(
            1
        ).execute()
        return True
    except SupabaseUnreachableError:
        raise
    except Exception as e:
        raise SupabaseUnreachableError(
            f"Supabase ping failed: {e}",
            context={"exception": str(e)},
        ) from e


__all__ = [
    "get_client",
    "core_table",
    "signals_table",
    "audit_table",
    "scoring_table",
    "access_table",
    "ai_table",
    "outcomes_table",
    "ping_supabase",
]
