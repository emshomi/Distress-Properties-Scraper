"""
Application configuration loaded from environment variables.

Uses pydantic-settings for typed, validated config with secrets handling.
Every setting has a clear type, an optional default, and (where relevant)
a SecretStr wrapper to prevent accidental logging.

The `settings` singleton at the bottom is imported throughout the codebase.
"""

from __future__ import annotations

import logging
from typing import Literal

from pydantic import Field, HttpUrl, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# stdlib logging, NOT src.utils.logger. config.py is imported by nearly
# everything including the logger itself, so importing the project logger here
# would be circular.
_log = logging.getLogger(__name__)


class Settings(BaseSettings):
    """
    Service configuration. Reads from environment variables (and .env in dev).
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ----- Service identity -----

    environment: Literal["development", "production"] = Field(
        default="production",
        description="Deployment environment",
    )

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Logging verbosity",
    )

    # ----- Admin authentication -----

    admin_api_key: SecretStr | None = Field(
        default=None,
        description="Shared secret for protected endpoints (X-Admin-Key header)",
    )

    # ----- Auth JWT verification -----

    jwt_public_key: SecretStr | None = Field(
        default=None,
        description="Public key (PEM) for verifying app_auth-issued JWTs; "
                    "optional, JWT tier checks are skipped when unset",
    )

    # ----- Supabase connection -----

    supabase_url: HttpUrl | None = Field(
        default=None,
        description="Supabase project URL",
    )

    supabase_service_role_key: SecretStr | None = Field(
        default=None,
        description="Supabase service_role key (secret, NOT anon key)",
    )

    # ----- Scheduler -----

    scheduler_timezone: str = Field(
        default="America/Chicago",
        description="Timezone for cron expressions",
    )

    # ----- LLM (Anthropic Claude) -----

    anthropic_api_key: SecretStr | None = Field(
        default=None,
        description="Anthropic API key for Claude (NL search, summaries, extraction)",
    )

    llm_model: str = Field(
        default="claude-haiku-4-5-20251001",
        description="Default Claude model for LLM features",
    )

    llm_max_tokens: int = Field(
        default=1024,
        ge=1,
        le=8192,
        description="Default max output tokens per LLM call",
    )

    llm_timeout_seconds: int = Field(
        default=30,
        ge=5,
        le=120,
        description="Timeout per LLM API call",
    )

    # ----- External service credentials -----

    minneapolis_311_app_token: SecretStr | None = Field(
        default=None,
        description="Socrata app token for Minneapolis 311 dataset",
    )

    mapbox_token: SecretStr | None = Field(
        default=None,
        description="Mapbox API token for geocoding",
    )

    # ADDED 2026-08-13 — Google Maps Platform, for Street View imagery.
    #
    # In Settings rather than os.environ for the reason the Resend block
    # above records: a credential that lives only in a standalone script
    # silently no-ops the day the service needs it too. Street View is
    # needed in BOTH places — the pano resolver job, and the API route that
    # serves pano IDs to entitled tiers.
    #
    # The pano ID is a LOCATOR: one call to Google's metadata endpoint turns
    # it into coordinates. It belongs behind the same tier gate as address
    # and lat/lng (_LOCATOR_FIELDS in src/utils/redaction.py), and must never
    # reach a below-Standard client.
    #
    # Google's terms permit storing the panorama ID indefinitely but PROHIBIT
    # storing, caching or rehosting the imagery itself. We store IDs, never
    # pixels; the browser fetches each image from Google directly.
    google_maps_api_key: SecretStr | None = Field(
        default=None,
        description="Google Maps Platform key — Street View Static imagery "
                    "and Maps JavaScript API",
    )

    nominatim_user_agent: str = Field(
        default="distress-properties-scraper/1.0",
        description="User-Agent for Nominatim (fallback geocoder)",
    )

    hud_usps_vacancy_url: HttpUrl | None = Field(
        default=None,
        description="Per-account HUD USPS vacancy CSV download URL",
    )

   # ----- CORS -----

    frontend_origin: HttpUrl | None = Field(
        default=None,
        description="Production frontend origin for CORS allow-list",
    )

    # ----- Transactional email (Resend) -----
    # Added 2026-07-29 for Connect magic-link auth. scripts/health_alert.py
    # already sends via Resend but reads os.environ directly, so these were
    # never in config. Without them the magic link silently no-ops.

    resend_api_key: SecretStr | None = Field(
        default=None,
        description="Resend API key for transactional email (magic links)",
    )

    alert_email_from: str | None = Field(
        default=None,
        description="Verified Resend sender, e.g. noreply@govire.com",
    )

    # ----- Scraper toggles -----

    scraper_mpls_311_enabled: bool = Field(default=True)
    scraper_hennepin_sheriff_enabled: bool = Field(default=True)
    scraper_hennepin_parcels_enabled: bool = Field(default=True)
    scraper_dakota_sheriff_enabled: bool = Field(default=True)
    scraper_dakota_parcels_enabled: bool = Field(default=True)
    scraper_ramsey_parcels_enabled: bool = Field(default=True)
    scraper_ramsey_tax_roll_enabled: bool = Field(default=True)
    scraper_ramsey_tfl_enabled: bool = Field(default=True)
    scraper_olmsted_parcels_enabled: bool = Field(default=True)
    scraper_olmsted_tax_detail_enabled: bool = Field(default=True)
    scraper_fillmore_parcels_enabled: bool = Field(default=True)
    scraper_fillmore_legal_enabled: bool = Field(default=True)
    scraper_fillmore_probate_enabled: bool = Field(default=True)
    scraper_anoka_parcels_enabled: bool = Field(default=True)
    scraper_wabasha_parcels_enabled: bool = Field(default=True)
    scraper_postbulletin_legal_enabled: bool = Field(default=True)
    scraper_anoka_sheriff_enabled: bool = Field(default=True)
    scraper_washington_sheriff_enabled: bool = Field(default=True)
    scraper_washington_parcels_enabled: bool = Field(default=True)
    scraper_ramsey_sheriff_enabled: bool = Field(default=True)
    scraper_mpls_vbr_enabled: bool = Field(default=True)
    scraper_saint_paul_vacant_enabled: bool = Field(default=True)
    scraper_mcro_probate_enabled: bool = Field(default=False)
    scraper_usps_vacancy_enabled: bool = Field(default=False)
    scraper_tax_forfeit_enabled: bool = Field(default=True)
    scraper_parcel_enrich_mngeo_enabled: bool = Field(default=True)

    # ADDED 2026-08-02. These scrapers were WRITING DATA with no toggle here.
    # scraper_enabled() falls back to False for an unknown attribute, so each
    # was "disabled" the moment anything routed it through BaseScraper.run() —
    # while the standalone runners, which called fetch/parse/write directly,
    # never consulted the flag and kept working.
    #
    # Found when scripts/run_hennepin_tax_roll.py was converted to run() and
    # the workflow failed with ScraperDisabledError. hennepin_tax_roll holds
    # 4,255 events, the largest single source in signals.distress_events, and
    # had written as recently as the previous day. Nobody had turned it off;
    # the field simply never existed.
    #
    # olmsted_delq_list (502 events) feeds the Olmsted forfeiture clocks and
    # has the same gap. Both default True because both are demonstrably
    # intended to run.
    scraper_hennepin_tax_roll_enabled: bool = Field(default=True)
    scraper_olmsted_delq_list_enabled: bool = Field(default=True)

    # ADDED 2026-08-19 — Olmsted probate, WITH the scraper rather than after
    # a failed run. scraper_enabled() below returns False for a missing
    # field, so the workflow env var alone would do nothing: extra="ignore"
    # discards the unknown key, hasattr fails, and the run dies with
    # ScraperDisabledError. That is the hennepin_tax_roll story above, and
    # the only reason it would surface fast here is that
    # run_olmsted_probate.py passes trigger="manual" so a disabled flag
    # raises instead of silently skipping.
    #
    # Defaults FALSE, unlike fillmore_probate. Not a judgement about the
    # source — a brand-new scraper against a live 75,039-parcel owner table
    # should not start writing because a deploy happened. The workflow sets
    # it 'true' explicitly, so the first run is a deliberate dispatch that
    # can be watched. Flip this to True once a manual run has been read.
    scraper_olmsted_probate_enabled: bool = Field(default=False)

    # ADDED 2026-08-05 — MnGeo statewide parcel spine.
    #
    # ONE field for FIFTY-ONE counties, deliberately. The config-driven
    # loader keeps a per-county source_name ('stearns_parcels') because
    # core.parcels.data_sources and core.owners.source must stay per-county
    # or provenance collapses — but it gates on this single umbrella key via
    # its enable_key attribute. Adding a field per county would re-create the
    # exact per-county artefact multiplication core.mngeo_county_load was
    # built to remove, and 51 more chances at the hennepin_tax_roll failure
    # documented in scraper_enabled() below.
    #
    # Per-county control lives in core.mngeo_county_load.enabled, so turning
    # a county on is an UPDATE, not a redeploy. This field is the master kill
    # switch for all of them.
    #
    # Defaults FALSE: 51 counties / 1,562,648 parcels should not start moving
    # because a deploy happened. Set it true deliberately when loading.
    scraper_mngeo_parcels_enabled: bool = Field(default=False)

    # ADDED 2026-08-08 — Tyler/iasWorld portal tax detail, vendor-grouped.
    #
    # ONE field for every Tyler county, the same shape as
    # scraper_mngeo_parcels_enabled above and for the same reason. Counties
    # buy iasWorld from Tyler rather than building their own portal, so one
    # scraper class serves all of them; a field per county would re-create
    # the artefact multiplication core.vendor_portals was built to remove.
    #
    # source_name stays PER-COUNTY ('olmsted_tax_detail',
    # 'carver_tax_detail') because audit.scraper_runs and
    # audit.source_health key on it — a collapsed source_name would mark
    # the whole vendor unhealthy when one county failed.
    #
    # Per-county control lives in core.vendor_portals.enabled, which cannot
    # be set true without a verified_url containing that county's own host
    # and its own jur (check constraint, not code). Turning a county on is
    # an UPDATE, not a redeploy.
    #
    # Defaults FALSE: a portal row landing in the registry should not start
    # hitting a county's server because a deploy happened. The hand-written
    # OlmstedTaxDetailScraper is unaffected — its enable_key is empty, so it
    # still gates on scraper_olmsted_tax_detail_enabled.
    scraper_tyler_tax_detail_enabled: bool = Field(default=False)

    # ----- Scraper behavior -----

    scraper_request_timeout_seconds: int = Field(
        default=30,
        ge=5,
        le=300,
        description="HTTP timeout per scraper request",
    )

    scraper_max_retries: int = Field(
        default=3,
        ge=0,
        le=10,
        description="Max retries on transient failures",
    )

    # ----- Geocoding -----

    geocoding_enabled: bool = Field(
        default=True,
        description="Master toggle for the geocoder service",
    )

    geocoding_cache_days: int = Field(
        default=90,
        ge=1,
        le=365,
        description="Days to cache geocoded coordinates before re-geocoding",
    )

    # ----- Validators -----

    @field_validator("nominatim_user_agent")
    @classmethod
    def _validate_nominatim_ua(cls, v: str) -> str:
        """Nominatim policy requires a non-empty, identifying User-Agent."""
        if not v or len(v.strip()) < 5:
            raise ValueError(
                "NOMINATIM_USER_AGENT must be a meaningful identifier "
                "(min 5 chars, format: 'service/version (contact)')"
            )
        return v.strip()

    # ----- Convenience helpers -----

    def scraper_enabled(self, source_name: str) -> bool:
        """
        Check whether a scraper is enabled by its source_name.

        Maps the source_name (e.g., 'mpls_311') to the env-toggle field
        (e.g., 'scraper_mpls_311_enabled').

        A MISSING field returns False, and warns loudly (2026-08-02).

        False is the right answer for an unknown scraper — defaulting an
        unrecognised name to enabled would let a typo silently run something.
        But returning it SILENTLY is how hennepin_tax_roll came to be disabled
        without anyone deciding to disable it: the field was never added, the
        standalone runner bypassed this check entirely, and the discrepancy
        only surfaced months later when that runner was changed to go through
        BaseScraper.run() and the workflow died with ScraperDisabledError.

        The warning distinguishes "deliberately off" from "never wired". Both
        return False; only one of them is a decision.
        """
        attr_name = f"scraper_{source_name}_enabled"
        if not hasattr(self, attr_name):
            _log.warning(
                "No config toggle for scraper %r (expected field %r) — "
                "treating as DISABLED. Add the field to Settings if this "
                "scraper is meant to run.",
                source_name,
                attr_name,
            )
            return False
        return bool(getattr(self, attr_name))


# ============================================================
# SINGLETON
# ============================================================

settings = Settings()
