"""
Application configuration loaded from environment variables.

Uses pydantic-settings for typed, validated config with secrets handling.
Every setting has a clear type, an optional default, and (where relevant)
a SecretStr wrapper to prevent accidental logging.

The `settings` singleton at the bottom is imported throughout the codebase.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, HttpUrl, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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

    # ----- External service credentials -----

    minneapolis_311_app_token: SecretStr | None = Field(
        default=None,
        description="Socrata app token for Minneapolis 311 dataset",
    )

    mapbox_token: SecretStr | None = Field(
        default=None,
        description="Mapbox API token for geocoding",
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

    # ----- Scraper toggles -----

    scraper_mpls_311_enabled: bool = Field(default=True)
    scraper_hennepin_sheriff_enabled: bool = Field(default=True)
    scraper_hennepin_parcels_enabled: bool = Field(default=True)
    scraper_dakota_sheriff_enabled: bool = Field(default=True)
    scraper_dakota_parcels_enabled: bool = Field(default=True)
    scraper_ramsey_parcels_enabled: bool = Field(default=True)
    scraper_ramsey_tax_roll_enabled: bool = Field(default=True)
    scraper_anoka_sheriff_enabled: bool = Field(default=True)
    scraper_washington_sheriff_enabled: bool = Field(default=True)
    scraper_ramsey_sheriff_enabled: bool = Field(default=True)
    scraper_mpls_vbr_enabled: bool = Field(default=True)
    scraper_saint_paul_vacant_enabled: bool = Field(default=True)
    scraper_mcro_probate_enabled: bool = Field(default=False)
    scraper_usps_vacancy_enabled: bool = Field(default=False)
    scraper_tax_forfeit_enabled: bool = Field(default=True)

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
        """
        attr_name = f"scraper_{source_name}_enabled"
        return bool(getattr(self, attr_name, False))


# ============================================================
# SINGLETON
# ============================================================

settings = Settings()
