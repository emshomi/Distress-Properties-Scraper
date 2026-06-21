"""
Tier resolution for property endpoints.

Determines the caller's tier from the request, in priority order:

    1. Valid X-Admin-Key  -> "admin"  (you; sees everything)
    2. Valid app_auth JWT -> its `tier` claim (when the frontend sends one)
    3. Valid X-Access-Key -> that row's `tier` in access.access_requests
    4. Nothing            -> "free" (anonymous; most redacted)

This is a non-raising dependency: it NEVER 401s. Unlike require_access_key
(which blocks anonymous callers), the tier model lets anonymous callers through
as `free` and lets redaction decide what they see. That is the spec: free users
browse locked cards.

Returns a small TierContext the endpoints pass to the redaction layer.
"""

from __future__ import annotations

import hmac
from dataclasses import dataclass
from typing import Annotated, Optional

from fastapi import Depends, Header

from src.config import settings
from src.utils.logger import logger

# JWT verification is optional: only runs if a token is present AND the public
# key is configured. Import lazily/guarded so the service still boots if PyJWT
# or the key isn't present yet.
try:
    import jwt as _pyjwt  # PyJWT
except Exception:  # pragma: no cover
    _pyjwt = None


_VALID_TIERS = {"free", "basic", "standard", "premium"}


@dataclass(frozen=True)
class TierContext:
    tier: str          # free | basic | standard | premium | admin
    is_admin: bool


def _admin_matches(x_admin_key: Optional[str]) -> bool:
    if not x_admin_key or settings.admin_api_key is None:
        return False
    expected = settings.admin_api_key.get_secret_value()
    return hmac.compare_digest(x_admin_key, expected)


def _tier_from_jwt(token: str) -> Optional[str]:
    """Verify an app_auth RS256 JWT and return its tier claim, or None.

    Requires JWT_PUBLIC_KEY to be configured (PEM). Verification failures are
    swallowed (treated as 'no valid token') — a bad token must not error the
    whole request; the caller simply falls through to the next tier source.
    """
    public_key = getattr(settings, "jwt_public_key", None)
    if _pyjwt is None or not public_key:
        return None
    # settings may wrap it as a SecretStr
    if hasattr(public_key, "get_secret_value"):
        public_key = public_key.get_secret_value()
    try:
        payload = _pyjwt.decode(
            token,
            public_key,
            algorithms=["RS256"],
            options={"require": ["exp"]},
        )
    except Exception as e:
        logger.info("jwt verify failed (ignored)", error_type=type(e).__name__)
        return None
    tier = (payload.get("tier") or "").lower()
    return tier if tier in _VALID_TIERS else "free"


def _tier_from_access_key(x_access_key: str) -> Optional[str]:
    """Look up an access key's tier in access.access_requests. Approved rows
    only; returns the row's `tier` (default 'free' if column null). Unknown or
    unapproved keys -> None (caller falls through to anonymous free)."""
    from src.db.supabase_client import access_table

    try:
        result = (
            access_table("access_requests")
            .select("status, tier")
            .eq("access_key", x_access_key)
            .limit(1)
            .execute()
        )
        rows = result.data or []
    except Exception as e:
        logger.warning("access-key tier lookup failed", error_type=type(e).__name__)
        return None

    if not rows or rows[0].get("status") != "approved":
        return None
    tier = (rows[0].get("tier") or "free").lower()
    return tier if tier in _VALID_TIERS else "free"


async def resolve_tier(
    x_admin_key: Annotated[Optional[str], Header(alias="X-Admin-Key")] = None,
    authorization: Annotated[Optional[str], Header(alias="Authorization")] = None,
    x_access_key: Annotated[Optional[str], Header(alias="X-Access-Key")] = None,
) -> TierContext:
    # 1. Admin key — full access.
    if _admin_matches(x_admin_key):
        return TierContext(tier="admin", is_admin=True)

    # 2. app_auth JWT (Bearer) — tier from claim.
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization[7:].strip()
        jwt_tier = _tier_from_jwt(token)
        if jwt_tier:
            return TierContext(tier=jwt_tier, is_admin=False)

    # 3. Access key — tier from its row.
    if x_access_key:
        ak_tier = _tier_from_access_key(x_access_key)
        if ak_tier:
            return TierContext(tier=ak_tier, is_admin=False)

    # 4. Anonymous -> free.
    return TierContext(tier="free", is_admin=False)


TierResolved = Depends(resolve_tier)

__all__ = ["resolve_tier", "TierResolved", "TierContext"]
