"""
Authenticated investor identity for WRITE endpoints.

Separate from src/middleware/tier.py on purpose. `resolve_tier` is documented
as NEVER raising: anonymous callers fall through to "free" and redaction
decides what they see. That is correct for browsing locked cards and wrong for
anything that writes a row.

Three differences that matter:

  1. It RAISES 401. There is no anonymous fallback.
  2. It requires a Bearer JWT specifically. `resolve_tier` also accepts
     X-Admin-Key and X-Access-Key — NEITHER of which carries a user. A QA
     access key resolves to a valid tier with nobody behind it, and an offer
     attributed to nobody, shown to a distressed owner as coming from a
     verified buyer, is the worst way this could fail.
  3. It requires `typ == "access"`. govire-auth stamps that claim
     (app/core/jwt_tokens.py) and nothing in the scraper has ever checked it.
     Harmless on reads; on a write it is the difference between accepting an
     access token and accepting a refresh token as authorization.

It returns the caller's id, so `marketplace.offers.buyer_user_id` can be
populated. `app_auth.users.id` is the canonical user identity for the whole
platform and marketplace.* keys off it — see BUILDLOG_govire-auth.md.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any, Optional

from fastapi import Depends, Header, HTTPException, status as http_status

from src.config import settings
from src.utils.logger import logger

try:
    import jwt as _pyjwt  # PyJWT
except Exception:  # pragma: no cover
    _pyjwt = None


# Who may make an offer. ONE PLACE, deliberately.
#
# Gated rather than open because a hand-raised listing is the most valuable
# inventory in the system: a scraped distress signal is a guess that someone
# might sell, while an owner who raised their hand has volunteered and asked
# for offers. Giving that away free while charging for scraped signals prices
# the product upside down.
#
# THE KNOWN RISK: this is a two-sided market with zero offers today, and
# restricting the buy side while it is empty is a cold-start risk. If offers
# stay at zero after the investor side ships, THIS CONSTANT is the first thing
# to reconsider — add "free" and the gate is gone.
#
# Note what this does NOT do: it does not try to filter out unserious buyers.
# A subscription says nothing about whether a sale closes. financing_type,
# is_preapproved and preapproval_lender are what let an owner judge that, and
# they are shown per offer.
_OFFER_TIERS = frozenset({"basic", "standard", "premium", "admin"})


@dataclass(frozen=True)
class InvestorContext:
    user_id: str          # app_auth.users.id — canonical platform identity
    email: Optional[str]
    tier: str
    subscription_status: Optional[str]


def _unauthorized(reason: str) -> HTTPException:
    """401 with a generic message.

    The reason is logged, never returned. A caller learning WHICH check failed
    can probe the difference between "no token", "expired" and "wrong type".
    """
    logger.info("investor auth rejected", reason=reason)
    return HTTPException(
        status_code=http_status.HTTP_401_UNAUTHORIZED,
        detail={"message": "Sign in to continue."},
        headers={"WWW-Authenticate": "Bearer"},
    )


async def resolve_investor(
    authorization: Annotated[Optional[str], Header(alias="Authorization")] = None,
) -> InvestorContext:
    """Verified investor identity, or 401.

    Deliberately does NOT accept X-Admin-Key or X-Access-Key. Both authorize a
    TIER without a user; see the module docstring.
    """
    if _pyjwt is None:
        # Misconfiguration, not the caller's fault — but failing open here
        # would mean writes with no authentication at all.
        logger.error("investor auth: PyJWT unavailable")
        raise _unauthorized("pyjwt_missing")

    public_key = getattr(settings, "jwt_public_key", None)
    if not public_key:
        logger.error("investor auth: JWT_PUBLIC_KEY unset")
        raise _unauthorized("no_public_key")
    if hasattr(public_key, "get_secret_value"):
        public_key = public_key.get_secret_value()

    if not authorization or not authorization.lower().startswith("bearer "):
        raise _unauthorized("no_bearer")
    token = authorization[7:].strip()
    if not token:
        raise _unauthorized("empty_bearer")

    try:
        payload: dict[str, Any] = _pyjwt.decode(
            token,
            public_key,
            algorithms=["RS256"],
            audience="govire",
            issuer="govire-auth",
            options={"require": ["exp", "sub"]},
        )
    except Exception as e:
        # Includes expiry, bad signature, wrong audience/issuer. All one 401.
        raise _unauthorized(f"decode_failed:{type(e).__name__}")

    if payload.get("typ") != "access":
        raise _unauthorized("wrong_token_type")

    user_id = str(payload.get("sub") or "").strip()
    if not user_id:
        raise _unauthorized("no_subject")

    tier = str(payload.get("tier") or "free").lower()
    sub_status = payload.get("subscription_status")

    if tier not in _OFFER_TIERS:
        # A real, signed-in person who simply has not subscribed. 403, not
        # 401 — re-authenticating would not help them and being sent back to
        # a login screen they already passed is its own kind of dead end.
        logger.info("investor auth: tier not permitted", tier=tier)
        raise HTTPException(
            status_code=http_status.HTTP_403_FORBIDDEN,
            detail={"message": (
                "Making an offer requires a subscription. Any paid plan "
                "includes it."
            )},
        )

    # Status is LOGGED, NOT ENFORCED. Stripe webhooks arrive out of order and
    # a new subscription briefly reports 'incomplete' before settling to
    # 'active' — documented in BUILDLOG_govire-billing.md, where trusting the
    # last event's status stranded a paying user. Tier is already synced from
    # billing on payment; gating on status as well would lock a paid buyer out
    # during settling, on the one action the marketplace exists for.
    if sub_status and sub_status not in ("active", "none"):
        logger.info("investor auth: unusual subscription status, allowing",
                    tier=tier, subscription_status=str(sub_status))

    return InvestorContext(
        user_id=user_id,
        email=payload.get("email"),
        tier=tier,
        subscription_status=(str(sub_status) if sub_status else None),
    )


InvestorResolved = Depends(resolve_investor)

__all__ = ["resolve_investor", "InvestorResolved", "InvestorContext"]
