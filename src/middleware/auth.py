"""
Admin authentication middleware for the FastAPI service.

Validates the X-Admin-Key header on protected endpoints. Uses
constant-time comparison (hmac.compare_digest) to defend against
timing attacks on the comparison loop.

=== AN EMPTY HEADER IS MISSING, NOT INVALID (2026-08-23) ===
This file used to test `if x_admin_key is None`. FastAPI does not hand back
None for a header sent with an empty value — it hands back the empty string.
So a request carrying `X-Admin-Key: ` skipped the missing-header branch
entirely, fell through to the comparison, failed it, and was reported to the
caller as "Invalid X-Admin-Key".

That is a materially wrong answer. It says the key you sent is wrong when in
fact no key was sent, and on 2026-08-23 it cost four failed attempts and the
better part of an hour hunting a value that was never in the request. The
server knew: every one of those rejections logged received_length=0 beside
expected_length=64. Only the log was right.

Both empty and absent now take the missing-header branch and say so.

=== AN EMPTY CONFIGURED KEY IS NOT A CONFIGURED KEY ===
The 503 branch used to test `settings.admin_api_key is None`. If the
environment ever holds ADMIN_API_KEY="" — a paste that saved blank, a
variable cleared during a rotation — that test passes, `expected` becomes the
empty string, and compare_digest("", "") returns True. An empty header would
then AUTHENTICATE against every admin endpoint in _SCRAPER_REGISTRY.

Verified not to be the case on 2026-08-23 (an empty header returned 401), but
it was one blank paste away and the two defects compound: the empty header
that should have been rejected as missing would have been accepted as valid.
The 503 branch now treats an empty configured key as unconfigured.

=== WHY THE COMPARISON IS ON BYTES ===
hmac.compare_digest raises TypeError on str inputs containing non-ASCII
characters, which would surface as a 500 rather than a 401. A key pasted
through an editor that substituted a typographic character would do it.
Encoding both sides to UTF-8 first removes the failure mode without changing
the constant-time property.
"""

from __future__ import annotations

import hmac
from typing import Annotated

from fastapi import Depends, Header, HTTPException, status

from src.config import settings
from src.utils.logger import logger


_ADMIN_HEADER_NAME: str = "X-Admin-Key"


async def require_admin_key(
    x_admin_key: Annotated[
        str | None,
        Header(
            alias=_ADMIN_HEADER_NAME,
            description=(
                "Admin secret for accessing protected endpoints. "
                "Must match ADMIN_API_KEY configured for the service."
            ),
        ),
    ] = None,
) -> None:
    """
    FastAPI dependency that validates the X-Admin-Key header.

    Raises:
        HTTPException 503: If no admin key is configured on the service, or
            the configured value is empty.
        HTTPException 401: If the header is missing, empty, or doesn't match.
    """
    expected: str | None = None
    if settings.admin_api_key is not None:
        expected = settings.admin_api_key.get_secret_value()

    # `not expected` covers both None and "". An empty configured key would
    # otherwise make compare_digest("", "") return True and open every admin
    # endpoint to a request with an empty header.
    if not expected:
        logger.error(
            "ADMIN_API_KEY not configured or empty — rejecting all admin "
            "requests",
            hint="Set ADMIN_API_KEY in environment variables and redeploy",
            configured=settings.admin_api_key is not None,
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Admin authentication not configured on this service",
        )

    # `not x_admin_key` rather than `is None`: FastAPI returns "" for a header
    # sent with an empty value, so the None test let an empty header reach the
    # comparison and be reported as invalid.
    if not x_admin_key:
        logger.warning(
            "Admin endpoint accessed without X-Admin-Key header",
            header_expected=_ADMIN_HEADER_NAME,
            header_present=x_admin_key is not None,
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or empty X-Admin-Key header",
            headers={"WWW-Authenticate": 'AdminKey realm="admin"'},
        )

    if not hmac.compare_digest(
        x_admin_key.encode("utf-8"), expected.encode("utf-8")
    ):
        logger.warning(
            "Admin endpoint received invalid X-Admin-Key",
            received_length=len(x_admin_key),
            expected_length=len(expected),
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid X-Admin-Key",
            headers={"WWW-Authenticate": 'AdminKey realm="admin"'},
        )


# Pre-built Depends for cleaner endpoint signatures
AdminKeyRequired = Depends(require_admin_key)


__all__ = ["require_admin_key", "AdminKeyRequired"]
