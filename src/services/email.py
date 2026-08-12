"""
Outbound email — one Resend sender for the whole service.

MOVED HERE 2026-08-11 from src/routes/connect_auth.py, where it lived as a
private `_resend_send`. Its own docstring anticipated this: "Shared rather
than copied. The owner classifier lived in five files and had drifted from
the requirement in all of them; this is the second caller and there will be
a third, so it gets factored now while there is still only one correct
version to preserve."

The third caller is the saved-search alert job, which runs on the scheduler.
A scheduler job importing a private function out of a route module would
break silently the next time anyone refactored that module, so the function
moves to where shared infrastructure belongs rather than being reached into.

Behaviour is unchanged. connect_auth.py now imports from here.
"""

from __future__ import annotations

import httpx

from src.config import settings
from src.utils.logger import logger


RESEND_URL = "https://api.resend.com/emails"


async def resend_send(
    to_email: str,
    subject: str,
    text: str,
    context: str,
) -> bool:
    """POST one email to Resend. Returns True on a 2xx, False on anything else.

    NEVER raises. Every caller runs after the thing the user actually asked
    for has already succeeded — a link row is written, a listing is saved, a
    saved search matched — so a mail failure must degrade to a logged False,
    not an exception that turns a completed action into an error the user
    sees.

    `context` appears in the logs so a failure can be traced to which kind of
    email failed — Railway drops loguru lines intermittently and a generic
    "Resend rejected" line would be unattributable.
    """
    api_key = getattr(settings, "resend_api_key", None)
    from_addr = getattr(settings, "alert_email_from", None)
    if api_key is None or not from_addr:
        logger.error(
            "RESEND_API_KEY or ALERT_EMAIL_FROM unset; cannot send email",
            context=context,
        )
        return False
    if hasattr(api_key, "get_secret_value"):
        api_key = api_key.get_secret_value()

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(
                RESEND_URL,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "from": from_addr,
                    "to": [to_email],
                    "subject": subject,
                    "text": text,
                },
            )
    except httpx.HTTPError as e:
        logger.error(
            "Resend unreachable",
            context=context,
            error_type=type(e).__name__,
            error=str(e)[:400],
        )
        return False

    if 200 <= resp.status_code < 300:
        return True
    logger.error(
        "Resend rejected send",
        context=context,
        status=resp.status_code,
        body=resp.text[:400],
    )
    return False


__all__ = ["resend_send", "RESEND_URL"]
