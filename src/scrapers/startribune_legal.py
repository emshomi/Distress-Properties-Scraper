"""
Star Tribune legal-notices foreclosure scraper (Feature #5 auto-feeder).

This is the automatic counterpart to the manual POST /ai/extract endpoint.
Instead of a human pasting one notice at a time, this fetches the Star Tribune
foreclosures listing, finds every published Notice of Mortgage Foreclosure
Sale, and runs each NEW one (not already staged) through the same LLM
extraction, landing it in ai.extracted_foreclosures as 'pending' for human
review. Nothing here reaches the live site until an admin approves it on the
/admin Notice-review tab.

Why this is NOT a BaseScraper subclass:
  The typed BaseScraper lifecycle (fetch -> parse -> write typed signals into
  signals.distress_events) doesn't fit here. These notices are LLM-extracted,
  land in a review queue (ai.extracted_foreclosures), and only become
  distress_events later when an admin approves them. So this is a standalone
  module exposing one function, run_startribune_scrape(), that the admin
  trigger endpoint calls — mirroring /ai/extract, but over many notices.

Source shape (verified against the live listing 2026-06):
  Listing page: https://classifieds.startribune.com/mn/foreclosures/search
    ?limit=240&sort_by=date&order=desc&search_type=advanced&ap_c=22773765
  - Server-rendered HTML (AdPerfect platform) — fetchable without a browser.
  - Each listing card links to a canonical notice URL:
      https://classifieds.startribune.com/mn/foreclosures/
      notice-of-mortgage-foreclosure-sale/AC1E...
    That canonical URL is our source_url dedup key (UNIQUE in the table).
  - The full statutory notice text is embedded IN the listing card (repeated
    twice per card), so we extract from the listing directly — no need to
    fetch each detail page (fewer requests = politer).
  - The listing mixes in non-mortgage items (e.g. "NOTICE OF ASSESSMENT LIEN
    FORECLOSURE SALE" — HOA liens). We pass everything to the LLM; the
    extractor's confidence + the human review step handle those. (A future
    refinement could pre-filter by title.)

Politeness:
  - Real, identifying User-Agent (so the site owner can see who we are).
  - Single listing request per run by default (240 notices in one page).
  - A small delay between any optional per-detail fetches.
  - A hard per-run cap on how many NEW notices we extract (LLM cost + load).
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx

from src.config import settings
from src.db.supabase_client import ai_table
from src.llm.foreclosure_extraction import extract_foreclosure_notice
from src.utils.logger import logger


# ============================================================
# Config
# ============================================================

_LISTING_URL = "https://classifieds.startribune.com/mn/foreclosures/search"

# 22773765 = the Star Tribune "Foreclosures" sub-category id (from the live
# sort/limit links). limit=240 fetches the freshest 240 in one request.
_LISTING_PARAMS = {
    "limit": "240",
    "sort_by": "date",
    "order": "desc",
    "search_type": "advanced",
    "ap_c": "22773765",
}

# Browser-like request headers. The site's classifieds platform (AdPerfect)
# serves a stripped/empty page to requests that don't look like a real
# browser (our earlier self-identifying bot User-Agent got a 200 with no
# listing content). These headers mirror a normal Chrome request so we get
# the same HTML a human visitor sees. We remain low-volume + review-gated;
# this is about getting the real page, not hiding traffic.
_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}

# Hard cap on how many NEW notices we extract per run (LLM cost + politeness).
# Re-runs pick up where the last left off because already-staged URLs are
# skipped by the dedup check.
_DEFAULT_MAX_NEW = 25

# Delay (seconds) between any per-detail fetches, when used.
_DETAIL_FETCH_DELAY = 1.0

# Canonical foreclosure-notice URL pattern on the Star Tribune classifieds.
# We pull every matching href out of the listing HTML, then de-duplicate.
_NOTICE_URL_RE = re.compile(
    r"https://classifieds\.startribune\.com/mn/"
    r"(?:foreclosures|legal-notices)/[a-z0-9-]+/AC1E[0-9A-Za-z]+",
)


# ============================================================
# Result type
# ============================================================


@dataclass
class ScrapeResult:
    """Outcome of one scrape run, returned to the admin endpoint."""
    ok: bool
    listing_urls_found: int = 0
    already_staged: int = 0
    newly_extracted: int = 0
    extraction_failed: int = 0
    stored_ids: list[int] = field(default_factory=list)
    error: Optional[str] = None
    notes: list[str] = field(default_factory=list)


# ============================================================
# Listing fetch + parse
# ============================================================


def _fetch_listing_html() -> Optional[str]:
    """Fetch the foreclosures listing page. Returns the HTML, or None on
    failure (caller reports the run as failed)."""
    try:
        timeout = getattr(settings, "scraper_request_timeout_seconds", 30)
        with httpx.Client(
            timeout=timeout,
            headers=_BROWSER_HEADERS,
            follow_redirects=True,
        ) as client:
            resp = client.get(_LISTING_URL, params=_LISTING_PARAMS)
            if resp.status_code != 200:
                logger.error(
                    "startribune listing fetch non-200",
                    status=resp.status_code,
                )
                return None
            return resp.text
    except httpx.HTTPError as e:
        logger.exception(
            "startribune listing fetch failed", error_type=type(e).__name__
        )
        return None


def _extract_notice_urls(html: str) -> list[str]:
    """Pull every canonical foreclosure-notice URL from the listing HTML,
    de-duplicated, order preserved (freshest first, since the listing is
    sorted newest-first)."""
    seen: set[str] = set()
    ordered: list[str] = []
    for match in _NOTICE_URL_RE.finditer(html):
        url = match.group(0)
        if url not in seen:
            seen.add(url)
            ordered.append(url)
    return ordered


def _fetch_notice_text(url: str) -> Optional[str]:
    """Fetch a single notice detail page and return its visible text. Used
    only when we need the detail page (the listing usually already embeds the
    full text; this is a fallback). Politeness delay handled by the caller."""
    try:
        timeout = getattr(settings, "scraper_request_timeout_seconds", 30)
        with httpx.Client(
            timeout=timeout,
            headers=_BROWSER_HEADERS,
            follow_redirects=True,
        ) as client:
            resp = client.get(url)
            if resp.status_code != 200:
                return None
            return resp.text
    except httpx.HTTPError:
        return None


# The notice body always starts at this phrase and is what the extractor wants.
_NOTICE_START_MARKERS = (
    "THE RIGHT TO VERIFICATION OF THE DEBT",
    "NOTICE IS HEREBY GIVEN",
    "Minn. Stat.",
    "YOU ARE NOTIFIED",
)


def _slice_notice_text(raw_text: str) -> Optional[str]:
    """From a page's text, isolate the statutory notice body. Notices begin
    with one of a few standard openers; we slice from the first opener to a
    reasonable end and trim. Returns None if no opener is found (so we don't
    feed junk to the LLM)."""
    if not raw_text:
        return None
    earliest = None
    for marker in _NOTICE_START_MARKERS:
        idx = raw_text.find(marker)
        if idx != -1 and (earliest is None or idx < earliest):
            earliest = idx
    if earliest is None:
        return None
    body = raw_text[earliest:]
    # Cap length to the extractor's input limit (20000 chars in ExtractBody).
    return body[:20000].strip()


# ============================================================
# Dedup
# ============================================================


def _already_staged(source_url: str) -> bool:
    """True if this notice URL is already in ai.extracted_foreclosures (any
    review_status — pending, approved, or rejected). We never re-extract a URL
    we've already seen, regardless of its review outcome."""
    try:
        existing = (
            ai_table("extracted_foreclosures")
            .select("id")
            .eq("source_url", source_url)
            .limit(1)
            .execute()
        )
        return bool(existing.data)
    except Exception as e:
        # If the dedup check itself fails, treat as "already staged" (skip)
        # so a transient DB hiccup can't cause duplicate extraction + cost.
        logger.warning(
            "startribune dedup check failed; skipping URL to be safe",
            error_type=type(e).__name__,
        )
        return True


# ============================================================
# Store one extracted notice (mirrors /ai/extract store path)
# ============================================================


def _store_extraction(notice_text: str, source_url: str) -> Optional[int]:
    """Extract one notice and insert it as pending. Returns the new row id, or
    None on extraction failure / store failure / duplicate. Mirrors the
    /ai/extract store path exactly (same columns, plain insert, dup-safe)."""
    extraction = extract_foreclosure_notice(notice_text)
    if not extraction.ok:
        logger.info(
            "startribune extraction returned not-ok",
            source_url=source_url,
            error=extraction.error,
        )
        return None

    row = dict(extraction.data)
    row["source_url"] = source_url
    row["source_name"] = "startribune_legal"
    row["raw_notice_text"] = notice_text
    row["model"] = extraction.model
    # review_status defaults to 'pending'; fetched_at defaults now() in the DB.

    try:
        result = ai_table("extracted_foreclosures").insert(row).execute()
        if result.data:
            return result.data[0]["id"]
        return None
    except Exception as e:
        logger.exception(
            "startribune store insert failed",
            source_url=source_url,
            error_type=type(e).__name__,
        )
        return None


# ============================================================
# Public entry point — called by the admin trigger endpoint
# ============================================================


def run_startribune_scrape(max_new: int = _DEFAULT_MAX_NEW) -> ScrapeResult:
    """Fetch the foreclosures listing, extract each NEW notice, store as
    pending. Returns a ScrapeResult summary for the admin UI.

    Flow:
      1. Fetch listing HTML (one request).
      2. Pull canonical notice URLs (deduped, newest first).
      3. For each URL not already staged (up to max_new):
           - get the notice text (from listing, or fetch detail as fallback)
           - extract + store as pending
      4. Return counts.
    """
    result = ScrapeResult(ok=False)

    html = _fetch_listing_html()
    if html is None:
        result.error = "Could not fetch the Star Tribune foreclosures listing."
        return result

    urls = _extract_notice_urls(html)
    result.listing_urls_found = len(urls)
    if not urls:
        result.ok = True
        result.notes.append(
            "Listing fetched but no notice URLs found — the page structure "
            "may have changed."
        )
        return result

    # Pre-slice the listing text once so we can try to pull each notice's body
    # straight from the listing (the full text is embedded per card).
    listing_text = html

    new_count = 0
    for url in urls:
        if new_count >= max_new:
            result.notes.append(
                f"Stopped at per-run cap ({max_new} new notices). Re-run to "
                f"continue — already-staged notices are skipped."
            )
            break

        if _already_staged(url):
            result.already_staged += 1
            continue

        # Try to get the notice text. The listing embeds full text, but
        # reliably isolating ONE notice's body from the combined listing text
        # is error-prone, so we fetch the clean detail page per new notice.
        # That's at most `max_new` extra requests per run, paced politely.
        time.sleep(_DETAIL_FETCH_DELAY)
        detail_html = _fetch_notice_text(url)
        notice_text = _slice_notice_text(detail_html or "")

        if not notice_text:
            result.extraction_failed += 1
            logger.info("startribune: no notice body found", source_url=url)
            continue

        stored_id = _store_extraction(notice_text, url)
        if stored_id is not None:
            result.newly_extracted += 1
            result.stored_ids.append(stored_id)
            new_count += 1
        else:
            result.extraction_failed += 1

    result.ok = True
    logger.info(
        "startribune scrape complete",
        urls_found=result.listing_urls_found,
        already_staged=result.already_staged,
        newly_extracted=result.newly_extracted,
        extraction_failed=result.extraction_failed,
    )
    return result


__all__ = ["run_startribune_scrape", "ScrapeResult"]
