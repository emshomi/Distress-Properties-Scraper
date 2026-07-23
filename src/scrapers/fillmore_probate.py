"""
Fillmore County probate-notice scraper — estate-owned parcels as signals.

THE ESTATE CHANNEL, CAUGHT AT INCEPTION (2026-07-23): our own eCRV
buying-patterns analysis measured estate-channel sales closing ~30%
below market (median $237.5k vs $340k statewide). Probate notices in the
Fillmore County Journal name decedents and court-appointed personal
representatives with express "power to sell real and personal property"
— months before any sale reaches an eCRV. Matching decedent names
against the Fillmore parcel spine's owner names identifies estate-owned
land the day the notice publishes.

MANUAL PILOT (same day, before this scraper existed): five notices
matched ~$8M of real estate, including a 641-acre farm estate (Nash,
172 ac of it in Fountain Township) and a 163-acre estate administered
from Knoxville TN (Anderson).

=== SOURCE ===
Same WordPress REST API as fillmore_legal (category "Legal Notice",
id 12). Probate notices are District Court PROBATE DIVISION filings:
"Estate of <Name>[ a/k/a <alias>...], Decedent/Deceased ... appointment
of <PR name>, whose address is <addr> ... power ... to sell real and
personal property". Case numbers look like 23-PR-26-93.

=== MATCHING RULES (lessons from the live pilot, 2026-07-23) ===
- SURNAME must match as a whole word in OWNERNAME (\bZACHER\b — the
  naive LIKE '%ZACHER%' matched living people named ZACHERY).
- FIRST name must match as a whole word.
- MIDDLE INITIAL: if both the decedent and the owner string carry one,
  they must AGREE (DONALD N ANDERSON is the estate; DONALD E ANDERSON
  is a different, living farmer). If either side lacks one, accept.
- One event PER MATCHED PARCEL; an estate with no owner-name match
  writes NOTHING (we only assert what ties to a real parcel).

Dedup identity: (parcel_id, 'probate_estate', <published date>, source).
"""

from __future__ import annotations

import html as _html
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, ClassVar

import httpx

from src.db.supabase_client import core_table
from src.models.signal import DistressEventInsert
from src.scrapers.base_scraper import BaseScraper
from src.services.event_writer import write_events_dedup
from src.utils.errors import SourceUnavailableError
from src.utils.logger import logger


_API_URL = "https://fillmorecountyjournal.com/wp-json/wp/v2/posts"
_CATEGORY_ID = 12
_NEWSPAPER = "Fillmore County Journal"
_WINDOW_DAYS = 365         # estates administer over a year; volume is tiny
                           # (19 posts/90d) and dedup makes the width free
_PAGE_SIZE = 100
_MAX_PAGES = 5
_REQUEST_TIMEOUT = 30.0

_TITLE = "Parcel owned by an estate in probate"
_DESC = (
    "The owner of record matches the decedent named in a probate notice "
    "published in the Fillmore County Journal. A court-appointed personal "
    "representative is being (or has been) appointed with power to sell "
    "real property. Estate transitions are a leading indicator of "
    "off-market sales; verify current case status via MCRO before acting."
)

_RE_TAG = re.compile(r"<[^>]+>")

# "Estate of LaDonna Marie Nash, Decedent" / "In Re the Estate of:
# Barbara Jo Laumb, Deceased" — aka chains split separately.
_RE_ESTATE_OF = re.compile(
    r"Estate of:?\s+(.{3,90}?)\s*,?\s*(?:a/k/a|aka|Decedent|Deceased)",
    re.I,
)
_RE_AKA = re.compile(r"(?:a/k/a|aka)\s+(.{3,60}?)(?=\s*(?:,|a/k/a|aka|Decedent|Deceased))", re.I)
_RE_CASE = re.compile(r"Court File No\.?\s*:?\s*([0-9]{1,3}-PR-[0-9]{2}-[0-9]{1,5})", re.I)
_RE_PR = re.compile(
    # Anchored on "whose address is" so BOTH names in co-PR notices match
    # ("appointment of Kim Kaster, whose address is ... and Gregory Nash,
    # whose address is ..."). The name is the short phrase preceding it.
    r"([A-Z][A-Za-z.'\- ]{2,60}?)\s*,\s*whose address is\s+(.{5,120}?)"
    r"(?=\s*(?:,?\s*and\s+[A-Z]|as\s+(?:Co-)?Personal|$))",
    re.S,
)
_RE_HEARING = re.compile(
    r"on\s+([A-Z][a-z]+ \d{1,2}, \d{4})\s*,?\s*at\s*\d{1,2}:\d{2}", re.I
)

_NAME_NOISE = {"JR", "SR", "II", "III", "IV"}


def _strip_html(rendered: str) -> str:
    text = _html.unescape(rendered)
    text = _RE_TAG.sub(" ", text)
    return " ".join(text.split())


def _clean_ws(text: str | None) -> str | None:
    if not text:
        return None
    s = " ".join(text.split())
    return s or None


def _parse_long_date(text: str | None) -> date | None:
    if not text:
        return None
    try:
        return datetime.strptime(text.strip(), "%B %d, %Y").date()
    except ValueError:
        return None


def _name_parts(name: str) -> tuple[str | None, str | None, str | None]:
    """(first, middle_initial, last) from a human name string.
    'LaDonna Marie Nash' -> ('LADONNA', 'M', 'NASH');
    'Donald N. Anderson' -> ('DONALD', 'N', 'ANDERSON');
    'Kim Kaster' -> ('KIM', None, 'KASTER')."""
    tokens = [
        re.sub(r"[^A-Za-z]", "", t).upper()
        for t in name.split()
    ]
    tokens = [t for t in tokens if t and t not in _NAME_NOISE]
    if len(tokens) < 2:
        return None, None, None
    first = tokens[0]
    last = tokens[-1]
    middle_initial = tokens[1][0] if len(tokens) >= 3 else None
    return first, middle_initial, last


def _owner_matches_decedent(
    owner_name: str,
    first: str,
    middle_initial: str | None,
    last: str,
) -> bool:
    """Word-boundary match of decedent (first, mi, last) against an
    OWNERNAME string like 'DONALD N ANDERSON' or 'LADONNA M NASH'.
    Rules per the 2026-07-23 pilot: surname AND first name as whole
    words; middle initials must agree when both sides have one."""
    up = owner_name.upper()
    if not re.search(rf"\b{re.escape(last)}\b", up):
        return False
    if not re.search(rf"\b{re.escape(first)}\b", up):
        return False
    if middle_initial:
        # Owner middle initial = single-letter token between other tokens
        # ('DONALD N ANDERSON' -> 'N'; 'PAMELA K BLUHM' -> 'K').
        m = re.search(rf"\b{re.escape(first)}\b\s+([A-Z])\b", up)
        if m and m.group(1) != middle_initial:
            return False
    return True


class FillmoreProbateScraper(BaseScraper[dict[str, Any], DistressEventInsert]):
    """Fillmore probate notices -> probate_estate events on matched parcels."""

    source_name: ClassVar[str] = "fillmore_probate"
    signal_type: ClassVar[str] = "probate_filing"
    county_code: ClassVar[str] = "fillmore"

    # ---- Fetch: same WP endpoint as fillmore_legal ----

    async def fetch(self, trigger: str) -> list[dict[str, Any]]:
        after = (
            datetime.now(timezone.utc) - timedelta(days=_WINDOW_DAYS)
        ).strftime("%Y-%m-%dT%H:%M:%S")
        posts: list[dict[str, Any]] = []
        try:
            async with httpx.AsyncClient(
                timeout=_REQUEST_TIMEOUT,
                headers={"User-Agent": "Mozilla/5.0 (compatible; govire/1.0)"},
            ) as client:
                for page in range(1, _MAX_PAGES + 1):
                    resp = await client.get(_API_URL, params={
                        "categories": _CATEGORY_ID,
                        "per_page": _PAGE_SIZE,
                        "page": page,
                        "orderby": "date",
                        "order": "desc",
                        "after": after,
                        "status": "publish",
                        "_fields": "id,date,link,title,content",
                    })
                    if resp.status_code == 400 and page > 1:
                        break
                    resp.raise_for_status()
                    batch = resp.json()
                    if not isinstance(batch, list):
                        raise SourceUnavailableError(
                            "WP API returned a non-list payload",
                            source=self.source_name,
                        )
                    posts.extend(p for p in batch if isinstance(p, dict))
                    if len(batch) < _PAGE_SIZE:
                        break
        except httpx.HTTPError as e:
            raise SourceUnavailableError(
                f"Fillmore County Journal WP API failed: {str(e)[:300]}",
                source=self.source_name,
            ) from e
        except ValueError as e:
            raise SourceUnavailableError(
                f"WP API returned non-JSON: {str(e)[:200]}",
                source=self.source_name,
            ) from e

        logger.info(
            "Fillmore probate fetch complete",
            source=self.source_name,
            posts=len(posts),
            window_days=_WINDOW_DAYS,
        )
        return posts

    # ---- Spine lookup: candidate owners per surname (server-side ilike,
    #      precise matching client-side) ----

    def _find_matching_parcels(
        self, first: str, middle_initial: str | None, last: str
    ) -> list[dict[str, Any]]:
        try:
            result = (
                core_table("parcels")
                .select(
                    "parcel_id,emv_total,city,address,"
                    "district:raw_data->>DISTRICT,"
                    "owner_name:raw_data->>OWNERNAME,"
                    "deeded_acres:raw_data->>DEEDEDACRE"
                )
                .eq("county_code", "fillmore")
                .ilike("raw_data->>OWNERNAME", f"%{last}%")
                .limit(200)
                .execute()
            )
        except Exception as e:
            logger.warning(
                "Spine owner lookup failed",
                source=self.source_name,
                surname=last,
                error=str(e)[:300],
            )
            return []
        rows = result.data or []
        return [
            r for r in rows
            if r.get("owner_name")
            and _owner_matches_decedent(
                r["owner_name"], first, middle_initial, last
            )
        ]

    # ---- Parse: notices -> one event per MATCHED parcel ----

    async def parse(
        self, raw_records: list[dict[str, Any]]
    ) -> list[DistressEventInsert]:
        signals: list[DistressEventInsert] = []
        skipped_not_probate = 0
        skipped_no_decedent = 0
        estates_no_match = 0
        seen_cases: set[str] = set()

        for post in raw_records:
            title = (post.get("title") or {}).get("rendered") or ""
            content = (post.get("content") or {}).get("rendered") or ""
            text = _strip_html(title + " " + content)
            up = text.upper()

            if "ESTATE OF" not in up or (
                "PROBATE" not in up and "-PR-" not in up
            ):
                skipped_not_probate += 1
                continue

            m = _RE_ESTATE_OF.search(text)
            if not m:
                skipped_no_decedent += 1
                logger.info(
                    "Probate notice without parseable decedent skipped",
                    source=self.source_name,
                )
                continue
            decedent = _clean_ws(m.group(1)) or ""
            first, middle_initial, last = _name_parts(decedent)
            if not first or not last:
                skipped_no_decedent += 1
                continue

            case_m = _RE_CASE.search(text)
            case_no = case_m.group(1).upper() if case_m else None
            # The Journal publishes each notice twice; case number (or
            # decedent name) dedups the runs within a fetch window.
            case_key = case_no or f"{first}-{last}"
            if case_key in seen_cases:
                continue
            seen_cases.add(case_key)

            prs = [
                {"name": _clean_ws(n), "address": _clean_ws(a)}
                for n, a in _RE_PR.findall(text)
            ]
            hearing = _parse_long_date(
                _RE_HEARING.search(text).group(1)
                if _RE_HEARING.search(text) else None
            )
            published = (post.get("date") or "")[:10]
            try:
                event_date = datetime.strptime(published, "%Y-%m-%d").date()
            except ValueError:
                event_date = None

            matches = self._find_matching_parcels(first, middle_initial, last)
            if not matches:
                estates_no_match += 1
                logger.info(
                    "Probate estate with no spine owner match (no event)",
                    source=self.source_name,
                    decedent=decedent,
                )
                continue

            for r in matches:
                signals.append(DistressEventInsert(
                    parcel_id=r["parcel_id"],
                    event_type="probate_filing",
                    event_subtype="probate_notice",
                    event_date=event_date,
                    event_value=None,
                    source=self.source_name,
                    source_id=f"{case_key}:{r['parcel_id']}",
                    severity="medium",  # type: ignore[arg-type]
                    title=_TITLE,
                    description=_DESC,
                    raw_data={
                        "decedent": decedent,
                        "case_number": case_no,
                        "personal_representatives": prs,
                        "hearing_date": (
                            hearing.isoformat() if hearing else None
                        ),
                        "matched_owner_name": r.get("owner_name"),
                        "match_basis": "owner_name_word_match",
                        "district": r.get("district"),
                        "deeded_acres": r.get("deeded_acres"),
                        "emv_total": r.get("emv_total"),
                        "notice_url": post.get("link"),
                        "published_date": post.get("date"),
                        "newspaper": _NEWSPAPER,
                    },
                    observed_at=datetime.now(timezone.utc),
                ))

        logger.info(
            "Fillmore probate notices parsed",
            source=self.source_name,
            events=len(signals),
            estates_seen=len(seen_cases),
            estates_no_match=estates_no_match,
            skipped_not_probate=skipped_not_probate,
            skipped_no_decedent=skipped_no_decedent,
        )
        return signals

    # ---- Write ----

    async def write(
        self, signals: list[DistressEventInsert]
    ) -> tuple[int, int, int]:
        if not signals:
            return 0, 0, 0
        new_events, failed_events = write_events_dedup(signals)
        logger.info(
            "Fillmore probate write complete",
            source=self.source_name,
            events_new=new_events,
            failed=failed_events,
        )
        return new_events, 0, failed_events


__all__ = ["FillmoreProbateScraper"]
