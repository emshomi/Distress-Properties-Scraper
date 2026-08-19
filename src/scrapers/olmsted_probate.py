"""
Olmsted County probate-notice scraper — estate-owned parcels as signals.

Same channel as fillmore_probate, four times the parcel base and a far
better owner-name format. Our own eCRV analysis measured estate-channel
sales closing ~30% below market (median $237.5k vs $340k statewide), and
a probate notice names the decedent and a personal representative with
express "power ... to sell real and personal property" months before any
sale reaches an eCRV.

=== SOURCE: Column, not WordPress ===
The Post Bulletin is Olmsted County's qualified newspaper and publishes
through Column, the same JSON endpoint postbulletin_legal.py already
uses. Fillmore's paper runs WordPress; that is the only structural
difference between the two scrapers.

Measured 2026-08-19 over the last 365 days: 1,191 Post Bulletin notices,
of which probate is the second-largest real-property category after
foreclosure — 15+ distinct Olmsted estates in a two-month sample, so
roughly 80-120 a year.

=== WHY THE noticetype FILTER IS NOT USED ===
Column tags most of these "Estate (Probate) Filings", and it would be
natural to filter on that the way postbulletin_legal filters on
"Foreclosure Sale". Measured over the same 365 days, that would MISS
real estates:

    Jan Marie Walchak     55-PR-26-4383   noticetype = "Notice to Creditors"
    Donald Raymond Nelson 55-PR-26-4228   noticetype = ""   (petition for
                                                             descent)

So the fetch asks for every Post Bulletin notice and classifies on TEXT,
exactly as fillmore_probate does: "ESTATE OF" plus ("PROBATE" or "-PR-").
A category that is right 90% of the time silently drops the other 10%,
and nothing downstream would ever show the gap.

=== MATCHING: core.owners, NOT parcels.raw_data ===
fillmore_probate reads core.parcels.raw_data->>'OWNERNAME'. That works in
Fillmore only because its MnGeo export happens to carry the field.
Measured 2026-08-19: OLMSTED HAS IT ON 0 OF 75,039 PARCELS. The real
owner table is core.owners, which has 75,039 Olmsted rows — one per
parcel, 100% coverage, every one joining to the spine on the composite
key (county_code, parcel_id), no fan-out.

fillmore_probate should be repointed at core.owners too; it is working on
luck, not design.

=== OWNER-NAME FORMAT — measured, not assumed ===
Owner-name style is a per-COUNTY property; each assessor exports its own.
Olmsted is the good case:

    county      owner rows   comma format   share
    olmsted         75,039         59,327     79%
    crow_wing       76,953         58,904     77%
    fillmore        19,636            571      3%

Olmsted reads "ZZIWA,GILBERT N" — surname, comma, first name, middle
initial. Fillmore's _owner_matches_decedent was written against
"DONALD N ANDERSON" and transfers UNCHANGED, because a comma is a
non-word character so \\b boundaries behave identically. Verified before
this file was written, against 14 real decedents from the live feed:

    Alan G. Ihde          vs IHDE,ALAN G                 match
    Stanley James Rathke  vs RATHKE,STANLEY J & MARY A   match (joint)
    Patrick Loras Byrnes  vs BYRNES, PATRICK L           match (space)
    Alan G. Ihde          vs IHDE,ALAN R                 REJECT (middle)
    Alan G. Ihde          vs IHDESON,ALAN G              REJECT (surname
                                                                substring)
    Gary Stuart Swanson   vs SWANSON,GARLAND S           REJECT (first-name
                                                                substring)
    Alan G. Ihde          vs IHDE,GILBERT N              REJECT (same
                                                          surname, other
                                                          person)

14 of 14. The negatives are the point: the 2026-07-23 Fillmore pilot
found that LIKE '%ZACHER%' matched living people named ZACHERY, and that
DONALD N ANDERSON is an estate while DONALD E ANDERSON is a different,
living farmer.

=== HONESTY RULES ===
- ONE EVENT PER MATCHED PARCEL. An estate that matches no owner writes
  NOTHING and is logged. We only assert what ties to a real parcel.
- No synthetic parcel ids, ever (the MPLS-VBR lesson).
- A probate notice is a SIGNAL, not a sale. The description says so and
  points at MCRO for current case status.

Dedup identity: (parcel_id, 'probate_filing', <published date>, source).
"""

from __future__ import annotations

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


_API_URL = (
    "https://us-central1-enotice-production.cloudfunctions.net"
    "/api/search/public-notices"
)
_NEWSPAPER = "Post Bulletin"
_COUNTY = "olmsted"

# Estates administer over a year and the dedup key makes a wide window
# free. postbulletin_legal uses 45 days because a foreclosure notice runs
# six weeks; a probate notice runs twice and then stops, so a narrow
# window would miss an estate opened three months ago whose parcel we
# only matched later.
_WINDOW_DAYS = 365
_PAGE_SIZE = 200
_MAX_PAGES = 6          # 1,191 notices/year measured; 6 x 200 covers it
_REQUEST_TIMEOUT = 30.0

# Server-side prefilter width. The precise match runs client-side; this
# only has to be generous enough not to miss a candidate. A surname like
# ANDERSON returns a few hundred rows in Olmsted, well inside this.
_OWNER_CANDIDATE_LIMIT = 500

_TITLE = "Parcel owned by an estate in probate"
_DESC = (
    "The owner of record matches the decedent named in a probate notice "
    "published in the Rochester Post Bulletin (Olmsted County's qualified "
    "newspaper). A court-appointed personal representative is being (or "
    "has been) appointed with power to sell real property. Estate "
    "transitions are a leading indicator of off-market sales; verify "
    "current case status via MCRO before acting."
)

# "Estate of Alan G. Ihde aka Alan Ihde aka Al Ihde, Decedent"
# "In Re: Estate of James L. Warren, Decedent"
# "Estate of Richard Lee Bennett, a/k/a Richard L. Bennett, Deceased"
_RE_ESTATE_OF = re.compile(
    r"Estate of:?\s+(.{3,90}?)\s*,?\s*(?:a/k/a|aka|Decedent|Deceased)",
    re.I,
)
_RE_AKA = re.compile(
    r"(?:a/k/a|aka)\s+(.{3,60}?)(?=\s*(?:,|a/k/a|aka|Decedent|Deceased))",
    re.I,
)
# Olmsted court files are 55-PR-26-nnnn; a Determination of Descent can
# arrive as 55-CV-26-nnnn under the PROBATE DIVISION heading, so the
# division prefix is two letters, not literally "PR".
_RE_CASE = re.compile(
    r"Court File (?:No\.?|Number:?)\s*:?\s*([0-9]{1,3}-[A-Z]{2}-[0-9]{2}-[0-9]{1,6})",
    re.I,
)
_RE_PR = re.compile(
    # Anchored on "whose address is" so BOTH names in co-PR notices match
    # ("appointment of John Rathke, whose address is ... and Steven Hines,
    # whose address is ..." -- 55-PR-26-5142 is exactly this shape).
    #
    # KNOWN LIMIT, measured 2026-08-19 on 55-PR-26-4383: a CORPORATE
    # personal representative written "Merchants Bank, N.A., whose address
    # is ..." extracts as "N.A.", because the pattern is non-greedy back to
    # the nearest comma and a corporate suffix carries one. Left as-is
    # deliberately: personal_representatives is raw_data enrichment, never
    # the match key, so a mangled corporate name costs a field in the JSON
    # and nothing else. Widening the pattern to swallow the suffix risks
    # swallowing the preceding clause on the individual PRs, which are the
    # common case and the useful one (an out-of-state PR -- Steven Hines,
    # Naples FL on 55-PR-26-5142 -- is a motivation signal).
    r"([A-Z][A-Za-z.'\- ]{2,60}?)\s*,\s*whose address is\s+(.{5,120}?)"
    r"(?=\s*(?:,?\s*and\s+[A-Z]|as\s+(?:Co-)?Personal|$))",
    re.S,
)
_RE_HEARING = re.compile(
    r"on\s+([A-Z][a-z]+ \d{1,2}, \d{4})\s*,?\s*at\s*\d{1,2}:\d{2}", re.I
)

_NAME_NOISE = {"JR", "SR", "II", "III", "IV"}


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


def _extract_notice_text(hit: dict[str, Any]) -> str | None:
    """The API hit's text field — name defensively probed (the response
    shape was observed, not documented). First non-trivial string wins.

    Copied from postbulletin_legal.py deliberately rather than imported:
    the two scrapers must be able to diverge if Column changes its shape
    for one notice type and not another, and a shared helper would make
    that a two-file problem.

    The 200-char floor is safe here — the shortest real probate notice
    measured was ~1,200 characters.
    """
    for key in ("noticecontent", "notice_content", "content", "text",
                "noticetext", "notice_text", "body", "cleanedtext",
                "searchabletext"):
        v = hit.get(key)
        if isinstance(v, str) and len(v) > 200:
            return v
    for v in hit.values():
        if isinstance(v, dict):
            inner = _extract_notice_text(v)
            if inner:
                return inner
    return None


def _hits_from_response(data: Any) -> list[dict[str, Any]]:
    """Walk the response for the results list, shape-agnostically."""
    if isinstance(data, list):
        return [h for h in data if isinstance(h, dict)]
    if not isinstance(data, dict):
        return []
    for key in ("results", "hits", "notices", "data", "items", "docs"):
        v = data.get(key)
        if isinstance(v, list) and v and all(isinstance(h, dict) for h in v):
            return v
        if isinstance(v, dict):
            inner = _hits_from_response(v)
            if inner:
                return inner
    for v in data.values():
        if isinstance(v, (dict, list)):
            inner = _hits_from_response(v)
            if inner:
                return inner
    return []


def _name_parts(name: str) -> tuple[str | None, str | None, str | None]:
    """(first, middle_initial, last) from a human name string.
    'Alan G. Ihde'         -> ('ALAN', 'G', 'IHDE')
    'Carol Ruth Mudderman' -> ('CAROL', 'R', 'MUDDERMAN')
    'Kim Kaster'           -> ('KIM', None, 'KASTER')

    Note the middle INITIAL is taken from a middle NAME's first letter, so
    'Carol Ruth Mudderman' matches an owner recorded 'MUDDERMAN,CAROL R'.
    """
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
    """Word-boundary match of decedent (first, mi, last) against an owner
    string. Rules from the 2026-07-23 Fillmore pilot, unchanged:

      - SURNAME as a whole word (\\bZACHER\\b -- the naive
        LIKE '%ZACHER%' matched living people named ZACHERY)
      - FIRST name as a whole word
      - MIDDLE INITIAL: if BOTH sides carry one they must AGREE
        (IHDE,ALAN G is the estate; IHDE,ALAN R is someone else). If
        either side lacks one, accept -- a missing initial is missing
        information, not a contradiction.

    Works on both 'IHDE,ALAN G' (Olmsted) and 'DONALD N ANDERSON'
    (Fillmore) because a comma is a non-word character.
    """
    up = owner_name.upper()
    if not re.search(rf"\b{re.escape(last)}\b", up):
        return False
    if not re.search(rf"\b{re.escape(first)}\b", up):
        return False
    if middle_initial:
        m = re.search(rf"\b{re.escape(first)}\b\s+([A-Z])\b", up)
        if m and m.group(1) != middle_initial:
            return False
    return True


class OlmstedProbateScraper(BaseScraper[dict[str, Any], DistressEventInsert]):
    """Post Bulletin probate notices -> probate_estate events on matched
    Olmsted parcels."""

    source_name: ClassVar[str] = "olmsted_probate"
    signal_type: ClassVar[str] = "probate_filing"
    county_code: ClassVar[str] = "olmsted"

    # ---- Fetch: Column public-notices API, no noticetype filter ----

    async def fetch(self, trigger: str) -> list[dict[str, Any]]:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        from_ms = now_ms - _WINDOW_DAYS * 24 * 3600 * 1000
        hits: list[dict[str, Any]] = []

        try:
            async with httpx.AsyncClient(
                timeout=_REQUEST_TIMEOUT,
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; govire/1.0)",
                    "Content-Type": "application/json",
                    "Origin": "https://postbulletin.column.us",
                    "Referer": "https://postbulletin.column.us/",
                },
            ) as client:
                for page in range(1, _MAX_PAGES + 1):
                    payload = {
                        "search": "",
                        "allFilters": [
                            {"publishedtimestamp": {"from": from_ms,
                                                    "to": now_ms}},
                            {"newspapername": [_NEWSPAPER]},
                            # NO noticetype filter -- see the module
                            # docstring. Classification happens on text.
                        ],
                        "isDemo": False,
                        "noneFilters": [],
                        "pageSize": _PAGE_SIZE,
                        "page": page,
                        "sort": [{"publishedtimestamp": "desc"}],
                    }
                    resp = await client.post(_API_URL, json=payload)
                    resp.raise_for_status()
                    data = resp.json()
                    batch = _hits_from_response(data)
                    if not batch:
                        break
                    hits.extend(batch)
                    if len(batch) < _PAGE_SIZE:
                        break
        except httpx.HTTPError as e:
            raise SourceUnavailableError(
                f"Column public-notices API failed: {str(e)[:300]}",
                source=self.source_name,
            ) from e
        except ValueError as e:
            raise SourceUnavailableError(
                f"Column API returned non-JSON: {str(e)[:200]}",
                source=self.source_name,
            ) from e

        logger.info(
            "Olmsted probate fetch complete",
            source=self.source_name,
            notices=len(hits),
            window_days=_WINDOW_DAYS,
        )
        return hits

    # ---- Spine lookup: candidate owners per surname ----

    def _find_matching_parcels(
        self, first: str, middle_initial: str | None, last: str
    ) -> list[dict[str, Any]]:
        """Candidate owners by surname server-side, precise match here.

        Reads core.owners -- NOT core.parcels.raw_data->>'OWNERNAME',
        which is NULL on all 75,039 Olmsted parcels (measured
        2026-08-19). is_current filters superseded ownership rows.
        """
        try:
            result = (
                core_table("owners")
                .select("parcel_id,owner_name,mailing_address,mailing_city,"
                        "mailing_state,mailing_zip,is_absentee,"
                        "is_out_of_state")
                .eq("county_code", _COUNTY)
                .eq("is_current", True)
                .ilike("owner_name", f"%{last}%")
                .limit(_OWNER_CANDIDATE_LIMIT)
                .execute()
            )
        except Exception as e:
            logger.warning(
                "Owner lookup failed",
                source=self.source_name,
                surname=last,
                error=str(e)[:300],
            )
            return []

        rows = result.data or []
        if len(rows) >= _OWNER_CANDIDATE_LIMIT:
            # Not fatal, but a truncated candidate set can silently drop a
            # real match -- the 50-row cap defect, in a different table.
            logger.warning(
                "Owner candidate set hit the limit; a match may be missed",
                source=self.source_name,
                surname=last,
                limit=_OWNER_CANDIDATE_LIMIT,
            )

        matched = [
            r for r in rows
            if r.get("owner_name")
            and _owner_matches_decedent(
                r["owner_name"], first, middle_initial, last
            )
        ]
        if not matched:
            return []

        # Enrich from the spine. Separate query rather than a join: the
        # PostgREST client cannot express a composite-key join, and a
        # single-column parcel_id join across counties is the silent
        # cross-county merge this codebase has been bitten by.
        parcel_ids = [r["parcel_id"] for r in matched if r.get("parcel_id")]
        by_id: dict[str, dict[str, Any]] = {}
        if parcel_ids:
            try:
                p = (
                    core_table("parcels")
                    .select("parcel_id,address,city,zip,emv_total,"
                            "property_type,year_built,legal_description")
                    .eq("county_code", _COUNTY)
                    .in_("parcel_id", parcel_ids)
                    .execute()
                )
                by_id = {r["parcel_id"]: r for r in (p.data or [])}
            except Exception as e:
                logger.warning(
                    "Parcel enrichment failed; events still written",
                    source=self.source_name,
                    error=str(e)[:300],
                )

        for r in matched:
            r["parcel"] = by_id.get(r.get("parcel_id") or "", {})
        return matched

    # ---- Parse: notices -> one event per MATCHED parcel ----

    async def parse(
        self, raw_records: list[dict[str, Any]]
    ) -> list[DistressEventInsert]:
        signals: list[DistressEventInsert] = []
        skipped_no_text = 0
        skipped_not_probate = 0
        skipped_no_decedent = 0
        estates_no_match = 0
        seen_cases: set[str] = set()

        for hit in raw_records:
            text = _extract_notice_text(hit)
            if not text:
                skipped_no_text += 1
                continue
            text = " ".join(text.split())
            up = text.upper()

            # Classify on TEXT, not on hit["noticetype"] -- see docstring.
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
            # Every notice runs at least twice by statute; the case number
            # (or the decedent's name) collapses the runs within a window.
            case_key = case_no or f"{first}-{last}"
            if case_key in seen_cases:
                continue
            seen_cases.add(case_key)

            aliases = [_clean_ws(a) for a in _RE_AKA.findall(text)]
            prs = [
                {"name": _clean_ws(n), "address": _clean_ws(a)}
                for n, a in _RE_PR.findall(text)
            ]
            hearing_m = _RE_HEARING.search(text)
            hearing = _parse_long_date(
                hearing_m.group(1) if hearing_m else None
            )

            published_ms = hit.get("publishedtimestamp")
            event_date: date | None = None
            if isinstance(published_ms, (int, float)):
                event_date = datetime.fromtimestamp(
                    published_ms / 1000, tz=timezone.utc
                ).date()

            matches = self._find_matching_parcels(first, middle_initial, last)
            if not matches:
                estates_no_match += 1
                logger.info(
                    "Probate estate with no owner match (no event)",
                    source=self.source_name,
                    decedent=decedent,
                    case_number=case_no,
                )
                continue

            for r in matches:
                parcel = r.get("parcel") or {}
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
                        "decedent_aliases": [a for a in aliases if a],
                        "case_number": case_no,
                        "personal_representatives": prs,
                        "hearing_date": (
                            hearing.isoformat() if hearing else None
                        ),
                        "matched_owner_name": r.get("owner_name"),
                        "match_basis": "owner_name_word_match",
                        "owner_mailing_address": r.get("mailing_address"),
                        "owner_mailing_city": r.get("mailing_city"),
                        "owner_mailing_state": r.get("mailing_state"),
                        "owner_mailing_zip": r.get("mailing_zip"),
                        "owner_is_absentee": r.get("is_absentee"),
                        "owner_is_out_of_state": r.get("is_out_of_state"),
                        "property_address": parcel.get("address"),
                        "property_city": parcel.get("city"),
                        "property_type": parcel.get("property_type"),
                        "year_built": parcel.get("year_built"),
                        "emv_total": parcel.get("emv_total"),
                        "notice_id": hit.get("id"),
                        "published_timestamp": published_ms,
                        "newspaper": _NEWSPAPER,
                    },
                    observed_at=datetime.now(timezone.utc),
                ))

        logger.info(
            "Olmsted probate notices parsed",
            source=self.source_name,
            events=len(signals),
            estates_seen=len(seen_cases),
            estates_no_match=estates_no_match,
            skipped_no_text=skipped_no_text,
            skipped_not_probate=skipped_not_probate,
            skipped_no_decedent=skipped_no_decedent,
        )
        return signals

    # ---- Write ----

    async def write(
        self, signals: list[DistressEventInsert]
    ) -> tuple[int, int, int]:
        if not signals:
            # No matched estates is honest state, not a failure: most
            # decedents in any given window own no Olmsted parcel.
            return 0, 0, 0
        new_events, failed_events = write_events_dedup(signals)
        logger.info(
            "Olmsted probate write complete",
            source=self.source_name,
            events_new=new_events,
            failed=failed_events,
        )
        return new_events, 0, failed_events


__all__ = ["OlmstedProbateScraper"]
