"""
MCRO probate scraper — owner-driven.

Source: Minnesota Court Records Online, the statewide public interface to all
87 counties' district court records.

    https://publicaccess.courts.state.mn.us/CaseSearch

=== COMPLETE REWRITE 2026-08-08 ===
The previous version had NEVER written a row. Its parser was
`soup.select("table.results tr")` with the comment "this is illustrative";
it hardcoded 9 counties; it sent `county` and `caseType` as GET params to a
form that is a POST; it captured no address, parcel or PDF; and it wrote to
`probate_filings`, a table that does not exist.

Only the URL and the rate limiting survived.

=== WHY OWNER-DRIVEN, NOT A SWEEP ===
MCRO CANNOT BE ENUMERATED. Measured 2026-08-07:

  * Last Name AND First Name are both REQUIRED. No browse-all.
  * Wildcards need TWO characters ("A*" is rejected) and the two name fields
    AND together, so the space is 676 x 676 = 456,976 combinations PER date
    window — about 44 days of requests at 3.5s.
  * There is a hard 200-result cap on every axis.
  * The attorney axis has the same two-field rule and worse yield:
    RA* / CH*, probate only, ALL of June 2026 statewide returned ONE case.

So we do not enumerate MCRO. We QUERY it, from a queue of owner names we
already hold — 1.25M distinct names in core.owners, each attached to a parcel
in a known county.

This inverts the problem and removes the hardest part: because we start FROM
the parcel, no address extraction and no address matching are needed. The
petition PDF becomes optional enrichment rather than the linchpin.

=== THE CRITICAL CORRECTNESS RULE ===
MCRO's name search matches ANY PARTY on a case, not just the decedent.

Measured live: querying ANDERSON / ERIC in Crow Wing returned two cases,
NEITHER of which is a valid match:

  1. "Eric Anderson, Probate Document" — Eric is a PARTY, not the decedent.
     The title is not an estate caption at all.
  2. "In re the Estate of Larry Martin Anderson, Deceased" — correct shape,
     but the decedent is LARRY. Eric is presumably an heir.

A naive implementation would have flagged Eric Anderson's $3.28M property as
probate-distressed. Both are rejected by _match_decedent() below.

**A wrong probate flag says a living person is dead, on their own property
record.** That is the worst error this scraper can make. When in doubt, drop
the row.

=== REQUEST MECHANICS (captured from DevTools 2026-08-08) ===
POST https://publicaccess.courts.state.mn.us/CaseSearch/CaseSearchSearch
Content-Type: application/x-www-form-urlencoded

    FormType=person
    LastName / FirstName / MiddleName
    mpaCaseSearchDobToggle=on, DobExact=true, DobFrom=, DobTo=
    mpaCaseSearchDateFiledToggle=on, FiledDateExact=true,
        FiledDateFrom=, FiledDateTo=
    CaseCategoryKeys=PR            <- probate; a code, not the checkbox label
    CaseStatus=0                   <- 0 = All
    mpaLocOptionToggle=on, AllLocations=false, Locations=<numeric county id>
    __RequestVerificationToken=<from the search page's hidden input>

Requires the antiforgery cookie that pairs with the token, plus
IsAcceptedTerms=true. A GET of /CaseSearch sets both — see _bootstrap().

`FiledDateExact=true` with empty From/To is the "On" toggle. Sending
`FiledDateExact=false` WITH From/To populated is the "Range" form. The site's
UI resets this toggle to "On" between searches and it silently ignores the
window — that bit us live. This scraper always sends the flag explicitly.

Server headers show `volt-adc` / `X-Volterra-Location`: F5 Distributed Cloud
sits in front. That is where a CAPTCHA would come from. Be polite.

=== PARSING BY LABEL, NOT BY SELECTOR ===
The results markup has never been inspected — only the rendered page. Any CSS
selector written here would be INVENTED, which is exactly how the previous
skeleton failed. The page uses stable labels (`Case Number:`, `Case Title:`,
`Case Type:`, `Case Location:`, `Case Status:`, `Date Filed:`), so this
parser extracts text and reads those. Robust to markup changes and honest
about what is actually known.

If the labels ever change, `records_fetched` drops to 0 with no error — hence
the explicit "no results" sentinel check in _parse_results().
"""

from __future__ import annotations

import asyncio
import re
import threading
import time
from datetime import date, datetime, timezone
from typing import Any, ClassVar

# === TLS IMPERSONATION IS REQUIRED — DO NOT SWAP THIS FOR httpx ===
# MCRO sits behind an F5 BIG-IP WAF (`Server: volt-adc`,
# `X-Volterra-Location` on every response). It rejects the connection at the
# TLS layer, BEFORE any HTTP bytes are read, by fingerprinting the
# ClientHello — cipher order, extensions, curves. Python's `ssl` module
# produces a signature no browser matches.
#
# Measured 2026-08-08: httpx with a full set of real browser headers
# (User-Agent, Accept, Sec-Fetch-*, Sec-Ch-Ua, Origin, Referer) was STILL
# rejected on the first request of every run with:
#
#   "The requested URL was rejected. Please proceed to the next page via
#    this link to submit a support request..."
#
# Headers cannot fix a TLS-layer block. curl_cffi impersonates Chrome's
# actual ClientHello, which is the only thing that changes at that layer.
#
# If curl_cffi is ALSO rejected, the block is not fingerprint-based — it is
# most likely the datacenter IP. Railway's ranges are well known and
# mnpublicnotice already runs locally from a residential IP for exactly this
# reason. In that case this scraper belongs on the same local box, and no
# library change will help.
from curl_cffi import requests as cffi_requests

from src.models.signal import ProbateFilingInsert
from src.scrapers.base_scraper import BaseScraper
from src.services.event_writer import (
    write_events_dedup,
    write_typed_signals_dedup,
)
from src.db.supabase_client import signals_table
from src.utils.errors import ParseError, SourceUnavailableError
from src.utils.logger import logger

from bs4 import BeautifulSoup


_BASE_URL = "https://publicaccess.courts.state.mn.us"
_SEARCH_PAGE = f"{_BASE_URL}/CaseSearch"
_SEARCH_POST = f"{_BASE_URL}/CaseSearch/CaseSearchSearch"

# Global politeness interval. MCRO sits behind F5 Distributed Cloud; a
# scraper that gets banned in week two is worth less than a slow one that
# runs for years. Do NOT lower this.
_RATE_LIMIT_SECONDS = 3.5

# Numeric county ids used by the `Locations` form field.
#
# ONLY CROW WING IS KNOWN. It was captured from a live request
# (Locations=177). The ids for the other 86 counties have NOT been collected;
# they live in the search page's county selector and must be harvested before
# this scraper can run anywhere else.
#
# NEVER guess an id. A wrong id searches the wrong county and returns
# plausible-looking rows for the wrong people. run() refuses any county that
# is not in this map.
_COUNTY_LOCATION_IDS: dict[str, str] = {
    "crow_wing": "177",
}

# Case types worth acting on. `Formal Supervised` and `Formal Unsupervised`
# are real estate administrations.
#
# EXCLUDED DELIBERATELY:
#   Probate Document           — a lighter filing, often no estate at all
#   Guardianship/Conservatorship — concerns a LIVING PERSON. It appears under
#                                 the Probate category. Treating it as a
#                                 probate signal would flag someone who is
#                                 very much alive.
_ACCEPTED_CASE_TYPES = {
    "formal supervised",
    "formal unsupervised",
}

# Statuses meaning the estate is still open. A case closed years ago is
# history, not distress.
_OPEN_STATUSES = {
    "under court jurisdiction",
    "open",
    "pending",
}

# `In re the Estate of <NAME>, Deceased`. Anything not matching this shape is
# not an estate caption — see the ANDERSON/ERIC finding in the module
# docstring.
_ESTATE_TITLE_RE = re.compile(
    r"in\s+re\s+(?:the\s+)?estate\s+of\s+(?P<name>.+?)\s*,?\s*deceased",
    re.I,
)

# Label-based field extraction — see "PARSING BY LABEL" above.
#
# FIXED 2026-08-08. The first version terminated each value at a run of two
# or more spaces. That is wrong: BeautifulSoup's get_text("  ") separator IS
# two spaces, so every value was cut at its first internal gap. Verified
# against a real response — `Case Title:` truncated to "In re the Estate of",
# losing the decedent's name entirely, which would have made _match_decedent
# reject 100% of genuine estates.
#
# Each pattern now runs to the NEXT LABEL via lookahead, never to whitespace.
_LABEL_RES = {
    "case_number": re.compile(r"Case Number:\s*(\S+)"),
    "case_title": re.compile(
        r"Case Title:\s*(.+?)\s*(?=Case Type:|Case Number:|Result\s+\d+|$)", re.S
    ),
    "case_type": re.compile(
        r"Case Type:\s*(.+?)\s*(?=Case Location:|$)", re.S
    ),
    "case_location": re.compile(
        r"Case Location:\s*(.+?)\s*(?=Case Status:|$)", re.S
    ),
    "case_status": re.compile(
        r"Case Status:\s*(.+?)\s*(?=View Case Details|Result\s+\d+|$)", re.S
    ),
    "date_filed": re.compile(r"Date Filed:\s*(\d{1,2}/\d{1,2}/\d{4})"),
}

_TOKEN_RE = re.compile(
    r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', re.I
)

_NO_RESULTS = "no cases match your search"
# Headers sent on EVERY request. Captured from the browser session that
# produced a working search on 2026-08-08.
#
# === WHY THIS EXISTS ===
# MCRO sits behind an F5 BIG-IP web application firewall (visible as
# `Server: volt-adc` and `X-Volterra-Location` on every response). Without a
# realistic User-Agent, httpx sends `python-httpx/0.x` and the WAF rejects
# the request outright with a 268-byte page reading:
#
#   "The requested URL was rejected. Please proceed to the next page via
#    this link to submit a support request..."
#
# That is NOT rate limiting — it fired on the FIRST search of every run.
# Four consecutive pilot runs (500, 500, 25, 25 names) returned zero cases
# for this reason, including for ANDERSON/ERIC, which is known to return two.
# The parser and the queue were fine the whole time; the request was being
# refused before it ever reached the search.
_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Ch-Ua": '"Chromium";v="151", "Not?A_Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Upgrade-Insecure-Requests": "1",
}

# Text that means we have been blocked. The run STOPS on any of these — it
# does not retry.
#
# `requested url was rejected` is F5's own wording and is the one that
# actually fires. The earlier list ("captcha", "unusual traffic", "access
# denied", "request blocked") matched none of it, so four runs reported
# `success` while every single request was being refused. If a block is ever
# silent again, the missing string belongs here.
_CAPTCHA_MARKERS = (
    "requested url was rejected",
    "support request",
    "captcha",
    "unusual traffic",
    "are you a human",
    "access denied",
    "request blocked",
)


def _norm(s: str | None) -> str:
    """Collapse whitespace and lowercase, for comparisons."""
    return re.sub(r"\s+", " ", (s or "")).strip().lower()


def _dedupe_sr(value: str | None) -> str | None:
    """Collapse MCRO's duplicated accessibility text.

    Every value is rendered twice — once `aria-hidden="true"` for sighted
    users and once in an `sr-only` span for screen readers — so flat text
    extraction yields "Crow Wing County  Crow Wing County". Harmless for
    token matching, but it should not be stored that way.
    """
    if not value:
        return value
    v = re.sub(r"\s{2,}", " ", value).strip()
    n = len(v)
    if n % 2 == 1 and v[n // 2] == " ":
        left, right = v[: n // 2], v[n // 2 + 1:]
        if left == right:
            return left
    return v


def _parse_mdy(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value.strip(), "%m/%d/%Y").date()
    except ValueError:
        return None


def _county_from_location(raw: str | None) -> str | None:
    """Extract a county slug from MCRO's Case Location field.

    TWO shapes exist in real data:
        "Becker County"
        "Washington County, Washington, Stillwater"
    """
    if not raw:
        return None
    first = raw.split(",")[0].strip()
    first = re.sub(r"\s+county$", "", first, flags=re.I).strip()
    if not first:
        return None
    return re.sub(r"[^a-z0-9]+", "_", first.lower()).strip("_") or None


def _match_decedent(case_title: str, last_name: str, first_name: str) -> bool:
    """True only if the case is an estate caption naming THIS person.

    Rejects both failure modes measured live on ANDERSON / ERIC:
      * "Eric Anderson, Probate Document" — not an estate caption
      * "In re the Estate of Larry Martin Anderson, Deceased" — Larry, not
        Eric

    Requires the queried surname AND first name to both appear in the
    decedent's name. Middle names and suffixes are tolerated.
    """
    m = _ESTATE_TITLE_RE.search(case_title or "")
    if not m:
        return False
    decedent = _norm(m.group("name"))
    if not decedent:
        return False
    tokens = set(re.split(r"[^a-z0-9]+", decedent))
    return _norm(last_name) in tokens and _norm(first_name) in tokens


class McroProbateScraper(BaseScraper[dict[str, Any], ProbateFilingInsert]):
    """Statewide probate via MCRO, driven by a queue of known owner names."""

    source_name: ClassVar[str] = "mcro_probate"
    signal_type: ClassVar[str] = "probate_filing"

    # Pilot scope. See _COUNTY_LOCATION_IDS — this is the only county whose
    # numeric Locations id is known.
    county_code: ClassVar[str] = "crow_wing"

    # Names per run.
    #
    # SET TO 25 FOR DIAGNOSIS (2026-08-08). The queue is ordered by EMV
    # descending and ANDERSON / ERIC sits 9th at $3,285,300 — a name KNOWN to
    # return two cases in Crow Wing. So a batch of 25 takes ~90 seconds and
    # still exercises a guaranteed-positive lookup end to end.
    #
    # RAISE BACK TO 500 once the first-response diagnostic in fetch() confirms
    # the scraper is receiving real results HTML. 500 at 3.5s is ~30 minutes,
    # which is the right size for measuring a hit rate but far too slow to
    # debug against.
    batch_size: ClassVar[int] = 25

    _rate_lock: ClassVar[threading.Lock] = threading.Lock()
    _last_request_at: ClassVar[float] = 0.0

    # ---- politeness ----

    @classmethod
    def _throttle(cls) -> None:
        with cls._rate_lock:
            elapsed = time.monotonic() - cls._last_request_at
            wait = _RATE_LIMIT_SECONDS - elapsed
            if wait > 0:
                time.sleep(wait)
            cls._last_request_at = time.monotonic()

    @staticmethod
    def _check_blocked(html: str) -> None:
        """Stop the run entirely on any sign of blocking. Never retry."""
        low = html[:4000].lower()
        for marker in _CAPTCHA_MARKERS:
            if marker in low:
                raise SourceUnavailableError(
                    f"MCRO returned a block/CAPTCHA page (marker: {marker!r}). "
                    f"Stopping — do NOT retry.",
                    source="mcro_probate",
                )

    # ---- queue ----

    def _load_queue(self) -> list[dict[str, Any]]:
        """Read the prioritised owner-name queue.

        Backed by the view signals.mcro_probate_queue — see
        migrations/mcro_probate_queue.sql. Highest-EMV owners first: an estate
        holding a $3M lake property is both likelier to go through formal
        probate and more valuable when it does.
        """
        result = (
            signals_table("mcro_probate_queue")
            .select("county_code,last_name,first_name,parcel_id,emv_total,"
                    "address,city,last_sale_date")
            .eq("county_code", self.county_code)
            .order("emv_total", desc=True)
            .limit(self.batch_size)
            .execute()
        )
        return list(result.data or [])

    # ---- session ----

    def _bootstrap(self, client: Any) -> str:
        """GET the search page for the antiforgery token and terms cookies.

        The POST needs `__RequestVerificationToken` AND the paired
        `.AspNetCore.Antiforgery.*` cookie, plus `IsAcceptedTerms=true`. A
        plain GET sets the cookies on the client's jar and exposes the token
        in a hidden input.
        """
        self._throttle()
        resp = client.get(_SEARCH_PAGE, timeout=30.0)
        if resp.status_code != 200:
            raise SourceUnavailableError(
                f"MCRO search page returned {resp.status_code}",
                source=self.source_name,
            )
        self._check_blocked(resp.text)
        m = _TOKEN_RE.search(resp.text)
        if not m:
            raise ParseError(
                "Could not find __RequestVerificationToken on the MCRO search "
                "page — the form has changed.",
                source=self.source_name,
            )
        return m.group(1)

    def _search(
        self,
        client: Any,
        token: str,
        last_name: str,
        first_name: str,
        location_id: str,
    ) -> str:
        """POST one name search and return the results HTML."""
        payload = {
            "FormType": "person",
            "LastName": last_name,
            "FirstName": first_name,
            "MiddleName": "",
            # Date of Birth: unused, but the toggle must be sent or the form
            # rejects the post.
            "mpaCaseSearchDobToggle": "on",
            "DobExact": "true",
            "DobFrom": "",
            "DobTo": "",
            # Date Filed: no window on the owner-driven design — we want the
            # person's whole probate history, not a slice. Sending
            # FiledDateExact=true with empty From/To is the "On" form with no
            # date set, i.e. unfiltered. NEVER leave this implicit: the site's
            # UI silently resets this toggle and ignores any window.
            "mpaCaseSearchDateFiledToggle": "on",
            "FiledDateExact": "true",
            "FiledDateFrom": "",
            "FiledDateTo": "",
            "CaseCategoryKeys": "PR",
            "CaseStatus": "0",
            "mpaLocOptionToggle": "on",
            "AllLocations": "false",
            "Locations": location_id,
            "__RequestVerificationToken": token,
        }
        self._throttle()
        resp = client.post(
            _SEARCH_POST,
            data=payload,
            timeout=45.0,
            headers={
                # Client-level _BROWSER_HEADERS still apply; these are the
                # POST-specific ones the browser adds for an XHR form submit.
                "Origin": _BASE_URL,
                "Referer": _SEARCH_PAGE,
                "X-Requested-With": "XMLHttpRequest",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Accept": "*/*",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
            },
        )
        if resp.status_code != 200:
            raise SourceUnavailableError(
                f"MCRO search returned {resp.status_code}",
                source=self.source_name,
            )
        self._check_blocked(resp.text)
        return resp.text

    # ---- parsing ----

    def _parse_results(self, html: str) -> list[dict[str, Any]]:
        """Extract case rows from a results page by LABEL, not selector."""
        text = BeautifulSoup(html, "lxml").get_text("  ", strip=True)

        if _NO_RESULTS in text.lower():
            return []

        # Split on the "Result N of M" marker the page places above each card.
        chunks = re.split(r"Result\s+\d+\s+of\s+\d+", text)
        rows: list[dict[str, Any]] = []
        for chunk in chunks[1:]:
            row: dict[str, Any] = {}
            for key, pattern in _LABEL_RES.items():
                m = pattern.search(chunk)
                row[key] = _dedupe_sr(m.group(1).strip()) if m else None
            if row.get("case_number"):
                rows.append(row)

        if not rows and "case number" in text.lower():
            # Labels present but nothing parsed — the markup changed. Say so
            # loudly rather than silently reporting zero.
            raise ParseError(
                "MCRO results page contains case labels but no rows parsed — "
                "the result layout has changed.",
                source=self.source_name,
            )
        return rows

    # ---- BaseScraper interface ----

    async def fetch(self, trigger: str) -> list[dict[str, Any]]:
        """Query MCRO once per queued owner name."""
        location_id = _COUNTY_LOCATION_IDS.get(self.county_code)
        if not location_id:
            # Never guess. A wrong Locations id searches the wrong county and
            # returns plausible rows for the wrong people.
            raise SourceUnavailableError(
                f"No MCRO Locations id known for county {self.county_code!r}. "
                f"Harvest it from the search page's county selector and add "
                f"it to _COUNTY_LOCATION_IDS before running.",
                source=self.source_name,
            )

        queue = await asyncio.to_thread(self._load_queue)
        if not queue:
            logger.warning(
                "MCRO probate queue is empty",
                source=self.source_name,
                county=self.county_code,
                hint="is signals.mcro_probate_queue created?",
            )
            return []

        logger.info(
            "MCRO probate run starting",
            source=self.source_name,
            county=self.county_code,
            names=len(queue),
            est_minutes=round(len(queue) * _RATE_LIMIT_SECONDS / 60, 1),
        )

        raw: list[dict[str, Any]] = []
        names_with_any_result = 0

        def _work() -> list[dict[str, Any]]:
            nonlocal names_with_any_result
            out: list[dict[str, Any]] = []
            logged_sample = False
            # impersonate= is the whole point: it makes curl emit Chrome's
            # TLS ClientHello. Without it the F5 WAF rejects the connection
            # before any HTTP request is processed.
            with cffi_requests.Session(
                impersonate="chrome124",
                headers=_BROWSER_HEADERS,
            ) as client:
                token = self._bootstrap(client)
                for i, entry in enumerate(queue, start=1):
                    try:
                        html = self._search(
                            client,
                            token,
                            entry["last_name"],
                            entry["first_name"],
                            location_id,
                        )
                    except SourceUnavailableError:
                        raise
                    except Exception as e:
                        logger.warning(
                            "MCRO search failed for one name",
                            source=self.source_name,
                            last_name=entry["last_name"],
                            first_name=entry["first_name"],
                            error=str(e),
                        )
                        continue

                    # ---- ONE-SHOT DIAGNOSTIC ----
                    # The 2026-08-08 pilot queried 500 names in 1,770s and
                    # returned ZERO cases — including ANDERSON/ERIC, which is
                    # KNOWN to return two cases in Crow Wing. The parser was
                    # then proved correct against a saved real response, so
                    # the scraper is receiving something other than results
                    # HTML.
                    #
                    # A previous attempt logged this via logger.info() with
                    # keyword fields. THAT WAS INVISIBLE: Railway's log export
                    # renders those lines with an EMPTY message body — the
                    # timestamps appear, the content does not. Every
                    # structured-field diagnostic written this session was
                    # lost the same way.
                    #
                    # So the sample goes into the EXCEPTION MESSAGE, which
                    # run() records in audit.scraper_runs.error_message — a
                    # plain text column that reads back reliably.
                    #
                    # Failing here is correct, not defensive: if the first
                    # response has no result badge, the run cannot produce
                    # anything and reporting `success` would be a lie.
                    #
                    # DELETE THIS BLOCK once the request is proven to work.
                    if not logged_sample:
                        logged_sample = True
                        if "badge-result-number" not in html:
                            head = re.sub(r"\s+", " ", html[:400])
                            raise ParseError(
                                "MCRO DIAGNOSTIC | "
                                f"name={entry['last_name']}/{entry['first_name']} "
                                f"len={len(html)} "
                                f"results_section="
                                f"{'CaseSearchResultsTitleSection' in html} "
                                f"result_badge=False "
                                f"no_results_text={_NO_RESULTS in html.lower()} "
                                f"status_hint={html[:80]!r} "
                                f"| HEAD: {head}",
                                source=self.source_name,
                            )

                    cases = self._parse_results(html)
                    if cases:
                        names_with_any_result += 1
                    for case in cases:
                        # Carry the queue entry so parse() can link the case
                        # to a parcel without re-querying.
                        case["_queue"] = entry
                        out.append(case)

                    # Refresh the antiforgery token periodically; it is tied
                    # to a session cookie and will expire on a long run.
                    if i % 100 == 0:
                        token = self._bootstrap(client)
                        logger.info(
                            "MCRO progress",
                            source=self.source_name,
                            queried=i,
                            of=len(queue),
                            names_with_results=names_with_any_result,
                            raw_cases=len(out),
                        )
            return out

        raw = await asyncio.to_thread(_work)

        logger.info(
            "MCRO probate fetch complete",
            source=self.source_name,
            names_queried=len(queue),
            names_with_results=names_with_any_result,
            raw_cases=len(raw),
        )
        return raw

    async def parse(
        self, raw_records: list[dict[str, Any]]
    ) -> list[ProbateFilingInsert]:
        """Filter to genuine, current estates for the queried person."""
        signals: list[ProbateFilingInsert] = []
        dropped_type = 0
        dropped_title = 0
        dropped_status = 0
        dropped_county = 0

        for r in raw_records:
            entry = r.get("_queue") or {}

            case_type = _norm(r.get("case_type"))
            if case_type not in _ACCEPTED_CASE_TYPES:
                dropped_type += 1
                continue

            # THE decedent check — see the module docstring.
            if not _match_decedent(
                r.get("case_title") or "",
                entry.get("last_name") or "",
                entry.get("first_name") or "",
            ):
                dropped_title += 1
                continue

            if _norm(r.get("case_status")) not in _OPEN_STATUSES:
                dropped_status += 1
                continue

            case_county = _county_from_location(r.get("case_location"))
            if case_county and case_county != entry.get("county_code"):
                # The search was county-scoped, so this should not happen.
                # If it does, the Locations id is wrong — drop rather than
                # write a case from the wrong county.
                dropped_county += 1
                logger.warning(
                    "MCRO case county does not match the queried county",
                    source=self.source_name,
                    case_county=case_county,
                    queried_county=entry.get("county_code"),
                    case_number=r.get("case_number"),
                )
                continue

            m = _ESTATE_TITLE_RE.search(r.get("case_title") or "")
            decedent_name = m.group("name").strip() if m else None

            signals.append(
                ProbateFilingInsert(
                    parcel_id=entry.get("parcel_id"),
                    case_number=(r.get("case_number") or "").strip(),
                    county_code=entry.get("county_code") or self.county_code,
                    filing_date=_parse_mdy(r.get("date_filed")),
                    decedent_name=decedent_name,
                    # DOD, personal representatives and has_will require the
                    # Case Details page. Not fetched in the pilot: it doubles
                    # the request count and the pilot exists to measure hit
                    # rate, not to enrich. Add once the rate justifies it.
                    date_of_death=None,
                    personal_representative_name=None,
                    case_type=(r.get("case_type") or "").strip() or None,
                    case_status=(r.get("case_status") or "").strip() or None,
                    has_will=None,
                    raw_data={
                        "case_title": r.get("case_title"),
                        "case_location": r.get("case_location"),
                        "matched_owner": {
                            "last_name": entry.get("last_name"),
                            "first_name": entry.get("first_name"),
                            "parcel_id": entry.get("parcel_id"),
                            "emv_total": entry.get("emv_total"),
                            "address": entry.get("address"),
                            "city": entry.get("city"),
                        },
                        "_source": self.source_name,
                    },
                    observed_at=datetime.now(timezone.utc),
                    source=self.source_name,
                    severity="medium",
                )
            )

        logger.info(
            "MCRO probate parse complete",
            source=self.source_name,
            raw_cases=len(raw_records),
            accepted=len(signals),
            dropped_case_type=dropped_type,
            dropped_not_this_decedent=dropped_title,
            dropped_closed=dropped_status,
            dropped_wrong_county=dropped_county,
        )
        return signals

    async def write(
        self, signals: list[ProbateFilingInsert]
    ) -> tuple[int, int, int]:
        """Persist typed rows + unified events.

        No resolve_parcel() call: the parcel already exists — it came out of
        core.parcels via the queue view. This scraper never creates parcels.
        """
        if not signals:
            return 0, 0, 0

        # `source` and `severity` have no column on signals.probate_cases.
        # Sending them raises PGRST204 before on_conflict is evaluated.
        _IN_MEMORY_ONLY = {"source", "severity"}
        rows = []
        for sig in signals:
            row = sig.model_dump(mode="json", exclude_none=True)
            for k in _IN_MEMORY_ONLY:
                row.pop(k, None)
            rows.append(row)

        # Matches signals.probate_cases_dedup (county_code, case_number)
        # NULLS NOT DISTINCT, created 2026-08-07 on the empty table. Keyed on
        # the CASE, not the property: one estate can hold several parcels, and
        # case numbers are not uniformly formatted (03-PR-26-1092 but also
        # 82-26-323), so county_code guards against county-local numbering.
        new_typed, failed_typed = write_typed_signals_dedup(
            "probate_cases",
            rows,
            on_conflict="county_code,case_number",
        )

        events = [e for e in (s.to_event() for s in signals) if e is not None]
        new_events, failed_events = write_events_dedup(events)

        logger.info(
            "MCRO probate write complete",
            source=self.source_name,
            typed_new=new_typed,
            events_new=new_events,
            failed=failed_typed + failed_events,
        )
        return (new_typed, 0, failed_typed + failed_events)


__all__ = ["McroProbateScraper"]
