"""
govire_mnpn_browser.py
================================================================================
Daily mnpublicnotice.com FULL-NOTICE scraper. Runs LOCALLY from a home IP
(which mnpublicnotice does NOT block, unlike Railway's datacenter IP).

PIPELINE
  1. Open mnpublicnotice.com in a real (Playwright) browser.
  2. Search "foreclosure" over a recent window; collect notice IDs from results.
  3. For each NEW notice (not already in ai.extracted_foreclosures), open its
     Details page, solve the Cloudflare Turnstile captcha, and read the COMPLETE
     notice text -- preferring the downloadable PDF (full text even for capped
     notices).
  4. Run that text through the SAME extraction prompt as the server pipeline
     (calling Anthropic directly), then insert into Supabase
     ai.extracted_foreclosures as 'pending' -- lands in the Notice-review tab.

SELF-CONTAINED: imports NO app code. Extraction prompt + coercion are copied
verbatim from src/llm/foreclosure_extraction.py so output is identical. Depends
only on installed libs: playwright, playwright-stealth, anthropic, supabase,
2captcha-python, pdfplumber. This file has NO secrets -- safe to commit to git.

CAPTCHA NOTE (July 2026): mnpublicnotice.com migrated from Google reCAPTCHA to
Cloudflare Turnstile. _maybe_solve_captcha now uses 2Captcha's turnstile method
and writes the token into the cf-turnstile-response field.

SETUP (one time)
  1. Keep this file in a PERMANENT folder, e.g. C:\\Users\\emsho\\govire-scrapers\\
  2. Create a .env NEXT TO it with:
        SUPABASE_URL=https://zdqwigbssxhqzlveisdz.supabase.co
        SUPABASE_SERVICE_KEY=<service_role key>
        ANTHROPIC_API_KEY=<anthropic key>
        TWOCAPTCHA_API_KEY=<2captcha key>
     Optional (silent-failure email alerts via Resend -- if omitted, an
     unhealthy run just logs a loud local ALERT line instead of emailing):
        RESEND_API_KEY=<resend key>
        ALERT_EMAIL_TO=<where alerts go, e.g. you@example.com>
        ALERT_EMAIL_FROM=<verified Resend sender, e.g. alerts@govire.com>
  3. Install the browser binary once:  python -m playwright install chromium
  4. Test:    py govire_mnpn_browser.py --max 1
     Daily:   py govire_mnpn_browser.py --max 50 --headless

Keys live ONLY in the local .env (gitignore it). This .py is safe in git.
================================================================================
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
except ImportError:
    sys.exit("playwright missing. Run: pip install playwright && python -m playwright install chromium")

try:
    from anthropic import Anthropic
except ImportError:
    sys.exit("anthropic missing. Run: pip install anthropic")

try:
    from supabase import create_client
except ImportError:
    sys.exit("supabase missing. Run: pip install supabase")

# Optional: stealth (reduces bot detection) and 2captcha (only if a captcha hits).
try:
    from playwright_stealth import stealth_sync
except Exception:
    stealth_sync = None
try:
    from twocaptcha import TwoCaptcha
except Exception:
    TwoCaptcha = None
# Optional: pdfplumber for extracting full text from the complete-notice PDF.
try:
    import pdfplumber
except Exception:
    pdfplumber = None


# ============================================================
# Config
# ============================================================

_HERE = Path(__file__).resolve().parent
_BASE = "https://www.mnpublicnotice.com"
_SEARCH_PAGE = f"{_BASE}/Search.aspx"
_MODEL = "claude-haiku-4-5-20251001"   # matches server llm pricing table
_DETAIL_FETCH_PAUSE = 1.5              # seconds between notices (politeness)


def _log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def _load_env() -> dict[str, str]:
    """Load KEY=VALUE pairs from a .env next to this script. Real env vars win."""
    env: dict[str, str] = {}
    p = _HERE / ".env"
    if p.exists():
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    for k in ("SUPABASE_URL", "SUPABASE_SERVICE_KEY", "ANTHROPIC_API_KEY", "TWOCAPTCHA_API_KEY"):
        if os.environ.get(k):
            env[k] = os.environ[k]
    return env


# ============================================================
# Extraction prompt + coercion (verbatim from server pipeline)
# ============================================================

_STRING_FIELDS = (
    "mortgagor", "mortgagee", "property_address", "city", "county",
    "parcel_id", "legal_description", "sale_time", "sale_location",
    "redemption_period", "attorney_firm", "attorney_file_no", "extraction_notes",
)
_NUMBER_FIELDS = ("original_principal", "amount_due")
_DATE_FIELDS = ("sale_date", "vacate_date")

_SYSTEM_PROMPT = (
    "You extract structured data from Minnesota mortgage foreclosure sale "
    "notices (published 'Notice of Mortgage Foreclosure Sale' legal notices).\n\n"
    "Return ONLY a single JSON object -- no prose, no markdown, no code "
    "fences -- with EXACTLY these keys:\n"
    '{"mortgagor","mortgagee","property_address","city","county",'
    '"parcel_id","legal_description","original_principal","amount_due",'
    '"sale_date","sale_time","sale_location","redemption_period",'
    '"vacate_date","attorney_firm","attorney_file_no","confidence",'
    '"extraction_notes"}\n\n'
    "RULES:\n"
    "- Use ONLY information explicitly stated. If a field is not present, use "
    "null. NEVER guess, infer, or fabricate.\n"
    "- mortgagor = the borrower being foreclosed on (labeled 'MORTGAGOR(S)').\n"
    "- mortgagee = the CURRENT holder/assignee foreclosing now. If there is an "
    "assignment chain, use the FINAL assignee, and record the full chain in "
    "extraction_notes.\n"
    "- attorney_file_no: a notice often begins or ends with a bare reference "
    "or file number (e.g. 24-117341) that is the attorney/trustee file number, "
    "even when it is not explicitly labeled. Capture it if present.\n"
    "- Dates in YYYY-MM-DD. A partial or ambiguous date -> null, explained in "
    "extraction_notes.\n"
    "- Money as plain numbers: 210895.10 (strip $, commas, and words).\n"
    "- redemption_period: copy the stated period as text, e.g. '6 months'.\n"
    "- confidence: a number 0.0-1.0 for how cleanly this notice mapped to the "
    "fields. Lower it for unusual notices (a condominium-association "
    "assessment lien rather than a mortgage, a missing property address, an "
    "ambiguous party).\n"
    "- extraction_notes: briefly note anything a human reviewer should check "
    "(assignment chain, lien type, missing fields). null if nothing notable.\n"
    "- Output the JSON object and nothing else."
)


_REDEMPTION_SYSTEM_PROMPT = (
    "You extract structured data from Minnesota NOTICE OF EXPIRATION OF "
    "REDEMPTION legal notices (Minn. Stat. 281 -- published by a county "
    "auditor when the redemption period after a tax judgment sale is about "
    "to expire).\n\n"
    "IMPORTANT: one published document may contain SEVERAL separate notices, "
    "each with its own bid-in date and delinquent tax year, and each notice "
    "may list MANY parcels. Return EVERY parcel from EVERY notice in the "
    "document.\n\n"
    "Return ONLY a single JSON object -- no prose, no markdown, no code "
    "fences -- of the form:\n"
    '{"county": "...", "parcels": [ { ... }, { ... } ]}\n\n'
    "Each object in \"parcels\" must have EXACTLY these keys:\n"
    '{"owner_name","mailing_address","mailing_city_state_zip",'
    '"parcel_id_raw","property_address","legal_description",'
    '"redemption_amount","bid_in_date","delinquent_tax_year",'
    '"redemption_expiry","do_not_mail","confidence","extraction_notes"}\n\n'
    "RULES:\n"
    "- Use ONLY information explicitly stated. If a field is not present, "
    "use null. NEVER guess, infer, or fabricate.\n"
    "- county: the county name as printed, e.g. 'LE SUEUR COUNTY, MN'. Do "
    "not normalise it.\n"
    "- parcel_id_raw: copy the parcel number EXACTLY as printed, including "
    "any prefix and punctuation -- 'RP 21.457.1102' and 'RP 41250822' are "
    "both correct as written. Do NOT strip, pad or reformat it.\n"
    "- owner_name: the owner/taxpayer/interested party as listed. Keep a "
    "trailing '&' if the notice prints one. If several parties are listed "
    "for ONE parcel, join them with ' & '.\n"
    "- mailing_address / mailing_city_state_zip: the party's address, which "
    "is OFTEN DIFFERENT from the property address. Keep them separate and "
    "do not substitute one for the other.\n"
    "- property_address: the address OF THE PARCEL, when stated separately.\n"
    "- redemption_amount: the amount necessary to redeem, as a plain number "
    "(strip $ and commas): 13403.41\n"
    "- bid_in_date: the date the parcel was 'bid in for the state', "
    "YYYY-MM-DD.\n"
    "- delinquent_tax_year: the tax year the judgment was for, as an "
    "integer.\n"
    "- redemption_expiry: the date the notice states the redemption period "
    "expires, YYYY-MM-DD. The COUNTY'S OWN STATED DATE -- never compute or "
    "adjust it.\n"
    "  It is stated ONCE PER NOTICE and applies to EVERY parcel in that "
    "notice, so use the SAME date for all of them. Look for any of these "
    "phrasings, which are the ones counties actually use:\n"
    "    * 'the redemption date of November 10, 2025'\n"
    "    * 'must be paid to redeem if paid on or before October 31, 2025'\n"
    "    * 'or May 11, 2026 whichever is later'\n"
    "    * 'The amounts listed above must be paid on or before ...'\n"
    "  Several counties define expiry as the LATER of three conditions -- "
    "60 days after service, the second Monday in May, or a stated calendar "
    "date. Take the STATED CALENDAR DATE; the other two are not dates.\n"
    "  **THE DATE YOU RETURN MUST APPEAR VERBATIM IN THE TEXT.** Do not "
    "resolve a condition into a date. 'the second Monday in May' is a "
    "CONDITION, not a date -- do NOT work out which day that falls on. "
    "'60 days after service of this notice' is a CONDITION -- the service "
    "date is not in the document and you cannot compute from it. If the "
    "notice states ONLY conditions and no calendar date, return null; that "
    "is the CORRECT answer and several counties (Red Lake, Renville) "
    "publish exactly that way.\n"
    "  Measured 2026-08-10: on a Jackson notice stating 'on or before "
    "February 28th, 2026' and 'the second Monday in May', a previous "
    "extraction returned 2026-05-11 -- the second Monday in May, computed. "
    "That string appears NOWHERE in the document. A homeowner told the "
    "wrong forfeiture date can lose their home; a null costs nothing but a "
    "review.\n"
    "  THE SURROUNDING TEXT MAY BE BADLY CORRUPTED and you should still use "
    "the date. Some counties publish multi-column pages whose PDF text "
    "layer interleaves character by character, e.g.\n"
    "    'c 3 o 1 n , t a 2 c 0 C P t 2L IT 5A Yt .T : hO 3 eF 5 0 CC a L Ar'\n"
    "  The date phrases above frequently survive intact inside that mess. "
    "Measured 2026-08-09: 35 of 42 Carlton parcels returned a null expiry "
    "with the note 'Redemption expiry date not stated', while the document "
    "plainly contained both 'the redemption date of November 10, 2025' and "
    "'must be paid to redeem if paid on or before October 31, 2025'. A "
    "legible date next to illegible text is still a legible date.\n"
    "- Dates YYYY-MM-DD. A partial or ambiguous date -> null, explained in "
    "extraction_notes.\n"
    "- do_not_mail: true if the notice marks this party 'DO NOT MAIL' (or "
    "an equivalent such as 'RETURNED MAIL' / 'UNDELIVERABLE'), false "
    "otherwise. This is the COUNTY AUDITOR'S OWN FLAG and it governs "
    "whether the party may be contacted -- report it exactly as printed "
    "and NEVER infer it from a missing address.\n"
    "- confidence: 0.0-1.0 per parcel, for how cleanly THE FIELDS THAT "
    "EXIST IN THE NOTICE were read.\n"
    "  Lower it for: a parcel number you could not read cleanly, an "
    "unreadable or ambiguous amount, an owner block you could not attribute "
    "to one parcel, text so garbled you had to infer, or a value that might "
    "belong to a neighbouring record.\n"
    "  Do NOT lower it because a field is ABSENT FROM THE SOURCE. Counties "
    "publish different amounts of detail and terseness is not an extraction "
    "problem. bid_in_date, delinquent_tax_year, mailing_address and "
    "property_address are frequently not stated at all; returning null for "
    "them is the CORRECT answer and should not cost confidence. Note the "
    "absence in extraction_notes instead.\n"
    "  Measured 2026-08-09: all 64 Lake County parcels scored 0.60-0.70 with "
    "every note reading 'No bid-in date or delinquent tax year provided in "
    "document'. Owner, parcel id, amount and expiry were all present and "
    "correct. The score was describing the SOURCE, not the extraction, and "
    "held back complete records.\n"
    "- extraction_notes: anything a human reviewer should check. null if "
    "nothing notable.\n"
    "- If the document is NOT a tax redemption notice, return "
    '{"county": null, "parcels": []}.\n'
    "- Output the JSON object and nothing else."
)

_REDEMPTION_STRING_FIELDS = (
    "owner_name", "mailing_address", "mailing_city_state_zip",
    "parcel_id_raw", "property_address", "legal_description",
    "extraction_notes",
)
_REDEMPTION_NUMBER_FIELDS = ("redemption_amount",)
_REDEMPTION_DATE_FIELDS = ("bid_in_date", "redemption_expiry")


def _strip_fences(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z]*\n?", "", t)
        t = re.sub(r"\n?```$", "", t)
    return t.strip()


def _extract_json_object(text: str) -> Optional[str]:
    """Pull the first complete JSON object out of a reply.

    _strip_fences() only removes a fence at the START and END of the whole
    reply. Observed 2026-08-09 on notice 1030898: the model returned a valid
    object followed by a closing fence AND a prose explanation --

        {"county": "CROW WING COUNTY", "parcels": []} ```
        The document is truncated and does not provide sufficient ...

    json.loads() then failed on text that contained a perfectly good object.
    Scans for the first '{' and walks to its matching '}', respecting string
    literals and escapes so a brace inside a legal description does not end
    the scan early. Returns None if no balanced object is present.
    """
    if not text:
        return None
    start = text.find("{")
    if start == -1:
        return None
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start:i + 1]
    return None


def _log_bad_json(cleaned: str, resp: Any) -> None:
    """Report an unrecoverable reply with a SAMPLE of what came back.

    "unparseable JSON" with no sample cost three runs of guessing -- the
    same failure mode as the MCRO block markers, which matched none of the
    real wording. Always show the text.
    """
    head = cleaned[:400].replace("\n", " ")
    tail = cleaned[-200:].replace("\n", " ") if len(cleaned) > 600 else ""
    _log(f"  redemption extraction returned unparseable JSON; "
         f"reply was {len(cleaned)} chars")
    _log(f"    reply starts: {head}")
    if tail:
        _log(f"    reply ends:   {tail}")
    if getattr(resp, "stop_reason", None) == "max_tokens":
        _log("    stop_reason=max_tokens -- the reply was CUT OFF. "
             "Raise max_tokens or split the document.")
    return None


def _to_number(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s or s.lower() == "null":
        return None
    s = re.sub(r"[,$\s]", "", s)
    try:
        return float(s)
    except ValueError:
        return None


def _to_date(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() == "null":
        return None
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return s
    except ValueError:
        return None


def _clean_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() == "null":
        return None
    return s


def _to_confidence(v: Any) -> float:
    n = _to_number(v)
    if n is None:
        return 0.0
    return max(0.0, min(1.0, n))


def extract_notice(client: Anthropic, notice_text: str) -> Optional[dict[str, Any]]:
    """Anthropic call with the exact server prompt; coerce to a dict keyed like
    ai.extracted_foreclosures (+ 'model'). Returns None on any failure."""
    text = (notice_text or "").strip()
    if not text:
        return None
    try:
        resp = client.messages.create(
            model=_MODEL,
            max_tokens=1000,
            temperature=0.0,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": text}],
        )
    except Exception as e:
        _log(f"  anthropic call failed: {type(e).__name__}: {str(e)[:160]}")
        return None
    parts = [b.text for b in resp.content if getattr(b, "type", None) == "text"]
    cleaned = _strip_fences("".join(parts).strip())
    try:
        parsed = json.loads(cleaned)
    except (json.JSONDecodeError, ValueError):
        _log("  extraction returned unparseable JSON; skipping")
        return None
    if not isinstance(parsed, dict):
        return None
    data: dict[str, Any] = {}
    for k in _STRING_FIELDS:
        data[k] = _clean_str(parsed.get(k))
    for k in _NUMBER_FIELDS:
        data[k] = _to_number(parsed.get(k))
    for k in _DATE_FIELDS:
        data[k] = _to_date(parsed.get(k))
    data["confidence"] = _to_confidence(parsed.get("confidence"))
    data["model"] = _MODEL
    return data


def _to_int(v: Any) -> Optional[int]:
    n = _to_number(v)
    if n is None:
        return None
    try:
        return int(n)
    except (ValueError, OverflowError):
        return None


_NOTICE_SPLIT_RE = re.compile(
    r"(?=NOTICE\s+OF\s+EXPIRATION\s+OF\s+REDEMPTION)", re.IGNORECASE)

# Below this, a chunk is a fragment (a stray heading, a page-break artefact)
# rather than a real notice with parcels in it.
_MIN_CHUNK_CHARS = 400

# Chunks are still capped: one county notice can list a hundred parcels.
# Anything longer is split again on parcel boundaries by _split_oversize().
# Maximum INPUT characters per piece.
#
# Lowered 12000 -> 7000 on 2026-08-10, from measured chunk sizes rather than
# a guess. Pine notice 1027317 split into just TWO chunks of 11,509 and
# 10,806 chars; both passed the 12,000 guard untouched and each produced
# 32,000+ chars of output, hitting stop_reason=max_tokens and losing the
# whole notice.
#
# Output is NOT predicted by the extracted record size -- Pine's
# avg_record_len is 232 chars, mid-range, and Jackson's is 402 and works.
# It is predicted by INPUT SIZE, because Pine's source text carries long
# metes-and-bounds legal descriptions that the model condenses:
#
#   county   sent    got     ratio
#   lake     4,651   6,394   1.4x
#   carlton  12,657  22,682  1.8x   (largest that succeeded)
#   pine     22,525  32,000+ FAILED at the ceiling
#
# 7,000 sits below Lake's working maximum and well below both Pine chunks.
# The trade is more API calls per document, which is cheap next to losing
# a notice entirely.
_MAX_CHUNK_CHARS = 7000


def _split_redemption_document(text: str) -> list[str]:
    """Split a published document into individual notices.

    A tax redemption PDF holds SEVERAL notices, each with its own bid-in
    date and delinquent year, and each listing many parcels. Crow Wing
    2026-06-17 was 45,482 chars and produced a 38,000-char reply that hit
    stop_reason=max_tokens -- the whole document was then discarded.

    Raising max_tokens does not fix this: it moves the wall. Splitting does,
    and it also means one bad notice costs one notice rather than the file.

    The seam is the statutory heading, which every notice repeats.
    """
    if not text:
        return []
    parts = [p.strip() for p in _NOTICE_SPLIT_RE.split(text)]
    parts = [p for p in parts if len(p) >= _MIN_CHUNK_CHARS]
    if not parts:
        # No heading found -- treat the whole thing as one chunk rather than
        # silently returning nothing.
        return [text.strip()] if text.strip() else []
    out: list[str] = []
    for p in parts:
        out.extend(_split_oversize(p))
    return out


# Parcel identifiers, FORMAT-AGNOSTIC.
#
# Built from real notices across four counties, NOT from one:
#   Crow Wing  RP 41250822      prefix + flat digits
#   Le Sueur   RP 21.457.1102   prefix + dotted
#   Crow Wing  SM 95471202      a DIFFERENT prefix (severed mineral)
#   Pine       08.0208.000      NO PREFIX AT ALL, dotted
#
# The first version was `RP\s+\d+`, built from the two counties I had seen.
# It matched NOTHING in Pine, so _split_oversize returned a 22,753-char
# chunk untouched and all four Pine notices died at stop_reason=max_tokens
# (~39,000 chars of reply, discarded). Same mistake as the line-anchored
# markers before it: a pattern generalised from too small a sample.
#
# Two alternatives, tried in order:
#   1. optional 2-letter prefix followed by digit groups
#   2. bare dotted/undotted digit runs of 8+ characters
# Anchored on word boundaries so a year or a dollar amount inside a legal
# description cannot masquerade as a parcel id.
_PARCEL_TOKEN_RE = re.compile(
    # Optional 2-letter prefix, then digit groups joined by DOTS or HYPHENS.
    r"(?<![\d$(.,-])(?:[A-Z]{2}\s+)?\d{2,}(?:[.\-]\d{2,5}){1,3}(?![\d.\-])"
    # Prefix + flat run:  RP 41250822, SM 95471202
    r"|(?<![\d$(.,-])[A-Z]{2}\s+\d{6,}(?![\d.\-])"
    # Bare flat run:      01000010001000
    r"|(?<![\d$(.,-])\d{8,}(?![\d.\-])",
    re.IGNORECASE)

# Minimum digits for a match to count as a parcel id. Without it the pattern
# also caught "361-1710" from a phone number and "403.41" from a dollar
# amount. Every real format carries at least 8.
_PARCEL_MIN_DIGITS = 8


def _is_parcel_token(text: str) -> bool:
    return sum(c.isdigit() for c in text) >= _PARCEL_MIN_DIGITS

# Parcels per piece. Chunks that extract cleanly carry 10-29 parcels
# (measured: 10 -> 7,309 chars out; 21 -> 11,415; 29 -> 15,809, all
# stop_reason=end_turn). 25 leaves headroom for parcels with long
# metes-and-bounds legal descriptions, which are far bigger per record.
# Measured: Crow Wing at 25-31 parcels produced 15-19k chars of reply and
# finished at end_turn. Pine carries metes-and-bounds legal descriptions
# ("SECT-20 TWP-039 RANGE-020 2.52 AC THAT PART OF SOUTHWEST 1/4 ...") that
# are several times longer PER PARCEL, and 22,753 chars of input produced
# ~39,000 chars of output. 15 leaves room for the worst case seen.
_PARCELS_PER_PIECE = 15

# Context carried onto every piece. Taken from BOTH ends because column
# interleaving scatters the statutory sentences: the bid-in date and
# delinquent year sit near the top, the redemption expiry near the bottom.
# A tax redemption notice that reaches us this short is the site's CAPPED
# WEB STUB, not the notice. mnpublicnotice renders only ~1,000 chars on the
# page and says so ("Web display limited to 1,000 characters. Please view the
# PDF"). Observed 2026-08-09 on notice 1030898: a third Turnstile challenge
# interrupted the PDF download, the fetcher fell back to the stub, and 432
# chars were sent to the model -- which correctly reported it could not
# extract anything. Reject it BEFORE the API call: it is a fetch failure,
# not an extraction failure, and mislabelling it hides the real cause.
_MIN_REDEMPTION_CHARS = 1500

_CONTEXT_HEAD_CHARS = 1400
_CONTEXT_TAIL_CHARS = 2600


# Where a notice's CLOSING statutory text begins. Everything from here to
# the end is safe to use as context: it carries the redemption deadline and
# contains no parcel records.
_CLOSING_MARKERS = (
    "FAILURE TO REDEEM",
    "The amounts listed above must be paid",
    "The time for redemption of the parcels",
)


def _closing_text(chunk: str) -> str:
    """The notice's closing statutory paragraphs, or ''.

    Taken from a MARKER, never from a blind character count.

    DEFECT FIXED 2026-08-09: the previous version used the last 2,600 chars
    (_CONTEXT_TAIL_CHARS) as context. In Lake County's notice that window
    lands INSIDE the parcel list, so every piece was prefixed with 13 real
    parcel records -- nine SWANSTROM mineral interests plus WALDRON,
    LERBAKKEN, MILLER and LEGENDRE. The model saw each of those parcels
    twice, once as "context" and once in a body, which is why 15-token cuts
    returned 28/26/28/22/13 parcels and why it reported duplicate and
    ownerless entries.
    """
    earliest = None
    for marker in _CLOSING_MARKERS:
        i = chunk.find(marker)
        if i != -1 and (earliest is None or i < earliest):
            earliest = i
    if earliest is None:
        return ""
    return chunk[earliest:earliest + _CONTEXT_TAIL_CHARS].strip()


# A record starts with its OWNER line(s), not with the parcel number, so a
# cut placed at a parcel token severs the owner from the parcel. Lake's
# layout makes this visible:
#
#     WALDRON JOSHUA A & HARRINGTON
#     29-5310-09015 SILVER CREEK TOWNSHIP ... $2,988.11
#
# Cutting at 29-5310-09015 leaves WALDRON dangling at the end of the
# previous piece ("owner name listed but no parcel information follows",
# confidence 0.0) and the parcel ownerless at the start of the next
# ("no owner name provided for this parcel", confidence 0.3).
# Digits are allowed: trust names carry dates ("KENDALL JANE E REV TRST AG
# 10-31-05"). A line is rejected as an owner only if it contains a PARCEL
# token, which _record_start checks separately.
_OWNER_LINE_RE = re.compile(r"^[A-Z][A-Z0-9 .,'&+/-]{3,}$")


def _record_start(chunk: str, token_start: int) -> int:
    """Walk back from a parcel token to the start of its owner block.

    Owner lines are ALL-CAPS names on their own line immediately above the
    parcel line, and a record may carry several (joint owners, trusts, FKA
    names). Walks back over consecutive owner-shaped lines and stops at the
    first blank line, which separates records.
    """
    line_start = chunk.rfind("\n", 0, token_start) + 1
    pos = line_start
    while pos > 0:
        prev_end = pos - 1                      # the \n before this line
        prev_start = chunk.rfind("\n", 0, prev_end) + 1
        line = chunk[prev_start:prev_end].strip()
        if not line:
            break                               # blank line = record boundary
        if not _OWNER_LINE_RE.match(line):
            break                               # not an owner line
        if _PARCEL_TOKEN_RE.search(line):
            break                               # previous record's parcel
        pos = prev_start
    return pos


def _split_oversize(chunk: str) -> list[str]:
    """Split one over-long notice into pieces the model can finish.

    Cuts on PARCEL TOKENS rather than lines, because several counties
    publish multi-column pages whose PDF text extraction interleaves the
    columns -- a single line can carry the notice header, an owner name, a
    parcel number and an amount from three different columns:

        CROW WING COUNTY County Auditor COTRONEO, MARY & RP 10161012 $220.26

    Measured on Crow Wing notice 1054514 chunk 4 (26,975 chars, 90 parcel
    tokens): line-start 'RP' = 0, line-start 'PRI' = 0. Line-anchored
    markers could never match, the chunk was returned untouched, and every
    attempt died at stop_reason=max_tokens.

    Each cut is then moved BACK to the start of that record's owner block
    (see _record_start), so no record is severed, and every piece carries
    the notice's opening text plus its CLOSING statutory paragraph (see
    _closing_text) so the deadline stays attached.
    """
    tokens = [m for m in _PARCEL_TOKEN_RE.finditer(chunk)
              if _is_parcel_token(m.group(0))]
    if len(chunk) <= _MAX_CHUNK_CHARS and len(tokens) <= _PARCELS_PER_PIECE:
        return [chunk]
    if len(tokens) <= 1:
        # Nothing to cut on. Return as-is rather than slicing mid-record;
        # an over-long single-parcel chunk is a different problem and will
        # be reported by stop_reason.
        return [chunk]

    head = chunk[:_CONTEXT_HEAD_CHARS].strip()
    tail = _closing_text(chunk)
    context = head + (("\n...\n" + tail) if tail else "")

    # Cut points, each pulled back to its record's owner block.
    cuts = [_record_start(chunk, tokens[i].start())
            for i in range(0, len(tokens), _PARCELS_PER_PIECE)]
    cuts = sorted(set(cuts))

    pieces: list[str] = []
    for n, start in enumerate(cuts):
        end = cuts[n + 1] if n + 1 < len(cuts) else len(chunk)
        body = chunk[start:end].strip()
        if not body:
            continue
        pieces.append(
            "=== NOTICE CONTEXT (dates apply to the parcels below) ===\n"
            + context
            + "\n=== PARCELS ===\n"
            + body
        )
    return pieces or [chunk]


# Month names as they appear in these notices, for verifying an extracted
# date actually occurs in the source text.
_MONTHS = ("January", "February", "March", "April", "May", "June", "July",
           "August", "September", "October", "November", "December")


def _date_appears_in_text(d: Any, text: str) -> bool:
    """Does this date occur VERBATIM in the notice, in any common form?

    THE MOST CONSEQUENTIAL CHECK IN THIS FILE.

    Accepts either a date object or an ISO 'YYYY-MM-DD' STRING.

    DEFECT 2026-08-10: it previously annotated `d: date` -- a name this
    module never imports (line 55 brings in datetime, timedelta and
    timezone only) -- and assumed a date object. `_to_date` returns a
    STRING. Every row carrying an expiry therefore raised
    `AttributeError: 'str' object has no attribute 'month'` and killed
    the entire run, which means THIS CHECK HAS NOT BEEN RUNNING. Python
    3.14 evaluates annotations lazily, so the undefined name never
    surfaced and the file compiled clean.

    Measured 2026-08-10, Jackson notice 972938: the notice states
    "on or before February 28th, 2026" and "the second Monday in May" as a
    CONDITION. The model emitted redemption_expiry=2026-05-11 -- the second
    Monday in May 2026, COMPUTED from the condition. The string
    "May 11, 2026" appears NOWHERE in the document.

    The prompt already says "never compute or adjust it". It computed
    anyway. An instruction cannot be relied on for a field that tells a
    homeowner the day they lose their home, so this verifies mechanically
    against the source and the model cannot talk its way past it.

    Accepts the forms counties actually print:
        May 11, 2026        May 11th, 2026      May 11 2026
        5/11/2026           05/11/2026          2026-05-11
    PDF text extraction also splits words ("Oct ober 31, 2025" is real, from
    Carlton), so the month name is matched with optional internal spaces.
    """
    if not d or not text:
        return False

    if isinstance(d, str):
        try:
            d = datetime.strptime(d.strip(), "%Y-%m-%d").date()
        except ValueError:
            # Not an ISO date. It cannot be verified, so it is not
            # accepted -- the caller nulls it and records why.
            return False

    month = _MONTHS[d.month - 1]
    # Allow a space between any two letters of the month name: column
    # interleaving in these PDFs inserts them.
    loose_month = r"\s*".join(month)
    day = str(d.day)
    yr = str(d.year)

    patterns = (
        rf"{loose_month}\s*{day}(?:st|nd|rd|th)?\s*,?\s*{yr}",
        rf"\b0?{d.month}/0?{d.day}/{yr}\b",
        rf"\b{yr}-0?{d.month}-0?{d.day}\b",
    )
    return any(re.search(p, text, re.IGNORECASE) for p in patterns)


def _parcel_appears_in_text(raw: str, text: str) -> bool:
    """Does this parcel identifier occur VERBATIM in the notice?

    The same defence as _date_appears_in_text, for the same reason:
    measured fabrication, not a hypothetical one.

    Carlton notice 845930, measured 2026-08-10. The county printed
        ML ONE LLC 21-010-5270 City of Moose Lake $928.07
    and the model emitted parcel 21-010-5240 at $1,549.81. The string
    "5240" appears NOWHERE in that document. That identifier then matched
    a REAL Carlton parcel, so a forfeiture deadline and a dollar amount
    were published against a property that never owed them -- worse than
    an unresolvable row, because nothing downstream could tell it apart
    from data.

    The cause is Carlton's PDF text layer, which interleaves adjacent
    columns character by character:
        NELSON-MORRISROE, ROSELLA G (218)2338-343-09-1022580 ...
    ("City Foaf xS:canlon" is "City of Scanlon" interleaved with "Fax:".)
    Handed that, the model produced THREE different readings of one
    parcel number. None is recoverable and none is trustworthy.

    Characters are joined with \\s* so the split-word mangling these PDFs
    produce is tolerated, exactly as the month name is above -- but
    nothing else is. An inserted DIGIT breaks the match, which is the
    whole point: 21-010-5240 must not match 21-010-5270.

    Measured false-rejection cost before this was written: 459 of 463
    staged rows across 16 counties and 7 identifier formats pass. All 4
    failures are Carlton, and all 4 are fabrications.

    Returns True when there is nothing to verify -- a row carrying no
    identifier is handled by the caller, not here.
    """
    if not raw or not text:
        return True
    loose = r"\s*".join(re.escape(ch) for ch in raw if not ch.isspace())
    if not loose:
        return True
    return re.search(loose, text, re.IGNORECASE) is not None


def _extract_redemption_chunk(
    client: Anthropic, notice_text: str
) -> Optional[list[dict[str, Any]]]:
    """Extract EVERY parcel from a tax-redemption document.

    Returns a LIST of parcel dicts keyed like ai.extracted_redemptions,
    or None on any failure. This differs from extract_notice(), which
    returns ONE dict: a mortgage foreclosure notice describes a single
    property, but one redemption document may hold several notices with
    several parcels each (the Le Sueur 2026-04-30 sample: four notices
    of 1, 1, 1 and 6 parcels).

    An EMPTY list is a valid result and is NOT an error -- it means the
    model judged the document not to be a tax redemption notice. The
    caller counts it separately from a failure."""
    text = (notice_text or "").strip()
    if not text:
        return None
    try:
        resp = client.messages.create(
            model=_MODEL,
            # A redemption document can carry dozens of parcels; each is a
            # ~12-field object. 8000 was hit on the first real notice.
            max_tokens=16000,
            temperature=0.0,
            system=_REDEMPTION_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": text}],
        )
    except Exception as e:
        _log(f"  anthropic call failed: {type(e).__name__}: {str(e)[:160]}")
        return None
    parts = [b.text for b in resp.content if getattr(b, "type", None) == "text"]
    cleaned = _strip_fences("".join(parts).strip())
    _log(f"  extraction: sent {len(text)} chars, got {len(cleaned)} chars, "
         f"stop_reason={resp.stop_reason}")
    try:
        parsed = json.loads(cleaned)
    except (json.JSONDecodeError, ValueError):
        # Retry with the first balanced object carved out of the reply --
        # a trailing fence or an explanatory paragraph should not cost us
        # a document that was extracted correctly.
        carved = _extract_json_object(cleaned)
        if carved:
            try:
                parsed = json.loads(carved)
                _log(f"    recovered JSON object from a {len(cleaned)}-char "
                     "reply with trailing content")
            except (json.JSONDecodeError, ValueError):
                carved = None
        if not carved:
            return _log_bad_json(cleaned, resp)
    if not isinstance(parsed, dict):
        return None

    county_raw = _clean_str(parsed.get("county"))
    raw_parcels = parsed.get("parcels")
    if not isinstance(raw_parcels, list):
        _log("  redemption extraction: 'parcels' was not a list; skipping")
        return None

    out: list[dict[str, Any]] = []
    rejected_parcel = 0
    for item in raw_parcels:
        if not isinstance(item, dict):
            continue
        row: dict[str, Any] = {}
        for k in _REDEMPTION_STRING_FIELDS:
            row[k] = _clean_str(item.get(k))
        for k in _REDEMPTION_NUMBER_FIELDS:
            row[k] = _to_number(item.get(k))
        for k in _REDEMPTION_DATE_FIELDS:
            row[k] = _to_date(item.get(k))
        # Set BEFORE the checks below, because the expiry check CAPS it.
        # DEFECT 2026-08-10: this assignment used to sit after that check
        # and overwrote the cap with the model's own figure, so the
        # documented "confidence capped at 0.5 on a rejected date" never
        # actually applied to a single row.
        row["confidence"] = _to_confidence(item.get("confidence"))

        # VERIFY the expiry against the source. A date the notice does not
        # contain is not the county's stated date, whatever the model says.
        # Null it and record why rather than publishing it: the promotion
        # gate requires an expiry, so the row is held for review instead of
        # becoming a forfeiture deadline nobody published.
        if row.get("redemption_expiry") and not _date_appears_in_text(
                row["redemption_expiry"], notice_text):
            bad = str(row["redemption_expiry"])
            row["redemption_expiry"] = None
            note = row.get("extraction_notes") or ""
            row["extraction_notes"] = (
                (note + " | " if note else "")
                + f"REJECTED computed expiry {bad}: that date does not "
                  "appear in the notice text. The notice states conditions "
                  "(e.g. 'the second Monday in May') rather than a calendar "
                  "date, and the model resolved one."
            )
            row["confidence"] = min(row["confidence"], 0.5)
            _log(f"    REJECTED computed expiry {bad} -- not present in the "
                 "notice text")

        # VERIFY the parcel identifier against the source, for the same
        # reason as the expiry above and by the same means.
        #
        # The whole ROW is discarded, not just the identifier, because
        # every field here was read off ONE line of the notice. Where
        # that line is corrupt the amount is corrupt with it: Carlton
        # 21-010-5240 came with $1,549.81 against a printed $928.07. A
        # row keyed on an unverifiable parcel cannot resolve, cannot
        # promote, and cannot be checked by a human against a number the
        # county never printed.
        if row.get("parcel_id_raw") and not _parcel_appears_in_text(
                row["parcel_id_raw"], notice_text):
            rejected_parcel += 1
            _log(f"    REJECTED parcel {row['parcel_id_raw']!r} -- not "
                 "present in the notice text; row discarded (interleaved "
                 "PDF column, the identifier was reconstructed)")
            continue

        row["delinquent_tax_year"] = _to_int(item.get("delinquent_tax_year"))
        # Belt and braces: trust the model's flag, but ALSO honour the
        # phrase if it appears in the notes. A missed DO NOT MAIL is a
        # compliance failure, a false positive costs one postcard.
        dnm = item.get("do_not_mail")
        notes = (row.get("extraction_notes") or "")
        row["do_not_mail"] = bool(dnm) or ("do not mail" in notes.lower())
        row["county_raw"] = county_raw
        row["model"] = _MODEL
        out.append(row)
    if rejected_parcel:
        _log(f"  {rejected_parcel} parcel(s) rejected: identifier not found "
             "verbatim in the notice text")
    return out


def extract_redemption_notice(
    client: Anthropic, notice_text: str
) -> Optional[list[dict[str, Any]]]:
    """Extract every parcel from a redemption DOCUMENT, chunk by chunk.

    Returns the concatenated parcel list, or None only if EVERY chunk
    failed. A partial result is returned rather than discarded: nine good
    notices should not be lost because the tenth was malformed.
    """
    text = (notice_text or "").strip()
    if len(text) < _MIN_REDEMPTION_CHARS:
        _log(f"  notice text is only {len(text)} chars -- that is the site's "
             "capped web stub, not the full notice (the PDF fetch did not "
             "land). Skipping WITHOUT calling the model.")
        return None

    chunks = _split_redemption_document(text)
    if not chunks:
        return None

    # Drop chunks carrying NO parcel token before spending an API call.
    #
    # DEFECT 2026-08-10: the scheduled run produced two extract_failed on
    # notices whose PDFs parsed fine (8,792 and 20,566 chars). The cause was
    # an 88-CHARACTER third chunk -- a runt from the tail of the document,
    # after the last parcel. The model answered correctly with
    # {"county": null, "parcels": []} (an 88-char fragment IS not a tax
    # redemption notice), that counted as a zero-parcel chunk, and with no
    # other chunk succeeding the whole notice was reported as a failure.
    #
    # _MIN_CHUNK_CHARS filters the output of _NOTICE_SPLIT_RE but NOT of
    # _split_oversize, which can emit a trailing piece of any size. A chunk
    # with no parcel identifier in it has nothing to extract by definition.
    usable = []
    dropped_empty = 0
    for c in chunks:
        if any(_is_parcel_token(m.group(0))
               for m in _PARCEL_TOKEN_RE.finditer(c)):
            usable.append(c)
        else:
            dropped_empty += 1
    if dropped_empty:
        _log(f"  dropped {dropped_empty} chunk(s) containing no parcel "
             "identifier (document tail / heading fragment)")
    if not usable:
        _log("  no chunk contains a parcel identifier -- nothing to extract")
        return None
    chunks = usable

    _log(f"  document split into {len(chunks)} notice chunk(s)")

    if _dump_chunks:
        try:
            os.makedirs("debug", exist_ok=True)
            for n, c in enumerate(chunks, 1):
                fn = os.path.join(
                    "debug", f"chunk_{_dump_notice_id}_{n}of{len(chunks)}"
                                f"_{len(c)}chars.txt")
                with open(fn, "w", encoding="utf-8") as fh:
                    fh.write(c)
            _log(f"    chunks written to debug/chunk_{_dump_notice_id}_*.txt")
        except Exception as e:
            _log(f"    chunk dump failed: {type(e).__name__}: {str(e)[:120]}")

    all_parcels: list[dict[str, Any]] = []
    ok = 0
    failed = 0
    for n, chunk in enumerate(chunks, 1):
        got = _extract_redemption_chunk(client, chunk)
        if got is None:
            failed += 1
            _log(f"    chunk {n}/{len(chunks)}: FAILED")
            continue
        ok += 1
        all_parcels.extend(got)
        if got:
            _log(f"    chunk {n}/{len(chunks)}: {len(got)} parcel(s)")
        else:
            # An empty list is a valid answer, not a failure: the model
            # judged this chunk not to be a tax redemption notice. Counted
            # as ok so one such chunk cannot condemn the whole document.
            _log(f"    chunk {n}/{len(chunks)}: no parcels (model judged it "
                 "not a redemption notice)")

    if ok == 0:
        _log(f"  all {failed} chunk(s) failed")
        return None
    if failed:
        _log(f"  {ok} chunk(s) extracted, {failed} failed -- returning "
             f"{len(all_parcels)} parcel(s) from the chunks that worked")
    return all_parcels


# ============================================================
# County resolution
# ============================================================

def _load_county_map(sb) -> dict[str, str]:
    """Map lowercased county NAME -> county_code, read from core.counties.

    Resolved from the DATABASE, never derived from the extracted string.
    The model returns 'LE SUEUR COUNTY, MN'; core.parcels joins on
    'le_sueur'. Deriving one from the other by string munging is the
    class of guess that puts a row under the wrong county."""
    try:
        res = sb.schema("core").table("counties").select(
            "county_code,county_name").eq("state", "MN").execute()
    except Exception as e:
        _log(f"  county map load failed: {type(e).__name__}: {str(e)[:160]}")
        return {}
    out: dict[str, str] = {}
    for r in (res.data or []):
        name = (r.get("county_name") or "").strip().lower()
        code = r.get("county_code")
        if name and code:
            out[name] = code
    return out


def _resolve_county_code(county_raw: Optional[str],
                         county_map: dict[str, str]) -> Optional[str]:
    """'LE SUEUR COUNTY, MN' -> 'le_sueur', via core.counties.

    Returns None when it cannot match. None is CORRECT here: the row
    still stages for review with county_raw intact, and the composite FK
    to core.parcels simply does not engage. A wrong code would attach
    the parcel to another county."""
    if not county_raw:
        return None
    t = county_raw.strip().lower()
    t = re.sub(r",?\s*(mn|minnesota)\s*$", "", t)
    t = re.sub(r"\s+county\s*$", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    if t in county_map:
        return county_map[t]
    # underscore form, e.g. a notice printing 'LE_SUEUR'
    u = t.replace(" ", "_")
    for name, code in county_map.items():
        if code == u or name.replace(" ", "_") == u:
            return code
    return None


# ============================================================
# Notice text isolation (mirrors server _slice_notice_text)
# ============================================================

_NOTICE_START_MARKERS = (
    "THE RIGHT TO VERIFICATION OF THE DEBT",
    "NOTICE IS HEREBY GIVEN",
    "NOTICE OF MORTGAGE FORECLOSURE",
    "Minn. Stat.",
    "YOU ARE NOTIFIED",
    # ADDED 2026-08-09 for tax-redemption notices. Without these a
    # NOTICE OF EXPIRATION OF REDEMPTION returns None from
    # _slice_notice_text() and is logged as "no full notice text found
    # (skipped)" -- a SILENT DROP that reads like a fetch failure.
    # The body reads "You are hereby notified", which does NOT match the
    # uppercase "YOU ARE NOTIFIED" literal above.
    "NOTICE OF EXPIRATION OF REDEMPTION",
    "You are hereby notified",
)

# ------------------------------------------------------------
# Notice classification
# ------------------------------------------------------------
# "redemption" matches BOTH notice types: tax redemption AND mortgage
# foreclosure (which states a six-month redemption period after the
# sheriff's sale). Searching on it returns mostly mortgage foreclosures.
#
# Markers below are taken VERBATIM from real notices, not assumed:
#   tax       - Le Sueur County News 2026-04-30, Crow Wing 2026-06-17
#   mortgage  - Red Lake County Gazette 2026-08/09
# The MCRO lesson: block/marker strings must match the ACTUAL wording.

_TAX_REDEMPTION_MARKERS = (
    "NOTICE OF EXPIRATION OF REDEMPTION",
    "bid in for the state",
    "tax judgment sale",
)

_MORTGAGE_MARKERS = (
    "NOTICE OF MORTGAGE FORECLOSURE",
    "MORTGAGOR",
    "DATE OF MORTGAGE",
)


def classify_notice(notice_text: str) -> str:
    """Return 'tax_redemption', 'mortgage_foreclosure', or 'unknown'.

    Case-insensitive on the phrase markers because publishers vary the
    casing of running text; the ALL-CAPS headings are matched as given.
    Returns 'unknown' rather than guessing when both or neither hit --
    an unknown is skipped and counted, never extracted with the wrong
    prompt."""
    if not notice_text:
        return "unknown"
    hay = notice_text.lower()
    tax = sum(1 for m in _TAX_REDEMPTION_MARKERS if m.lower() in hay)
    mtg = sum(1 for m in _MORTGAGE_MARKERS if m.lower() in hay)
    if tax and tax > mtg:
        return "tax_redemption"
    if mtg and mtg > tax:
        return "mortgage_foreclosure"
    return "unknown"


# Character cap applied when slicing a notice out of the page/PDF text.
#
# 20,000 was sized for MORTGAGE foreclosure notices, which describe ONE
# property. A tax redemption document is a different shape: several notices,
# each listing many parcels. Crow Wing 2026-06-17 hit the cap exactly
# ("PDF text extracted (20000 chars)") and the truncated document could not
# be extracted.
_SLICE_CAP_DEFAULT = 20000
_SLICE_CAP_REDEMPTION = 120000

# Set once at startup from --mode so the shared fetch path slices
# appropriately without threading a parameter through five call sites.
_slice_cap = _SLICE_CAP_DEFAULT

# When true, every chunk is written to debug/ before extraction so a failure
# can be diagnosed from the ACTUAL text rather than guessed at. Set by
# --dump-chunks. _dump_notice_id is set per notice so filenames identify
# which document a chunk came from.
_dump_chunks = False
_dump_notice_id = "unknown"


def _slice_notice_text(full_text: str) -> Optional[str]:
    if not full_text:
        return None
    text = re.sub(r"[ \t\r\f\v]+", " ", full_text)
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text).strip()
    earliest = None
    for marker in _NOTICE_START_MARKERS:
        idx = text.find(marker)
        if idx != -1 and (earliest is None or idx < earliest):
            earliest = idx
    if earliest is None:
        return None
    return text[earliest:][:_slice_cap].strip()


def _canonical_source_url(notice_id: str) -> str:
    """SID-less dedup key / source_url. MUST match the server scraper exactly."""
    return f"{_BASE}/Details.aspx?ID={notice_id}"


# ============================================================
# Supabase
# ============================================================

def _make_supabase(env: dict[str, str]):
    url = env.get("SUPABASE_URL")
    key = env.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        sys.exit("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY (.env next to script).")
    return create_client(url, key)


def _already_staged(sb, source_url: str, mode: str = "foreclosure") -> bool:
    """Has this notice URL already been staged?

    MUST consult the table for the CURRENT mode. Checking
    ai.extracted_foreclosures while running in redemption mode would report
    a redemption notice as already staged because a MORTGAGE notice with the
    same URL was processed by the daily job -- suppressing it silently.
    Observed 2026-08-09: already_staged=1 in a redemption run.
    """
    table = ("extracted_redemptions" if mode == "redemption"
             else "extracted_foreclosures")
    try:
        res = (sb.schema("ai").table(table)
               .select("id").eq("source_url", source_url).limit(1).execute())
        return bool(res.data)
    except Exception as e:
        _log(f"  dedup check failed ({type(e).__name__}); skipping URL to be safe")
        return True


def _store(sb, data: dict[str, Any], source_url: str, notice_text: str) -> Optional[int]:
    row = dict(data)
    row["source_url"] = source_url
    row["source_name"] = "mnpublicnotice"
    row["raw_notice_text"] = notice_text
    row["fetched_at"] = datetime.now(timezone.utc).isoformat()
    row["review_status"] = "pending"
    try:
        res = sb.schema("ai").table("extracted_foreclosures").insert(row).execute()
        return res.data[0]["id"] if res.data else None
    except Exception as e:
        _log(f"  store insert failed: {type(e).__name__}: {str(e)[:200]}")
        return None


def _store_redemptions(sb, parcels: list[dict[str, Any]], source_url: str,
                       notice_text: str, county_map: dict[str, str],
                       published_date: Optional[str],
                       publication: Optional[str]) -> tuple[int, int]:
    """Insert one row per parcel. Returns (stored, skipped_duplicate).

    Duplicates are EXPECTED, not exceptional. Minn. Stat. requires repeat
    publication -- the Le Sueur notice ran 2026-04-30 AND 2026-05-07, and
    Crow Wing published the same parcels through three separate papers on
    one day. Each is a different source_url, so URL-level dedup (which the
    foreclosure path uses) would keep every copy.

    Identity is (county_code, parcel_id_raw, redemption_expiry), enforced
    by the unique index extracted_redemptions_dedup. Rows are inserted ONE
    AT A TIME so a duplicate does not abort the rest of the batch."""
    stored = 0
    dupes = 0
    skipped_null_expiry = 0
    cleared_stale = 0
    # Parcels given a DATED row by THIS call, which `dated_raw` cannot know.
    dated_this_call: set[str] = set()
    skipped_empty = 0
    fetched = datetime.now(timezone.utc).isoformat()

    # A parcel already carrying a DATED row is not improved by a second row
    # with a NULL expiry.
    #
    # DEFECT 2026-08-10: the dedup index is (county_code, parcel_id_raw,
    # redemption_expiry) with NULLS NOT DISTINCT, so (county, raw, NULL) and
    # (county, raw, 2026-05-11) are genuinely DIFFERENT keys and both
    # survive. Jackson republished the same parcels in two notices; the
    # extractor found the expiry in one and not the other, leaving TWELVE
    # parcels with a useless NULL-expiry twin. Those rows can never be
    # promoted (the gate requires an expiry) and never resolve, so they sit
    # in the review queue forever.
    #
    # Checked per notice against the parcels ALREADY IN THE TABLE for this
    # county. It cannot suppress a genuine county-wide no-date notice
    # (Red Lake and Renville publish no calendar date at all, and have no
    # dated rows to be twinned with).
    dated_raw: set[str] = set()
    counties = {row.get("county_raw") for row in parcels}
    codes = {c for c in (_resolve_county_code(cr, county_map)
                         for cr in counties) if c}
    for code in codes:
        try:
            res = (sb.schema("ai").table("extracted_redemptions")
                   .select("parcel_id_raw")
                   .eq("county_code", code)
                   .not_.is_("redemption_expiry", "null")
                   .execute())
            dated_raw.update(r["parcel_id_raw"] for r in (res.data or [])
                             if r.get("parcel_id_raw"))
        except Exception as e:
            _log(f"  dated-parcel lookup failed for {code}: "
                 f"{type(e).__name__}: {str(e)[:120]}")

    for pc in parcels:
        # A parcel with NO identifier AND no amount carries no information.
        # It cannot resolve against core.parcels, cannot promote (the gate
        # needs a parcel and an amount), and cannot be acted on -- it just
        # sits in the review queue. Two such rows appeared per Pine run and
        # were cleared by hand twice.
        if not pc.get("parcel_id_raw") and pc.get("redemption_amount") is None:
            skipped_empty += 1
            continue

        # `dated_raw` is a snapshot taken BEFORE this notice's rows exist,
        # so it cannot see parcels dated EARLIER IN THIS SAME CALL.
        #
        # Pine notice 946720 lists 08.0208.000 and 46.0012.005 TWICE -- once
        # with a 2026-03-31 deadline and once without, because the document
        # contains several internal notices and one of them is condition-
        # only. The dated rows were stored first (ids 2832, 2833) and the
        # NULL rows eight seconds later (2853, 2854), both from the same
        # source_url, because the snapshot never learned about the first
        # two. `dated_this_call` closes that window.
        if (pc.get("redemption_expiry") is None
                and (pc.get("parcel_id_raw") in dated_raw
                     or pc.get("parcel_id_raw") in dated_this_call)):
            skipped_null_expiry += 1
            continue
        row = dict(pc)
        row["source_url"] = source_url
        row["source_name"] = "mnpublicnotice"
        row["raw_notice_text"] = notice_text
        row["fetched_at"] = fetched
        row["review_status"] = "pending"
        row["county_code"] = _resolve_county_code(
            row.get("county_raw"), county_map)
        if published_date:
            row["published_date"] = published_date
        if publication:
            row["publication"] = publication
        try:
            res = (sb.schema("ai").table("extracted_redemptions")
                   .insert(row).execute())
            if res.data:
                stored += 1
                if row.get("redemption_expiry") and row.get("parcel_id_raw"):
                    dated_this_call.add(row["parcel_id_raw"])
                # A DATED row has just landed for this parcel. Any earlier
                # NULL-expiry row for the same parcel is now STALE -- it
                # says "no deadline found" about a parcel whose deadline we
                # now hold, it can never promote (the gate requires an
                # expiry), and it never resolves.
                #
                # The guard above only blocks NULL-after-dated. Pine hit the
                # REVERSE order: a condition-only notice ran first and
                # inserted 8 NULL-expiry rows, then a dated notice inserted
                # rows for the same parcels. Different dedup key
                # (county, raw, NULL) vs (county, raw, date), so both
                # persisted -- 20 twins. Jackson produced 12 the same way.
                # Cleaned by hand twice; this stops it recurring.
                if row.get("redemption_expiry") and row.get("parcel_id_raw"):
                    try:
                        gone = (sb.schema("ai")
                                .table("extracted_redemptions")
                                .delete()
                                .eq("county_code", row.get("county_code"))
                                .eq("parcel_id_raw", row["parcel_id_raw"])
                                .is_("redemption_expiry", "null")
                                .is_("promoted_at", "null")
                                .execute())
                        n = len(gone.data or [])
                        if n:
                            cleared_stale += n
                    except Exception as e:
                        _log("  stale NULL-expiry cleanup failed for "
                             f"{row['parcel_id_raw']}: {type(e).__name__}: "
                             f"{str(e)[:120]}")
        except Exception as e:
            msg = str(e)
            # 23505 = unique_violation against extracted_redemptions_dedup.
            # That is the republication case and is a SUCCESS for dedup, so
            # it is counted, not logged as an error.
            if "23505" in msg or "duplicate key" in msg.lower():
                dupes += 1
                # A dated row that collides is a dated row that ALREADY
                # EXISTS, so it must still suppress a NULL twin arriving
                # later in this same call.
                if row.get("redemption_expiry") and row.get("parcel_id_raw"):
                    dated_this_call.add(row["parcel_id_raw"])
                continue
            _log(f"  redemption insert failed: {type(e).__name__}: {msg[:200]}")

    if skipped_empty:
        _log(f"  skipped {skipped_empty} parcel(s) with no identifier and no "
             "amount (nothing to resolve, promote or act on)")
    if cleared_stale:
        _log(f"  cleared {cleared_stale} stale NULL-expiry row(s) superseded "
             "by a dated row for the same parcel")
    if skipped_null_expiry:
        _log(f"  skipped {skipped_null_expiry} parcel(s) with no expiry that "
             "already have a DATED row (republished notice where the date "
             "was not found this time)")
    return stored, dupes


# ============================================================
# Browser scrape
# ============================================================

_DETAIL_ID_RE = re.compile(r'Details\.aspx\?SID=([A-Za-z0-9]+)&(?:amp;)?ID=(\d+)')


def _maybe_solve_captcha(page, env: dict[str, str]) -> bool:
    """Solve the Cloudflare Turnstile gating a Details/DetailsPrint page, then
    fire the 'View Notice' ASP.NET postback so the notice renders.

    Mechanics confirmed from the live page HTML (July 2026 -- the site migrated
    from Google reCAPTCHA to Cloudflare Turnstile):
      - Widget: <div class="cf-turnstile" data-sitekey="0x4AAAAAADs-..."
        id="recaptcha">, containing hidden input name="cf-turnstile-response".
      - No data-callback / data-action: plain token-into-hidden-field flow.
      - Submit button: name ctl00$ContentPlaceHolder1$PublicNoticeDetailsBody1$
        btnViewNotice, driven by __doPostBack.
    We solve via 2Captcha's turnstile method, write the token into the
    cf-turnstile-response field, then trigger the button's postback via
    __doPostBack with the exact target name (preserves the token through submit).
    """
    try:
        el = page.query_selector("[data-sitekey]")
    except Exception:
        el = None
    sitekey = None
    if el:
        try:
            sitekey = el.get_attribute("data-sitekey")
        except Exception:
            sitekey = None
    if not sitekey:
        try:
            html = page.content()
            m = re.search(r"data-sitekey=['\"]([A-Za-z0-9_-]{20,})['\"]", html)
            if m:
                sitekey = m.group(1)
        except Exception:
            sitekey = None
    if not sitekey:
        return False  # no captcha on this page

    key = env.get("TWOCAPTCHA_API_KEY")
    if not key or TwoCaptcha is None:
        _log("  CAPTCHA present but no 2Captcha key/lib configured; skipping page")
        return False
    try:
        _log("  Turnstile detected; solving via 2Captcha (may take ~15-45s)...")
        solver = TwoCaptcha(key)
        result = solver.turnstile(sitekey=sitekey, url=page.url)
        token = result.get("code") if isinstance(result, dict) else None
        if not token:
            _log("  2Captcha returned no token; skipping page")
            return False

        # Write the token into cf-turnstile-response (create it if missing),
        # then fire the exact postback target for the 'View Notice' button.
        # Doing both in one JS step keeps the token in place at submit time.
        page.evaluate(
            """(tok) => {
                const setTok = () => {
                    let inputs = Array.from(
                        document.getElementsByName('cf-turnstile-response'));
                    if (inputs.length === 0) {
                        const inp = document.createElement('input');
                        inp.type = 'hidden';
                        inp.name = 'cf-turnstile-response';
                        const f = document.forms['aspnetForm']
                                  || document.querySelector('form');
                        (f || document.body).appendChild(inp);
                        inputs = [inp];
                    }
                    inputs.forEach(t => { t.value = tok; });
                };
                setTok();
                // Find the View Notice submit button and its postback target name.
                let target = null;
                document.querySelectorAll("input[type='submit']").forEach(b => {
                    if ((b.value || '').indexOf('View Notice') !== -1) {
                        target = b.getAttribute('name');
                    }
                });
                setTok();
                if (target && typeof __doPostBack === 'function') {
                    __doPostBack(target, '');
                } else {
                    const f = document.forms['aspnetForm']
                              || document.querySelector('form');
                    if (f) f.submit();
                }
            }""",
            token,
        )
        _log("  Turnstile token set; postback fired for 'View Notice'...")
        try:
            page.wait_for_load_state("domcontentloaded", timeout=20000)
        except Exception:
            pass
        page.wait_for_timeout(3000)
        return True
    except Exception as e:
        _log(f"  2Captcha solve failed: {type(e).__name__}: {str(e)[:160]}")
        return False


def _ids_in_html(html: str, seen: set) -> list[str]:
    """Pull distinct Details notice IDs from a results HTML blob."""
    out: list[str] = []
    for m in _DETAIL_ID_RE.finditer(html):
        nid = m.group(2)
        if nid not in seen:
            seen.add(nid)
            out.append(nid)
    return out


def _select_county(page, county_label: str) -> bool:
    """Tick one county checkbox in the Advanced Search filter.

    Uses the element's OWN .click(), which fires the page's onclick handler
    (__doPostBack) so the server records the selection. This is the lesson
    from the date radio: setting .checked from outside changes the DOM but
    NOT __VIEWSTATE, so the server ignored it (measured 2026-08-09 --
    radio_selected=True yet the site still applied its own window).

    The county list lives in a collapsed <div id="countyDiv">, so
    page.check() would refuse to act on an invisible element. Calling
    .click() in page context has no visibility requirement.
    """
    try:
        found = bool(page.evaluate("""(label) => {
            const want = label.trim().toLowerCase();
            const lis = document.querySelectorAll("#countyDiv ul li");
            for (const li of lis) {
                const lab = li.querySelector("label");
                if (!lab) continue;
                if (lab.textContent.trim().toLowerCase() === want) {
                    const cb = li.querySelector("input[type=checkbox]");
                    if (!cb) return false;
                    if (!cb.checked) cb.click();
                    return true;
                }
            }
            return false;
        }""", county_label))
    except Exception as e:
        _log(f"  county select failed ({county_label}): "
             f"{type(e).__name__}: {str(e)[:120]}")
        return False
    if not found:
        _log(f"  county '{county_label}' not present in the site's list")
        return False
    # The click fires an async postback; wait for the panel label to report
    # a selection rather than sleeping a fixed amount.
    try:
        page.wait_for_function(
            """() => {
                const el = document.querySelector(
                    "#ctl00_ContentPlaceHolder1_as1_divCounty .label");
                return el && /selected/i.test(el.textContent);
            }""",
            timeout=20000,
        )
    except Exception:
        _log(f"  WARNING: no 'selected' confirmation after ticking "
             f"{county_label}; the postback may not have registered")
        return False
    return True


def _run_search_collect_ids(page, window_days: int,
                            keyword: str = "foreclosure",
                            county: str | None = None) -> list[str]:
    """Fill the advanced search (keyword, recent date window),
    submit, WAIT for the results grid to actually render, then collect distinct
    notice IDs -- following pagination if there is more than one page.

    The site is ASP.NET WebForms: clicking GO triggers a postback and the
    results render on a (possibly redirected) page. networkidle alone returns
    too early, so we POLL for Details.aspx links to appear before reading."""
    _log("Opening search page...")
    page.goto(_SEARCH_PAGE, wait_until="domcontentloaded", timeout=60000)
    _maybe_solve_captcha(page, _ENV)

    today = datetime.now()
    # d_from/d_to are no longer sent -- the From/To range is not the control
    # the server honours (see the block below). Retained only because other
    # log lines reference the requested span.
    d_from = (today - timedelta(days=window_days)).strftime("%m/%d/%Y")
    d_to = today.strftime("%m/%d/%Y")

    def _fill(selector: str, value: str) -> bool:
        try:
            page.fill(selector, value, timeout=8000)
            return True
        except Exception:
            return False

    if county:
        if _select_county(page, county):
            _log(f"  county filter: {county}")
        else:
            _log(f"  WARNING: county filter for '{county}' did NOT apply; "
                 "this search is STATEWIDE, not scoped. Skipping it rather "
                 "than returning mislabelled results.")
            return []

    filled_kw = (
        _fill("#ContentPlaceHolder1_as1_txtSearch", keyword)
        or _fill("input[name$='txtSearch']", keyword)
    )
    _log(f"  search keyword: {keyword!r}")
    # USE "In the last N MONTHS", NOT the From/To date range.
    #
    # THREE approaches to the From/To range FAILED, in this order:
    #   1. page.check() on #ctl00_ContentPlaceHolder1_as1_rbRange -- times
    #      out. The radio sits inside <div class="list"> under the COLLAPSED
    #      "Date Range" filter group and Playwright will not act on an
    #      invisible element.
    #   2. Setting .checked via page.evaluate -- reports success
    #      (radio_selected=True) and the server IGNORES it. The selected
    #      radio lives in __VIEWSTATE; a client-side DOM change never
    #      reaches the server. Measured: requested 04/11/2026, site applied
    #      6/9/2026.
    #   3. Two full runs at 60 and 120 days returned the IDENTICAL 10 notice
    #      IDs, ~30 minutes and ~36 Turnstile solves for nothing.
    #
    # Setting it BY HAND in a browser then worked, and the page source shows
    # why the months option is the better target:
    #
    #     <input id="..._rbLastNumMonths" ... value="rbLastNumMonths"
    #            checked="checked" />
    #     In the last <input name="...$txtLastNumMonths" value="12" /> months
    #
    # and the criteria banner read "Published Date From: 8/9/2025". One radio
    # plus one integer -- no date formatting, no locale ambiguity, no second
    # field to keep consistent. 12 months is also the site's own maximum for
    # the current search ("Notices for the past 12 months are available in
    # the current search. Use the Archive Search to find notices older than
    # 12 months."), so nothing beyond it was ever reachable this way.
    #
    # The panel is EXPANDED FIRST by clicking the group header, which is
    # what a person does and what the site's own JS expects. Only then is
    # the radio clicked for real, so the postback carries it.
    months = max(1, min(12, -(-window_days // 30)))
    # Approximate start of the requested window, for the read-back check.
    expected_from = today - timedelta(days=months * 31)

    picked_range = False
    try:
        # Expand the "Date Range" filter group. The header is the element
        # the page's own toggle is bound to.
        for sel in ("#ctl00_ContentPlaceHolder1_as1_divDateRange label.header",
                    "#ctl00_ContentPlaceHolder1_as1_divDateRange .header"):
            try:
                page.click(sel, timeout=4000)
                break
            except Exception:
                continue
        page.wait_for_timeout(400)

        # Now a REAL click on a visible radio, not a DOM assignment.
        for sel in ("#ctl00_ContentPlaceHolder1_as1_rbLastNumMonths",
                    "input[name$='dateRange'][value='rbLastNumMonths']"):
            try:
                page.check(sel, timeout=6000)
                picked_range = True
                break
            except Exception:
                continue

        if picked_range:
            for sel in ("#ctl00_ContentPlaceHolder1_as1_txtLastNumMonths",
                        "input[name$='txtLastNumMonths']"):
                try:
                    page.fill(sel, str(months))
                    break
                except Exception:
                    continue
    except Exception as e:
        _log(f"  date-window setup failed: {type(e).__name__}: {str(e)[:120]}")

    if not picked_range:
        _log("  WARNING: could not select the 'In the last N months' radio; "
             "the site will apply its OWN default window and the requested "
             "window_days will be SILENTLY IGNORED.")

    _log(f"  date window requested: last {months} month(s) "
         f"(from --window-days {window_days}, radio_selected={picked_range})")
    if not filled_kw:
        _log("  WARNING: keyword field not found; page layout may have changed.")

    # Click GO.
    clicked = False
    for sel in ("#ContentPlaceHolder1_as1_btnGo", "input[name$='btnGo']", "text=GO"):
        try:
            page.click(sel, timeout=8000)
            clicked = True
            break
        except Exception:
            continue
    if not clicked:
        _log("  WARNING: could not click GO.")

    # POLL for the results grid to render. The postback + optional redirect can
    # take several seconds; we wait until Details.aspx links appear (or give up).
    seen: set = set()
    ids: list[str] = []
    grid_ready = False
    for attempt in range(20):  # ~20 * 1.5s = up to 30s
        try:
            html = page.content()
        except Exception:
            html = ""
        if "Details.aspx?SID=" in html:
            grid_ready = True
            break
        # The "use the Advanced Search Menu" message means results haven't
        # rendered yet (or the submit didn't take) -- keep waiting/retry once.
        page.wait_for_timeout(1500)

    if not grid_ready:
        _log("  Results grid did not render (no Details links). The search may "
             "not have submitted, or there are genuinely no recent notices.")
        # One retry: press Enter in the keyword box to force submit, then poll again.
        try:
            page.focus("#ContentPlaceHolder1_as1_txtSearch")
            page.keyboard.press("Enter")
            for _ in range(15):
                html = page.content()
                if "Details.aspx?SID=" in html:
                    grid_ready = True
                    break
                page.wait_for_timeout(1500)
        except Exception:
            pass

    if not grid_ready:
        return []

    _maybe_solve_captcha(page, _ENV)

    # Collect IDs from page 1.
    _html = page.content()
    _harvest_publications(_html)
    _harvest_published_dates(_html)
    ids.extend(_ids_in_html(_html, seen))

    # Follow pagination: click "next" while it exists, up to a safety cap.
    for _page_num in range(1, 10):  # cap at 10 pages
        next_clicked = False
        for sel in ("text=Next", "a[title='Next']", "input[value='>']",
                    "a:has-text('>')"):
            try:
                el = page.query_selector(sel)
                if el:
                    el.click(timeout=5000)
                    next_clicked = True
                    break
            except Exception:
                continue
        if not next_clicked:
            break
        # Wait for the grid to refresh.
        page.wait_for_timeout(2500)
        before = len(seen)
        _html = page.content()
        _harvest_publications(_html)
        _harvest_published_dates(_html)
        ids.extend(_ids_in_html(_html, seen))
        if len(seen) == before:
            break  # no new IDs -> stop

    # READ BACK what the site ACTUALLY applied, rather than trusting the
    # fill. The results page renders a criteria banner reading e.g.
    #   "Published Date From: 6/9/2026  Published Date To: 12/31/2026"
    # Filling a field and hoping is how the window silently stayed at the
    # site's 60-day default across two runs without a single warning.
    try:
        crit = page.inner_text(".criteria")
        crit = re.sub(r"\s+", " ", crit).strip()
        if crit:
            _log(f"  site applied: {crit[:240]}")
            # Verify against the MONTHS window actually requested, not the
            # old From/To dates. The banner renders m/d/yyyy without zero
            # padding, so compare on the year and month rather than an exact
            # string: "Published Date From: 8/9/2025".
            # \s does NOT match a non-breaking space (U+00A0), which is what
            # the page emits between the label and the value (&nbsp; in the
            # criteria markup). The first version silently failed on every
            # county while the window was in fact applied correctly.
            m = re.search(
                r"Published\s*Date\s*From:[\s\u00a0]*(\d{1,2})/(\d{1,2})/(\d{4})",
                crit)
            if m:
                applied = date(int(m.group(3)), int(m.group(1)), 1)
                wanted = date(expected_from.year, expected_from.month, 1)
                if applied > wanted:
                    _log(f"  WARNING: the site applied a window starting "
                         f"{m.group(0).split(':', 1)[1].strip()}, but "
                         f"{months} month(s) was requested "
                         f"(~{expected_from.isoformat()}). Results below are "
                         "NARROWER than asked for.")
            else:
                _log("  (could not parse the applied start date from the "
                     "criteria banner)")
    except Exception:
        _log("  (could not read the criteria banner to verify the applied "
             "date range)")

    _log(f"Found {len(ids)} notice IDs in the recent window.")
    # TEMPORARY PROBE 2026-08-19 — verifies the grid harvest fires on the
    # LIVE page, not just on the one row captured by hand. Safe to delete
    # once the counts have been read; it only prints.
    _log(f"  [probe] harvested dates: {len(_notice_published)} / "
         f"publications: {len(_notice_publication)}")
    _log(f"  [probe] sample dates: "
         f"{dict(list(_notice_published.items())[:3])}")
    _log(f"  [probe] sample pubs : "
         f"{dict(list(_notice_publication.items())[:2])}")
    return ids


# notice_id -> publication name, harvested from the results grid so a
# publication can be skipped BEFORE paying for its Turnstile solves.
_notice_publication: dict[str, str] = {}

# notice_id -> ISO publication date, harvested from the SAME grid row.
#
# ADDED 2026-08-19. Every one of the 456 rows in ai.extracted_redemptions
# has published_date NULL, and the reason was not a parse failure: the
# call site passed the literal `None`. _store_redemptions has taken a
# published_date parameter since it was written and handles it correctly.
#
# It matters because of the 84 pending redemption rows with no
# redemption_expiry. Those notices state the deadline as "60 days after
# service of notice" and never give the service date -- the model reported
# that accurately and declined to invent one. But SERVICE MUST PRECEDE
# PUBLICATION, so a publication date is a real lower bound on a deadline
# that is otherwise unbounded. Not the exact date; a floor. The same
# distinction signals.tax_delinquency_status already makes with
# forfeiture_basis: record how a date was derived rather than pretending
# it was published.
#
# The grid row (captured live 2026-08-19) reads:
#     <input ... hdnPKValue ... value="1071273">
#     ...
#     <div class="left"><strong>Star Tribune (Minneapolis)</strong><br>
#     Tuesday, August 18, 2026</div>
# so the date sits immediately after the publication, inside the same
# span _harvest_publications already traverses.
_notice_published: dict[str, str] = {}

# Publications observed publishing SCANNED (image-only) PDFs this run.
#
# Measured 2026-08-09, Crow Wing: Pineandlakes Echo Journal and Brainerd
# Dispatch return 9 MB / 17 MB single-page PDFs with NO text layer, while
# Crosby-Ironton Courier publishes the SAME notices as real text (45,482
# chars, extracted cleanly). Minn. Stat. republication across several papers
# is what makes this recoverable -- we only need one text version, and the
# dedup index collapses the rest.
#
# Each scanned notice costs ~3 Turnstile solves and ~90s for nothing.
_scanned_publications: set[str] = set()

# Scanned PDFs identified by what is KNOWN AT DOWNLOAD TIME, not by parsing
# the results grid.
#
# The publication-name harvest proved fragile: it mapped only 2 of 10 notice
# IDs (measured 2026-08-09, scanned_skipped=0), because each grid row repeats
# the id in both an onclick and a hidden field and the layout varies.
#
# URL and byte size need no grid parsing and cannot drift. Crow Wing serves
# the SAME physical newspaper page for several notice ids -- exactly two
# distinct files appeared across eight notices, 9,249,723 and 17,599,660
# bytes. Caching either identifier skips the repeat on sight.
_scanned_pdf_urls: set[str] = set()
_scanned_pdf_sizes: set[int] = set()


def _harvest_publications(html: str) -> None:
    """Map notice id -> publication from the search results grid.

    Each result row carries the id in an onclick 'Details.aspx?...&ID=<n>'
    and the publication in a <strong> immediately after, e.g.
        <strong>Crosby-Ironton Courier, Inc.</strong>
    """
    try:
        # Anchor on hdnPKValue, which carries the notice id and sits
        # immediately before the row's publication block. The earlier
        # pattern anchored on the onclick Details.aspx link and mapped only
        # 2 of 10 ids, because each row repeats the id in BOTH places and
        # the distance to <strong> varies with the row's markup.
        for m in re.finditer(
            r'hdnPKValue[^>]*?value="(\d+)".{0,1500}?<strong>(.*?)</strong>',
            html, re.I | re.S,
        ):
            nid = m.group(1)
            pub = re.sub(r"<[^>]+>", "", m.group(2))
            pub = re.sub(r"\s+", " ", pub).strip()
            if nid and pub and nid not in _notice_publication:
                _notice_publication[nid] = pub
    except Exception:
        pass


def _harvest_published_dates(html: str) -> None:
    """Map notice id -> ISO publication date from the search results grid.

    A SEPARATE regex from _harvest_publications, not a widened one, and
    deliberately so. The comment on that function records an earlier grid
    parse that mapped only 2 of 10 ids because "each grid row repeats the
    id in BOTH places and the distance to <strong> varies with the row's
    markup". Folding the date into that pattern would make a row without a
    date lose its publication too. Two passes cost one extra regex scan of
    HTML already in memory, and a date miss cannot break the publication
    map.

    The day name is optional in the pattern ("Tuesday, August 18, 2026"
    observed live, but a row rendering only "August 18, 2026" still
    matches), and an unparseable date is skipped rather than stored as a
    string -- a bad date on a redemption deadline is worse than no date.
    """
    try:
        for m in re.finditer(
            r'hdnPKValue[^>]*?value="(\d+)'
            r'".{0,1500}?</strong>\s*<br\s*/?>\s*'
            r'(?:[A-Za-z]+,\s*)?([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})',
            html, re.I | re.S,
        ):
            nid, mon, day, yr = m.groups()
            if not nid or nid in _notice_published:
                continue
            try:
                d = datetime.strptime(f"{mon} {day} {yr}", "%B %d %Y").date()
            except ValueError:
                continue
            _notice_published[nid] = d.isoformat()
    except Exception:
        pass


def _current_sid(page) -> Optional[str]:
    """Extract the ASP.NET session SID from the current URL, e.g.
    .../(S(<sid>))/Details.aspx... -> <sid>. None if not present."""
    try:
        m = re.search(r"/\(S\(([A-Za-z0-9]+)\)\)/", page.url)
        return m.group(1) if m else None
    except Exception:
        return None


def _read_notice_text(page) -> str:
    """Return the best-available notice text from the page: prefer the
    #right_content.notice div (where this site renders the unlocked notice),
    fall back to full body text."""
    for sel in ("#right_content", "div.notice", "#ctl00_ContentPlaceHolder1_pnlNotice"):
        try:
            el = page.query_selector(sel)
            if el:
                t = el.inner_text()
                if t and len(t.strip()) > 200:
                    return t
        except Exception:
            continue
    try:
        return page.inner_text("body")
    except Exception:
        return page.content()


def _get_pdf_url(page) -> Optional[str]:
    """After the captcha is accepted, the download anchor (id ...lnkDownload)
    has its href populated with the complete-notice PDF URL. Confirmed real
    format (relative, no (S(sid)) prefix):
      href="PDFDocument.aspx?SID=<sid+digits>&FileName=<file>.pdf"
    A populated href is ALSO the signal the captcha was accepted. Poll for it
    using query_selector/get_attribute (which return None instead of throwing).
    Returns an absolute URL (resolved against the page's own URL) or None."""

    def _resolve(href: str) -> str:
        href = href.replace("&amp;", "&").strip()
        if href.startswith("http"):
            return href
        if href.startswith("/"):
            return f"{_BASE}{href}"
        # Relative (e.g. "PDFDocument.aspx?...") -> resolve against the directory
        # of the CURRENT page URL, which already carries the (S(sid)) segment.
        cur = page.url
        base_dir = cur.rsplit("/", 1)[0]  # strip the last path segment
        return f"{base_dir}/{href.lstrip('/')}"

    for _ in range(12):  # up to ~18s for the postback to populate the anchor
        # Path A: read the anchor's href directly (non-throwing).
        href = None
        try:
            a = page.query_selector("a[id$='lnkDownload']")
            if a:
                href = a.get_attribute("href")
        except Exception:
            href = None
        if href and "PDFDocument" in href:
            return _resolve(href)

        # Path B: scan the raw HTML for the PDFDocument link (covers cases where
        # the element read misses but the href is present in the markup).
        try:
            html = page.content()
        except Exception:
            html = ""
        m = re.search(r'href=["\']([^"\']*PDFDocument\.aspx\?[^"\']+)["\']', html, re.I)
        if not m:
            m = re.search(r'([A-Za-z0-9_./()-]*PDFDocument\.aspx\?[^"\'\s<>]+)', html, re.I)
        if m:
            return _resolve(m.group(1))

        page.wait_for_timeout(1500)
    return None


def _pdf_text(context, pdf_url: str) -> Optional[str]:
    """Download the complete-notice PDF in the authenticated browser session and
    extract its text with pdfplumber. Returns the full text or None."""
    if pdfplumber is None:
        _log("  pdfplumber not installed; cannot read PDF (pip install pdfplumber)")
        return None
    if pdf_url in _scanned_pdf_urls:
        _log("  PDF URL already known to have NO text layer -- skipping the "
             "download entirely.")
        return None
    try:
        resp = context.request.get(pdf_url, timeout=45000)
        if not resp.ok:
            _log(f"  PDF download HTTP {resp.status}")
            return None
        body = resp.body()
    except Exception as e:
        _log(f"  PDF download failed: {type(e).__name__}: {str(e)[:120]}")
        return None

    # Say what actually arrived. A silent failure here cost three runs:
    # the download succeeds, pdfplumber opens the file, and the old
    # `return text or None` returned None with NO log line at all, so the
    # caller fell back to the site's 432-char web stub and the real cause
    # was invisible.
    ctype = ""
    try:
        ctype = (resp.headers or {}).get("content-type", "")
    except Exception:
        pass
    _log(f"  PDF response: {len(body)} bytes, content-type={ctype!r}")
    if len(body) in _scanned_pdf_sizes:
        _log("  same byte size as a PDF already found to have NO text layer "
             "-- the site serves one scanned newspaper page for several "
             "notices. Not re-parsing.")
        _scanned_pdf_urls.add(pdf_url)
        return None
    if not body[:5].startswith(b"%PDF"):
        head = body[:200].decode("utf-8", "replace").replace("\n", " ")
        _log(f"  NOT a PDF (missing %PDF magic). First 200 bytes: {head}")
        return None
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tf:
            tf.write(body)
            tmp_path = tf.name
        parts = []
        pages_with_text = 0
        n_pages = 0
        with pdfplumber.open(tmp_path) as pdf:
            n_pages = len(pdf.pages)
            for pg in pdf.pages:
                t = pg.extract_text() or ""
                if t:
                    pages_with_text += 1
                    parts.append(t)
        text = "\n".join(parts).strip()
        if not text:
            # NEVER return silently here. An image-only PDF is a
            # SCANNED publication with no text layer -- a completely
            # different problem from a failed download, and the two were
            # indistinguishable while this returned a bare None.
            _scanned_pdf_urls.add(pdf_url)
            _scanned_pdf_sizes.add(len(body))
            pub = _notice_publication.get(_dump_notice_id)
            if pub:
                _scanned_publications.add(pub)
                _log(f"  PDF opened OK ({n_pages} page(s)) but contains NO "
                     f"TEXT LAYER -- {pub!r} publishes SCANNED images. "
                     "Skipping further notices from it this run; another "
                     "paper publishes the same notice as real text.")
            else:
                _log(f"  PDF opened OK ({n_pages} page(s)) but contains NO "
                     "TEXT LAYER -- this is a SCANNED publication. Another "
                     "newspaper usually publishes the same notice as real "
                     "text; dedup will collapse them.")
            return None
        _log(f"  PDF parsed: {n_pages} page(s), {pages_with_text} with text, "
             f"{len(text)} chars")
        return text
    except Exception as e:
        _log(f"  PDF parse failed: {type(e).__name__}: {str(e)[:120]}")
        return None
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass


def _fetch_full_notice(page, notice_id: str, context=None) -> Optional[str]:
    """Return the full notice text. Strategy:
      1. Open Details.aspx; if Turnstile-gated, solve it in place (the postback
         reveals the notice AND populates the PDF download link).
      2. PREFER the complete-notice PDF: read the populated lnkDownload href and
         download+parse the PDF (full text even for the ~1000-char-capped
         notices). This is the most reliable full-text source.
      3. Fall back to the on-page HTML text (and DetailsPrint) if no PDF.
    """
    url = f"{_BASE}/Details.aspx?ID={notice_id}"
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=45000)
    except Exception as e:
        _log(f"  notice {notice_id}: navigation failed ({type(e).__name__})")
        return None

    def _gated() -> bool:
        try:
            b = page.inner_text("body")
        except Exception:
            b = ""
        return ("complete the reCAPTCHA" in b) or ("Verifying you are human" in b) \
            or bool(page.query_selector("[data-sitekey]"))

    def _try_pdf() -> Optional[str]:
        """Try to grab + parse the complete-notice PDF from the CURRENT page
        state. Returns full text or None. Safe to call repeatedly."""
        if context is None:
            return None
        pdf_url = _get_pdf_url(page)
        if not pdf_url:
            return None
        _log("  found PDF link; downloading complete notice...")
        raw = _pdf_text(context, pdf_url)
        if not raw:
            return None
        sliced = _slice_notice_text(raw)
        best = sliced if (sliced and len(sliced) > 200) else raw
        if best and len(best) > 200:
            _log(f"  PDF text extracted ({len(best)} chars)")
            return best
        return None

    # Solve on Details.aspx if gated, then poll for the notice to appear.
    if _gated():
        _maybe_solve_captcha(page, _ENV)
        for _ in range(10):
            if not _gated():
                break
            page.wait_for_timeout(1500)

    # PRIMARY: the complete-notice PDF (also confirms the captcha was accepted).
    pdf_text_result = _try_pdf()
    if pdf_text_result:
        return pdf_text_result

    sid = _current_sid(page)

    # FALLBACK 1: on-page HTML text.
    sliced = _slice_notice_text(_read_notice_text(page))

    # FALLBACK 2: if only the capped stub (or nothing), try the print view.
    if (not sliced) or len(sliced) < 900:
        print_url = (f"{_BASE}/(S({sid}))/DetailsPrint.aspx?SID={sid}&ID={notice_id}"
                     if sid else f"{_BASE}/DetailsPrint.aspx?ID={notice_id}")
        try:
            page.goto(print_url, wait_until="domcontentloaded", timeout=45000)
            if _gated():
                _maybe_solve_captcha(page, _ENV)
                for _ in range(10):
                    if not _gated():
                        break
                    page.wait_for_timeout(1500)
            # The PDF link may now be populated on this print page -- try again.
            pdf_text_result = _try_pdf()
            if pdf_text_result:
                return pdf_text_result
            full = _slice_notice_text(_read_notice_text(page))
            if full and len(full) > (len(sliced) if sliced else 0):
                sliced = full
        except Exception:
            pass

    # LAST CHANCE: the PDF link populates a bit late on some notices. Before
    # falling back to the capped stub, navigate back to Details and try the PDF
    # one more time on the now-cleared session.
    if (not sliced) or len(sliced) < 900:
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=45000)
            if _gated():
                _maybe_solve_captcha(page, _ENV)
                for _ in range(10):
                    if not _gated():
                        break
                    page.wait_for_timeout(1500)
            pdf_text_result = _try_pdf()
            if pdf_text_result:
                return pdf_text_result
            full = _slice_notice_text(_read_notice_text(page))
            if full and len(full) > (len(sliced) if sliced else 0):
                sliced = full
        except Exception:
            pass

    if sliced and len(sliced) > 200:
        return sliced

    # DIAGNOSTIC dump on failure.
    try:
        dbg_dir = _HERE / "debug"
        dbg_dir.mkdir(exist_ok=True)
        page.screenshot(path=str(dbg_dir / f"notice_{notice_id}.png"), full_page=True)
        (dbg_dir / f"notice_{notice_id}.html").write_text(page.content(), encoding="utf-8")
        _log(f"  DEBUG notice {notice_id}: no full text; saved debug/notice_{notice_id}.*")
    except Exception:
        pass
    return None


# ============================================================
# Alerting (silent-failure guard)
# ============================================================

# Below this many NEW notices attempted, a run that stores nothing is
# reported PARTIAL rather than HARD. Three, because one bad LLM
# response is a document problem and three in a row is a site
# problem -- and because the 2026-08-18 12:12 run had exactly one.
_HARD_FAILURE_MIN_ATTEMPTS = 3


def _maybe_alert(env: dict[str, str], stats: dict[str, int]) -> None:
    """Email an alert via Resend when a run looks unhealthy, so a silent
    failure (e.g. a captcha migration that makes every notice fail) surfaces
    the same day instead of sitting unnoticed.

    Alert conditions:
      HARD  -- ids_found > 0 but stored == 0  (found notices, saved none).
      SOFT  -- no_text > 0                     (some notices failed to yield text).
    Either condition sends one email summarizing the RESULT counts.

    Fails safe: any error here is logged and swallowed -- alerting must never
    crash or block the scraper. If RESEND_API_KEY is absent, it logs a loud
    local ALERT line instead so the signal still appears in the run output.
    """
    ids = stats.get("ids", 0)
    stored = stats.get("stored", 0)
    no_text = stats.get("no_text", 0)
    mode = stats.get("_mode", "foreclosure")

    if mode == "redemption":
        # `ids > 0 and stored == 0` is WRONG here and would fire on a
        # healthy run. The search keyword 'expiration redemption' also
        # matches MORTGAGE foreclosure notices (they state a six-month
        # redemption period), and in a typical window MOST hits are
        # mortgages. Rejecting them is the classifier SUCCEEDING, so a
        # window with no tax notices legitimately stores nothing.
        #
        # Measured 2026-08-09: 3 notices -> 2 mortgage, 1 session failure,
        # 0 stored. The old rule emailed a HARD FAILURE for that.
        #
        # Real failures in this mode:
        #   - tax notices found but NOTHING extracted  -> extraction broke
        #   - parcels extracted but nothing stored/deduped -> write broke
        tax_notices = stats.get("tax_notices", 0)
        parcels = stats.get("parcels", 0)
        dupes = stats.get("dupes", 0)
        hard = (
            (tax_notices > 0 and parcels == 0)
            or (parcels > 0 and stored == 0 and dupes == 0)
        )
        # Not used in this mode -- the redemption rule already reasons about
        # tax_notices/parcels/dupes rather than raw counts. Bound so the
        # shared `soft` line below cannot raise NameError.
        thin_miss = False
    else:
        # FIXED 2026-08-19. This was `ids > 0 and stored == 0`, which is the
        # exact rule the redemption branch above was corrected away from on
        # 2026-08-09 -- and for the same reason, one mode later.
        #
        # ids counts notices FOUND, and in a healthy window most of them are
        # ones we already have: the search returns the last N days and a
        # foreclosure notice republishes for six statutory weeks, so a feed
        # that is fully caught up finds ten and stores none. That is the
        # dedup SUCCEEDING, and it emailed HARD FAILURE for it.
        #
        # TWO CHANGES, and the second is the one that matters.
        #
        # 1. Gate on new_attempted, not ids. A run with nothing new to do
        #    cannot have failed to do it.
        #
        # 2. Gate on the FAILURE RATE, not the count. Measured on the
        #    2026-08-18 12:12 run: ids=10, already_staged=9,
        #    new_attempted=1, stored=0, extract_failed=1 -- ONE new notice
        #    whose extraction returned unparseable JSON. Under
        #    `new_attempted > 0 and stored == 0` that is still a HARD
        #    FAILURE, at the same volume as "twelve new notices and the site
        #    is down". It is not the same event. A site change or a captcha
        #    break fails EVERYTHING; one malformed LLM response fails one
        #    document.
        #
        #    So a hard failure needs a meaningful attempt behind it. Below
        #    the floor the run is reported PARTIAL, which still emails --
        #    nothing goes silent that used to alarm, it is only ranked
        #    honestly.
        #
        # The 2026-08-17 07:44 run -- ids=12, already_staged=2,
        # new_attempted=9, stored=0 -- still fires HARD, correctly: nine
        # notices were new and none landed.
        new_attempted = stats.get("new", 0)
        hard = new_attempted >= _HARD_FAILURE_MIN_ATTEMPTS and stored == 0
        # A partial: something new was tried, nothing stored, but too few
        # attempts to tell a broken site from one bad document.
        thin_miss = 0 < new_attempted < _HARD_FAILURE_MIN_ATTEMPTS and stored == 0
    soft = no_text > 0 or thin_miss
    if not (hard or soft):
        return  # healthy run, nothing to do

    severity = "HARD FAILURE" if hard else "PARTIAL FAILURE"
    summary = (
        f"[{severity}] govire mnpn scraper\n\n"
        # new_attempted ADDED 2026-08-19. Without it the counters do not
        # reconcile and the email cannot be diagnosed from the email: the
        # 08-17 alert read ids=12 already_staged=2 no_text=1
        # extract_failed=0 stored=0, which accounts for 3 of 12 and leaves
        # nine unexplained. new_attempted is the number that closes the gap,
        # and it is now also the number the severity rule gates on.
        f"ids_found={ids}  already_staged={stats.get('already', 0)}  "
        f"new_attempted={stats.get('new', 0)}  stored={stored}  "
        f"no_text={no_text}  "
        f"extract_failed={stats.get('extract_fail', 0)}\n\n"
        + ("Attempted several NEW notices and stored NONE of them -- "
           "likely a site/captcha change blocking extraction. Check the run "
           "log for the failure mode.\n"
           if hard else
           "Some notices produced no text, or a small number of new "
           "notices were attempted and none stored (one bad extraction "
           "reads like this). Check the run log.\n")
        + f"\nRun finished {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} local."
    )

    # Always emit a loud local line so the signal is in the run output too.
    _log("!" * 60)
    _log(f"ALERT  {severity}: ids={ids} new_attempted={stats.get('new', 0)} "
         f"stored={stored} no_text={no_text}")
    _log("!" * 60)

    api_key = env.get("RESEND_API_KEY")
    to_addr = env.get("ALERT_EMAIL_TO")
    from_addr = env.get("ALERT_EMAIL_FROM")
    if not api_key:
        _log("  (no RESEND_API_KEY in .env; alert logged locally only)")
        return
    if not to_addr or not from_addr:
        _log("  (RESEND_API_KEY set but ALERT_EMAIL_TO/ALERT_EMAIL_FROM missing; "
             "alert logged locally only)")
        return

    try:
        import requests
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "from": from_addr,
                "to": [to_addr],
                "subject": f"[{severity}] govire mnpn scraper -- "
                           f"stored={stored}/ids={ids}",
                "text": summary,
            },
            timeout=20,
        )
        if 200 <= resp.status_code < 300:
            _log(f"  alert email sent via Resend to {to_addr}")
        else:
            _log(f"  Resend alert failed: HTTP {resp.status_code} "
                 f"{resp.text[:160]}")
    except Exception as e:
        _log(f"  Resend alert error: {type(e).__name__}: {str(e)[:160]}")


# ============================================================
# Main
# ============================================================

_ENV: dict[str, str] = {}


def main() -> int:
    global _ENV
    ap = argparse.ArgumentParser(description="mnpublicnotice full-notice scraper")
    ap.add_argument("--max", type=int, default=50, help="max NEW notices to process")
    ap.add_argument("--window-days", type=int, default=14, help="recent date window")
    ap.add_argument("--headless", action="store_true", help="run browser headless")
    ap.add_argument(
        "--mode",
        choices=["foreclosure", "redemption"],
        default="foreclosure",
        help="which notice type to collect. 'foreclosure' is the original "
             "behaviour and is UNCHANGED (default, so the existing scheduled "
             "task keeps working untouched). 'redemption' collects Minn. "
             "Stat. 281 NOTICE OF EXPIRATION OF REDEMPTION into "
             "ai.extracted_redemptions.",
    )
    ap.add_argument(
        "--dump-chunks",
        action="store_true",
        help="write every redemption chunk to debug/ before extraction, so a "
             "chunk that fails can be read rather than guessed at.",
    )
    ap.add_argument(
        "--counties",
        default=None,
        help="comma-separated county names to search ONE AT A TIME, e.g. "
             "'Crow Wing,Le Sueur,Otter Tail'. Each county is a separate "
             "search from a fresh page load. Use 'all' to sweep every county "
             "in core.counties. Redemption mode only; ignored otherwise.",
    )
    ap.add_argument(
        "--keyword",
        default=None,
        help="override the search keyword. Defaults to 'foreclosure' in "
             "foreclosure mode and 'expiration redemption' in redemption mode.",
    )
    args = ap.parse_args()

    _ENV = _load_env()
    anth_key = _ENV.get("ANTHROPIC_API_KEY")
    if not anth_key:
        sys.exit("Missing ANTHROPIC_API_KEY (.env next to script).")
    anthropic_client = Anthropic(api_key=anth_key)
    sb = _make_supabase(_ENV)

    _log("=" * 60)
    keyword = args.keyword or (
        "expiration redemption" if args.mode == "redemption" else "foreclosure"
    )
    _log(f"RUN START  mode={args.mode} keyword={keyword!r} max={args.max} "
         f"window_days={args.window_days} headless={args.headless}")

    stats = {"ids": 0, "already": 0, "new": 0, "stored": 0, "no_text": 0, "extract_fail": 0}
    # read by _maybe_alert, which applies a DIFFERENT health rule per mode
    stats["_mode"] = args.mode

    global _slice_cap
    _slice_cap = (_SLICE_CAP_REDEMPTION if args.mode == "redemption"
                  else _SLICE_CAP_DEFAULT)
    _log(f"notice slice cap: {_slice_cap} chars")

    global _dump_chunks
    _dump_chunks = bool(args.dump_chunks)
    if _dump_chunks:
        _log("chunk dump ENABLED -> debug/chunk_<noticeid>_<n>of<N>_<len>.txt")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=args.headless)
        context = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/125.0.0.0 Safari/537.36"),
            viewport={"width": 1366, "height": 900},
        )
        page = context.new_page()
        if stealth_sync is not None:
            try:
                stealth_sync(page)
            except Exception:
                pass

        county_map: dict[str, str] = {}
        if args.mode == "redemption":
            county_map = _load_county_map(sb)
            _log(f"county map loaded: {len(county_map)} MN counties")
            if not county_map:
                _log("  WARNING: county map is EMPTY -- every row will stage "
                     "with county_code NULL and the parcel FK will not engage.")
            stats["parcels"] = 0
            stats["dupes"] = 0
            stats["wrong_type"] = 0
            stats["unknown_type"] = 0
            stats["tax_notices"] = 0
            stats["zero_parcels"] = 0

        # ---- Collect notice IDs ----
        #
        # Per-county sweep, not one statewide search.
        #
        # Statewide 'expiration redemption' returns overwhelmingly MORTGAGE
        # foreclosure notices (they state a six-month redemption period):
        # measured 2026-08-09, 19 of 19 fetched notices were mortgages, at
        # two Turnstile solves and ~90s each, all discarded. Meanwhile a
        # county-scoped browser search for Crow Wing returned TEN tax
        # redemption notices the statewide search never surfaced.
        #
        # Each county is searched from a FRESH page load. The site is ASP.NET
        # WebForms and the filter state lives in __VIEWSTATE; re-navigating
        # resets it completely, which is far safer than trying to untick the
        # previous county between iterations.
        county_names: list[str] = []
        if args.mode == "redemption" and args.counties:
            if args.counties.strip().lower() == "all":
                # Title-case the core.counties names for the site's labels
                # ('crow_wing' -> 'Crow Wing'). The site is matched on LABEL
                # TEXT, so this is presentation only -- county_code still
                # comes from _resolve_county_code against core.counties.
                county_names = sorted(
                    n.title() for n in county_map.keys()
                ) if county_map else []
            else:
                county_names = [c.strip() for c in args.counties.split(",")
                                if c.strip()]

        ids: list[str] = []
        if county_names:
            _log(f"county sweep: {len(county_names)} count(y/ies)")
            seen: set[str] = set()
            for cname in county_names:
                _log(f"--- county: {cname} ---")
                try:
                    got = _run_search_collect_ids(
                        page, args.window_days, keyword, cname)
                except Exception as e:
                    _log(f"  county {cname} search failed: "
                         f"{type(e).__name__}: {str(e)[:160]}")
                    continue
                fresh = [i for i in got if i not in seen]
                seen.update(fresh)
                ids.extend(fresh)
                _log(f"  {cname}: {len(got)} id(s), {len(fresh)} new "
                     f"(running total {len(ids)})")
        else:
            ids = _run_search_collect_ids(page, args.window_days, keyword)

        stats["ids"] = len(ids)

        processed_new = 0
        global _dump_notice_id
        for nid in ids:
            if processed_new >= args.max:
                break
            _dump_notice_id = str(nid)
            pub = _notice_publication.get(str(nid))
            if pub and pub in _scanned_publications:
                stats["scanned_skipped"] = stats.get("scanned_skipped", 0) + 1
                _log(f"Notice {nid}: {pub!r} already known to publish scanned "
                     "images this run -- skipped before fetching (saves ~3 "
                     "Turnstile solves)")
                continue

            source_url = _canonical_source_url(nid)
            if _already_staged(sb, source_url, args.mode):
                stats["already"] += 1
                continue
            stats["new"] += 1
            processed_new += 1
            _log(f"Notice {nid}: fetching full text...")
            notice_text = _fetch_full_notice(page, nid, context)
            if not notice_text:
                # Name the known stub rather than logging a generic failure.
                # Seen 2026-08-09 on notice 1060254: the Details page renders
                # its template with EVERY field blank and the message
                # "Sorry, there was a problem loading the search results page"
                # -- the session lost its results state, so there was never a
                # PDF link to find. Three Turnstile solves were spent on it.
                # This is transient and per-notice, NOT a site change.
                try:
                    _body = page.inner_text("body")
                except Exception:
                    _body = ""
                if "problem loading the search results page" in _body:
                    _log(f"  notice {nid}: session lost its search-results "
                         "state (site returned the empty-details stub); "
                         "retryable")
                _log(f"  notice {nid}: no full notice text found (skipped)")
                stats["no_text"] += 1
                continue
            if args.mode == "redemption":
                kind = classify_notice(notice_text)
                if kind == "mortgage_foreclosure":
                    # Expected: 'redemption' also matches mortgage notices,
                    # which state a six-month redemption period. Not an error
                    # for THIS mode -- but the notice is a real foreclosure and
                    # its FULL TEXT is already in hand.
                    #
                    # CHANGED 2026-08-16. This branch used to `continue`, which
                    # threw the notice away. Measured on the 07:38 run: SIX
                    # mortgage foreclosures (mnpublicnotice ids 1060610,
                    # 1060609, 1060608, 1060255, 1060254, 1060253) were
                    # fetched, cost ~12 Turnstile solves at 2Captcha rates,
                    # and were discarded. NONE of the six existed in
                    # ai.extracted_foreclosures afterwards -- verified by
                    # source_url lookup, with the lookup pattern itself proven
                    # against three known-present ids first.
                    #
                    # Handing them over is safe because the two pipelines are
                    # independent by design: _already_staged() is mode-scoped
                    # and _store() writes ai.extracted_foreclosures. The
                    # foreclosure-mode check below is what stops a notice the
                    # daily foreclosure run already staged from being staged
                    # twice.
                    #
                    # No extra fetch and no extra captcha: the text was
                    # obtained for the classification that just rejected it.
                    stats["wrong_type"] += 1
                    if _already_staged(sb, source_url, "foreclosure"):
                        _log(f"  notice {nid}: mortgage foreclosure -- already "
                             "staged by the foreclosure pipeline, skipped")
                        continue
                    fc_data = extract_notice(anthropic_client, notice_text)
                    if fc_data is None:
                        stats["handoff_fail"] = stats.get("handoff_fail", 0) + 1
                        _log(f"  notice {nid}: mortgage foreclosure -- handoff "
                             "extraction failed")
                        continue
                    fc_id = _store(sb, fc_data, source_url, notice_text)
                    if fc_id:
                        stats["handed_off"] = stats.get("handed_off", 0) + 1
                        _log(f"  notice {nid}: mortgage foreclosure -> HANDED "
                             f"OFF to foreclosure pipeline (row {fc_id})")
                    else:
                        stats["handoff_fail"] = stats.get("handoff_fail", 0) + 1
                        _log(f"  notice {nid}: mortgage foreclosure -- handoff "
                             "store returned no row")
                    continue
                if kind == "unknown":
                    stats["unknown_type"] += 1
                    _log(f"  notice {nid}: could not classify -- skipped")
                    continue
                # A source that publishes SCANNED IMAGES yields the site's
                # capped web stub instead of the notice, and that is NOT an
                # extraction failure -- it is a known, handled property of the
                # publisher, already named in the log above.
                #
                # ADDED 2026-08-16. extract_redemption_notice() returns None
                # for BOTH this case and a genuine failure, so both landed in
                # extract_fail while tax_notices was already incremented. The
                # alert rule (tax_notices > 0 AND parcels == 0) then read a
                # correctly-skipped scan as "extraction broke" and emailed a
                # HARD FAILURE. Measured on the 07:38 run: ids=12, every
                # notice accounted for, nothing wrong, alert sent anyway.
                #
                # Testing the length HERE keeps the notice out of tax_notices
                # entirely, so the alert rule means what it says without
                # changing the rule.
                if len(notice_text.strip()) < _MIN_REDEMPTION_CHARS:
                    stats["stub_skipped"] = stats.get("stub_skipped", 0) + 1
                    _log(f"  notice {nid}: only "
                         f"{len(notice_text.strip())} chars -- the site's "
                         "capped web stub, not the full notice (scanned-image "
                         "publisher). Skipped, NOT counted as a failure.")
                    continue
                stats["tax_notices"] += 1
                parcels = extract_redemption_notice(
                    anthropic_client, notice_text)
                if parcels is None:
                    stats["extract_fail"] += 1
                    continue
                if not parcels:
                    stats["zero_parcels"] += 1
                    _log(f"  notice {nid}: classified tax redemption but the "
                         "model returned no parcels -- skipped")
                    continue
                # Was `None, None` (fixed 2026-08-19). Both values were
                # already in hand -- _notice_publication has been populated
                # since the scanned-PDF work, and _notice_published is
                # harvested from the same grid row -- and both were being
                # discarded at the call site. publication is what says WHICH
                # paper ran a notice in a county served by several, which is
                # exactly the Crow Wing case where one publication prints
                # text and two print scans.
                n_ok, n_dupe = _store_redemptions(
                    sb, parcels, source_url, notice_text, county_map,
                    _notice_published.get(str(nid)),
                    _notice_publication.get(str(nid)))
                stats["parcels"] += len(parcels)
                stats["dupes"] += n_dupe
                if n_ok:
                    stats["stored"] += 1
                unresolved = sum(
                    1 for pc in parcels
                    if _resolve_county_code(pc.get("county_raw"),
                                            county_map) is None)
                _log(f"  notice {nid}: {len(parcels)} parcel(s) -> "
                     f"{n_ok} stored, {n_dupe} duplicate, "
                     f"{unresolved} with unresolved county")
            else:
                data = extract_notice(anthropic_client, notice_text)
                if data is None:
                    stats["extract_fail"] += 1
                    continue
                new_id = _store(sb, data, source_url, notice_text)
                if new_id:
                    stats["stored"] += 1
                    conf = data.get("confidence")
                    _log(f"  notice {nid}: stored (row {new_id}, "
                         f"confidence {conf})")
            import time as _t
            _t.sleep(_DETAIL_FETCH_PAUSE)

        context.close()
        browser.close()

    _log("-" * 60)
    _log(f"RESULT  mode={args.mode}  ids_found={stats['ids']}  "
         f"already_staged={stats['already']}  new_attempted={stats['new']}  "
         f"stored={stats['stored']}  no_text={stats['no_text']}  "
         f"extract_failed={stats['extract_fail']}")
    if args.mode == "redemption":
        _log(f"        tax_notices={stats.get('tax_notices', 0)}  "
             f"parcels_extracted={stats.get('parcels', 0)}  "
             f"duplicates={stats.get('dupes', 0)}  "
             f"mortgage_skipped={stats.get('wrong_type', 0)}  "
             f"unclassified={stats.get('unknown_type', 0)}  "
             f"zero_parcels={stats.get('zero_parcels', 0)}  "
             f"scanned_skipped={stats.get('scanned_skipped', 0)}")
        # Counters added 2026-08-16. handed_off is the recovery this run
        # produced for the FORECLOSURE pipeline; stub_skipped is the count
        # that used to masquerade as extract_failed.
        _log(f"        handed_off={stats.get('handed_off', 0)}  "
             f"handoff_failed={stats.get('handoff_fail', 0)}  "
             f"stub_skipped={stats.get('stub_skipped', 0)}")
    _log("RUN END")
    _log("=" * 60)

    _maybe_alert(_ENV, stats)
    return 0


if __name__ == "__main__":
    sys.exit(main())
