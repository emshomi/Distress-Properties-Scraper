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


def _strip_fences(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z]*\n?", "", t)
        t = re.sub(r"\n?```$", "", t)
    return t.strip()


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


# ============================================================
# Notice text isolation (mirrors server _slice_notice_text)
# ============================================================

_NOTICE_START_MARKERS = (
    "THE RIGHT TO VERIFICATION OF THE DEBT",
    "NOTICE IS HEREBY GIVEN",
    "NOTICE OF MORTGAGE FORECLOSURE",
    "Minn. Stat.",
    "YOU ARE NOTIFIED",
)


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
    return text[earliest:][:20000].strip()


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


def _already_staged(sb, source_url: str) -> bool:
    try:
        res = (sb.schema("ai").table("extracted_foreclosures")
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


def _run_search_collect_ids(page, window_days: int) -> list[str]:
    """Fill the advanced search (keyword 'foreclosure', recent date window),
    submit, WAIT for the results grid to actually render, then collect distinct
    notice IDs -- following pagination if there is more than one page.

    The site is ASP.NET WebForms: clicking GO triggers a postback and the
    results render on a (possibly redirected) page. networkidle alone returns
    too early, so we POLL for Details.aspx links to appear before reading."""
    _log("Opening search page...")
    page.goto(_SEARCH_PAGE, wait_until="domcontentloaded", timeout=60000)
    _maybe_solve_captcha(page, _ENV)

    today = datetime.now()
    d_from = (today - timedelta(days=window_days)).strftime("%m/%d/%Y")
    d_to = today.strftime("%m/%d/%Y")

    def _fill(selector: str, value: str) -> bool:
        try:
            page.fill(selector, value, timeout=8000)
            return True
        except Exception:
            return False

    filled_kw = (
        _fill("#ContentPlaceHolder1_as1_txtSearch", "foreclosure")
        or _fill("input[name$='txtSearch']", "foreclosure")
    )
    (_fill("#ContentPlaceHolder1_as1_txtDateFrom", d_from)
     or _fill("input[name$='txtDateFrom']", d_from))
    (_fill("#ContentPlaceHolder1_as1_txtDateTo", d_to)
     or _fill("input[name$='txtDateTo']", d_to))
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
    ids.extend(_ids_in_html(page.content(), seen))

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
        ids.extend(_ids_in_html(page.content(), seen))
        if len(seen) == before:
            break  # no new IDs -> stop

    _log(f"Found {len(ids)} notice IDs in the recent window.")
    return ids


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
    try:
        resp = context.request.get(pdf_url, timeout=45000)
        if not resp.ok:
            _log(f"  PDF download HTTP {resp.status}")
            return None
        body = resp.body()
    except Exception as e:
        _log(f"  PDF download failed: {type(e).__name__}: {str(e)[:120]}")
        return None
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tf:
            tf.write(body)
            tmp_path = tf.name
        parts = []
        with pdfplumber.open(tmp_path) as pdf:
            for pg in pdf.pages:
                t = pg.extract_text() or ""
                if t:
                    parts.append(t)
        text = "\n".join(parts).strip()
        return text or None
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

    hard = ids > 0 and stored == 0
    soft = no_text > 0
    if not (hard or soft):
        return  # healthy run, nothing to do

    severity = "HARD FAILURE" if hard else "PARTIAL FAILURE"
    summary = (
        f"[{severity}] govire mnpn scraper\n\n"
        f"ids_found={ids}  stored={stored}  no_text={no_text}  "
        f"extract_failed={stats.get('extract_fail', 0)}  "
        f"already_staged={stats.get('already', 0)}\n\n"
        + ("Found notices but stored NONE -- likely a site/captcha change "
           "blocking extraction. Check the run log for the failure mode.\n"
           if hard else
           "Some notices produced no text -- possible partial site change or "
           "flaky captcha solves. Check the run log.\n")
        + f"\nRun finished {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} local."
    )

    # Always emit a loud local line so the signal is in the run output too.
    _log("!" * 60)
    _log(f"ALERT  {severity}: ids={ids} stored={stored} no_text={no_text}")
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
    args = ap.parse_args()

    _ENV = _load_env()
    anth_key = _ENV.get("ANTHROPIC_API_KEY")
    if not anth_key:
        sys.exit("Missing ANTHROPIC_API_KEY (.env next to script).")
    anthropic_client = Anthropic(api_key=anth_key)
    sb = _make_supabase(_ENV)

    _log("=" * 60)
    _log(f"RUN START  max={args.max} window_days={args.window_days} headless={args.headless}")

    stats = {"ids": 0, "already": 0, "new": 0, "stored": 0, "no_text": 0, "extract_fail": 0}

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

        ids = _run_search_collect_ids(page, args.window_days)
        stats["ids"] = len(ids)

        processed_new = 0
        for nid in ids:
            if processed_new >= args.max:
                break
            source_url = _canonical_source_url(nid)
            if _already_staged(sb, source_url):
                stats["already"] += 1
                continue
            stats["new"] += 1
            processed_new += 1
            _log(f"Notice {nid}: fetching full text...")
            notice_text = _fetch_full_notice(page, nid, context)
            if not notice_text:
                _log(f"  notice {nid}: no full notice text found (skipped)")
                stats["no_text"] += 1
                continue
            data = extract_notice(anthropic_client, notice_text)
            if data is None:
                stats["extract_fail"] += 1
                continue
            new_id = _store(sb, data, source_url, notice_text)
            if new_id:
                stats["stored"] += 1
                conf = data.get("confidence")
                _log(f"  notice {nid}: stored (row {new_id}, confidence {conf})")
            import time as _t
            _t.sleep(_DETAIL_FETCH_PAUSE)

        context.close()
        browser.close()

    _log("-" * 60)
    _log(f"RESULT  ids_found={stats['ids']}  already_staged={stats['already']}  "
         f"new_attempted={stats['new']}  stored={stats['stored']}  "
         f"no_text={stats['no_text']}  extract_failed={stats['extract_fail']}")
    _log("RUN END")
    _log("=" * 60)

    _maybe_alert(_ENV, stats)
    return 0


if __name__ == "__main__":
    sys.exit(main())
