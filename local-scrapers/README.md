# Local Scrapers

Scrapers that **run on a local Windows machine**, NOT on Railway. They live here
in version control so they can never be lost (a previous deletion of the only
local copy took the mnpublicnotice pipeline down for a week — see lessons below).

## govire_mnpn_browser.py — mnpublicnotice.com foreclosure notices

### Why it runs locally (not on Railway)
mnpublicnotice.com **blocks Railway's datacenter IP** ("You are not permitted to
view public notices from this computer"). A home/residential IP is NOT blocked.
So this scraper must run from a home machine, not the Railway server.

### What it does
1. Opens mnpublicnotice.com in a headless Playwright browser (home IP).
2. Searches "foreclosure" over a recent window; collects notice IDs.
3. For each NEW notice (dedup by source_url), opens the Details page.
4. Solves the Google reCAPTCHA via 2Captcha, clicks "View Notice".
5. Reads the full notice text (DetailsPrint.aspx when available).
6. Extracts structured fields with Claude (same prompt as the server pipeline).
7. Inserts into Supabase ai.extracted_foreclosures as 'pending' (Notice-review queue).

### Setup on the machine that runs it
1. Folder: `C:\Users\<user>\govire-scrapers\` (NOT inside OneDrive\Desktop — that
   location's sync/cleanup is what deleted the original).
2. Copy `.env.example` to `.env` in that folder, fill in real keys.
3. One-time: `python -m playwright install chromium`
4. Test: `py govire_mnpn_browser.py --max 1`
5. Daily: a Windows Task Scheduler task "govire mnpn daily" runs
   `run_govire_mnpn.bat` at 7:00 AM, which calls
   `py govire_mnpn_browser.py --max 50 --headless` and logs to govire_mnpn_log.txt.

### Dependencies
playwright, playwright-stealth, 2captcha-python, anthropic, supabase, pdfplumber

### Known limitation
Most notices return full text. Some (e.g. certain Mower / St. Louis County notices)
cap the web display at 1,000 chars and only carry the full text in the downloadable
PDF; those store as lower-confidence partial leads. A PDF-download fallback is a
planned future enhancement.
