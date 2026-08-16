"""
probe_source.py — can OUR client actually fetch this URL, from where it RUNS?

Run this BEFORE writing a scraper against any new source. Dispatch it from
.github/workflows/probe-source.yml with the candidate URL as the input.

=== WHY THIS EXISTS ===
On 2026-08-16 the Minneapolis VBR scraper was repointed to a Tableau CSV that
had been "verified" by downloading it in Chrome, twice, an hour apart. The
files were byte-identical, which proved the FILE was stable and proved nothing
about whether a Python client could obtain it. The deployed scraper got 403 on
its first scheduled run, and the source it replaced — stale, but producing —
had to be restored.

The error was verifying with the wrong client from the wrong place. A browser
download is not evidence of scraper access. Neither is a curl from a laptop:
Railway and GitHub Actions run from datacenter IP ranges that some sources
block outright (mnpublicnotice does exactly this).

So this probe runs on a GitHub Actions runner — same network class as
production — and tries the fetch the same three ways the fleet can, reporting
what each one gets:

  1. httpx, bare              what most scrapers do today
  2. httpx + browser headers  User-Agent / Accept / Accept-Language / Referer
  3. curl_cffi impersonate    real browser TLS fingerprint (JA3)

Tier 3 matters because some WAFs fingerprint the TLS handshake itself, not the
headers. Against tableau.minneapolismn.gov every header combination failed
from BOTH a residential IP and Railway, including a full Chrome User-Agent and
a prior session request — which is the signature of TLS fingerprinting rather
than a header or IP check.

=== READING THE RESULT ===
  Tier 1 succeeds   → write a normal httpx scraper.
  Only 2 succeeds   → httpx is fine, carry the browser headers.
  Only 3 succeeds   → the source fingerprints TLS. Use curl_cffi. Note that
                      this is a deliberate choice to look like a browser, so
                      check the source's terms before adopting it.
  None succeed      → do NOT write the scraper. The options are Playwright
                      (installed, Chromium ships in the image), a local
                      residential-IP runner (the mnpublicnotice pattern), or
                      asking the publisher for a real feed.

This NEVER fails the workflow on an HTTP error. A 403 is the finding, not a
fault; exiting non-zero would bury it in a red X.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)

# Sent by tier 2 and 3. Referer is derived from the URL's own origin, because
# a mismatched Referer is itself a bot signal on some servers.
_BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": _BROWSER_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
              "text/csv,application/json,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
}

_TIMEOUT = 45.0
# Bytes of the body to show. Enough to see a CSV header row or a JSON opening
# without dumping a 30 MB parcel payload into the Actions log.
_PREVIEW_BYTES = 1200


def _origin(url: str) -> str:
    parts = url.split("/", 3)
    return f"{parts[0]}//{parts[2]}/" if len(parts) >= 3 else url


def _report(tier: str, **kw: Any) -> dict[str, Any]:
    row = {"tier": tier, **kw}
    status = row.get("status")
    ok = isinstance(status, int) and 200 <= status < 300
    mark = "PASS" if ok else "FAIL"
    print(f"\n{'=' * 70}")
    print(f"[{mark}] {tier}")
    print(f"{'=' * 70}")
    for k, v in row.items():
        if k in ("tier", "preview"):
            continue
        print(f"  {k:16} {v}")
    if row.get("preview"):
        print(f"  {'-' * 66}")
        for line in str(row["preview"]).splitlines()[:12]:
            print(f"  | {line[:160]}")
    return row


def _summarise(resp_status: int, content: bytes, headers: Any,
               elapsed: float) -> dict[str, Any]:
    try:
        preview = content[:_PREVIEW_BYTES].decode("utf-8-sig", errors="replace")
    except Exception:
        preview = "<binary>"
    return {
        "status": resp_status,
        "bytes": len(content),
        "content_type": dict(headers).get("content-type")
                        or dict(headers).get("Content-Type"),
        "elapsed_s": round(elapsed, 2),
        "preview": preview,
    }


def probe_httpx_bare(url: str) -> dict[str, Any]:
    """What most scrapers in this repo do today."""
    import httpx
    t0 = time.monotonic()
    try:
        r = httpx.get(url, timeout=_TIMEOUT, follow_redirects=True)
        return _report("1. httpx (bare)",
                       **_summarise(r.status_code, r.content, r.headers,
                                    time.monotonic() - t0))
    except Exception as e:
        return _report("1. httpx (bare)", status=None,
                       error=f"{type(e).__name__}: {e}",
                       elapsed_s=round(time.monotonic() - t0, 2))


def probe_httpx_browser(url: str) -> dict[str, Any]:
    """httpx carrying a browser's headers — but httpx's own TLS fingerprint."""
    import httpx
    headers = dict(_BROWSER_HEADERS)
    headers["Referer"] = _origin(url)
    t0 = time.monotonic()
    try:
        r = httpx.get(url, timeout=_TIMEOUT, follow_redirects=True,
                      headers=headers)
        return _report("2. httpx + browser headers",
                       **_summarise(r.status_code, r.content, r.headers,
                                    time.monotonic() - t0))
    except Exception as e:
        return _report("2. httpx + browser headers", status=None,
                       error=f"{type(e).__name__}: {e}",
                       elapsed_s=round(time.monotonic() - t0, 2))


def probe_curl_cffi(url: str, target: str = "chrome") -> dict[str, Any]:
    """curl_cffi impersonating a real browser, TLS fingerprint included.

    `impersonate` is passed through **kwargs in curl_cffi 0.16; it does not
    appear in the signature, so do not go looking for it there.
    """
    label = f"3. curl_cffi (impersonate={target})"
    t0 = time.monotonic()
    try:
        from curl_cffi import requests as creq
    except ImportError as e:
        return _report(label, status=None,
                       error=f"curl_cffi not installed: {e}")
    try:
        headers = dict(_BROWSER_HEADERS)
        headers["Referer"] = _origin(url)
        r = creq.get(url, timeout=_TIMEOUT, impersonate=target,
                     headers=headers)
        return _report(label,
                       **_summarise(r.status_code, r.content, r.headers,
                                    time.monotonic() - t0))
    except Exception as e:
        return _report(label, status=None,
                       error=f"{type(e).__name__}: {e}",
                       elapsed_s=round(time.monotonic() - t0, 2))


def main() -> int:
    url = (sys.argv[1] if len(sys.argv) > 1 else os.environ.get("PROBE_URL", "")).strip()
    if not url:
        print("FATAL: no URL. Pass it as argv[1] or set PROBE_URL.", flush=True)
        return 2
    if not url.startswith(("http://", "https://")):
        print(f"FATAL: URL must include the scheme: {url!r}", flush=True)
        return 2

    impersonate = os.environ.get("PROBE_IMPERSONATE", "chrome").strip() or "chrome"

    print(f"PROBE  url         : {url}")
    print(f"PROBE  origin      : {_origin(url)}")
    print(f"PROBE  impersonate : {impersonate}")
    print(f"PROBE  runner      : {os.environ.get('RUNNER_OS', 'local')} "
          f"/ {os.environ.get('GITHUB_ACTIONS', 'not-actions')}")

    results = [
        probe_httpx_bare(url),
        probe_httpx_browser(url),
        probe_curl_cffi(url, impersonate),
    ]

    print(f"\n{'=' * 70}")
    print("SUMMARY")
    print(f"{'=' * 70}")
    any_pass = False
    for r in results:
        s = r.get("status")
        ok = isinstance(s, int) and 200 <= s < 300
        any_pass = any_pass or ok
        detail = f"{s}" if s is not None else r.get("error", "error")
        size = f"  {r.get('bytes')} bytes" if r.get("bytes") is not None else ""
        print(f"  {'PASS' if ok else 'FAIL'}  {r['tier']:34} {detail}{size}")

    print()
    if not any_pass:
        print("  VERDICT: no client reached this URL. Do NOT write a scraper")
        print("           against it yet. Consider Playwright (Chromium is in")
        print("           the image), a local residential-IP runner, or asking")
        print("           the publisher to expose a real feed.")
    else:
        print("  VERDICT: at least one client succeeded. Use the LOWEST tier")
        print("           that passed — plain httpx if it works, browser")
        print("           impersonation only when nothing simpler does.")

    # Always 0. A 403 is a finding; a red X would hide it.
    return 0


if __name__ == "__main__":
    sys.exit(main())
