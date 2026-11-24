"""
best_deals_report.py
================================================================================
Weekly "best of the best" investor report, emailed to ALERT_EMAIL_TO only.

Ranks open foreclosure redemption windows by real equity spread (market value
minus what a buyer actually pays) and emails the top N, with the rows a buyer
must check before trusting the number tagged rather than hidden.

INTERNAL ONLY. Property-level detail (address, amounts, dates) is the paid
Standard-tier product and never goes on a public page. This report goes to one
address -- the operator's -- and to nobody else. Do not point ALERT_EMAIL_TO at
a list.

POOL (built and checked as Tasks 233-239, 2026-11-23)
-----------------------------------------------------
  * event_type = sheriff_sale, redemption_days_left >= MIN_DAYS_LEFT (21).
  * event_date <= today. signals.distress_with_parcel computes a countdown for
    SCHEDULED sales too -- 179 of 817 rows on 2026-11-23 were sales that had
    not happened (169 mnpublicnotice, 10 postbulletin_legal). A redemption
    window does not exist until the sale does.
  * One row per parcel (county_slug, eff_parcel_id); hennepin_sheriff wins
    over a newspaper notice of the same sale, then the newest sale date.

WHAT A BUYER PAYS (price_basis column)
  final_bid    hennepin_sheriff raw_data.finalBidAmount
  amount_due   mnpublicnotice / postbulletin_legal / startribune_legal notice
  event_value  everything else (washington_sheriff sale amount)

EXCLUDED FROM THE TOP LIST, AND COUNTED IN THE EMAIL
  * Association liens. Ported verbatim from _is_association_lien in
    src/routes/properties.py (Minn. Stat. 515B.3-116(b): the lien is junior
    to the first mortgage, which survives the sale, so the "spread" is not a
    spread). Positive detection only, as there -- PLUS a ratio guard: buyer
    pays under 5% of value. The dry run (Task 238) put 13 of 25 rows under 5%,
    including 5948 Hermitage Trail and 2469 Jaber Ave, the two cases recorded
    in August.
  * Commercial and industrial property: a $10M building wins on dollars by
    being large. Listed in its own short section.

RANKED BEST FIRST (Task 244) -- see WORK_DAYS_MIN / PAYS_PCT_* below.
  Rank 1 is the best deal. Each row carries a `why` sentence: the numbers,
  then absentee / homestead / distress signals / postponements / an old
  assessment year / intent to redeem. Rows that are not ranked follow in the
  CSV with the reason first. No `source` column.

SELF-CONTAINED, like health_alert.py: Supabase REST with httpx, Resend HTTP.
No app code imported.

ENV
  SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, RESEND_API_KEY,
  ALERT_EMAIL_TO, ALERT_EMAIL_FROM      as health_alert.py
  REPORT_COUNTY   optional county slug, e.g. hennepin (blank = all counties)
  REPORT_TOP_N    default 25
  DRY_RUN         "1" prints the report and sends nothing

Exit 0 on a completed run. Non-zero only if Supabase or Resend is unreachable,
so the Actions run goes red when the report itself is broken.
================================================================================
"""

from __future__ import annotations

import base64
import csv
import html
import io
import os
import re
import sys
from datetime import date, datetime, timezone

import httpx

MIN_DAYS_LEFT = 21
LIEN_RATIO_GUARD = 0.05      # buyer pays < 5% of value -> treated as a lien
VERIFY_RATIO_MAX = 0.10      # 5-10% and no association detected -> "verify"
NON_RESIDENTIAL = {"commercial", "industrial"}

# THE DEFAULT RANKING (Task 244, 2026-11-23). Rank 1 is the best deal; the
# reader never re-sorts. A row is RANKED only if it is workable:
#   * residential, not an association lien, property type or age known
#   * WORK_DAYS_MIN+ days left -- under that there is no time to find the
#     owner, agree terms and close
#   * buyer pays PAYS_PCT_MIN-PAYS_PCT_MAX % of value -- under 10% is mostly
#     liens or data errors, over 60% leaves too little after costs
# then by spread, largest first. Everything else stays in the CSV BELOW the
# ranked rows, unranked, with the reason in the `why` column.
#
# Fitted to buying FROM THE OWNER during the window (Govire Connect). For a
# certificate buyer, low-% windows mostly do not convert: the August read of
# scoring.redemption_rates had bid-under-50% not converting ~72% (n=25).
WORK_DAYS_MIN = 45
PAYS_PCT_MIN = 10.0
PAYS_PCT_MAX = 60.0
PAGE = 1000

# ---- Ported VERBATIM from src/routes/properties.py (2026-11-23). ----------
# Keep in step with that file; see its comments for why each word is there.
_ASSOCIATION_RE = re.compile(
    r"\b("
    r"assoc\w*|homeowners?|home\s?owners?|condominium|condo\b|"
    r"townhome\w*|villas?\b|hoa\b|coa\b"
    r")",
    re.IGNORECASE,
)
_LENDER_RE = re.compile(
    r"national\s+association|\bn\.\s?a\.|\bbank\b|\bmortgage\b|"
    r"\bsavings\b|\bcredit\s+union\b|\bfederal\b|\bfannie\b|\bfreddie\b",
    re.IGNORECASE,
)


def is_association_lien(type_of_sale, mortgagee, title) -> bool:
    if str(type_of_sale or "").strip().lower() == "assessment":
        return True
    if isinstance(mortgagee, str) and _LENDER_RE.search(mortgagee):
        return False
    for v in (mortgagee, title):
        if isinstance(v, str) and _ASSOCIATION_RE.search(v):
            return True
    return False
# --------------------------------------------------------------------------


def _env(name, required=True, default=None):
    val = os.environ.get(name, default)
    if required and not val:
        print(f"FATAL: missing required env var {name}", flush=True)
        sys.exit(2)
    return val


def _num(v):
    """Numeric from a number or a money string like '$123,456.78'."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = re.sub(r"[^0-9.]", "", str(v))
    try:
        return float(s) if s else None
    except ValueError:
        return None


def _flag(v):
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower() if v is not None else ""
    if s in ("true", "t", "y", "yes", "1"):
        return True
    if s in ("false", "f", "n", "no", "0"):
        return False
    return None


def _get_all(base, key, schema, table, params):
    """Page through a REST table/view. PostgREST caps a response at 1,000 rows
    and truncates SILENTLY, so a single read would under-report."""
    headers = {"apikey": key, "Authorization": f"Bearer {key}",
               "Accept-Profile": schema}
    rows, offset = [], 0
    while True:
        p = dict(params, limit=str(PAGE), offset=str(offset))
        try:
            r = httpx.get(f"{base}/rest/v1/{table}", headers=headers,
                          params=p, timeout=60)
        except httpx.HTTPError as e:
            print(f"FATAL: could not reach Supabase: {type(e).__name__}: {e}",
                  flush=True)
            sys.exit(2)
        if r.status_code != 200:
            print(f"FATAL: {schema}.{table} returned {r.status_code}: "
                  f"{r.text[:300]}", flush=True)
            sys.exit(2)
        batch = r.json() or []
        rows.extend(batch)
        if len(batch) < PAGE:
            return rows
        offset += PAGE
        if offset > 100 * PAGE:
            print("WARNING: stopped at 100 pages; pool may be incomplete",
                  flush=True)
            return rows


def fetch_pool(base, key, county):
    params = {
        "select": ("id,source,title,event_date,event_value,emv_total,emv_year,"
                   "year_built,sqft,property_type,county_slug,eff_parcel_id,"
                   "redemption_days_left,postponement_count,raw_data"),
        "event_type": "eq.sheriff_sale",
        "event_date": f"lte.{date.today().isoformat()}",
        "redemption_days_left": f"gte.{MIN_DAYS_LEFT}",
        "order": "id.asc",          # stable paging
    }
    if county:
        params["county_slug"] = f"eq.{county}"
    return _get_all(base, key, "signals", "distress_with_parcel", params)


def fetch_overlay(base, key):
    rows = _get_all(base, key, "signals", "parcel_distress_overlay", {
        "select": "county,parcel_id,distinct_signal_count,signal_families",
        "order": "county.asc,parcel_id.asc",
    })
    out = {}
    for r in rows:
        if r.get("parcel_id"):
            out[((r.get("county") or "").lower(), r["parcel_id"])] = r
    return out


def shape(row, overlay):
    raw = row.get("raw_data") or {}
    detail = raw.get("detail") or {}
    src = row.get("source") or ""

    if src == "washington_sheriff":
        address, city = detail.get("gis_site_address"), detail.get("gis_city")
    elif src == "postbulletin_legal":
        address, city = raw.get("property_address"), raw.get("property_city")
    else:
        address, city = raw.get("address"), raw.get("city")

    if "finalBidAmount" in raw:
        basis, pays = "final_bid", _num(raw.get("finalBidAmount"))
    elif "amount_due" in raw:
        basis, pays = "amount_due", _num(raw.get("amount_due"))
    else:
        basis, pays = "event_value", None
    if pays is None:
        basis, pays = "event_value", _num(row.get("event_value"))

    value = _num(row.get("emv_total"))
    ratio = (pays / value) if (pays is not None and value) else None
    ptype = (row.get("property_type") or "").strip()
    county = (row.get("county_slug") or "").lower()
    ov = overlay.get((county, row.get("eff_parcel_id"))) or {}
    fams = ov.get("signal_families")
    if isinstance(fams, list):
        fams = ", ".join(str(f) for f in fams)

    assoc = is_association_lien(raw.get("typeOfSale"), raw.get("mortgagee"),
                                row.get("title"))
    return {
        "county": county,
        "city": (city or "").strip() or None,
        "address": (address or "").strip() or None,
        "parcel_id": row.get("eff_parcel_id"),
        "source": src,
        "property_type": ptype or None,
        "year_built": row.get("year_built"),
        "sqft": row.get("sqft"),
        "value": value,
        "emv_year": row.get("emv_year"),
        "buyer_pays": pays,
        "price_basis": basis,
        "spread": (value - pays) if (value is not None and pays is not None) else None,
        "pays_pct": round(ratio * 100, 1) if ratio is not None else None,
        "sale_date": row.get("event_date"),
        "days_left": row.get("redemption_days_left"),
        "postponements": row.get("postponement_count") or 0,
        "homestead": detail.get("gis_homestead"),
        "absentee": _flag(detail.get("gis_is_absentee")),
        "intent_filed": _flag(raw.get("noticeOfIntent")),
        "mortgagee": raw.get("mortgagee"),
        "signal_count": ov.get("distinct_signal_count") or 1,
        "signals": fams,
        "assoc_named": assoc,
        "lien": assoc or (ratio is not None and ratio < LIEN_RATIO_GUARD),
        "non_residential": ptype.lower() in NON_RESIDENTIAL,
    }


def dedupe(shaped):
    """One row per parcel: hennepin_sheriff first, then the newest sale."""
    best, no_key = {}, []
    for s in shaped:
        k = (s["county"], s["parcel_id"]) if s["parcel_id"] else None
        if k is None:
            no_key.append(s)
            continue
        cur = best.get(k)
        rank = (s["source"] == "hennepin_sheriff", s["sale_date"] or "")
        if cur is None or rank > (cur["source"] == "hennepin_sheriff",
                                  cur["sale_date"] or ""):
            best[k] = s
    return list(best.values()) + no_key


def _short_money(v):
    if v is None:
        return "—"
    return f"${v/1e6:.2f}M" if abs(v) >= 1e6 else f"${v/1e3:,.0f}K"


def exclusions(s):
    """Why a scored row is NOT ranked. Empty list = ranked."""
    out = []
    if s["lien"]:
        out.append("association lien — the first mortgage survives the sale, "
                   "so this spread is not real")
        return out
    if s["non_residential"]:
        out.append("commercial/industrial — different buyer; listed separately")
        return out
    pct = s["pays_pct"]
    if pct is not None and pct < PAYS_PCT_MIN:
        out.append(f"pays only {pct}% of value — check for an unpublished "
                   f"senior lien before trusting it")
    if pct is not None and pct > PAYS_PCT_MAX:
        out.append(f"pays {pct}% of value — thin margin after costs")
    d = s["days_left"]
    if isinstance(d, (int, float)) and d < WORK_DAYS_MIN:
        out.append(f"only {d} days left — too short to reach the owner and close")
    if not s["property_type"] and not s["year_built"]:
        out.append("property type and age unknown (could be land) — look it up")
    return out


def notes(s):
    """Facts worth knowing about a row, for the `why` sentence."""
    n = []
    if s["intent_filed"]:
        n.append("owner FILED intent to redeem — likely to keep it")
    if s["absentee"] is True:
        n.append("absentee owner")
    if str(s["homestead"] or "").upper() == "H":
        n.append("owner-occupied (homestead)")
    if (s["signal_count"] or 1) >= 2:
        n.append(f"{s['signal_count']} distress signals"
                 + (f" ({s['signals']})" if s["signals"] else ""))
    if s["postponements"]:
        n.append(f"sale postponed {s['postponements']}x")
    try:
        lag = date.today().year - int(s["emv_year"])
        if lag >= 2:
            n.append(f"value is a {s['emv_year']} assessment — likely understated")
    except (TypeError, ValueError):
        pass
    if s["year_built"]:
        n.append(f"built {s['year_built']}")
    return n


def why(s):
    if s["spread"] is None:
        return "Not ranked: no county value or no price published."
    head = (f"Pays {s['pays_pct']}% of value — {_short_money(s['spread'])} "
            f"spread, {s['days_left']} days left")
    ex = exclusions(s)
    extra = notes(s)
    if ex:
        return ("Not ranked: " + "; ".join(ex) + ". "
                + f"({_short_money(s['spread'])} spread, {s['days_left']} days left.)")
    if extra:
        tail = "; ".join(extra)
        return head + ". " + tail[0].upper() + tail[1:] + "."
    return head + "."


def build(shaped, top_n, county):
    rows = dedupe(shaped)
    scored = [s for s in rows if s["spread"] is not None]
    unscored = [s for s in rows if s["spread"] is None]
    by_spread = sorted(scored, key=lambda s: s["spread"], reverse=True)
    ranked = [s for s in by_spread if not exclusions(s)]
    for i, s in enumerate(ranked, 1):
        s["rank"] = i
    liens = [s for s in by_spread if s["lien"]]
    comm = [s for s in by_spread if not s["lien"] and s["non_residential"]]
    outside = [s for s in by_spread if exclusions(s)
               and not s["lien"] and not s["non_residential"]]
    top = ranked[:top_n]
    top_ids = {id(s) for s in top}
    stacked = [s for s in by_spread if not s["lien"] and not s["non_residential"]
               and (s["signal_count"] or 1) >= 2 and id(s) not in top_ids][:10]
    per_county = {}
    for s in top:
        per_county[s["county"]] = per_county.get(s["county"], 0) + 1
    return {
        "pool": len(rows), "unscored": len(unscored), "liens": liens,
        "ranked_n": len(ranked), "outside_n": len(outside),
        "top": top, "stacked": stacked, "commercial": comm[:5],
        "per_county": sorted(per_county.items(), key=lambda kv: -kv[1]),
        "county": county,
        # CSV order: best first, then everything that was not ranked.
        "csv_rows": ranked + outside + comm + liens + unscored,
    }


def _money(v):
    return "—" if v is None else f"${v:,.0f}"


def _place(s):
    a = s["address"] or f"parcel {s['parcel_id'] or '?'}"
    # mnpublicnotice often writes the full line ("1251 MACARTHUR AVE, WEST ST
    # PAUL, MN 55118") into address; appending city again doubled it.
    if s["city"] and s["city"].lower() not in a.lower():
        return f"{a}, {s['city']}"
    return a


def _header(r):
    scope = r["county"].title() if r["county"] else "all counties"
    L = [f"Govire best deals — {date.today():%a %b %d, %Y} — {scope}",
         f"Ranked best first: residential, {WORK_DAYS_MIN}+ days left, buyer "
         f"pays {PAYS_PCT_MIN:.0f}-{PAYS_PCT_MAX:.0f}% of value, then largest "
         f"spread. {r['ranked_n']} qualify out of {r['pool']} open windows.",
         f"Not ranked: {r['outside_n']} outside those limits, "
         f"{len(r['liens'])} association liens, {r['unscored']} with no value "
         f"or price — all in the CSV below the ranked rows, with the reason."]
    if r["per_county"]:
        L.append("Top list by county: " + ", ".join(
            f"{c.title()} {n}" for c, n in r["per_county"]))
    return L


def _sections(r):
    L = []
    for title, items in (("STACKED DISTRESS — 2+ signals, not in the top list",
                          r["stacked"]),
                         ("COMMERCIAL / INDUSTRIAL — top 5", r["commercial"])):
        if items:
            L += ["", title]
            for s in items:
                L.append(f"  - {_place(s)}: {why(s)}")
    if r["liens"]:
        L += ["", f"EXCLUDED AS LIKELY ASSOCIATION LIENS ({len(r['liens'])}) — "
                  f"largest five:"]
        for s in r["liens"][:5]:
            how = "named association" if s["assoc_named"] else "pays <5% of value"
            L.append(f"  - {_place(s)}: pays {_money(s['buyer_pays'])} "
                     f"against {_money(s['value'])} ({how})")
    L += ["", "How to read this: spread = county market value minus what a "
              "buyer pays (final bid for Hennepin, notice amount due "
              "elsewhere). It is room in the deal, not profit — the owner can "
              "still redeem, other liens may exist, and the value is the "
              "assessor's, not an appraisal. Ranking fits buying from the "
              "owner during the window. Anoka, Dakota and Ramsey sheriff "
              "feeds are not reporting; those counties appear only through "
              "newspaper notices."]
    return L


def render_text(r):
    L = _header(r) + ["", f"TOP {len(r['top'])} — best first"]
    for s in r["top"]:
        L.append(f"{s['rank']:>2}. {_place(s)} ({s['county'].title()})")
        L.append(f"    value {_money(s['value'])}  pays {_money(s['buyer_pays'])}"
                 f"  spread {_money(s['spread'])}")
        L.append(f"    {why(s)}")
    return "\n".join(L + _sections(r))


def render_html(r):
    def row(s):
        return (
            f"<tr><td>{s['rank']}</td>"
            f"<td><b>{html.escape(_place(s))}</b><br>"
            f"<small>{html.escape(s['county'].title())}</small><br>"
            f"<small>{html.escape(why(s))}</small></td>"
            f"<td align=right>{_money(s['spread'])}</td>"
            f"<td align=right>{_money(s['value'])}</td>"
            f"<td align=right>{_money(s['buyer_pays'])}<br>"
            f"<small>{s['pays_pct']}%</small></td>"
            f"<td align=right>{s['days_left']}</td></tr>")
    head = "<br>".join(html.escape(x) for x in _header(r))
    rows = "".join(row(s) for s in r["top"])
    rest = html.escape("\n".join(_sections(r)).strip())
    return (
        "<div style='font-family:Arial,sans-serif;font-size:14px'>"
        f"<p>{head}</p>"
        f"<h3>Top {len(r['top'])} — best first</h3>"
        "<table cellpadding=6 cellspacing=0 border=1 "
        "style='border-collapse:collapse;border-color:#ddd'>"
        "<tr style='background:#f3f3f3'><th>#</th><th>Property and why</th>"
        "<th>Spread</th><th>Value</th><th>Buyer pays</th><th>Days left</th></tr>"
        f"{rows}</table>"
        "<pre style='font-family:Arial,sans-serif;white-space:pre-wrap'>"
        f"{rest}</pre></div>")


def to_csv(r):
    """Best first. Ranked rows carry rank 1..N; every other row follows,
    unranked, with its reason in `why`. No `source` column (operator asked
    2026-11-23); price_basis still says where the price came from."""
    buf = io.StringIO()
    cols = ["rank", "why", "county", "address", "city", "parcel_id",
            "property_type", "year_built", "sqft", "value", "emv_year",
            "buyer_pays", "price_basis", "pays_pct", "spread", "sale_date",
            "days_left", "postponements", "homestead", "absentee",
            "intent_filed", "signal_count", "signals", "mortgagee"]
    w = csv.writer(buf)
    w.writerow(cols)
    for s in r["csv_rows"]:
        vals = dict(s, why=why(s), rank=s.get("rank"))
        w.writerow([vals.get(c) if vals.get(c) is not None else "" for c in cols])
    return buf.getvalue()


def send(api_key, to_addr, from_addr, subject, text, html_body, csv_text):
    payload = {
        "from": from_addr, "to": [to_addr], "subject": subject,
        "text": text, "html": html_body,
        "attachments": [{
            "filename": f"govire_best_deals_{date.today().isoformat()}.csv",
            "content": base64.b64encode(csv_text.encode("utf-8")).decode("ascii"),
        }],
    }
    try:
        resp = httpx.post("https://api.resend.com/emails",
                          headers={"Authorization": f"Bearer {api_key}",
                                   "Content-Type": "application/json"},
                          json=payload, timeout=30)
    except httpx.HTTPError as e:
        print(f"FATAL: could not reach Resend: {type(e).__name__}: {e}", flush=True)
        sys.exit(2)
    if 200 <= resp.status_code < 300:
        print(f"Report emailed to {to_addr} ({subject})", flush=True)
    else:
        print(f"FATAL: Resend returned {resp.status_code}: {resp.text[:300]}",
              flush=True)
        sys.exit(2)


def main() -> int:
    base = _env("SUPABASE_URL").rstrip("/")
    key = _env("SUPABASE_SERVICE_ROLE_KEY")
    dry = os.environ.get("DRY_RUN", "").strip() == "1"
    county = (os.environ.get("REPORT_COUNTY") or "").strip().lower().replace(" ", "_")
    try:
        top_n = int(os.environ.get("REPORT_TOP_N") or 25)
    except ValueError:
        top_n = 25

    pool = fetch_pool(base, key, county)
    overlay = fetch_overlay(base, key)
    report = build([shape(r, overlay) for r in pool], top_n, county)

    text = render_text(report)
    print(text, flush=True)          # also in the Actions log
    if dry:
        print("\nDRY_RUN=1 — nothing sent.", flush=True)
        return 0

    scope = county.title() if county else "all counties"
    subject = (f"[Govire deals] Top {len(report['top'])} — {scope} — "
               f"{date.today():%b %d}")
    send(_env("RESEND_API_KEY"), _env("ALERT_EMAIL_TO"), _env("ALERT_EMAIL_FROM"),
         subject, text, render_html(report), to_csv(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
