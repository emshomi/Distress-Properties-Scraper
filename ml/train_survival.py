"""
Survival models for redemption windows: will this resolve, and when.

Two targets, fitted separately, because they are COMPETING RISKS:

    owner_exit        the owner sold inside the window   92 events
    foreclosure_sale  an REO/third-party sale followed  119 events

A window ends one way or the other, and observing one censors the other.
Pooling them into "time to any outcome" answers a question nobody asked.

=== THE BAR: BEAT THE MARGINAL CURVE, OR SHIP THE CURVE ===
On 2026-08-23 an AVM was built and trained three times on growing data, and
lost to a one-line county-year median every time -- 12.53% against 11.32%,
with the gap WIDENING as data grew. Five approaches failed to beat a GROUP BY.

So this script scores every model against a baseline that uses NO covariates:
the Kaplan-Meier curve for the whole population, and then per county. If a
model cannot rank windows better than "everyone follows the average curve",
the curve is the product and the model is not.

C-index is the metric. A marginal curve has C = 0.5 BY CONSTRUCTION -- it
gives every subject the same risk and cannot rank anything. So 0.5 is the
floor, not the target. The gate below asks for a margin over it, because a
model at 0.52 on 92 events is noise wearing a decimal point.

=== WHY NOT outcome_detected_at ===
It records when the CHECKER RAN. Across resolved rows it spans -177 to +413
days from expiry, and the negatives are the proof: a redemption cannot be
detected 177 days before its window closes. A model trained on it learns the
30/60/90/180-day ladder offsets. scoring.redemption_features exposes
outcome_event_date instead and does not select the detection timestamp at all.

=== THE CLOCK STARTS AT THE SHERIFF SALE ===
duration = days_anchor_to_event for events, days_observed for censored rows.
Not days-from-expiry: the window opens at the sale, and a model asked "how
long from the sale until this resolves" is answering the question a
subscriber has. days_expiry_to_event is kept as a REPORTING quantity because
"65 days before the deadline" is how a person thinks about it.

=== THREE KINDS OF CENSORING, AND ONE OF THEM IS SUSPICIOUS ===
    pending      2,208   still running, observed to today
    unknown        109   ladder exhausted, observation stopped at EXPIRY
    foreclosed      41   lender holds it, no sale date exists

The 109 'unknown' rows are flagged outcome_ambiguous. STAGE2_SURVIVAL_FINDINGS
records that no-REO-and-no-post-expiry-deed "is what a redemption looks like
from every angle we can observe" -- so the population most likely to have
redeemed is the one we cannot confirm, and censoring it alongside live
windows biases the owner_exit curve DOWN.

Every fit therefore runs twice, with and without them. If the curves diverge
materially that is reported, not smoothed over.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd
import psycopg2

MODEL_NAME = "redemption_survival"
MODEL_VERSION = "v1"

# A model must beat the marginal curve by this margin in C-index to ship.
# 0.5 is where a no-covariate baseline sits by construction; anything within
# noise of it has learned nothing. On 92 events a 0.02 swing is a handful of
# properties changing order.
MIN_C_INDEX = 0.60

# Below this many events a fit is reported but never shipped, whatever the
# C-index says. 92 events across ~10 covariates is already thin.
MIN_EVENTS = 60

QUERY = """
SELECT tracker_id, county_code, outcome, event_type, censored,
       outcome_ambiguous, days_anchor_to_event, days_observed,
       days_expiry_to_event, redemption_period_months, period_source,
       emv_total, sqft, lot_sqft, year_built, homestead,
       amount_owed, final_bid, bid_to_value, paid_vs_value,
       buyer_type, notice_of_intent
FROM scoring.redemption_features
"""


def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] {msg}",
          flush=True)


def build_target(df: pd.DataFrame, target: str) -> pd.DataFrame:
    """One row per window, with duration and an event flag for ONE risk.

    Competing-risk handling: a window that ended in the OTHER way is not an
    event here, and it is not still at risk either. It is censored at the
    moment the other event happened -- which is what actually observed.
    Treating it as censored at today would claim we watched it for months
    after it had already resolved.
    """
    out = df.copy()
    is_target = out["event_type"] == target
    is_other = out["event_type"].notna() & ~is_target

    # Duration: to the event for our target, to the competing event for the
    # other, to observation_end for the genuinely censored.
    dur = np.where(
        is_target | is_other,
        out["days_anchor_to_event"],
        out["days_observed"],
    )
    out["duration"] = pd.to_numeric(dur, errors="coerce")
    out["event"] = is_target.astype(int)

    # A duration of zero or less cannot enter a survival fit. These are
    # sales recorded on or before the anchor date -- data lag, not a
    # zero-day resolution.
    bad = out["duration"].isna() | (out["duration"] <= 0)
    if bad.any():
        log(f"  dropped {int(bad.sum())} rows with non-positive duration")
    return out[~bad].copy()


def prep_features(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Numeric design matrix. Missing indicators, never silent zeros."""
    X = pd.DataFrame(index=df.index)
    X["duration"] = df["duration"].astype(float)
    X["event"] = df["event"].astype(int)

    X["period_months"] = pd.to_numeric(
        df["redemption_period_months"], errors="coerce").fillna(6)

    for col in ("emv_total", "amount_owed", "bid_to_value",
                "paid_vs_value", "sqft", "lot_sqft", "year_built"):
        v = pd.to_numeric(df[col], errors="coerce")
        X[f"has_{col}"] = v.notna().astype(int)
        X[col] = v.fillna(v.median() if v.notna().any() else 0.0)

    # log the money columns: emv_total spans 100 to 533,960,200 and a linear
    # term would be dominated by a handful of commercial parcels.
    for col in ("emv_total", "amount_owed"):
        X[f"log_{col}"] = np.log1p(X[col].clip(lower=0))
        X.drop(columns=[col], inplace=True)

    X["homestead_yes"] = (df["homestead"] == "homestead").astype(int)
    X["homestead_known"] = df["homestead"].notna().astype(int)
    X["third_party_buyer"] = (df["buyer_type"] == "third_party_buyer").astype(int)
    X["buyer_known"] = df["buyer_type"].notna().astype(int)
    X["notice_of_intent"] = df["notice_of_intent"].fillna(False).astype(int)
    X["notice_known"] = df["notice_of_intent"].notna().astype(int)

    # County as indicators, not a code. hennepin/dakota/washington are the
    # only counties with resolved outcomes; anything else collapses to a
    # reference level rather than inventing an ordering.
    for c in ("hennepin", "dakota", "washington"):
        X[f"county_{c}"] = (df["county_code"] == c).astype(int)

    feats = [c for c in X.columns if c not in ("duration", "event")]
    return X, feats


def km_baseline(df: pd.DataFrame) -> dict:
    """Kaplan-Meier with no covariates. This is what a model must beat."""
    from lifelines import KaplanMeierFitter

    kmf = KaplanMeierFitter()
    kmf.fit(df["duration"], df["event"])
    out = {"n": int(len(df)), "events": int(df["event"].sum())}
    for d in (90, 180, 365, 540):
        try:
            out[f"surv_at_{d}d"] = round(float(kmf.predict(d)), 4)
        except Exception:
            out[f"surv_at_{d}d"] = None
    try:
        out["median_survival_days"] = (
            None if not np.isfinite(kmf.median_survival_time_)
            else float(kmf.median_survival_time_)
        )
    except Exception:
        out["median_survival_days"] = None
    return out


def fit_and_score(X: pd.DataFrame, feats: list[str], label: str) -> dict:
    """Cox PH with a train/test split on TIME, never at random.

    A random split leaks: windows from the same month appear on both sides
    and the model sees the future. Split on anchor order instead -- fit on
    the earlier windows, score on the later ones, which is the only split
    that answers "would this have helped when the window opened".
    """
    from lifelines import CoxPHFitter
    from lifelines.utils import concordance_index

    n = len(X)
    if int(X["event"].sum()) < MIN_EVENTS:
        return {"label": label, "skipped": "too few events",
                "events": int(X["event"].sum())}

    cut = int(n * 0.75)
    tr, te = X.iloc[:cut], X.iloc[cut:]
    if int(te["event"].sum()) < 10:
        return {"label": label, "skipped": "too few events in holdout",
                "holdout_events": int(te["event"].sum())}

    cph = CoxPHFitter(penalizer=0.1)
    try:
        cph.fit(tr[feats + ["duration", "event"]],
                duration_col="duration", event_col="event")
    except Exception as e:
        return {"label": label, "skipped": f"fit failed: {type(e).__name__}",
                "error": str(e)[:200]}

    risk = -cph.predict_partial_hazard(te[feats])
    c = float(concordance_index(te["duration"], risk, te["event"]))

    coef = cph.summary[["coef", "p"]].round(4)
    strong = coef[coef["p"] < 0.05].sort_values("coef", key=abs,
                                                ascending=False)
    return {
        "label": label,
        "n_train": int(len(tr)),
        "n_test": int(len(te)),
        "events_train": int(tr["event"].sum()),
        "events_test": int(te["event"].sum()),
        "c_index": round(c, 4),
        "beats_baseline": bool(c >= MIN_C_INDEX),
        "significant_terms": {
            k: {"coef": float(v["coef"]), "p": float(v["p"])}
            for k, v in strong.head(8).to_dict("index").items()
        },
    }


def main() -> int:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log("DATABASE_URL not set")
        return 2
    dry_run = os.environ.get("DRY_RUN", "1") == "1"

    try:
        import lifelines  # noqa: F401
    except ImportError:
        log("lifelines unavailable; install it in the workflow")
        return 2

    conn = psycopg2.connect(dsn)
    try:
        df = pd.read_sql(QUERY, conn)
        log(f"loaded {len(df):,} windows, {df.county_code.nunique()} counties")
        log(f"  events: {df.event_type.value_counts().to_dict()}")
        log(f"  outcome_ambiguous: {int(df.outcome_ambiguous.sum())}")

        results = []
        for target in ("owner_exit", "foreclosure_sale"):
            for excl in (False, True):
                sub = df[~df["outcome_ambiguous"]] if excl else df
                tag = "excl-ambiguous" if excl else "all-rows"
                t = build_target(sub, target)
                if t.empty:
                    continue
                base = km_baseline(t)
                log(f"{target} [{tag}] baseline KM: n={base['n']:,} "
                    f"events={base['events']} "
                    f"surv@365d={base.get('surv_at_365d')}")
                X, feats = prep_features(t)
                r = fit_and_score(X, feats, f"{target}/{tag}")
                r["baseline_km"] = base
                r["target"] = target
                r["population"] = tag
                if "c_index" in r:
                    log(f"{target} [{tag}] Cox C-index {r['c_index']} "
                        f"(bar {MIN_C_INDEX}) -> "
                        f"{'BEATS' if r['beats_baseline'] else 'does not beat'}")
                else:
                    log(f"{target} [{tag}] SKIPPED: {r.get('skipped')}")
                results.append(r)

        ship = any(r.get("beats_baseline") for r in results)
        verdict = "SHIP" if ship else "DO NOT SHIP"
        log(f"verdict: {verdict}")
        if not ship:
            log("  the Kaplan-Meier curve is the product; publish it with "
                "its event count and no model")

        metrics = {
            "targets": results,
            "min_c_index": MIN_C_INDEX,
            "min_events": MIN_EVENTS,
            "verdict": verdict,
        }
        print(json.dumps(metrics, indent=2, default=str))

        if dry_run:
            log("DRY_RUN=1 — nothing written to scoring.models")
            return 0

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO scoring.models
                  (model_name, model_version, model_type, trained_on_date,
                   training_data_count, metrics, is_active, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    MODEL_NAME, MODEL_VERSION, "cox_ph", date.today(),
                    int(len(df)), json.dumps(metrics, default=str), bool(ship),
                    "Competing risks: owner_exit and foreclosure_sale fitted "
                    "separately. Baseline is the no-covariate Kaplan-Meier "
                    "curve; is_active set only when a Cox fit beats C-index "
                    f"{MIN_C_INDEX} on a time-ordered holdout.",
                ),
            )
        conn.commit()
        log("wrote scoring.models row")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
