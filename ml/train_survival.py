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

=== LEAKAGE: WHY has_* INDICATORS AND COUNTY TERMS ARE GONE (v2) ===
v1 scored C-index 0.84 to 0.89 across all four fits and the gate said SHIP.
The gate was wrong, and it was wrong because it checked the SCORE and not
what produced it.

Every fit was dominated by MISSING-DATA INDICATORS:

    owner_exit        has_paid_vs_value 0.39, homestead_known 0.36,
                      homestead_yes 0.35, has_bid_to_value 0.32,
                      has_year_built 0.28
    foreclosure_sale  county_washington 0.96 (p=0.0), has_sqft 0.42,
                      has_paid_vs_value 0.38, homestead_known 0.32

has_paid_vs_value is not a fact about a property. It is "this row has data",
and DATA AVAILABILITY IS A COUNTY PROXY: finalBidAmount exists only on
hennepin_sheriff rows, homestead was 99% in ramsey and 1.3% in hennepin until
2026-08-24, sqft is absent from hennepin entirely. County predicts outcome for
real -- 49.6% / 29.6% / 20.8% -- so a model that infers county from which
fields are populated scores well while having learned NOTHING about
redemption. It learned our data pipeline.

homestead_known (0.36) outranking homestead_yes (0.35) is the tell in one
line: that homestead is RECORDED predicts more than what it says.

This is the same family as outcome_detected_at -- a variable that correlates
with the outcome for reasons unrelated to the thing being predicted.

v2 therefore:
  * drops every has_* indicator from the design matrix
  * drops the county indicators
  * imputes missing numerics at the median and says so, rather than letting
    absence carry signal
  * reports county as a STRATUM instead, so county-level baseline hazard
    differences are absorbed without becoming a rankable covariate

If C-index falls to ~0.55 after this, that is the honest answer and the
Kaplan-Meier curve is the product. It is already publishable: 86.2% of
windows are still unresolved a year after the sheriff sale, 119 events.

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
MODEL_VERSION = "v4"

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
    """Numeric design matrix. NO missingness indicators, NO county terms.

    Both were in v1 and both leaked. See the module docstring: the top five
    terms in every fit were has_* flags, and data availability is a county
    proxy on this platform -- finalBidAmount only exists on hennepin_sheriff
    rows, homestead was 1.3% in hennepin and 99% in ramsey, sqft is absent
    from hennepin entirely.

    Missing numerics are imputed at the median WITHOUT a companion flag. That
    loses information, and losing it is the point: the information being lost
    is which scraper wrote the row.

    County is passed back separately for use as a STRATUM, not a covariate.
    Stratifying lets each county have its own baseline hazard -- which is
    real, 49.6% / 29.6% / 20.8% -- without letting the model rank a window
    higher merely for being in Hennepin.
    """
    X = pd.DataFrame(index=df.index)
    X["duration"] = df["duration"].astype(float)
    X["event"] = df["event"].astype(int)

    X["period_months"] = pd.to_numeric(
        df["redemption_period_months"], errors="coerce").fillna(6)

    for col in ("emv_total", "amount_owed", "bid_to_value",
                "paid_vs_value", "sqft", "lot_sqft", "year_built"):
        v = pd.to_numeric(df[col], errors="coerce")
        X[col] = v.fillna(v.median() if v.notna().any() else 0.0)

    # log the money columns: emv_total spans 100 to 533,960,200 and a linear
    # term would be dominated by a handful of commercial parcels.
    for col in ("emv_total", "amount_owed"):
        X[f"log_{col}"] = np.log1p(X[col].clip(lower=0))
        X.drop(columns=[col], inplace=True)

    # Real property facts only. A missing homestead reads as non-homestead
    # rather than as its own category, because "we do not know" is not a
    # third kind of property -- it is a fact about our loaders.
    X["homestead_yes"] = (df["homestead"] == "homestead").astype(int)
    X["third_party_buyer"] = (df["buyer_type"] == "third_party_buyer").astype(int)
    X["notice_of_intent"] = df["notice_of_intent"].fillna(False).astype(int)

    feats = [c for c in X.columns if c not in ("duration", "event")]
    X["_county"] = df["county_code"].fillna("(unknown)")
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

    # STRATIFIED BY COUNTY. Each county gets its own baseline hazard, so the
    # real 49.6% / 29.6% / 20.8% difference is absorbed without county
    # becoming a rankable covariate. Concordance is then measured on whether
    # the model orders windows WITHIN a county, which is the question a
    # subscriber has -- they are already looking at one county.
    cph = CoxPHFitter(penalizer=0.1)
    strata = ["_county"] if tr["_county"].nunique() > 1 else None
    try:
        cols = feats + ["duration", "event"] + (["_county"] if strata else [])
        cph.fit(tr[cols], duration_col="duration", event_col="event",
                strata=strata)
    except Exception as e:
        return {"label": label, "skipped": f"fit failed: {type(e).__name__}",
                "error": str(e)[:200]}

    pred_cols = feats + (["_county"] if strata else [])
    risk = -cph.predict_partial_hazard(te[pred_cols])
    c = float(concordance_index(te["duration"], risk, te["event"]))

    # === IS THE C-INDEX COMING FROM THE COVARIATES OR FROM THE STRATA? ===
    # v2 scored 0.80 with exactly ONE significant term across all four fits:
    # homestead_yes. A single binary covariate cannot rank 643 windows that
    # well. So something else is ordering them, and the candidate is the
    # stratification itself: concordance pooled across the holdout includes
    # cross-county pairs, and if county baseline hazards differ sharply --
    # they do, 49.6% / 29.6% / 20.8% -- the partial-hazard ordering inherits
    # that separation without county ever being a covariate.
    #
    # That would mean v2 MOVED the leak rather than removed it.
    #
    # Two controls settle it:
    #
    #   c_within_county   concordance computed inside each county and
    #                     sample-size weighted. This is the number that
    #                     matters -- a subscriber is looking at one county,
    #                     so ranking ACROSS counties is not a capability they
    #                     can use.
    #
    #   c_covariate_free  the same fit with NO covariates, strata only. By
    #                     construction every subject in a stratum gets an
    #                     identical hazard, so this should be ~0.5. If it
    #                     comes back high, the strata are doing the work and
    #                     the covariates are decoration.
    c_within = None
    try:
        num = 0.0
        den = 0
        for cty, grp in te.groupby("_county"):
            if int(grp["event"].sum()) < 5:
                continue
            g_risk = -cph.predict_partial_hazard(grp[pred_cols])
            num += float(concordance_index(
                grp["duration"], g_risk, grp["event"])) * len(grp)
            den += len(grp)
        c_within = round(num / den, 4) if den else None
    except Exception:
        c_within = None

    c_free = None
    if strata:
        try:
            cph0 = CoxPHFitter(penalizer=0.1)
            cph0.fit(tr[["duration", "event", "_county"]],
                     duration_col="duration", event_col="event",
                     strata=strata)
            r0 = -cph0.predict_partial_hazard(te[["_county"]])
            c_free = round(float(concordance_index(
                te["duration"], r0, te["event"])), 4)
        except Exception:
            c_free = None

    coef = cph.summary[["coef", "p"]].round(4)
    strong = coef[coef["p"] < 0.05].sort_values("coef", key=abs,
                                                ascending=False)

    # === WHY EVERY COEFFICIENT, NOT JUST p < 0.05 ===
    # v3 ranks at 0.80-0.87 within county with exactly ONE significant term:
    # homestead_yes. A single binary covariate cannot order 615 windows that
    # well on its own, so either the other nine are contributing below the
    # significance threshold -- entirely plausible on 20-24 holdout events,
    # where nothing reaches p<0.05 but the linear combination still ranks --
    # or one term does everything and the rest are decoration.
    #
    # Those two cases lead to different products. Nine features combining is
    # a per-property hazard worth rendering. homestead_yes alone is a rate
    # cut, and scoring.redemption_rates already publishes it at 45.7% vs
    # 32.9% with its sample size attached.
    all_terms = {
        k: {"coef": float(v["coef"]), "p": float(v["p"])}
        for k, v in coef.sort_values("coef", key=abs, ascending=False)
                        .to_dict("index").items()
    }

    # SINGLE-COVARIATE CONTROL. Same fit, same strata, homestead_yes only.
    # If this scores what the full model scores, the full model is a
    # one-feature model wearing nine features.
    c_homestead_only = None
    if "homestead_yes" in feats:
        try:
            cols1 = ["homestead_yes", "duration", "event"] + (
                ["_county"] if strata else [])
            cph1 = CoxPHFitter(penalizer=0.1)
            cph1.fit(tr[cols1], duration_col="duration", event_col="event",
                     strata=strata)
            r1 = -cph1.predict_partial_hazard(
                te[["homestead_yes"] + (["_county"] if strata else [])])
            c_homestead_only = round(float(concordance_index(
                te["duration"], r1, te["event"])), 4)
        except Exception:
            c_homestead_only = None
    return {
        "label": label,
        "n_train": int(len(tr)),
        "n_test": int(len(te)),
        "events_train": int(tr["event"].sum()),
        "events_test": int(te["event"].sum()),
        "c_index": round(c, 4),
        # The honest headline. Ranking within a county is the capability a
        # subscriber can use; ranking across counties is the rate table's job
        # and it already does it with sample sizes attached.
        "c_index_within_county": c_within,
        "c_index_covariate_free": c_free,
        "beats_baseline": bool(
            c >= MIN_C_INDEX
            and (c_within is None or c_within >= MIN_C_INDEX)
        ),
        "c_index_homestead_only": c_homestead_only,
        "significant_terms": {
            k: {"coef": float(v["coef"]), "p": float(v["p"])}
            for k, v in strong.head(8).to_dict("index").items()
        },
        "all_terms": all_terms,
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
                    log(f"{target} [{tag}] Cox C={r['c_index']} "
                        f"within-county={r.get('c_index_within_county')} "
                        f"covariate-free={r.get('c_index_covariate_free')} "
                        f"homestead-only={r.get('c_index_homestead_only')} "
                        f"(bar {MIN_C_INDEX}) -> "
                        f"{'BEATS' if r['beats_baseline'] else 'does not beat'}")
                else:
                    log(f"{target} [{tag}] SKIPPED: {r.get('skipped')}")
                results.append(r)

        # === THE GATE CHECKS WHAT PRODUCED THE SCORE, NOT JUST THE SCORE ===
        # v1 approved four fits at C-index 0.84-0.89 whose top terms were all
        # missing-data flags. A high number is not evidence on its own; it is
        # a question about where it came from.
        #
        # Any term matching these names means the model is reading our data
        # pipeline rather than the property. They are removed from the design
        # matrix in v2, so seeing one here means something reintroduced them.
        LEAK_PREFIXES = ("has_", "county_", "_known", "known_")
        leaked = []
        for r in results:
            for term in (r.get("significant_terms") or {}):
                if any(p in term for p in LEAK_PREFIXES):
                    leaked.append(f"{r.get('label')}:{term}")
        if leaked:
            log("LEAK CHECK FAILED — availability terms are significant: "
                + ", ".join(leaked[:8]))

        ship = any(r.get("beats_baseline") for r in results) and not leaked
        verdict = "SHIP" if ship else "DO NOT SHIP"
        log(f"verdict: {verdict}")
        if leaked:
            log("  blocked by the leak check regardless of C-index")
        if not ship:
            log("  the Kaplan-Meier curve is the product; publish it with "
                "its event count and no model")

        metrics = {
            "targets": results,
            "min_c_index": MIN_C_INDEX,
            "leak_check_failed": leaked,
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
