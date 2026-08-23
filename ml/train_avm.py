"""
AVM training: predict the sale-to-assessment ratio, not the price.

=== WHY THE RATIO AND NOT THE PRICE ===
core.parcels.emv_total is a county assessment, and it is what the platform
shows today: equity spread is emv_total - amount_owed on every row of the data
page and underneath all of the Premium deal math. Modelling the RATIO
purchase_amt / emv_total corrects that number without needing the features a
price model would require.

It also sidesteps the constraint measured 2026-08-23: interior square footage
is unavailable on the largest county. Hennepin's parcel layer is a tax roll —
104 attributes, no building area — its GIS org publishes 34 items and none is
a building-characteristics layer, and the LAND_PROPERTY MapServer holds three
layers and zero tables. A hedonic price model would train on a sqft-rich
subset and be blind on 51,026 of 182,020 training rows.

=== WHY LOG SPACE ===
Measured 2026-08-23 on the real training set:

    year   spearman   corr(log)   corr(raw)
    2024      0.282       0.247       0.022
    2025      0.327       0.384       0.036
    2026      0.361       0.490       0.142

corr_raw at 0.022 against corr_log at 0.247 is the same data seen two ways.
The ratio is heavy-tailed — median 1.076, p95 2.66, max far beyond — so a
model fitting the raw target chases a few hundred extreme rows and learns
almost nothing about the 178,000 ordinary ones. Fit ln(ratio); exponentiate
to predict.

=== THE BASELINE THAT ACTUALLY MATTERS ===
GOVIRE_AI_PREDICTION_STRATEGY says "if it does not beat the assessor, ship
nothing." Taken literally that means beating ratio = 1.0, which is a low bar:
the statewide median ratio is 1.076 and rises ~2.3%/year, so a constant 1.076
already beats it.

This script therefore scores against TWO baselines:

  1. assessor      - predict ratio = 1.0 (emv_total unadjusted)
  2. county_median - predict the county's median ratio from the TRAINING
                     years only. No model, one GROUP BY, and it is the
                     honest competitor.

The model ships only if it beats BOTH. §2.3 of the strategy document argues a
calibrated rate table is itself a prediction and is the stronger product when
it exposes its sample size — so if county_median wins, that is a real result
and not a failure.

=== TEMPORAL VALIDATION, AND WHY THE WINDOW IS THREE YEARS ===
Train on 2024-2025, test on 2026. Never a random split: it leaks future prices
into the past.

The strategy document describes eCRV as "1972-2026, 338,033 with a price".
True of outcomes.ecrv_sales. But joined to a parcel that has an assessment,
arms-length, primary-parcel only, the usable depth collapses:

    2015-2022      116 sales     (0.06%)
    2023         3,264
    2024        68,179
    2025        70,241
    2026        40,220  (through August)

The old sales survive the join only where the parcel still carries a CURRENT
assessment, and a 2015 price over a 2026 assessment is not a meaningful ratio
anyway. Three years is the real window.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd
import psycopg2


TRAIN_END = date(2026, 1, 1)
MODEL_NAME = "avm_ratio"
MODEL_VERSION = "v1"

# primary_parcel and the emv floor are applied in scoring.avm_training_set;
# see that view. They are not optional filters — without primary_parcel the
# target's MEAN is 88.6 (a multi-parcel sale writes the whole price against
# each parcel) and the model is fitting nonsense.
QUERY = """
SELECT ecrv_id, county_code, deed_date, purchase_amt, emv_total,
       sale_to_assessment, lat, lng, lot_sqft, sqft, year_built,
       property_type, homestead_status, deed_type, finance_type,
       knn_ratio, knn_count
FROM scoring.avm_training
WHERE sale_to_assessment > 0
"""


def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] {msg}", flush=True)


def load(conn) -> pd.DataFrame:
    df = pd.read_sql(QUERY, conn)
    log(f"loaded {len(df):,} rows, {df.county_code.nunique()} counties")
    return df


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["deed_date"] = pd.to_datetime(df["deed_date"])
    # Months since the window opens. The ratio drifts ~2.3%/year as
    # assessments fall behind a rising market, so a time index is not
    # optional — measured Ramsey medians 1.239 (2023) -> 1.332 (2026).
    df["t_months"] = (
        (df["deed_date"].dt.year - 2024) * 12 + df["deed_date"].dt.month
    )
    df["year"] = df["deed_date"].dt.year

    # sqft is present on 28,577 of 182,020 rows (15.7%) and is STRUCTURALLY
    # absent on Hennepin. A missing indicator lets one model use it where it
    # exists rather than splitting the training set and losing Hennepin's
    # 51,026 rows from the sqft-aware half.
    df["has_sqft"] = df["sqft"].notna().astype(int)
    df["sqft_f"] = df["sqft"].fillna(0.0)
    df["has_year_built"] = df["year_built"].notna().astype(int)
    df["year_built_f"] = df["year_built"].fillna(0.0)
    df["lot_sqft_f"] = df["lot_sqft"].fillna(0.0)
    df["has_knn"] = df["knn_ratio"].notna().astype(int)
    # 1.0 is the neutral fill: it asserts "no local information", not "the
    # neighbours sold at assessment".
    df["knn_f"] = df["knn_ratio"].fillna(1.0).astype(float)
    df["knn_count_f"] = df["knn_count"].fillna(0).astype(float)

    df["homestead_y"] = (df["homestead_status"].astype(str).str.upper().str[:1] == "Y").astype(int)
    df["y"] = np.log(df["sale_to_assessment"].astype(float))
    return df


def mdape(actual: np.ndarray, predicted: np.ndarray) -> float:
    """Median absolute percentage error on the PRICE, not the ratio.

    The ratio is an internal quantity; a subscriber sees a dollar value, so
    the error is reported where they would feel it. Median rather than mean
    because the target is heavy-tailed and a mean would report the outliers.
    """
    return float(np.median(np.abs(predicted - actual) / actual) * 100.0)


def main() -> int:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log("DATABASE_URL not set")
        return 2
    dry_run = os.environ.get("DRY_RUN", "1") == "1"

    conn = psycopg2.connect(dsn)
    try:
        df = load(conn)
        df = add_features(df)

        train = df[df["deed_date"] < pd.Timestamp(TRAIN_END)]
        test = df[df["deed_date"] >= pd.Timestamp(TRAIN_END)]
        log(f"train {len(train):,} (through 2025)   test {len(test):,} (2026)")
        if len(train) < 10_000 or len(test) < 1_000:
            log("insufficient rows for a temporal split; refusing to train")
            return 1

        actual_price = test["purchase_amt"].astype(float).to_numpy()
        emv = test["emv_total"].astype(float).to_numpy()

        # --- Baseline 1: the assessor, unadjusted (ratio = 1.0) ---
        base_assessor = mdape(actual_price, emv * 1.0)

        # --- Baseline 2: county median ratio, TRAINING YEARS ONLY ---
        # Computed on train to avoid leaking the test period. Counties absent
        # from train fall back to the global training median.
        global_med = float(train["sale_to_assessment"].median())
        cmed = train.groupby("county_code")["sale_to_assessment"].median()
        test_cmed = test["county_code"].map(cmed).fillna(global_med).astype(float).to_numpy()
        base_county = mdape(actual_price, emv * test_cmed)

        log(f"baseline assessor      (ratio=1.0)      MdAPE {base_assessor:.2f}%")
        log(f"baseline county_median (train medians)  MdAPE {base_county:.2f}%")

        # --- Model ---
        try:
            from lightgbm import LGBMRegressor
        except ImportError:
            log("lightgbm unavailable; install it in the workflow")
            return 2

        feats = [
            "t_months", "lat", "lng", "lot_sqft_f",
            "sqft_f", "has_sqft", "year_built_f", "has_year_built",
            "knn_f", "has_knn", "knn_count_f", "homestead_y",
        ]
        Xtr = train[feats].astype(float)
        Xte = test[feats].astype(float)
        # County as a categorical rather than lat/lng alone: assessment
        # practice is set county by county, so the level shift is a county
        # fact, not a spatial one.
        ctr = train["county_code"].astype("category")
        Xtr = Xtr.assign(county=ctr.cat.codes)
        Xte = Xte.assign(
            county=pd.Categorical(test["county_code"], categories=ctr.cat.categories).codes
        )

        model = LGBMRegressor(
            n_estimators=600,
            learning_rate=0.05,
            num_leaves=63,
            min_child_samples=50,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
        )
        model.fit(Xtr, train["y"].astype(float))

        pred_ratio = np.exp(model.predict(Xte))
        model_mdape = mdape(actual_price, emv * pred_ratio)
        log(f"model  lgbm ln(ratio)                   MdAPE {model_mdape:.2f}%")

        beats_assessor = model_mdape < base_assessor
        beats_county = model_mdape < base_county
        verdict = "SHIP" if (beats_assessor and beats_county) else "DO NOT SHIP"
        log(
            f"verdict: {verdict} "
            f"(beats assessor={beats_assessor}, beats county_median={beats_county})"
        )

        metrics = {
            "mdape_model": round(model_mdape, 3),
            "mdape_baseline_assessor": round(base_assessor, 3),
            "mdape_baseline_county_median": round(base_county, 3),
            "train_rows": int(len(train)),
            "test_rows": int(len(test)),
            "counties": int(df.county_code.nunique()),
            "target": "ln(purchase_amt / emv_total)",
            "features": feats + ["county"],
            "beats_assessor": bool(beats_assessor),
            "beats_county_median": bool(beats_county),
            "verdict": verdict,
        }
        print(json.dumps(metrics, indent=2))

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
                    MODEL_NAME,
                    MODEL_VERSION,
                    "lightgbm_regressor",
                    date.today(),
                    len(train),
                    json.dumps(metrics),
                    bool(beats_assessor and beats_county),
                    "Target ln(sale_to_assessment). is_active set only when the "
                    "model beats BOTH the unadjusted assessor and the county "
                    "median ratio table.",
                ),
            )
        conn.commit()
        log("wrote scoring.models row")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
