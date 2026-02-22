"""
steps/step5_scores.py — Fouling risk score computation.

Aggregates voyage exposure, loitering, gaps, and environmental
conditions into a normalised 0-100 fouling risk score per vessel.
"""

import numpy as np
import pandas as pd
from sqlalchemy import text
from config import LOITERING_WEIGHT, GAP_WEIGHT


def _sst_weight(sst: float) -> float:
    """
    SST multiplier for fouling risk.
    Cold water (<5°C) = 0.5 (slow growth), warm water (30°C) = 1.5 (fast growth).
    Returns 1.0 (neutral) if SST is missing.
    """
    if pd.isna(sst):
        return 1.0
    return float(max(0.5, min(1.5, 0.5 + (sst / 30))))


def _chl_weight(chl: float) -> float:
    """
    Chlorophyll-a multiplier for fouling risk (log scale).
    Low CHL (0.1 mg/m³) = 0.7, moderate (1.0) = 1.0, high (5.0+) = 1.4.
    Returns 1.0 (neutral) if CHL is missing.
    """
    if pd.isna(chl):
        return 1.0
    return float(max(0.5, min(1.5, 1.0 + 0.2 * np.log10(max(chl, 0.01)))))


def run_step5(engine, df_voyages: pd.DataFrame,
              df_events: pd.DataFrame, df_env: pd.DataFrame):
    """
    Compute per-vessel fouling exposure scores and write to vessel_scores table.
    """
    print("\n" + "="*60)
    print("STEP 5 — Computing fouling risk scores")
    print("="*60)

    # ── Aggregate voyage days ──────────────────────────────────────────────
    voyage_agg = df_voyages.groupby("vessel_id").agg(
        total_days_at_sea   = ("days_at_sea", "sum"),
        n_voyages           = ("days_at_sea", "count"),
        avg_days_per_voyage = ("days_at_sea", "mean"),
    ).reset_index()

    # ── Aggregate loitering hours ──────────────────────────────────────────
    loiter_agg = (
        df_events[df_events["event_type"] == "loitering"]
        .groupby("vessel_id")["duration_hrs"].sum()
        .reset_index()
        .rename(columns={"duration_hrs": "total_loitering_hrs"})
    )

    # ── Aggregate AIS gap hours ────────────────────────────────────────────
    gap_agg = (
        df_events[df_events["event_type"] == "gap"]
        .groupby("vessel_id")["duration_hrs"].sum()
        .reset_index()
        .rename(columns={"duration_hrs": "total_gap_hrs"})
    )

    # ── Average environmental conditions per vessel ────────────────────────
    env_agg = (
        df_env.groupby("vessel_id")
        .agg(
            avg_sst_celsius = ("sst_celsius", "mean"),
            avg_chl_mg_m3   = ("chl_mg_m3",   "mean"),
        )
        .reset_index()
    )

    # ── Pull vessel list from DB ───────────────────────────────────────────
    with engine.connect() as conn:
        df_vessels = pd.read_sql(
            "SELECT vessel_id FROM vessels WHERE usable = TRUE", conn
        )

    # ── Merge all signals ──────────────────────────────────────────────────
    df_scored = (
        df_vessels
        .merge(voyage_agg, on="vessel_id", how="left")
        .merge(loiter_agg, on="vessel_id", how="left")
        .merge(gap_agg,    on="vessel_id", how="left")
        .merge(env_agg,    on="vessel_id", how="left")
    )

    for col in ["total_days_at_sea", "n_voyages", "avg_days_per_voyage",
                "total_loitering_hrs", "total_gap_hrs"]:
        df_scored[col] = df_scored[col].fillna(0)

    # ── Compute weights ────────────────────────────────────────────────────
    df_scored["sst_weight"] = df_scored["avg_sst_celsius"].apply(_sst_weight)
    df_scored["chl_weight"] = df_scored["avg_chl_mg_m3"].apply(_chl_weight)

    # ── Composite score ────────────────────────────────────────────────────
    raw_score = (
        df_scored["total_days_at_sea"]   * df_scored["sst_weight"] * df_scored["chl_weight"]
        + df_scored["total_loitering_hrs"] / 24 * LOITERING_WEIGHT
        + df_scored["total_gap_hrs"]       / 24 * GAP_WEIGHT
    )
    max_score = raw_score.max()
    df_scored["fouling_exposure_score"] = (
        (raw_score / max_score * 100).round(1) if max_score > 0 else 0.0
    )

    # ── Risk category ──────────────────────────────────────────────────────
    df_scored["risk_category"] = pd.cut(
        df_scored["fouling_exposure_score"],
        bins=[0, 25, 50, 75, 100],
        labels=["Low", "Medium", "High", "Critical"],
        include_lowest=True
    ).astype(str)

    # ── Write to DB ────────────────────────────────────────────────────────
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE vessel_scores CASCADE"))
        conn.commit()
    df_scored.to_sql("vessel_scores", engine, if_exists="append", index=False, method="multi", chunksize=500)

    print(f"✓ {len(df_scored):,} vessel scores written to PostgreSQL")
    print(f"\nRisk distribution:")
    print(df_scored["risk_category"].value_counts().to_string())

    print(f"\nTop 10 by fouling exposure score:")
    with engine.connect() as conn:
        top10 = pd.read_sql("""
            SELECT v.ship_name, v.flag, v.vessel_type_query,
                   s.total_days_at_sea, s.avg_sst_celsius,
                   s.avg_chl_mg_m3, s.fouling_exposure_score, s.risk_category
            FROM vessel_scores s
            JOIN vessels v ON s.vessel_id = v.vessel_id
            ORDER BY s.fouling_exposure_score DESC
            LIMIT 10
        """, conn)
    print(top10.to_string(index=False))