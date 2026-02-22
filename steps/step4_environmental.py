"""
steps/step4_environmental.py — CMEMS environmental data sampling.

Downloads monthly SST and CHL grids (strided to ~1° resolution),
then performs fast local lookups per voyage segment.
"""

import time
import warnings
import numpy as np
import pandas as pd
import copernicusmarine
from sqlalchemy import text
from config import (
    SST_DATASET, CHL_DATASET,
    SST_STRIDE, CHL_STRIDE,
    SPATIAL_BUFFER, MONTHS
)

warnings.filterwarnings("ignore")


def _open_strided(dataset_id: str, variable: str, start_str: str, end_str: str, stride: int):
    """
    Open a CMEMS dataset for one month, downsample via stride, load into memory.
    Returns xarray Dataset or None on failure.
    """
    try:
        raw  = copernicusmarine.open_dataset(
            dataset_id=dataset_id,
            variables=[variable],
            start_datetime=start_str,
            end_datetime=end_str,
        )
        grid = raw.isel(
            latitude =slice(None, None, stride),
            longitude=slice(None, None, stride),
        ).load()
        raw.close()
        return grid
    except Exception as e:
        print(f"  ✗ Failed {variable} {start_str[:7]}: {e}")
        return None


def _lookup(grid, variable: str, mid_lat: float, mid_lon: float,
            start_dt, end_dt) -> float:
    """
    Extract mean value from a pre-loaded grid for a spatial/temporal window.
    Returns np.nan if no data found.
    """
    try:
        subset = grid[variable].sel(
            latitude =slice(mid_lat - SPATIAL_BUFFER, mid_lat + SPATIAL_BUFFER),
            longitude=slice(mid_lon - SPATIAL_BUFFER, mid_lon + SPATIAL_BUFFER),
            time     =slice(start_dt, end_dt),
        )
        vals = subset.values
        if vals.size == 0:
            return np.nan
        return float(np.nanmean(vals))
    except Exception:
        return np.nan


def run_step4(engine, df_voyages: pd.DataFrame) -> pd.DataFrame:
    """
    Sample SST and CHL for every voyage segment.
    Writes results to the environmental table.
    Returns df_env for use in step5.
    """
    print("\n" + "="*60)
    print("STEP 4 — CMEMS environmental sampling")
    print("="*60)

    # Prepare voyages
    df = df_voyages.dropna(subset=["from_lat", "from_lon", "to_lat", "to_lon",
                                    "port_departure_time", "port_arrival_time"]).copy()
    df["mid_lat"] = ((df["from_lat"] + df["to_lat"]) / 2).clip(-89, 89)
    df["mid_lon"] = ((df["from_lon"] + df["to_lon"]) / 2).clip(-179, 179)
    df["month"]   = (
        pd.to_datetime(df["port_departure_time"])
        .dt.tz_localize(None)
        .dt.to_period("M")
    )

    # Join voyage_ids assigned by PostgreSQL SERIAL
    with engine.connect() as conn:
        db_voyages = pd.read_sql(
            "SELECT voyage_id, vessel_id, port_departure_time FROM voyages", conn
        )
    db_voyages["port_departure_time"] = pd.to_datetime(db_voyages["port_departure_time"], utc=True)
    df["port_departure_time"]         = pd.to_datetime(df["port_departure_time"],         utc=True)
    df = df.merge(
        db_voyages[["voyage_id", "vessel_id", "port_departure_time"]],
        on=["vessel_id", "port_departure_time"],
        how="left"
    )

    all_results = []

    for (start_str, end_str) in MONTHS:
        month_period  = pd.Period(start_str[:7], freq="M")
        month_voyages = df[df["month"] == month_period].copy()

        if len(month_voyages) == 0:
            continue

        print(f"\n{month_period} — {len(month_voyages):,} voyages")

        sst_grid = _open_strided(SST_DATASET, "analysed_sst", start_str, end_str, SST_STRIDE)
        if sst_grid is not None:
            print(f"  ✓ SST loaded {sst_grid['analysed_sst'].shape}")

        chl_grid = _open_strided(CHL_DATASET, "CHL", start_str, end_str, CHL_STRIDE)
        if chl_grid is not None:
            print(f"  ✓ CHL loaded {chl_grid['CHL'].shape}")

        for _, row in month_voyages.iterrows():
            start_dt = pd.Timestamp(row["port_departure_time"]).tz_localize(None)
            end_dt   = pd.Timestamp(row["port_arrival_time"]).tz_localize(None)
            if end_dt <= start_dt:
                end_dt = start_dt + pd.Timedelta(days=1)

            sst_k = _lookup(sst_grid, "analysed_sst", row["mid_lat"], row["mid_lon"], start_dt, end_dt) \
                    if sst_grid is not None else np.nan
            sst_c = sst_k - 273.15 if not np.isnan(sst_k) else np.nan
            chl   = _lookup(chl_grid, "CHL",           row["mid_lat"], row["mid_lon"], start_dt, end_dt) \
                    if chl_grid is not None else np.nan

            all_results.append({
                "voyage_id":   row.get("voyage_id"),
                "vessel_id":   row["vessel_id"],
                "mid_lat":     round(row["mid_lat"], 4),
                "mid_lon":     round(row["mid_lon"], 4),
                "start_date":  start_dt.date(),
                "end_date":    end_dt.date(),
                "sst_celsius": round(sst_c, 3) if not np.isnan(sst_c) else None,
                "chl_mg_m3":   round(chl, 4)   if not np.isnan(chl)   else None,
            })

        try:
            if sst_grid: sst_grid.close()
            if chl_grid: chl_grid.close()
        except Exception:
            pass
        del sst_grid, chl_grid
        time.sleep(2)

    df_env = pd.DataFrame(all_results)
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE environmental CASCADE"))
        conn.commit()
    df_env.to_sql("environmental", engine, if_exists="append", index=False, method="multi", chunksize=1000)

    print(f"\n✓ {len(df_env):,} environmental records written to PostgreSQL")
    print(f"  SST coverage: {df_env['sst_celsius'].notna().sum():,} / {len(df_env):,}")
    print(f"  CHL coverage: {df_env['chl_mg_m3'].notna().sum():,} / {len(df_env):,}")
    return df_env