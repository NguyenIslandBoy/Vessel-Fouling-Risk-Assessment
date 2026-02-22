"""
steps/step3_voyages.py — Voyage segment computation.

Derives voyage segments from consecutive port visit events
and writes them to the voyages table.
"""

import pandas as pd
from sqlalchemy import text

def compute_voyages(df_events: pd.DataFrame) -> pd.DataFrame:
    """
    For each vessel, compute time-at-sea gaps between consecutive port visits.
    Returns a DataFrame of voyage segments.
    """
    df_ports = df_events[df_events["event_type"] == "port_visit"].copy()
    df_ports = df_ports.sort_values(["vessel_id", "start"])

    voyage_rows = []

    for vessel_id, grp in df_ports.groupby("vessel_id"):
        grp = grp.reset_index(drop=True)
        for i in range(len(grp) - 1):
            departure   = grp.loc[i,   "end"]
            arrival     = grp.loc[i+1, "start"]
            days_at_sea = (arrival - departure).total_seconds() / 86400

            if days_at_sea <= 0:
                continue

            voyage_rows.append({
                "vessel_id":           vessel_id,
                "port_departure_time": departure,
                "port_arrival_time":   arrival,
                "days_at_sea":         round(days_at_sea, 2),
                "from_lat":            grp.loc[i,   "lat"],
                "from_lon":            grp.loc[i,   "lon"],
                "to_lat":              grp.loc[i+1, "lat"],
                "to_lon":              grp.loc[i+1, "lon"],
                "from_eez":            grp.loc[i,   "eez"],
                "to_eez":              grp.loc[i+1, "eez"],
            })

    return pd.DataFrame(voyage_rows)


def run_step3(engine, df_events: pd.DataFrame) -> pd.DataFrame:
    """
    Compute voyages and write to PostgreSQL.
    Returns df_voyages for use in step4.
    """
    print("\n" + "="*60)
    print("STEP 3 — Computing voyage segments")
    print("="*60)

    df_voyages = compute_voyages(df_events)

    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE environmental, voyages CASCADE"))
        conn.commit()
    df_voyages.to_sql("voyages", engine, if_exists="append", index=False, method="multi", chunksize=1000)

    print(f"✓ {len(df_voyages):,} voyage segments written to PostgreSQL")
    print(f"  Vessels with voyages: {df_voyages['vessel_id'].nunique():,}")
    print(f"\n  Days at sea stats:")
    print(df_voyages["days_at_sea"].describe().round(2).to_string())
    return df_voyages