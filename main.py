"""
main.py — Vessel Fouling Risk Pipeline Orchestrator

Runs all 5 steps in sequence:
  1. Vessel extraction     (GFW Vessel API)
  2. Events extraction     (GFW Events API)
  3. Voyage computation    (derived from port visits)
  4. Environmental data    (CMEMS SST + CHL)
  5. Fouling risk scores   (composite scoring model)

Usage:
  python main.py

  Or in Colab:
  !python main.py

  Or step-by-step (import individually):
  from db import get_engine, create_schema
  from steps.step1_vessels import run_step1
  ...
"""

import os
from db import get_engine, create_schema
from steps.step1_vessels      import run_step1
from steps.step2_events       import run_step2
from steps.step3_voyages      import run_step3
from steps.step4_environmental import run_step4
from steps.step5_scores       import run_step5


# def main():
#     print("=" * 60)
#     print("Vessel Fouling Risk Pipeline")
#     print("=" * 60)

#     # ── Database connection ────────────────────────────────────────────────
#     engine = get_engine()
#     create_schema(engine)

#     # ── Step 1: Vessels ────────────────────────────────────────────────────
#     vessel_ids = run_step1(engine)

#     # ── Step 2: Events ─────────────────────────────────────────────────────
#     df_events = run_step2(engine, vessel_ids)

#     # ── Step 3: Voyages ────────────────────────────────────────────────────
#     df_voyages = run_step3(engine, df_events)

#     # ── Step 4: Environmental ──────────────────────────────────────────────
#     df_env = run_step4(engine, df_voyages)

#     # ── Step 5: Scores ─────────────────────────────────────────────────────
#     run_step5(engine, df_voyages, df_events, df_env)

#     print("\n" + "=" * 60)
#     print("Pipeline complete. All data in PostgreSQL.")
#     print("=" * 60)

#================================================
# To get data from db and run step # onwards
#================================================
def main():
    engine = get_engine()
    create_schema(engine)

    # vessel_ids = run_step1(engine)
    # df_events  = run_step2(engine, vessel_ids)

    # Load from DB instead
    from sqlalchemy import text
    import pandas as pd

    with engine.connect() as conn:
        vessel_ids = pd.read_sql("SELECT vessel_id FROM vessels WHERE usable = TRUE", conn)["vessel_id"].tolist()
        df_events  = pd.read_sql("SELECT * FROM events", conn)
        df_events["start"] = pd.to_datetime(df_events["start"], utc=True)
        df_events["end"]   = pd.to_datetime(df_events["end"],   utc=True)

    df_voyages = run_step3(engine, df_events)
    df_env     = run_step4(engine, df_voyages)
    run_step5(engine, df_voyages, df_events, df_env)


if __name__ == "__main__":
    main()