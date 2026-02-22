"""
test.py — Smoke tests for the vessel fouling risk pipeline.

Tests each function with minimal inputs to verify it runs correctly.
Run individual sections by commenting out what you don't need.

Usage in Colab:
    !python test.py

Or run one section at a time by copying into a notebook cell.
"""

import os
import sys
import pandas as pd
import numpy as np

# ── Make sure imports work ─────────────────────────────────────────────────
# In Colab, set your working directory to where main.py lives:
# import os; os.chdir("/content/drive/MyDrive/Colab Notebooks/Vessel Project")

print("=" * 60)
print("Pipeline Smoke Tests")
print("=" * 60)

PASS = "  ✓ PASS"
FAIL = "  ✗ FAIL"


# =============================================================================
# TEST 0 — Config loads correctly
# =============================================================================
print("\n[0] config.py")
try:
    from config import (
        GFW_TOKEN, BASE_URL, DATE_FROM, DATE_TO,
        EU_FLAGS, VESSEL_TYPES, EVENT_DATASETS,
        DB_URL, SST_DATASET, CHL_DATASET, MONTHS
    )
    assert GFW_TOKEN != "YOUR_GFW_TOKEN_HERE",   "GFW_TOKEN not set"
    assert "YOUR_PASSWORD" not in DB_URL,         "DB_PASSWORD not set"
    assert len(EU_FLAGS) > 0,                     "EU_FLAGS empty"
    assert len(MONTHS) == 6,                      "Expected 6 months"
    print(f"{PASS} — config loaded, {len(EU_FLAGS)} flags, {len(MONTHS)} months")
except Exception as e:
    print(f"{FAIL} — {e}")


# =============================================================================
# TEST 1 — Database connection and schema
# =============================================================================
print("\n[1] db.py — connection + schema")
try:
    from db import get_engine, create_schema
    engine = get_engine()
    print(f"{PASS} — connected to PostgreSQL")

    create_schema(engine)
    print(f"{PASS} — schema created / verified")

    # Verify tables exist
    from sqlalchemy import text
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """))
        tables = [r[0] for r in result]
    expected = {"vessels", "events", "voyages", "environmental", "vessel_scores"}
    missing  = expected - set(tables)
    if missing:
        print(f"{FAIL} — missing tables: {missing}")
    else:
        print(f"{PASS} — all 5 tables present: {sorted(tables)}")
except Exception as e:
    print(f"{FAIL} — {e}")
    sys.exit("DB connection failed — fix before continuing")


# =============================================================================
# TEST 2 — Step 1: Vessel API (single type + flag only)
# =============================================================================
print("\n[2] step1_vessels.py — fetch_vessels + flatten_vessel")
try:
    from steps.step1_vessels import fetch_vessels, flatten_vessel

    # Single small request — don't run full loop in tests
    entries = fetch_vessels("CARGO", "GBR")
    assert isinstance(entries, list),  "Expected list"
    assert len(entries) > 0,           "No entries returned"
    print(f"{PASS} — fetch_vessels returned {len(entries)} entries")

    row = flatten_vessel(entries[0], "CARGO")
    assert "vessel_id"   in row, "Missing vessel_id"
    assert "mmsi"        in row, "Missing mmsi"
    assert "ship_name"   in row, "Missing ship_name"
    assert "vessel_type_query" in row, "Missing vessel_type_query"
    print(f"{PASS} — flatten_vessel output: {row['ship_name']} | {row['flag']} | {row['vessel_type_gfw']}")

    # Test DB write with 3 rows
    test_rows = [flatten_vessel(e, "CARGO") for e in entries[:3]]
    df_test   = pd.DataFrame(test_rows)
    df_test["usable"] = True

    with engine.connect() as conn:
        conn.execute(text("DELETE FROM vessels WHERE vessel_id = ANY(:ids)"),
                     {"ids": df_test["vessel_id"].tolist()})
        conn.commit()

    df_test.to_sql("vessels", engine, if_exists="append", index=False)

    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM vessels")).scalar()
    print(f"{PASS} — DB write OK, vessels table has {count} row(s)")

except Exception as e:
    print(f"{FAIL} — {e}")


# =============================================================================
# TEST 3 — Step 2: Events API (2 vessel IDs only)
# =============================================================================
print("\n[3] step2_events.py — fetch_events + flatten_event")
try:
    from steps.step2_events import fetch_events, flatten_event

    # Pull 2 vessel_ids from DB to test with
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT vessel_id FROM vessels LIMIT 2")).fetchall()
    test_ids = [r[0] for r in rows]
    assert len(test_ids) > 0, "No vessel_ids in DB — run test 2 first"

    events = fetch_events(test_ids, "public-global-port-visits-c2-events:latest")
    assert isinstance(events, list), "Expected list"
    print(f"{PASS} — fetch_events returned {len(events)} events for {len(test_ids)} vessels")

    if events:
        flat = flatten_event(events[0])
        assert "event_id"     in flat, "Missing event_id"
        assert "vessel_id"    in flat, "Missing vessel_id"
        assert "event_type"   in flat, "Missing event_type"
        assert "duration_hrs" in flat, "Missing duration_hrs"
        print(f"{PASS} — flatten_event output: type={flat['event_type']} | duration={flat['duration_hrs']}h")
    else:
        print(f"  ⚠ No events returned for test vessels (may be normal)")

except Exception as e:
    print(f"{FAIL} — {e}")


# =============================================================================
# TEST 4 — Step 3: Voyage computation (synthetic data)
# =============================================================================
print("\n[4] step3_voyages.py — compute_voyages")
try:
    from steps.step3_voyages import compute_voyages
    

    # Build synthetic events — 3 port visits for one vessel
    synthetic_events = pd.DataFrame([
        {
            "vessel_id":  "test-vessel-001",
            "event_type": "port_visit",
            "start":      pd.Timestamp("2025-07-01 08:00", tz="UTC"),
            "end":        pd.Timestamp("2025-07-03 08:00", tz="UTC"),
            "lat": 51.9, "lon": 4.5, "eez": "NLD",
        },
        {
            "vessel_id":  "test-vessel-001",
            "event_type": "port_visit",
            "start":      pd.Timestamp("2025-07-10 08:00", tz="UTC"),
            "end":        pd.Timestamp("2025-07-12 08:00", tz="UTC"),
            "lat": 53.5, "lon": 9.9, "eez": "DEU",
        },
        {
            "vessel_id":  "test-vessel-001",
            "event_type": "port_visit",
            "start":      pd.Timestamp("2025-07-20 08:00", tz="UTC"),
            "end":        pd.Timestamp("2025-07-22 08:00", tz="UTC"),
            "lat": 55.7, "lon": 12.6, "eez": "DNK",
        },
    ])

    df_voyages = compute_voyages(synthetic_events)

    assert len(df_voyages) == 2,                      f"Expected 2 voyages, got {len(df_voyages)}"
    assert "days_at_sea"  in df_voyages.columns,      "Missing days_at_sea"
    assert "from_lat"     in df_voyages.columns,      "Missing from_lat"
    assert (df_voyages["days_at_sea"] > 0).all(),     "All days_at_sea should be > 0"

    print(f"{PASS} — compute_voyages returned {len(df_voyages)} segments")
    print(f"         days_at_sea: {df_voyages['days_at_sea'].tolist()}")

except Exception as e:
    print(f"{FAIL} — {e}")


# =============================================================================
# TEST 5 — Step 4: CMEMS lookup (one tiny region + date)
# =============================================================================
print("\n[5] step4_environmental.py — CMEMS open + lookup")
try:
    import copernicusmarine
    from steps.step4_environmental import _open_strided, _lookup
    from config import SST_DATASET, SST_STRIDE

    # Tiny request — North Sea, 3 days
    grid = _open_strided(SST_DATASET, "analysed_sst", "2025-07-01", "2025-07-03", SST_STRIDE)
    assert grid is not None, "Grid returned None"
    print(f"{PASS} — SST grid opened: {grid['analysed_sst'].shape}")

    # Lookup for one point
    val = _lookup(grid, "analysed_sst",
                  mid_lat=52.0, mid_lon=4.0,
                  start_dt=pd.Timestamp("2025-07-01"),
                  end_dt=pd.Timestamp("2025-07-03"))

    sst_c = val - 273.15 if not np.isnan(val) else np.nan
    assert not np.isnan(sst_c), "Lookup returned NaN for North Sea — unexpected"
    print(f"{PASS} — SST lookup: {sst_c:.2f}°C (North Sea, Jul 2025)")

    grid.close()

except Exception as e:
    print(f"{FAIL} — {e}")


# =============================================================================
# TEST 6 — Step 5: Scoring functions (synthetic data)
# =============================================================================
print("\n[6] step5_scores.py — _sst_weight + _chl_weight")
try:
    from steps.step5_scores import _sst_weight, _chl_weight

    # SST weights
    assert _sst_weight(np.nan) == 1.0,  "NaN SST should return neutral weight 1.0"
    assert _sst_weight(0)   < 1.0,      "Cold water should reduce weight"
    assert _sst_weight(30)  > 1.0,      "Warm water should increase weight"
    assert _sst_weight(30)  <= 1.5,     "Weight should not exceed cap 1.5"
    assert _sst_weight(0)   >= 0.5,     "Weight should not go below floor 0.5"
    print(f"{PASS} — _sst_weight: 0°C={_sst_weight(0):.2f}, 15°C={_sst_weight(15):.2f}, 30°C={_sst_weight(30):.2f}")

    # CHL weights
    assert _chl_weight(np.nan) == 1.0,  "NaN CHL should return neutral weight 1.0"
    assert _chl_weight(0.1) < 1.0,      "Low CHL should reduce weight"
    assert _chl_weight(5.0) > 1.0,      "High CHL should increase weight"
    print(f"{PASS} — _chl_weight: 0.1={_chl_weight(0.1):.2f}, 1.0={_chl_weight(1.0):.2f}, 5.0={_chl_weight(5.0):.2f}")

except Exception as e:
    print(f"{FAIL} — {e}")


# =============================================================================
# SUMMARY
# =============================================================================
print("\n" + "=" * 60)
print("Tests complete.")
print("Fix any FAIL items before running main.py")
print("=" * 60)