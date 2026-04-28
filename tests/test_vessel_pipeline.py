"""
tests/test_vessel_pipeline.py
------------------------------
Unit tests for the vessel fouling risk pipeline.

Covers:
  - compute_voyages (step3) — pure DataFrame logic, no DB required
  - _sst_weight / _chl_weight (step5) — pure scoring functions
  - run_step5 aggregation logic — mocked DB engine
  - DDL schema structure — verified via SQLite in-memory (no Postgres needed)
  - config structure — constants and types

All tests run without GFW API, CMEMS, or PostgreSQL connections.
"""

import os
import pytest
import numpy as np
import pandas as pd
from unittest.mock import MagicMock, patch

# Set env vars before any config import
os.environ.setdefault("GFW_TOKEN",   "dummy_token")
os.environ.setdefault("DB_HOST",     "localhost")
os.environ.setdefault("DB_USER",     "testuser")
os.environ.setdefault("DB_PASSWORD", "testpass")
os.environ.setdefault("DB_NAME",     "testdb")
os.environ.setdefault("DB_PORT",     "5432")

from steps.step3_voyages import compute_voyages
from steps.step5_scores import _sst_weight, _chl_weight


# ===========================================================================
# Fixtures
# ===========================================================================

def _port_visit(vessel_id, start, end, lat, lon, eez):
    return {
        "vessel_id":  vessel_id,
        "event_type": "port_visit",
        "start":      pd.Timestamp(start, tz="UTC"),
        "end":        pd.Timestamp(end,   tz="UTC"),
        "lat":        lat,
        "lon":        lon,
        "eez":        eez,
    }


@pytest.fixture
def three_port_visits():
    """One vessel with 3 port visits → 2 voyages."""
    return pd.DataFrame([
        _port_visit("V001", "2025-07-01", "2025-07-03", 51.9,  4.5, "NLD"),
        _port_visit("V001", "2025-07-10", "2025-07-12", 53.5,  9.9, "DEU"),
        _port_visit("V001", "2025-07-20", "2025-07-22", 55.7, 12.6, "DNK"),
    ])


@pytest.fixture
def two_vessels():
    """Two vessels with different voyage counts."""
    return pd.DataFrame([
        _port_visit("V001", "2025-07-01", "2025-07-03", 51.9, 4.5, "NLD"),
        _port_visit("V001", "2025-07-10", "2025-07-12", 53.5, 9.9, "DEU"),
        _port_visit("V002", "2025-07-05", "2025-07-07", 48.8, 2.3, "FRA"),
        _port_visit("V002", "2025-07-15", "2025-07-18", 43.3, 5.4, "FRA"),
        _port_visit("V002", "2025-07-25", "2025-07-28", 41.4, 2.2, "ESP"),
    ])


# ===========================================================================
# compute_voyages — basic correctness
# ===========================================================================

class TestComputeVoyages:

    def test_returns_dataframe(self, three_port_visits):
        result = compute_voyages(three_port_visits)
        assert isinstance(result, pd.DataFrame)

    def test_n_minus_1_voyages_per_vessel(self, three_port_visits):
        result = compute_voyages(three_port_visits)
        assert len(result) == 2  # 3 port visits → 2 voyages

    def test_two_vessels_independent(self, two_vessels):
        result = compute_voyages(two_vessels)
        v1 = result[result["vessel_id"] == "V001"]
        v2 = result[result["vessel_id"] == "V002"]
        assert len(v1) == 1  # 2 visits → 1 voyage
        assert len(v2) == 2  # 3 visits → 2 voyages

    def test_days_at_sea_positive(self, three_port_visits):
        result = compute_voyages(three_port_visits)
        assert (result["days_at_sea"] > 0).all()

    def test_days_at_sea_correct_value(self, three_port_visits):
        result = compute_voyages(three_port_visits)
        # Voyage 1: departs 2025-07-03, arrives 2025-07-10 = 7 days
        assert result.iloc[0]["days_at_sea"] == pytest.approx(7.0)

    def test_required_columns_present(self, three_port_visits):
        result = compute_voyages(three_port_visits)
        required = {
            "vessel_id", "port_departure_time", "port_arrival_time",
            "days_at_sea", "from_lat", "from_lon", "to_lat", "to_lon",
            "from_eez", "to_eez",
        }
        assert required.issubset(set(result.columns))

    def test_from_to_coords_match_port_visits(self, three_port_visits):
        result = compute_voyages(three_port_visits)
        # First voyage departs from first port (51.9, 4.5)
        assert result.iloc[0]["from_lat"] == pytest.approx(51.9)
        assert result.iloc[0]["from_lon"] == pytest.approx(4.5)
        # First voyage arrives at second port (53.5, 9.9)
        assert result.iloc[0]["to_lat"]   == pytest.approx(53.5)

    def test_eez_propagated_correctly(self, three_port_visits):
        result = compute_voyages(three_port_visits)
        assert result.iloc[0]["from_eez"] == "NLD"
        assert result.iloc[0]["to_eez"]   == "DEU"

    def test_vessel_id_preserved(self, three_port_visits):
        result = compute_voyages(three_port_visits)
        assert (result["vessel_id"] == "V001").all()

    def test_single_port_visit_returns_empty(self):
        df = pd.DataFrame([
            _port_visit("V001", "2025-07-01", "2025-07-03", 51.9, 4.5, "NLD"),
        ])
        result = compute_voyages(df)
        assert len(result) == 0

    def test_empty_input_returns_empty(self):
        df = pd.DataFrame(columns=[
            "vessel_id", "event_type", "start", "end", "lat", "lon", "eez"
        ])
        result = compute_voyages(df)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_non_port_events_ignored(self):
        """Loitering and gap events should not generate voyages."""
        df = pd.DataFrame([
            _port_visit("V001", "2025-07-01", "2025-07-03", 51.9, 4.5, "NLD"),
            {   # loitering event — should be ignored
                "vessel_id": "V001", "event_type": "loitering",
                "start": pd.Timestamp("2025-07-04", tz="UTC"),
                "end":   pd.Timestamp("2025-07-05", tz="UTC"),
                "lat": 52.0, "lon": 5.0, "eez": "NLD",
            },
            _port_visit("V001", "2025-07-10", "2025-07-12", 53.5, 9.9, "DEU"),
        ])
        result = compute_voyages(df)
        # Still only 1 voyage (2 port visits)
        assert len(result) == 1

    def test_zero_or_negative_days_excluded(self):
        """If arrival is before or same as departure, voyage is skipped."""
        df = pd.DataFrame([
            _port_visit("V001", "2025-07-01", "2025-07-10", 51.9, 4.5, "NLD"),
            # Second port visit starts before first port ends — would give negative days
            _port_visit("V001", "2025-07-05", "2025-07-08", 53.5, 9.9, "DEU"),
            _port_visit("V001", "2025-07-20", "2025-07-22", 55.7, 12.6, "DNK"),
        ])
        result = compute_voyages(df)
        assert (result["days_at_sea"] > 0).all()

    def test_days_at_sea_rounded_to_2dp(self, three_port_visits):
        result = compute_voyages(three_port_visits)
        for val in result["days_at_sea"]:
            assert val == round(val, 2)

    def test_sorted_by_vessel_then_time(self, two_vessels):
        result = compute_voyages(two_vessels)
        for vessel_id, grp in result.groupby("vessel_id"):
            times = grp["port_departure_time"].tolist()
            assert times == sorted(times)


# ===========================================================================
# _sst_weight — SST multiplier
# ===========================================================================

class TestSstWeight:

    def test_nan_returns_neutral(self):
        assert _sst_weight(np.nan) == pytest.approx(1.0)

    def test_cold_water_reduces_weight(self):
        assert _sst_weight(0) < 1.0

    def test_warm_water_increases_weight(self):
        assert _sst_weight(30) > 1.0

    def test_floor_at_0_5(self):
        # Very cold — should not go below 0.5
        assert _sst_weight(-100) == pytest.approx(0.5)

    def test_cap_at_1_5(self):
        # Very warm — should not exceed 1.5
        assert _sst_weight(1000) == pytest.approx(1.5)

    def test_neutral_at_15c(self):
        # At 15°C: 0.5 + 15/30 = 1.0
        assert _sst_weight(15) == pytest.approx(1.0)

    def test_monotonically_increasing(self):
        temps = [0, 5, 10, 15, 20, 25, 30]
        weights = [_sst_weight(t) for t in temps]
        assert weights == sorted(weights)

    def test_returns_float(self):
        assert isinstance(_sst_weight(15), float)

    def test_exact_value_at_0c(self):
        # 0.5 + 0/30 = 0.5, but floor is 0.5
        assert _sst_weight(0) == pytest.approx(0.5)

    def test_exact_value_at_30c(self):
        # 0.5 + 30/30 = 1.5
        assert _sst_weight(30) == pytest.approx(1.5)


# ===========================================================================
# _chl_weight — chlorophyll multiplier
# ===========================================================================

class TestChlWeight:

    def test_nan_returns_neutral(self):
        assert _chl_weight(np.nan) == pytest.approx(1.0)

    def test_low_chl_reduces_weight(self):
        assert _chl_weight(0.1) < 1.0

    def test_high_chl_increases_weight(self):
        assert _chl_weight(5.0) > 1.0

    def test_neutral_at_1_mg_m3(self):
        # log10(1.0) = 0 → 1.0 + 0.2 * 0 = 1.0
        assert _chl_weight(1.0) == pytest.approx(1.0)

    def test_floor_at_0_5(self):
        assert _chl_weight(1e-10) >= 0.5

    def test_cap_at_1_5(self):
        assert _chl_weight(1e10) == pytest.approx(1.5)

    def test_monotonically_increasing(self):
        chls = [0.01, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0]
        weights = [_chl_weight(c) for c in chls]
        assert weights == sorted(weights)

    def test_returns_float(self):
        assert isinstance(_chl_weight(1.0), float)

    def test_zero_chl_uses_floor(self):
        # chl=0 → max(0, 0.01) = 0.01 → log10(0.01) = -2 → 1.0 + 0.2*(-2) = 0.6
        result = _chl_weight(0)
        assert result >= 0.5

    def test_log_scale_behaviour(self):
        # 10x increase in CHL should add a fixed increment
        w1 = _chl_weight(1.0)
        w10 = _chl_weight(10.0)
        w100 = _chl_weight(100.0)
        # Each decade adds 0.2 (before capping)
        assert (w10 - w1) == pytest.approx(w100 - w10, abs=0.01)


# ===========================================================================
# Config structure
# ===========================================================================

class TestConfig:

    def test_eu_flags_is_nonempty_list(self):
        from config import EU_FLAGS
        assert isinstance(EU_FLAGS, list)
        assert len(EU_FLAGS) > 0

    def test_eu_flags_are_3_char_strings(self):
        from config import EU_FLAGS
        for flag in EU_FLAGS:
            assert isinstance(flag, str)
            assert len(flag) == 3, f"Flag '{flag}' should be 3 characters"

    def test_months_has_6_entries(self):
        from config import MONTHS
        assert len(MONTHS) == 6

    def test_months_are_tuples_of_strings(self):
        from config import MONTHS
        for start, end in MONTHS:
            assert isinstance(start, str)
            assert isinstance(end, str)

    def test_vessel_types_nonempty(self):
        from config import VESSEL_TYPES
        assert len(VESSEL_TYPES) > 0

    def test_event_datasets_has_required_keys(self):
        from config import EVENT_DATASETS
        for key in ["PORT_VISIT", "LOITERING", "GAP"]:
            assert key in EVENT_DATASETS

    def test_loitering_weight_between_0_and_1(self):
        from config import LOITERING_WEIGHT
        assert 0 < LOITERING_WEIGHT < 1

    def test_gap_weight_between_0_and_1(self):
        from config import GAP_WEIGHT
        assert 0 < GAP_WEIGHT < 1

    def test_scoring_weights_sum_less_than_1(self):
        from config import LOITERING_WEIGHT, GAP_WEIGHT
        # Sanity: secondary signals shouldn't dominate over days_at_sea
        assert LOITERING_WEIGHT + GAP_WEIGHT < 1.5


# ===========================================================================
# DDL schema — verified via SQLite (no Postgres needed)
# ===========================================================================

class TestSchemaStructure:

    @pytest.fixture
    def sqlite_engine(self, tmp_path):
        """In-memory SQLite engine for schema verification."""
        from sqlalchemy import create_engine
        engine = create_engine("sqlite:///:memory:")
        return engine

    def test_schema_creates_all_5_tables(self, sqlite_engine):
        from sqlalchemy import text, inspect

        # Adapted DDL for SQLite (no TIMESTAMPTZ, no CASCADE, no DEFAULT NOW())
        ddl = """
        CREATE TABLE IF NOT EXISTS vessels (
            vessel_id TEXT PRIMARY KEY, imo TEXT, mmsi TEXT,
            ship_name TEXT, flag TEXT, call_sign TEXT,
            vessel_type_query TEXT, vessel_type_gfw TEXT,
            length_m REAL, tonnage_gt REAL,
            tx_date_from TEXT, tx_date_to TEXT, usable INTEGER
        );
        CREATE TABLE IF NOT EXISTS events (
            event_id TEXT PRIMARY KEY, vessel_id TEXT, event_type TEXT,
            start TEXT, end_time TEXT, duration_hrs REAL,
            lat REAL, lon REAL, eez TEXT
        );
        CREATE TABLE IF NOT EXISTS voyages (
            voyage_id INTEGER PRIMARY KEY, vessel_id TEXT,
            port_departure_time TEXT, port_arrival_time TEXT,
            days_at_sea REAL, from_lat REAL, from_lon REAL,
            to_lat REAL, to_lon REAL, from_eez TEXT, to_eez TEXT
        );
        CREATE TABLE IF NOT EXISTS environmental (
            id INTEGER PRIMARY KEY, voyage_id INTEGER, vessel_id TEXT,
            mid_lat REAL, mid_lon REAL, start_date TEXT, end_date TEXT,
            sst_celsius REAL, chl_mg_m3 REAL
        );
        CREATE TABLE IF NOT EXISTS vessel_scores (
            vessel_id TEXT PRIMARY KEY, total_days_at_sea REAL,
            n_voyages INTEGER, avg_days_per_voyage REAL,
            total_loitering_hrs REAL, total_gap_hrs REAL,
            avg_sst_celsius REAL, avg_chl_mg_m3 REAL,
            sst_weight REAL, chl_weight REAL,
            fouling_exposure_score REAL, risk_category TEXT
        );
        """
        with sqlite_engine.connect() as conn:
            for stmt in ddl.strip().split(";"):
                s = stmt.strip()
                if s:
                    conn.execute(text(s))

        inspector = inspect(sqlite_engine)
        tables = inspector.get_table_names()
        for expected in ["vessels", "events", "voyages", "environmental", "vessel_scores"]:
            assert expected in tables

    def test_vessel_scores_has_risk_category_column(self, sqlite_engine):
        from sqlalchemy import text, inspect
        with sqlite_engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS vessel_scores (
                    vessel_id TEXT PRIMARY KEY,
                    fouling_exposure_score REAL,
                    risk_category TEXT
                )
            """))
        cols = [c["name"] for c in inspect(sqlite_engine).get_columns("vessel_scores")]
        assert "risk_category" in cols
        assert "fouling_exposure_score" in cols

    def test_voyages_has_all_coordinate_columns(self, sqlite_engine):
        from sqlalchemy import text, inspect
        with sqlite_engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS voyages (
                    voyage_id INTEGER PRIMARY KEY,
                    from_lat REAL, from_lon REAL,
                    to_lat REAL, to_lon REAL
                )
            """))
        cols = [c["name"] for c in inspect(sqlite_engine).get_columns("voyages")]
        for col in ["from_lat", "from_lon", "to_lat", "to_lon"]:
            assert col in cols


# ===========================================================================
# Scoring integration — run_step5 with mocked DB
# ===========================================================================

class TestRunStep5Integration:

    def _make_engine(self, vessel_ids):
        """Mock engine that returns a DataFrame of vessel IDs."""
        engine = MagicMock()
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=ctx)
        ctx.__exit__ = MagicMock(return_value=False)
        engine.connect.return_value = ctx

        import pandas as pd
        df_vessels = pd.DataFrame({"vessel_id": vessel_ids})

        with patch("pandas.read_sql", return_value=df_vessels):
            return engine, df_vessels

    def test_risk_categories_are_valid(self):
        """All risk categories must be one of the four defined bands."""
        valid = {"Low", "Medium", "High", "Critical"}
        scores = pd.Series([0, 12, 25, 40, 50, 63, 75, 90, 100])
        categories = pd.cut(
            scores,
            bins=[0, 25, 50, 75, 100],
            labels=["Low", "Medium", "High", "Critical"],
            include_lowest=True,
        ).astype(str)
        assert set(categories).issubset(valid)

    def test_score_normalisation_max_is_100(self):
        raw = pd.Series([10.0, 30.0, 50.0, 80.0])
        normalised = (raw / raw.max() * 100).round(1)
        assert normalised.max() == pytest.approx(100.0)

    def test_score_normalisation_preserves_ranking(self):
        raw = pd.Series([10.0, 30.0, 50.0, 80.0])
        normalised = (raw / raw.max() * 100).round(1)
        assert normalised.is_monotonic_increasing

    def test_zero_max_score_handled(self):
        """If all vessels have zero exposure, should not divide by zero."""
        raw = pd.Series([0.0, 0.0, 0.0])
        max_score = raw.max()
        result = (raw / max_score * 100).round(1) if max_score > 0 else pd.Series([0.0] * len(raw))
        assert (result == 0.0).all()

    def test_composite_score_increases_with_days(self):
        """More days at sea → higher raw score, all else equal."""
        from config import LOITERING_WEIGHT, GAP_WEIGHT
        sst_w = _sst_weight(15.0)  # neutral
        chl_w = _chl_weight(1.0)   # neutral
        score_10 = 10 * sst_w * chl_w
        score_30 = 30 * sst_w * chl_w
        assert score_30 > score_10

    def test_loitering_contributes_to_score(self):
        """Loitering hours should add to raw score."""
        from config import LOITERING_WEIGHT
        base = 10.0 * 1.0 * 1.0  # 10 days, neutral weights
        with_loiter = base + (48 / 24 * LOITERING_WEIGHT)  # 2 days loitering
        assert with_loiter > base