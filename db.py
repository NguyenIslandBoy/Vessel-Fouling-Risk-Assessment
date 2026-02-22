"""
db.py — Database engine creation and schema initialisation.
"""

from sqlalchemy import create_engine, text
from config import DB_URL


def get_engine():
    """Create and return a SQLAlchemy engine. Tests connection on creation."""
    engine = create_engine(DB_URL, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("✓ Connected to PostgreSQL")
    return engine


def create_schema(engine):
    """Create all pipeline tables if they don't already exist."""
    ddl = """
    CREATE TABLE IF NOT EXISTS vessels (
        vessel_id         TEXT PRIMARY KEY,
        imo               TEXT,
        mmsi              TEXT,
        ship_name         TEXT,
        flag              TEXT,
        call_sign         TEXT,
        vessel_type_query TEXT,
        vessel_type_gfw   TEXT,
        length_m          FLOAT,
        tonnage_gt        FLOAT,
        tx_date_from      TEXT,
        tx_date_to        TEXT,
        usable            BOOLEAN,
        loaded_at         TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS events (
        event_id                  TEXT PRIMARY KEY,
        vessel_id                 TEXT REFERENCES vessels(vessel_id),
        event_type                TEXT,
        start                     TIMESTAMPTZ,
        "end"                     TIMESTAMPTZ,
        duration_hrs              FLOAT,
        lat                       FLOAT,
        lon                       FLOAT,
        dist_from_shore_start_km  FLOAT,
        dist_from_shore_end_km    FLOAT,
        dist_from_port_start_km   FLOAT,
        dist_from_port_end_km     FLOAT,
        eez                       TEXT,
        major_fao                 TEXT,
        loaded_at                 TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS voyages (
        voyage_id            SERIAL PRIMARY KEY,
        vessel_id            TEXT REFERENCES vessels(vessel_id),
        port_departure_time  TIMESTAMPTZ,
        port_arrival_time    TIMESTAMPTZ,
        days_at_sea          FLOAT,
        from_lat             FLOAT,
        from_lon             FLOAT,
        to_lat               FLOAT,
        to_lon               FLOAT,
        from_eez             TEXT,
        to_eez               TEXT,
        loaded_at            TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS environmental (
        id           SERIAL PRIMARY KEY,
        voyage_id    INTEGER REFERENCES voyages(voyage_id),
        vessel_id    TEXT    REFERENCES vessels(vessel_id),
        mid_lat      FLOAT,
        mid_lon      FLOAT,
        start_date   DATE,
        end_date     DATE,
        sst_celsius  FLOAT,
        chl_mg_m3    FLOAT,
        loaded_at    TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS vessel_scores (
        vessel_id               TEXT PRIMARY KEY REFERENCES vessels(vessel_id),
        total_days_at_sea       FLOAT,
        n_voyages               INTEGER,
        avg_days_per_voyage     FLOAT,
        total_loitering_hrs     FLOAT,
        total_gap_hrs           FLOAT,
        avg_sst_celsius         FLOAT,
        avg_chl_mg_m3           FLOAT,
        sst_weight              FLOAT,
        chl_weight              FLOAT,
        fouling_exposure_score  FLOAT,
        risk_category           TEXT,
        loaded_at               TIMESTAMP DEFAULT NOW()
    );
    """
    with engine.connect() as conn:
        for stmt in ddl.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))
        conn.commit()
    print("✓ Schema created / verified")