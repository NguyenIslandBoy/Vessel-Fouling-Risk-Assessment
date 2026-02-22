"""
config.py — All configuration constants for the vessel fouling pipeline.
Edit this file when changing time windows, credentials, or API settings.
"""
from dotenv import load_dotenv
import os
from urllib.parse import quote_plus
load_dotenv()
# =============================================================================
# GFW API
# =============================================================================
GFW_TOKEN = os.getenv("GFW_TOKEN")
BASE_URL  = "https://gateway.api.globalfishingwatch.org"
HEADERS   = {"Authorization": f"Bearer {GFW_TOKEN}"}

DATE_FROM = "2025-07-01"
DATE_TO   = "2025-12-31"

EU_FLAGS = [
    "GBR", "DEU", "GRC", "NOR", "DNK", "NLD", "ITA",
    "FRA", "ESP", "PRT", "SWE", "FIN", "BEL", "POL",
    "HRV", "MLT", "CYP", "EST", "LVA", "LTU", "IRL"
]

VESSEL_TYPES = ["CARGO", "BUNKER_OR_TANKER", "CARRIER", "SUPPORT", "OTHER_NON_FISHING"]

EVENT_DATASETS = {
    "PORT_VISIT": "public-global-port-visits-c2-events:latest",
    "LOITERING":  "public-global-loitering-events:latest",
    "ENCOUNTER":  "public-global-encounters-events:latest",
    "GAP":        "public-global-gaps-events:latest",
}

BATCH_SIZE  = 10    # vessel IDs per Events API request
EVENT_LIMIT = 50    # max events per page
RATE_SLEEP  = 0.4   # seconds between GFW requests

# =============================================================================
# PostgreSQL (Azure)
# =============================================================================
DB_HOST     = os.getenv("DB_HOST")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME     = os.getenv("DB_NAME")
DB_PORT     = os.getenv("DB_PORT")

DB_safe_PASSWORD = quote_plus(DB_PASSWORD)

DB_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_safe_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
)

# =============================================================================
# CMEMS
# =============================================================================
SST_DATASET    = "METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2"
CHL_DATASET    = "cmems_obs-oc_glo_bgc-plankton_my_l4-gapfree-multi-4km_P1D"
SST_STRIDE     = 20       # 0.05° × 20 = ~1° effective resolution
CHL_STRIDE     = 25       # 0.04° × 25 = ~1° effective resolution
SPATIAL_BUFFER = 2.0      # degrees for voyage-level spatial window

MONTHS = [
    ("2025-07-01", "2025-07-31"),
    ("2025-08-01", "2025-08-31"),
    ("2025-09-01", "2025-09-30"),
    ("2025-10-01", "2025-10-31"),
    ("2025-11-01", "2025-11-30"),
    ("2025-12-01", "2025-12-31"),
]

# =============================================================================
# SCORING WEIGHTS
# =============================================================================
LOITERING_WEIGHT = 0.5   # loitering days relative to sailing days
GAP_WEIGHT       = 0.3   # AIS gap days relative to sailing days