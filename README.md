# Vessel-Fouling-Risk-Assessment
An end-to-end ETL pipeline that combines vessel tracking data with oceanographic data to compute fouling risk scores for commercial vessels.# Vessel Fouling Risk Assessment Pipeline

## Overview
An end-to-end ETL pipeline that combines vessel tracking data with oceanographic data to compute fouling risk scores for commercial vessels. The pipeline extracts vessel movement events, derives voyage segments, samples environmental conditions, and produces a normalised fouling exposure score per vessel — visualised in a Power BI dashboard.

## Pipeline Architecture

```
GFW Vessel API → GFW Events API → Voyage Computation
                                        ↓
                              CMEMS Environmental Sampling
                              (SST + Chlorophyll-a)
                                        ↓
                              Fouling Risk Scoring
                                        ↓
                              PostgreSQL Database
                                        ↓
                              Power BI Dashboard
```

## Data Sources
- **Global Fishing Watch (GFW) API v3** — vessel identities, port visits, loitering events, AIS gaps
- **CMEMS OSTIA SST** — daily sea surface temperature (global, 0.05° resolution)
- **CMEMS Ocean Colour CHL** — daily chlorophyll-a concentration (global, 4km resolution)

Both data sources require free registration:
- GFW: https://globalfishingwatch.org/our-apis/
- CMEMS: https://data.marine.copernicus.eu/register

## Scoring Methodology
Fouling risk is computed per vessel using:

```
score = (days_at_sea × SST_weight × CHL_weight)
      + (loitering_hrs / 24 × 0.5)
      + (gap_hrs / 24 × 0.3)
```

Where:
- `SST_weight` scales 0.5–1.5 based on sea surface temperature (warmer = higher risk)
- `CHL_weight` scales 0.5–1.5 based on chlorophyll-a on a log scale (more phytoplankton = higher risk)
- Weights default to 1.0 (neutral) where environmental data is unavailable

Final score is normalised 0–100 across the fleet.

**Risk categories:**
| Category | Score Range |
|---|---|
| Low | 0 – 25 |
| Medium | 25 – 50 |
| High | 50 – 75 |
| Critical | 75 – 100 |

## Key Findings

### Fleet Overview
- The fleet spans major global shipping lanes including European waters, Southeast Asia, and the North Atlantic — consistent with EU-flagged commercial shipping patterns
- Cargo vessels represent the largest vessel type segment, followed by tankers and carriers
- Total loitering hours across the fleet are substantial, indicating loitering is a meaningful contributor to fouling exposure beyond sailing time alone

### Fouling Risk Rankings
- A small proportion of vessels fall into the Critical risk category, providing a focused and actionable target list for hull inspection or recoating
- There is a clear positive correlation between total days at sea and fouling exposure score — the model behaves as expected
- High-scoring vessels tend to combine long sailing periods with elevated loitering hours, suggesting extended anchorage in warm waters is a key risk driver
- Fleet average score sits in the Medium risk band, meaning most vessels have moderate but manageable fouling exposure

### Environmental Conditions
- Sea surface temperature and chlorophyll-a both decline steadily from July to December — the expected Northern Hemisphere seasonal cooling pattern
- The negative correlation between SST and CHL is scientifically consistent: warm oligotrophic waters have lower phytoplankton concentrations
- The majority of voyages occur in cold waters (below 10°C), reflecting heavy Arctic and North Sea routing among EU-flagged vessels
- This cold-water concentration partly explains environmental data gaps, as CMEMS chlorophyll retrievals are limited at high latitudes in winter due to reduced sunlight and cloud cover

## Project Structure

```
vessel_project/
├── main.py                     # Pipeline orchestrator
├── config.py                   # Constants and environment variable loading
├── db.py                       # PostgreSQL engine and schema creation
├── test.py                     # Smoke tests for all modules
├── requirements.txt
├── .env.example                # Environment variable template
└── steps/
    ├── __init__.py
    ├── step1_vessels.py        # GFW Vessel API extraction
    ├── step2_events.py         # GFW Events API extraction
    ├── step3_voyages.py        # Voyage segment computation
    ├── step4_environmental.py  # CMEMS SST + CHL sampling
    └── step5_scores.py         # Fouling risk score computation
```

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure environment variables
Copy `.env.example` to `.env` and fill in your credentials:
```bash
cp .env.example .env
```

```
GFW_TOKEN=your_gfw_token_here
PGHOST=your_server.postgres.database.azure.com
PGUSER=your_admin_username
PGPASSWORD=your_password_here
PGDATABASE=postgres
PGPORT=5432
```

### 3. Run smoke tests
Verify all components work before running the full pipeline:
```bash
python test.py
```

Expected output: all 6 tests passing.

### 4. Run the pipeline
```bash
python main.py
```

## Database Schema

```
vessels ──< events
vessels ──< voyages ──< environmental
vessels ──< vessel_scores
```

| Table | Description |
|---|---|
| `vessels` | Vessel identities and metadata |
| `events` | Port visits, loitering, encounters, AIS gaps |
| `voyages` | Derived voyage segments between port visits |
| `environmental` | SST and CHL sampled per voyage midpoint |
| `vessel_scores` | Aggregated fouling risk scores per vessel |

## Tech Stack
- **Python** — pipeline, data processing
- **PostgreSQL** — data storage
- **SQLAlchemy + psycopg2** — database connectivity
- **xarray + copernicusmarine** — CMEMS data access
- **pandas + numpy** — data transformation
- **Power BI** — dashboard and reporting

## Notes on Environmental Data Coverage
CMEMS chlorophyll-a data has known gaps in polar regions (no sunlight in winter) and under heavy cloud cover. Where SST or CHL data is unavailable, environmental weights default to 1.0 (neutral) so the fouling score still reflects vessel exposure time.

## License
This project uses data from:
- Global Fishing Watch — subject to [GFW Terms of Use](https://globalfishingwatch.org/terms-of-use/)
- Copernicus Marine Service — subject to [CMEMS Licence](https://marine.copernicus.eu/user-corner/service-commitments-and-licence)

Both are free for non-commercial research use.
