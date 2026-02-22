"""
steps/step1_vessels.py — GFW Vessel API extraction.

Fetches EU-flagged commercial vessels and writes them to the vessels table.
"""

import time
import requests
import pandas as pd
from sqlalchemy import text
from config import (
    BASE_URL, HEADERS, DATE_FROM,
    EU_FLAGS, VESSEL_TYPES, RATE_SLEEP
)


def fetch_vessels(vessel_type: str, flag: str) -> list:
    """Fetch up to 50 vessels for one (type, flag) pair."""
    resp = requests.get(
        f"{BASE_URL}/v3/vessels/search",
        headers=HEADERS,
        params={
            "datasets[0]": "public-global-vessel-identity:latest",
            "where":        f"geartypes='{vessel_type}' AND flag='{flag}'",
            "limit":        50,
            "includes[0]": "MATCH_CRITERIA",
            "includes[1]": "OWNERSHIP",
        }
    )
    if resp.status_code == 429:
        print("   Rate limited — sleeping 15s...")
        time.sleep(15)
        return fetch_vessels(vessel_type, flag)
    if resp.status_code != 200:
        print(f"   ERROR {resp.status_code} on {vessel_type}/{flag}: {resp.text[:150]}")
        return []
    return resp.json().get("entries", [])


def flatten_vessel(entry: dict, vtype_query: str) -> dict:
    """Extract flat fields from a raw GFW vessel entry."""
    reg = (entry.get("registryInfo")        or [{}])[0]
    sri = (entry.get("selfReportedInfo")    or [{}])[0]
    csi = (entry.get("combinedSourcesInfo") or [{}])[0]
    vessel_id = sri.get("id") or csi.get("vesselId")
    return {
        "vessel_id":         vessel_id,
        "imo":               reg.get("imo")      or sri.get("imo"),
        "mmsi":              reg.get("ssvid")    or sri.get("ssvid"),
        "ship_name":         reg.get("shipname") or sri.get("shipname"),
        "flag":              reg.get("flag")     or sri.get("flag"),
        "call_sign":         reg.get("callsign") or sri.get("callsign"),
        "vessel_type_query": vtype_query,
        "vessel_type_gfw":   (reg.get("geartypes") or [None])[0],
        "length_m":          reg.get("lengthM"),
        "tonnage_gt":        reg.get("tonnageGt"),
        "tx_date_from":      reg.get("transmissionDateFrom") or sri.get("transmissionDateFrom"),
        "tx_date_to":        reg.get("transmissionDateTo")   or sri.get("transmissionDateTo"),
    }


def run_step1(engine) -> list:
    """
    Full vessel extraction loop.
    Returns list of usable vessel_ids for the Events API.
    """
    print("\n" + "="*60)
    print("STEP 1 — Fetching vessels from GFW")
    print("="*60)

    all_rows, seen_ids = [], set()

    for vtype in VESSEL_TYPES:
        print(f"\n== {vtype} ==")
        for flag in EU_FLAGS:
            entries = fetch_vessels(vtype, flag)
            for entry in entries:
                row = flatten_vessel(entry, vtype)
                vid = row["vessel_id"]
                if vid and vid not in seen_ids:
                    seen_ids.add(vid)
                    all_rows.append(row)
            time.sleep(RATE_SLEEP)

    df = pd.DataFrame(all_rows)

    # Keep only vessels active during our analysis window
    df["tx_date_to_dt"] = pd.to_datetime(df["tx_date_to"], utc=True, errors="coerce")
    date_from_dt = pd.Timestamp(DATE_FROM, tz="UTC")
    df = df[df["tx_date_to_dt"].isna() | (df["tx_date_to_dt"] >= date_from_dt)].copy()
    df = df.drop(columns=["tx_date_to_dt"])
    df["usable"] = df["vessel_id"].notna() & (df["imo"].notna() | df["mmsi"].notna())

    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE vessel_scores, environmental, voyages, events, vessels CASCADE"))
        conn.commit()
    df.to_sql("vessels", engine, if_exists="append", index=False, method="multi", chunksize=500)

    vessel_ids = df.loc[df["usable"], "vessel_id"].tolist()
    print(f"\n✓ {len(df)} vessels written to PostgreSQL")
    print(f"  Usable for Events API: {len(vessel_ids)}")
    return vessel_ids