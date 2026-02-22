"""
steps/step2_events.py — GFW Events API extraction.

Fetches port visits, loitering, encounters, and AIS gaps
for all vessels and writes them to the events table.
"""

import time
import requests
import pandas as pd
from config import (
    BASE_URL, HEADERS, DATE_FROM, DATE_TO,
    EVENT_DATASETS, BATCH_SIZE, EVENT_LIMIT, RATE_SLEEP
)


def fetch_events(vessel_id_batch: list, dataset: str) -> list:
    """
    Fetch all events for a batch of vessel IDs using GET + offset pagination.
    """
    all_events, offset = [], 0

    while True:
        params = {
            "datasets[0]": dataset,
            "start-date":  DATE_FROM,
            "end-date":    DATE_TO,
            "limit":       EVENT_LIMIT,
            "offset":      offset,
        }
        for i, vid in enumerate(vessel_id_batch):
            params[f"vessels[{i}]"] = vid

        resp = requests.get(f"{BASE_URL}/v3/events", headers=HEADERS, params=params)

        if resp.status_code == 429:
            print("   Rate limited — sleeping 15s...")
            time.sleep(15)
            continue
        if resp.status_code != 200:
            print(f"   ERROR {resp.status_code}: {resp.text[:200]}")
            break

        data        = resp.json()
        entries     = data.get("entries", [])
        total       = data.get("total", 0)
        next_offset = data.get("nextOffset")

        all_events.extend(entries)
        if not next_offset or len(all_events) >= total:
            break
        offset = next_offset
        time.sleep(RATE_SLEEP)

    return all_events


def flatten_event(event: dict) -> dict:
    """Extract flat fields from a raw GFW event entry."""
    pos     = event.get("position",  {})
    dist    = event.get("distances", {})
    regions = event.get("regions",   {})
    vessel  = event.get("vessel",    {})
    start   = pd.Timestamp(event["start"])
    end     = pd.Timestamp(event["end"])
    return {
        "event_id":                 event.get("id"),
        "vessel_id":                vessel.get("id"),
        "event_type":               event.get("type"),
        "start":                    event.get("start"),
        "end":                      event.get("end"),
        "duration_hrs":             round((end - start).total_seconds() / 3600, 2),
        "lat":                      pos.get("lat"),
        "lon":                      pos.get("lon"),
        "dist_from_shore_start_km": dist.get("startDistanceFromShoreKm"),
        "dist_from_shore_end_km":   dist.get("endDistanceFromShoreKm"),
        "dist_from_port_start_km":  dist.get("startDistanceFromPortKm"),
        "dist_from_port_end_km":    dist.get("endDistanceFromPortKm"),
        "eez":                      (regions.get("eez")      or [None])[0],
        "major_fao":                (regions.get("majorFao") or [None])[0],
    }


def run_step2(engine, vessel_ids: list) -> pd.DataFrame:
    """
    Full events extraction loop.
    Returns df_events DataFrame for use in step3.
    """
    print("\n" + "="*60)
    print("STEP 2 — Fetching events from GFW")
    print("="*60)

    batches = [vessel_ids[i:i+BATCH_SIZE] for i in range(0, len(vessel_ids), BATCH_SIZE)]
    print(f"{len(vessel_ids)} vessels → {len(batches)} batches of {BATCH_SIZE}")

    all_event_rows = []

    for event_type, dataset in EVENT_DATASETS.items():
        print(f"\n== {event_type} ==")
        for i, batch in enumerate(batches):
            events = fetch_events(batch, dataset)
            for e in events:
                all_event_rows.append(flatten_event(e))
            print(f"   Batch {i+1}/{len(batches)}: running total {len(all_event_rows):,}", end="\r")
            time.sleep(RATE_SLEEP)
        print(f"\n   Done")

    df_events = pd.DataFrame(all_event_rows)
    df_events["start"] = pd.to_datetime(df_events["start"], utc=True)
    df_events["end"]   = pd.to_datetime(df_events["end"],   utc=True)

    df_events.to_sql("events", engine, if_exists="replace", index=False, method="multi", chunksize=1000)

    print(f"\n✓ {len(df_events):,} events written to PostgreSQL")
    print(df_events["event_type"].value_counts().to_string())
    return df_events