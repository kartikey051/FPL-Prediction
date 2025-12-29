import os
import pandas as pd

from Api_calls.get_available_gameweeks import get_available_gameweeks
from Utils.state import load_last_event, save_last_event
from Api_calls.events import fetch_event_live
from Utils.json_flattner import json_to_dataframe


def test_pipeline_run():
    print("\n🔎 Checking available gameweeks...")
    available = get_available_gameweeks()
    print("Available:", available)

    print("\n📂 Loading state...")
    last = load_last_event()
    print("Last processed event:", last)

    to_fetch = [gw for gw in available if gw > last]
    print("\n➡️ Gameweeks to fetch:", to_fetch)

    if not to_fetch:
        print("\n🌴 No new gameweeks. Pipeline idle. OK.")
        return

    frames = []

    for gw in to_fetch:
        print(f"\n⏬ Fetching GW {gw}...")
        data = fetch_event_live(gw)

        elements = data.get("elements", [])
        df = json_to_dataframe(elements)
        df["event_id"] = gw

        print(f"✓ Flattened {len(df)} rows")

        frames.append(df)

        print("💾 Updating state...")
        save_last_event(gw)

    result = pd.concat(frames, ignore_index=True)

    print("\n🎉 PIPELINE TEST SUCCESS")
    print(result.head())
    print(f"\nRows: {result.shape[0]} | Columns: {result.shape[1]}")


if __name__ == "__main__":
    test_pipeline_run()
