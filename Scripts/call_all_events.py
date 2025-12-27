import pandas as pd
import os

from Api_calls.events import fetch_event_live
from Utils.json_flattner import json_to_dataframe


def fetch_events_dataframe(start=1, end=18) -> pd.DataFrame:
    all_frames = []

    for event_id in range(start, end + 1):
        print(f"Fetching event {event_id}...")

        data = fetch_event_live(event_id)

        elements = data.get("elements", [])

        df = json_to_dataframe(elements)

        # tag which gameweek this row belongs to
        df["event_id"] = event_id

        all_frames.append(df)

    full = pd.concat(all_frames, ignore_index=True)

    # prettier column names
    full.columns = full.columns.str.replace(r"\.+", "_", regex=True)

    return full


if __name__ == "__main__":
    df = fetch_events_dataframe(1, 18)

    print(df.head())
    print(f"\nRows: {df.shape[0]}  |  Columns: {df.shape[1]}")

    os.makedirs("data", exist_ok=True)
    df.to_csv("data/events_1_18.csv", index=False)

    print("Saved to data/events_1_18.csv")
