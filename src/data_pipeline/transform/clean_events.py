import pandas as pd

def clean_events(df):
    """Clean and preprocess events data."""
    print("Cleaning events data...")

    # Drop duplicates and missing keys
    df = df.drop_duplicates()
    df = df.dropna(subset=["visitorid", "itemid", "event"])

    # Ensure correct data types
    df["timestamp"] = df["timestamp"].astype("int64")
    df["visitorid"] = df["visitorid"].astype("int32")
    df["itemid"] = df["itemid"].astype("int32")

    # Normalize event type
    df["event"] = df["event"].str.lower().str.strip()

    print(f"✅ Cleaned events: {len(df):,} rows remaining out of 2,756,101.")
    return df
