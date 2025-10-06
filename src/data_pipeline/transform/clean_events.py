import pandas as pd

def clean_events(df):
    """Clean and preprocess events data."""
    print("[INFO] Cleaning events data...")

    # Drop duplicates and missing keys
    initial_count = len(df)
    df = df.drop_duplicates()
    df = df.dropna(subset=["visitorid", "itemid", "event"])
    
    duplicates_removed = initial_count - len(df)
    print(f"  - Removed {duplicates_removed:,} duplicates/nulls")

    # Ensure correct data types - FIXED: visitorid is int64 (BIGINT)
    df["timestamp"] = df["timestamp"].astype("int64")
    df["visitorid"] = df["visitorid"].astype("int64")  # CHANGED from int32
    df["itemid"] = df["itemid"].astype("int32")
    
    # Handle transactionid (can be null)
    if "transactionid" in df.columns:
        df["transactionid"] = df["transactionid"].fillna(0).astype("int32")

    # Normalize event type
    df["event"] = df["event"].str.lower().str.strip()
    
    # Validate event types
    valid_events = ["view", "addtocart", "transaction"]
    df = df[df["event"].isin(valid_events)]

    print(f"[OK] Cleaned events: {len(df):,} rows remaining")
    return df