import pandas as pd

def clean_item_properties(df):
    """Clean and enrich item_properties data."""
    print(" Cleaning item_properties...")

    # Drop null or invalid itemids
    df = df.dropna(subset=["itemid"]).drop_duplicates()

    # Convert timestamp (keep most recent record per item)
    df["timestamp"] = df["timestamp"].astype("int64")
    df = df.sort_values("timestamp").drop_duplicates("itemid", keep="last")

    # Compute property count
    property_counts = df.groupby("itemid")["property"].count().rename("property_count")
    df = df.merge(property_counts, on="itemid")

    # Flag if has metadata
    df["has_metadata"] = df["property_count"] > 0

    print(f"✅ Cleaned item_properties: {len(df):,} items processed out of 2,0275,902.")
    return df
