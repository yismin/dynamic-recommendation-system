import pandas as pd

def clean_item_properties(df):
    """Clean and pivot item_properties to one row per item"""
    print("[INFO] Cleaning item_properties...")

    initial_count = len(df)
    
    # Drop null itemids
    df = df.dropna(subset=["itemid"]).drop_duplicates()
    
    # Ensure correct data types
    df["itemid"] = df["itemid"].astype("int32")
    df["timestamp"] = df["timestamp"].astype("int64")
    
    # Keep most recent value per item-property
    df = df.sort_values("timestamp").drop_duplicates(
        subset=["itemid", "property"], 
        keep="last"
    )
    
    # Extract categoryid before pivoting
    category_df = df[df["property"] == "categoryid"][["itemid", "value"]].copy()
    category_df["categoryid"] = pd.to_numeric(category_df["value"], errors="coerce").astype("Int32")
    category_df = category_df[["itemid", "categoryid"]].drop_duplicates(subset=["itemid"])
    
    # Count properties per item
    property_counts = df.groupby("itemid").size().reset_index(name="property_count")
    
    # Get all unique items from the data
    unique_items = df[["itemid"]].drop_duplicates()
    
    # Merge everything
    result = unique_items.merge(category_df, on="itemid", how="left")
    result = result.merge(property_counts, on="itemid", how="left")
    
    # Fill missing values
    result["property_count"] = result["property_count"].fillna(0).astype("int32")
    result["has_metadata"] = result["property_count"] > 0
    
    # Get latest timestamp per item
    latest_timestamps = df.groupby("itemid")["timestamp"].max().reset_index()
    result = result.merge(latest_timestamps, on="itemid", how="left")
    
    print(f"[OK] Cleaned to {len(result):,} unique items (from {initial_count:,} property records)")
    
    # Show metadata coverage
    with_meta = result["has_metadata"].sum()
    without_meta = len(result) - with_meta
    print(f"  - Items WITH metadata: {with_meta:,}")
    print(f"  - Items WITHOUT metadata: {without_meta:,}")
    
    return result