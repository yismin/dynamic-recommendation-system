import pandas as pd

def clean_category(df):
    """Clean and validate category tree."""
    print("Cleaning category tree...")

    # Drop duplicates and invalids
    df = df.drop_duplicates().dropna(subset=["categoryid"])

    # Ensure integer IDs
    df["categoryid"] = df["categoryid"].astype("int32")
    df["parentid"] = df["parentid"].fillna(-1).astype("int32")

    # Identify orphans (parent not found)
    known_ids = set(df["categoryid"])
    df["is_orphan"] = ~df["parentid"].isin(known_ids) & (df["parentid"] != -1)

    orphan_count = df["is_orphan"].sum()
    if orphan_count > 0:
        print(f"⚠️ Found {orphan_count} orphan categories — will keep them flagged.")

    print(f"✅ Cleaned categories: {len(df):,} total.")
    return df
