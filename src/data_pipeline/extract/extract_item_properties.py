import pandas as pd
import os

def extract_events(data_path="../data/processed/item_properties.csv"):
    print(" Extracting item properties data...")
    df = pd.read_csv(data_path)
    print(f"✅ Loaded {len(df):,} rows from item_properties.csv")
    return df
