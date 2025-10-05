import pandas as pd
import os

def extract_events(data_path="../data/raw/category_tree.csv"):
    print(" Extracting category data...")
    df = pd.read_csv(data_path)
    print(f"✅ Loaded {len(df):,} rows from events.csv")
    return df
