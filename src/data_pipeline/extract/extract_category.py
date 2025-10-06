import pandas as pd
from pathlib import Path

def extract_category():
    """Extract category tree data"""
    print("[INFO] Extracting category data...")
    
    project_root = Path(__file__).parent.parent.parent.parent
    data_path = project_root / "data" / "raw" / "category_tree.csv"
    
    if not data_path.exists():
        raise FileNotFoundError(f"File not found: {data_path}")
    
    df = pd.read_csv(data_path)
    print(f"[OK] Loaded {len(df):,} rows from category_tree.csv")
    return df