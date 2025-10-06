import pandas as pd
from pathlib import Path

def extract_events():
    """Extract events data"""
    print("[INFO] Extracting events data...")
    
    # Get project root (2 levels up from this file)
    project_root = Path(__file__).parent.parent.parent.parent
    data_path = project_root / "data" / "raw" / "events.csv"
    
    if not data_path.exists():
        raise FileNotFoundError(f"File not found: {data_path}")
    
    df = pd.read_csv(data_path)
    print(f"[OK] Loaded {len(df):,} rows from events.csv")
    return df