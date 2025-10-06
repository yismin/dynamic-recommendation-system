import pandas as pd
import os
from pathlib import Path

def merge_item_properties():
    """Merge item_properties parts into one DataFrame."""
    print(" Merging item_properties...")
    
    # Find project root
    cwd = Path(os.getcwd())
    PROJECT_ROOT = cwd if (cwd / "data").exists() else cwd.parent
    
    DATA_DIR = PROJECT_ROOT / "data" / "raw" 
    OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"
    
    # Read and merge
    part1 = pd.read_csv(DATA_DIR / "item_properties_part1.csv", dtype={'timestamp': 'int64'})
    part2 = pd.read_csv(DATA_DIR / "item_properties_part2.csv", dtype={'timestamp': 'int64'})
    merged = pd.concat([part1, part2], ignore_index=True)
    
    # Save
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUTPUT_DIR / "item_properties.csv", index=False)

    print(f"✅ Merged {len(part1):,} + {len(part2):,} = {len(merged):,} rows")
    return merged