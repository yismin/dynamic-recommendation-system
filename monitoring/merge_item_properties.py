import pandas as pd
import os
from pathlib import Path

cwd = Path(os.getcwd())
if (cwd / "data").exists():
    PROJECT_ROOT = cwd
else:
    PROJECT_ROOT = cwd.parent  
print("project root:", PROJECT_ROOT)

DATA_DIR = PROJECT_ROOT / "data" / "raw" / "retailrocket"

part1 = pd.read_csv(DATA_DIR / "item_properties_part1.csv",dtype={'timestamp': 'int64'})
part2 = pd.read_csv(DATA_DIR / "item_properties_part1.csv",dtype={'timestamp': 'int64'})

merged = pd.concat([part1, part2], ignore_index=True)

merged.to_csv("data/processed/item_properties.csv", index=False)

print("✅ Merged successfully! New file saved at data/processed/item_properties.csv")
