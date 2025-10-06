import sys
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_pipeline.extract.extract_events import extract_events
from data_pipeline.extract.extract_item_properties import extract_item_properties
from data_pipeline.extract.extract_category import extract_category

from data_pipeline.transform.clean_events import clean_events
from data_pipeline.transform.merge_item_properties import merge_item_properties
from data_pipeline.transform.clean_item_properties import clean_item_properties
from data_pipeline.transform.clean_category import clean_category

from data_pipeline.load.load_to_postgres import load_to_postgres

def main():
    print("\n" + "="*60)
    print("STARTING ETL PIPELINE")
    print("="*60 + "\n")

    try:
        # === EXTRACT ===
        print("[STEP 1/3] EXTRACTING DATA")
        print("-"*60)
        events_df = extract_events()
        merged_props_df = merge_item_properties()
        categories_df = extract_category()
        print()

        # === TRANSFORM ===
        print("[STEP 2/3] TRANSFORMING DATA")
        print("-"*60)
        clean_events_df = clean_events(events_df)
        clean_props_df = clean_item_properties(merged_props_df)
        clean_categories_df = clean_category(categories_df)
        print()

        # === LOAD ===
        print("[STEP 3/3] LOADING TO POSTGRESQL")
        print("-"*60)
        
        # Load events
        load_to_postgres(
            clean_events_df[["timestamp", "visitorid", "event", "itemid", "transactionid"]], 
            "events"
        )
        
        # Load item properties (keep all columns from cleaning)
        load_to_postgres(clean_props_df, "item_properties")
        
        # Load categories
        load_to_postgres(clean_categories_df, "categories")

        print("\n" + "="*60)
        print("[SUCCESS] ETL PIPELINE COMPLETED!")
        print("="*60 + "\n")
        
        return True

    except Exception as e:
        print(f"\n[ERROR] Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)