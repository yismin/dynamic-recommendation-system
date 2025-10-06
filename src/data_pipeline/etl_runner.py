from extract.extract_events import extract_events
from extract.extract_item_properties import extract_item_properties
from extract.extract_category import extract_category

from transform.clean_events import clean_events
from transform.merge_item_properties import merge_item_properties
from transform.clean_item_properties import clean_item_properties
from transform.clean_category import clean_category

from load.load_to_postgres import load_to_postgres

def main():
    print("🚀 Starting ETL pipeline...\n")

    # === EXTRACT ===
    events_df = extract_events()
    part_df = merge_item_properties()
    categories_df = extract_category()

    # === TRANSFORM ===
    clean_events_df = clean_events(events_df)
    item_props_df = clean_item_properties(part_df)
    clean_categories_df = clean_category(categories_df)

    # === LOAD ===
    load_to_postgres(clean_events_df, "events")
    load_to_postgres(item_props_df, "item_properties")
    load_to_postgres(clean_categories_df, "categories")

    print("\n🎯 ETL pipeline finished successfully!")

if __name__ == "__main__":
    main()
