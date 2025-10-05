from extract.extract_events import extract_events
from transform.clean_events import clean_events
from load.load_to_postgres import load_to_postgres

def main():
    print("Starting ETL Pipeline...\n")

    # 1. Extract
    events_df = extract_events()

    # 2. Transform
    clean_df = clean_events(events_df)

    # 3. Load
    load_to_postgres(clean_df, table_name="events")

    print("\n ETL Pipeline completed successfully!")

if __name__ == "__main__":
    main()
