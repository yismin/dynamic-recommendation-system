import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

def create_train_test_split():
    """
    Create train/test split based on TIME
    
    Strategy:
    - Train: First 80% of time period
    - Test: Last 20% of time period
    
    This simulates real-world scenario: train on past, predict future
    """
    print("\n" + "="*60)
    print("CREATING TRAIN/TEST SPLIT")
    print("="*60 + "\n")
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Get time range
    print("[1/4] Analyzing data time range...")
    cursor.execute("""
        SELECT 
            MIN(timestamp) as min_ts,
            MAX(timestamp) as max_ts,
            to_timestamp(MIN(timestamp)/1000) as min_date,
            to_timestamp(MAX(timestamp)/1000) as max_date,
            COUNT(*) as total_events
        FROM events
    """)
    
    result = cursor.fetchone()
    min_ts, max_ts, min_date, max_date, total_events = result
    
    print(f"[OK] Data range: {min_date} to {max_date}")
    print(f"[OK] Total events: {total_events:,}")
    
    # Calculate 80/20 split point
    split_ts = min_ts + (max_ts - min_ts) * 0.8
    split_date = pd.to_datetime(split_ts, unit='ms')
    
    print(f"\n[INFO] Split point: {split_date}")
    print(f"[INFO] Train: {min_date} to {split_date}")
    print(f"[INFO] Test:  {split_date} to {max_date}")
    
    # Drop existing tables if they exist
    print("\n[2/4] Dropping old train/test tables...")
    cursor.execute("DROP TABLE IF EXISTS train_set CASCADE")
    cursor.execute("DROP TABLE IF EXISTS test_set CASCADE")
    conn.commit()
    
    # Create train set
    print("\n[3/4] Creating train set...")
    cursor.execute(f"""
        CREATE TABLE train_set AS
        SELECT *
        FROM events
        WHERE timestamp < {split_ts}
    """)
    conn.commit()
    
    cursor.execute("SELECT COUNT(*) FROM train_set")
    train_count = cursor.fetchone()[0]
    print(f"[OK] Train set: {train_count:,} events ({train_count/total_events*100:.1f}%)")
    
    # Create test set
    print("\n[4/4] Creating test set...")
    cursor.execute(f"""
        CREATE TABLE test_set AS
        SELECT *
        FROM events
        WHERE timestamp >= {split_ts}
    """)
    conn.commit()
    
    cursor.execute("SELECT COUNT(*) FROM test_set")
    test_count = cursor.fetchone()[0]
    print(f"[OK] Test set: {test_count:,} events ({test_count/total_events*100:.1f}%)")
    
    # Create indexes for performance
    print("\n[INFO] Creating indexes...")
    cursor.execute("CREATE INDEX idx_train_visitor ON train_set(visitorid)")
    cursor.execute("CREATE INDEX idx_train_item ON train_set(itemid)")
    cursor.execute("CREATE INDEX idx_test_visitor ON test_set(visitorid)")
    cursor.execute("CREATE INDEX idx_test_item ON test_set(itemid)")
    conn.commit()
    print("[OK] Indexes created")
    
    # Stats
    print("\n" + "="*60)
    print("SPLIT STATISTICS")
    print("="*60)
    
    cursor.execute("""
        SELECT 
            'Train' as dataset,
            COUNT(*) as events,
            COUNT(DISTINCT visitorid) as users,
            COUNT(DISTINCT itemid) as items
        FROM train_set
        UNION ALL
        SELECT 
            'Test' as dataset,
            COUNT(*) as events,
            COUNT(DISTINCT visitorid) as users,
            COUNT(DISTINCT itemid) as items
        FROM test_set
    """)
    
    print(f"\n{'Dataset':<10} {'Events':<12} {'Users':<12} {'Items':<12}")
    print("-" * 50)
    for row in cursor.fetchall():
        dataset, events, users, items = row
        print(f"{dataset:<10} {events:<12,} {users:<12,} {items:<12,}")
    
    cursor.close()
    conn.close()
    
    print("\n" + "="*60)
    print("[SUCCESS] Train/test split created!")
    print("="*60 + "\n")


if __name__ == "__main__":
    create_train_test_split()