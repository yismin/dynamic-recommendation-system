import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

def create_train_test_split():
    """Create time-based 80/20 train/test split"""
    print("CREATING TRAIN/TEST SPLIT")
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Get 80% timestamp cutoff
    cursor.execute("""
        SELECT PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY timestamp)
        FROM events
    """)
    split_timestamp = cursor.fetchone()[0]
    
    print(f"[INFO] Split timestamp: {split_timestamp}")
    
    # Create train/test tables
    cursor.execute("""
        DROP TABLE IF EXISTS events_train CASCADE;
        CREATE TABLE events_train AS
        SELECT * FROM events WHERE timestamp <= %s;
        
        DROP TABLE IF EXISTS events_test CASCADE;
        CREATE TABLE events_test AS
        SELECT * FROM events WHERE timestamp > %s;
    """, (split_timestamp, split_timestamp))
    
    conn.commit()
    
    # Get counts
    cursor.execute("SELECT COUNT(*) FROM events_train")
    train_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM events_test")
    test_count = cursor.fetchone()[0]
    
    print(f"[OK] Train set: {train_count:,} events ({train_count/(train_count+test_count)*100:.1f}%)")
    print(f"[OK] Test set:  {test_count:,} events ({test_count/(train_count+test_count)*100:.1f}%)")
    
    cursor.close()
    conn.close()
    
    print("[SUCCESS] Train/test split complete")

if __name__ == "__main__":
    create_train_test_split()