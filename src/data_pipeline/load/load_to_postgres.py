import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

def load_to_postgres(df, table_name, batch_size=10000):
    """Load dataframe to PostgreSQL table"""
    print(f"[INFO] Loading {len(df):,} rows to {table_name}...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Convert DataFrame to list of tuples
        columns = df.columns.tolist()
        records = [tuple(row) for row in df.values]
        
        # Build insert query
        cols = ", ".join(columns)
        query = f"INSERT INTO {table_name} ({cols}) VALUES %s"
        
        # Insert in batches with progress bar
        total_batches = (len(records) + batch_size - 1) // batch_size
        
        with tqdm(total=len(records), desc=f"Loading {table_name}") as pbar:
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                execute_values(cursor, query, batch, page_size=1000)
                conn.commit()
                pbar.update(len(batch))
        
        # Verify count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        db_count = cursor.fetchone()[0]
        
        print(f"[OK] Loaded {db_count:,} rows to {table_name}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Loading to {table_name}: {e}")
        import traceback
        traceback.print_exc()
        return False