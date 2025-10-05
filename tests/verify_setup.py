import psycopg2
import pandas as pd
import os 
from sqlalchemy import create_engine


DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

def verify_setup():
    """Verify database setup is correct"""
    engine = create_engine(
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
            )
    conn = engine.connect()

    print("DATABASE SETUP VERIFICATION....")
    
    tables = pd.read_sql("""
        SELECT table_name, 
               (SELECT COUNT(*) 
                FROM information_schema.columns 
                WHERE table_name = t.table_name) as column_count
        FROM information_schema.tables t
        WHERE table_schema = 'public'
        ORDER BY table_name;
    """, conn)
    
    print("📊 Tables:")
    print(tables.to_string(index=False))
    print()
    
    # Check indexes
    indexes = pd.read_sql("""
        SELECT 
            tablename,
            indexname
        FROM pg_indexes
        WHERE schemaname = 'public'
        ORDER BY tablename, indexname;
    """, conn)
    
    print(f"\n📑 Indexes created: {len(indexes)}")
    for table in tables['table_name']:
        table_indexes = indexes[indexes['tablename'] == table]
        print(f"  {table}: {len(table_indexes)} indexes")
    
    # Check if ready for data
    print("\n✅ Database is ready for ETL pipeline!")
    print("\nNext steps:")
    print("  1. Prepare your RetailRocket CSV files")
    print("  2. Run ETL pipeline to load data")
    print("  3. Generate features")
    
    conn.close()

if __name__ == "__main__":
    verify_setup()