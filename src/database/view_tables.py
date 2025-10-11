import pandas as pd
import os
import sqlalchemy
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

def get_engine():
    """Create a SQLAlchemy engine"""
    return sqlalchemy.create_engine(
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )

def view_all_tables():
    """View summary of all tables"""
    engine=get_engine()
    
    print("DATABASE TABLES OVERVIEW")
    
    tables = ['events', 'item_properties', 'categories', 'user_features', 'item_features']
    
    for table in tables:
        print(f"\n{'='*80}")
        print(f"TABLE: {table.upper()}")
        print(f"{'='*80}")
        
        # Row count
        count_query = f"SELECT COUNT(*) FROM {table}"
        count = pd.read_sql(count_query, engine).iloc[0, 0]
        print(f"Total Rows: {count:,}\n")
        
        # Sample data
        sample_query = f"SELECT * FROM {table} LIMIT 5"
        df = pd.read_sql(sample_query, engine)
        print(df.to_string(index=False))
        print()
    
    engine.dispose()

def view_statistics():
    """View key statistics"""
    engine=get_engine()
    
    print("\n" + "="*80)
    print("KEY STATISTICS")
    print("="*80 + "\n")
    
    # Events breakdown
    print("EVENT TYPES:")
    events_stats = pd.read_sql("""
        SELECT 
            event,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
        FROM events
        GROUP BY event
        ORDER BY count DESC
    """, engine)
    print(events_stats.to_string(index=False))
    
    # Metadata coverage
    print("\n\nITEM METADATA COVERAGE:")
    metadata_stats = pd.read_sql("""
        SELECT 
            has_metadata,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
        FROM item_properties
        GROUP BY has_metadata
        ORDER BY has_metadata DESC
    """, engine)
    print(metadata_stats.to_string(index=False))
    
    # User segments (if features generated)
    try:
        print("\n\nUSER SEGMENTS:")
        user_stats = pd.read_sql("""
            SELECT 
                user_segment,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
            FROM user_features
            GROUP BY user_segment
            ORDER BY count DESC
        """, engine)
        print(user_stats.to_string(index=False))
    except:
        print("  (User features not generated yet)")
    
    # Top items by views
    print("\n\nTOP 10 MOST VIEWED ITEMS:")
    top_items = pd.read_sql("""
        SELECT 
            itemid,
            COUNT(*) as views
        FROM events
        WHERE event = 'view'
        GROUP BY itemid
        ORDER BY views DESC
        LIMIT 10
    """, engine)
    print(top_items.to_string(index=False))
    
    # Conversion funnel
    print("\n\nCONVERSION FUNNEL:")
    funnel = pd.read_sql("""
        SELECT 
            SUM(CASE WHEN event = 'view' THEN 1 ELSE 0 END) as views,
            SUM(CASE WHEN event = 'addtocart' THEN 1 ELSE 0 END) as add_to_cart,
            SUM(CASE WHEN event = 'transaction' THEN 1 ELSE 0 END) as transactions
        FROM events
    """, engine)
    
    views = funnel['views'].iloc[0]
    carts = funnel['add_to_cart'].iloc[0]
    trans = funnel['transactions'].iloc[0]
    
    print(f"  Views:         {views:>10,}")
    print(f"  Add to Cart:   {carts:>10,}  ({carts/views*100:.2f}% of views)")
    print(f"  Transactions:  {trans:>10,}  ({trans/carts*100:.2f}% of carts, {trans/views*100:.2f}% overall)")
    
    engine.dispose()
    
    print("\n" + "="*80 + "\n")

if __name__ == "__main__":
    view_all_tables()
    view_statistics()