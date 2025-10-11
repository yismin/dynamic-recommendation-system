import psycopg2
import pandas as pd
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

def generate_user_features():
    """Generate aggregated user behavior features"""
    print("GENERATING USER FEATURES")
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    print("[INFO] Calculating user behavior metrics...")
    
    query = """
    INSERT INTO user_features
    SELECT 
        visitorid,
        COUNT(*) as total_events,
        SUM(CASE WHEN event = 'view' THEN 1 ELSE 0 END) as total_views,
        SUM(CASE WHEN event = 'addtocart' THEN 1 ELSE 0 END) as total_addtocarts,
        SUM(CASE WHEN event = 'transaction' THEN 1 ELSE 0 END) as total_transactions,
        NULL as favorite_category,
        NULL as avg_session_duration,
        MAX(timestamp) as last_interaction_timestamp,
        CASE 
            WHEN COUNT(*) > 100 THEN 'power_user'
            WHEN SUM(CASE WHEN event = 'transaction' THEN 1 ELSE 0 END) > 0 THEN 'converter'
            ELSE 'casual'
        END as user_segment,
        CURRENT_TIMESTAMP as created_at
    FROM events
    GROUP BY visitorid
    ON CONFLICT (visitorid) DO NOTHING
    """
    
    cursor.execute(query)
    conn.commit()
    
    # Get statistics
    cursor.execute("SELECT COUNT(*) FROM user_features")
    user_count = cursor.fetchone()[0]
    
    cursor.execute("""
        SELECT user_segment, COUNT(*) as count
        FROM user_features
        GROUP BY user_segment
        ORDER BY count DESC
    """)
    segments = cursor.fetchall()
    
    print(f"[OK] Generated features for {user_count:,} users\n")
    print("User Segments:")
    for segment, count in segments:
        print(f"  - {segment}: {count:,}")
    
    cursor.close()
    conn.close()

def generate_item_features():
    """Generate item popularity and conversion metrics"""
    print("GENERATING ITEM FEATURES")
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    print("[INFO] Calculating item metrics...")
    
    query = """
    INSERT INTO item_features
    SELECT 
        itemid,
        SUM(CASE WHEN event = 'view' THEN 1 ELSE 0 END) as total_views,
        SUM(CASE WHEN event = 'addtocart' THEN 1 ELSE 0 END) as total_addtocarts,
        SUM(CASE WHEN event = 'transaction' THEN 1 ELSE 0 END) as total_transactions,
        CASE 
            WHEN SUM(CASE WHEN event = 'view' THEN 1 ELSE 0 END) > 0 
            THEN SUM(CASE WHEN event = 'transaction' THEN 1 ELSE 0 END)::FLOAT / 
                 SUM(CASE WHEN event = 'view' THEN 1 ELSE 0 END)
            ELSE 0
        END as conversion_rate,
        NULL as avg_time_to_purchase,
        LOG(1 + SUM(CASE WHEN event = 'view' THEN 1 ELSE 0 END)::FLOAT) as popularity_score,
        NULL as trending_score,
        CURRENT_TIMESTAMP as created_at
    FROM events
    GROUP BY itemid
    ON CONFLICT (itemid) DO NOTHING
    """
    
    cursor.execute(query)
    conn.commit()
    
    # Get statistics
    cursor.execute("SELECT COUNT(*) FROM item_features")
    item_count = cursor.fetchone()[0]
    
    cursor.execute("""
        SELECT 
            COUNT(*) as total_items,
            AVG(conversion_rate) as avg_conversion,
            MAX(conversion_rate) as max_conversion,
            COUNT(CASE WHEN total_transactions > 0 THEN 1 END) as items_with_sales
        FROM item_features
    """)
    stats = cursor.fetchone()
    
    print(f"[OK] Generated features for {item_count:,} items\n")
    print("Item Statistics:")
    print(f"  - Total items: {stats[0]:,}")
    print(f"  - Avg conversion rate: {stats[1]*100:.2f}%")
    print(f"  - Max conversion rate: {stats[2]*100:.2f}%")
    print(f"  - Items with sales: {stats[3]:,}")
    
    # Top converting items
    cursor.execute("""
        SELECT itemid, total_views, total_transactions, 
               ROUND(conversion_rate::numeric, 4) as conv_rate
        FROM item_features
        WHERE total_views > 100
        ORDER BY conversion_rate DESC
        LIMIT 5
    """)
    top_items = cursor.fetchall()
    
    print("\nTop 5 Converting Items (with 100+ views):")
    for itemid, views, trans, conv in top_items:
        print(f"  - Item {itemid}: {views} views → {trans} sales ({conv*100:.2f}%)")
    
    cursor.close()
    conn.close()

def main():
    print("FEATURE GENERATION PIPELINE")
    
    generate_user_features()
    generate_item_features()
    
    print("[SUCCESS] Feature generation completed!")

if __name__ == "__main__":
    main()