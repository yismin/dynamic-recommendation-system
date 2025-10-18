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

def fill_favorite_category():
    """Fill favorite_category based on user's most interacted category"""
    print("\n[1] Filling favorite_category...")
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    query = """
    UPDATE user_features uf
    SET favorite_category = subq.categoryid
    FROM (
        SELECT 
            e.visitorid,
            ip.categoryid,
            COUNT(*) as interaction_count,
            ROW_NUMBER() OVER (PARTITION BY e.visitorid ORDER BY COUNT(*) DESC) as rn
        FROM events e
        INNER JOIN item_properties ip ON e.itemid = ip.itemid
        WHERE ip.categoryid IS NOT NULL
        GROUP BY e.visitorid, ip.categoryid
    ) subq
    WHERE uf.visitorid = subq.visitorid
      AND subq.rn = 1
      AND uf.favorite_category IS NULL
    """
    
    cursor.execute(query)
    rows_updated = cursor.rowcount
    conn.commit()
    
    print(f"[OK] Updated {rows_updated:,} users")
    
    cursor.close()
    conn.close()

def fill_trending_score():
    """Fill trending_score with time-decayed popularity"""
    print("\n[2] Filling trending_score...")
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    query = """
    UPDATE item_features if
    SET trending_score = subq.trending
    FROM (
        SELECT 
            itemid,
            SUM(
                CASE WHEN event = 'view' THEN 1 ELSE 0 END * 
                EXP(-(EXTRACT(EPOCH FROM NOW() - to_timestamp(timestamp/1000))/(86400*30)))
            ) as trending
        FROM events
        GROUP BY itemid
    ) subq
    WHERE if.itemid = subq.itemid
      AND if.trending_score IS NULL
    """
    
    cursor.execute(query)
    rows_updated = cursor.rowcount
    conn.commit()
    
    print(f"[OK] Updated {rows_updated:,} items")
    
    cursor.close()
    conn.close()

def verify_completion():
    """Check completion status"""
    print("\n[3] Verifying completion...")
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE favorite_category IS NULL) as null_cat
        FROM user_features
    """)
    user_stats = cursor.fetchone()
    
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE trending_score IS NULL) as null_trend
        FROM item_features
    """)
    item_stats = cursor.fetchone()
    
    print(f"\nUser Features: {user_stats[0]:,} total, {user_stats[1]:,} NULL categories")
    print(f"Item Features: {item_stats[0]:,} total, {item_stats[1]:,} NULL trending")
    
    conn.close()

def main():
    print("\n" + "="*60)
    print("COMPLETING FEATURE GENERATION")
    print("="*60)
    
    fill_favorite_category()
    fill_trending_score()
    verify_completion()
    
    print("\n" + "="*60)
    print("[SUCCESS] Features completed!")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()