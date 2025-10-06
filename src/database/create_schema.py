import psycopg2
import sys
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

def create_schema():
    """Create all tables for the recommendation system"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("Creating tables...")

        # 1. Events table - FIXED: visitorid is now BIGINT
        print("Creating events table...")
        cursor.execute("""
            DROP TABLE IF EXISTS events CASCADE;
            
            CREATE TABLE events (
                timestamp BIGINT NOT NULL,
                visitorid BIGINT NOT NULL,  -- CHANGED from INTEGER
                event VARCHAR(50) NOT NULL,
                itemid INTEGER NOT NULL,
                transactionid INTEGER
            );
            
            CREATE INDEX idx_events_visitorid ON events(visitorid);
            CREATE INDEX idx_events_itemid ON events(itemid);
            CREATE INDEX idx_events_event ON events(event);
            CREATE INDEX idx_events_timestamp ON events(timestamp);
        """)
        print("[OK] Events table created")
        
        # Item properties table - ONE ROW PER ITEM
        print("Creating item_properties table...")
        cursor.execute("""
            DROP TABLE IF EXISTS item_properties CASCADE;
            
            CREATE TABLE item_properties (
                itemid INTEGER PRIMARY KEY,
                categoryid INTEGER,
                timestamp BIGINT,
                property_count INTEGER DEFAULT 0,
                has_metadata BOOLEAN DEFAULT FALSE
            );
            
            CREATE INDEX idx_item_categoryid ON item_properties(categoryid);
            CREATE INDEX idx_item_has_metadata ON item_properties(has_metadata);
        """)
        print("[OK] Item properties table created")
        
        # 3. Categories
        print("Creating categories table...")
        cursor.execute("""
            DROP TABLE IF EXISTS categories CASCADE;
            
            CREATE TABLE categories (
                categoryid INTEGER PRIMARY KEY,
                parentid INTEGER,
                level INTEGER,
                root_category INTEGER,
                is_orphan BOOLEAN DEFAULT FALSE  -- ADDED from your cleaning
            );
            
            CREATE INDEX idx_cat_parentid ON categories(parentid);
            CREATE INDEX idx_cat_root ON categories(root_category);
        """)
        print("[OK] Categories table created")
        
        # 4. User features - FIXED: visitorid is now BIGINT
        print("Creating user_features table...")
        cursor.execute("""
            DROP TABLE IF EXISTS user_features CASCADE;
            
            CREATE TABLE user_features (
                visitorid BIGINT PRIMARY KEY,  -- CHANGED from INTEGER
                total_events INTEGER DEFAULT 0,
                total_views INTEGER DEFAULT 0,
                total_addtocarts INTEGER DEFAULT 0,
                total_transactions INTEGER DEFAULT 0,
                favorite_category INTEGER,
                avg_session_duration FLOAT,
                last_interaction_timestamp BIGINT,
                user_segment VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX idx_user_segment ON user_features(user_segment);
        """)
        print("[OK] User features table created")
        
        # 5. Item features
        print("Creating item_features table...")
        cursor.execute("""
            DROP TABLE IF EXISTS item_features CASCADE;
            
            CREATE TABLE item_features (
                itemid INTEGER PRIMARY KEY,
                total_views INTEGER DEFAULT 0,
                total_addtocarts INTEGER DEFAULT 0,
                total_transactions INTEGER DEFAULT 0,
                conversion_rate FLOAT DEFAULT 0.0,
                avg_time_to_purchase FLOAT,
                popularity_score FLOAT DEFAULT 0.0,
                trending_score FLOAT DEFAULT 0.0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX idx_item_popularity ON item_features(popularity_score DESC);
            CREATE INDEX idx_item_trending ON item_features(trending_score DESC);
        """)
        print("[OK] Item features table created")
        
        conn.commit()
        
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        
        tables = cursor.fetchall()
        print("\n[SUCCESS] All tables created!")
        print("\nTables in database:")
        for table in tables:
            print(f"  - {table[0]}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Creating schema: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = create_schema()
    sys.exit(0 if success else 1)