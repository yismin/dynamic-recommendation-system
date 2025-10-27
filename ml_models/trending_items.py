import pandas as pd
import psycopg2
import pickle
from pathlib import Path
import os
from dotenv import load_dotenv
import warnings
warnings.filterwarnings("ignore")

load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

class TrendingRecommender:
    """
    Recommend what's trending
    Uses time-decay: recent events weighted higher
    """
    
    def __init__(self):
        self.trending_items = None
        self.category_trending = {}
        
    def train(self):
        """Build trending rankings"""
        print("TRAINING TRENDING RECOMMENDER")
        
        conn = psycopg2.connect(**DB_CONFIG)
        
        time_info = pd.read_sql("""
            SELECT 
                MIN(timestamp) as min_ts,
                MAX(timestamp) as max_ts,
                to_timestamp(MIN(timestamp)/1000) as min_date,
                to_timestamp(MAX(timestamp)/1000) as max_date
            FROM events
        """, conn)
        
        print(f"[INFO] Data range: {time_info['min_date'].iloc[0]} to {time_info['max_date'].iloc[0]}")
        
        min_ts = time_info['min_ts'].iloc[0]
        max_ts = time_info['max_ts'].iloc[0]
        
        # Calculate time decay based on data's actual time range
        cutoff_ts = min_ts + (max_ts - min_ts) * 0.8
                
        # Overall trending (time-weighted by recency within dataset)
        print("\n[1/2] Computing trending items...")
        self.trending_items = pd.read_sql(f"""
            SELECT 
                itemid,
                SUM(
                    CASE WHEN event = 'view' THEN 1 
                         WHEN event = 'addtocart' THEN 3
                         WHEN event = 'transaction' THEN 5 
                    END * 
                    (timestamp - {min_ts}) / ({max_ts} - {min_ts} + 1)
                ) as trending_score
            FROM events
            WHERE timestamp >= {cutoff_ts}
            GROUP BY itemid
            HAVING SUM(
                CASE WHEN event = 'view' THEN 1 
                     WHEN event = 'addtocart' THEN 3
                     WHEN event = 'transaction' THEN 5 
                END
            ) > 10
            ORDER BY trending_score DESC
            LIMIT 100
        """, conn)['itemid'].tolist()
        
        print(f"[OK] Found {len(self.trending_items)} trending items")
        
        # Trending by category
        print("\n[2/2] Computing category trends...")
        category_trends = pd.read_sql(f"""
            WITH trending_scores AS (
                SELECT 
                    ip.categoryid,
                    e.itemid,
                    SUM(
                        CASE WHEN e.event = 'view' THEN 1 
                             WHEN e.event = 'addtocart' THEN 3
                             WHEN e.event = 'transaction' THEN 5 
                        END * 
                        (e.timestamp - {min_ts}) / ({max_ts} - {min_ts} + 1)
                    ) as trending_score,
                    ROW_NUMBER() OVER (
                        PARTITION BY ip.categoryid 
                        ORDER BY SUM(
                            CASE WHEN e.event = 'view' THEN 1 
                                 WHEN e.event = 'addtocart' THEN 3
                                 WHEN e.event = 'transaction' THEN 5 
                            END * 
                            (e.timestamp - {min_ts}) / ({max_ts} - {min_ts} + 1)
                        ) DESC
                    ) as rn
                FROM events e
                JOIN item_properties ip ON e.itemid = ip.itemid
                WHERE e.timestamp >= {cutoff_ts}
                  AND ip.categoryid IS NOT NULL
                GROUP BY ip.categoryid, e.itemid
            )
            SELECT categoryid, itemid
            FROM trending_scores
            WHERE rn <= 20
            ORDER BY categoryid, rn
        """, conn)
        
        for cat_id in category_trends['categoryid'].unique():
            items = category_trends[category_trends['categoryid'] == cat_id]['itemid'].tolist()
            self.category_trending[int(cat_id)] = items
        
        print(f"[OK] Found trends for {len(self.category_trending)} categories")
        
        conn.close()
        
        print("[SUCCESS] Trained!")
    
    def recommend(self, category_id=None, n=10):
        """Get trending recommendations"""
        if category_id and category_id in self.category_trending:
            return self.category_trending[category_id][:n]
        
        return self.trending_items[:n]
    
    def save_model(self, filepath="data/models/trending_model.pkl"):
        """Save model"""
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        
        with open(filepath, 'wb') as f:
            pickle.dump({
                'trending_items': self.trending_items,
                'category_trending': self.category_trending
            }, f)
        
        print(f"[OK] Model saved to {filepath}")

def main():
    model = TrendingRecommender()
    model.train()
    model.save_model()
    
    print("TESTING")
    
    print("\nTrending Now (Overall):")
    recs = model.recommend(n=10)
    print(f"Items: {recs}")
    
    if len(model.category_trending) > 0:
        test_cat = list(model.category_trending.keys())[0]
        print(f"\nTrending in Category {test_cat}:")
        recs = model.recommend(category_id=test_cat, n=5)
        print(f"Items: {recs}")
    

if __name__ == "__main__":
    main()