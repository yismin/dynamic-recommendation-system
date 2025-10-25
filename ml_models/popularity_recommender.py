import pandas as pd
import psycopg2
import pickle
import os
from dotenv import load_dotenv
from pathlib import Path
import warnings
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")


load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

class PopularityRecommender:
    """Smart popularity-based recommender"""
    
    def __init__(self):
        self.popular_items = None
        self.category_popular = {}
        
    def train(self):
        """Build popularity rankings"""
        print("\n" + "="*60)
        print("TRAINING POPULARITY RECOMMENDER")
        print("="*60 + "\n")
        
        conn = psycopg2.connect(**DB_CONFIG)
        
        # Overall popular items
        print("[1/2] Loading overall popular items...")
        self.popular_items = pd.read_sql("""
            SELECT itemid, popularity_score, total_transactions
            FROM item_features
            WHERE total_transactions > 0
            ORDER BY popularity_score DESC
            LIMIT 100
        """, conn)['itemid'].tolist()
        
        print(f"[OK] Loaded {len(self.popular_items)} popular items")
        
        # Popular by category - FIXED: Load ALL at once instead of loop
        print("\n[2/2] Loading category-specific popular items...")
        
        # Get top 30 items per category in one query
        category_items = pd.read_sql("""
            WITH ranked_items AS (
                SELECT 
                    ip.categoryid,
                    ip.itemid,
                    if.popularity_score,
                    ROW_NUMBER() OVER (PARTITION BY ip.categoryid ORDER BY if.popularity_score DESC) as rn
                FROM item_properties ip
                JOIN item_features if ON ip.itemid = if.itemid
                WHERE ip.categoryid IS NOT NULL
                  AND if.total_transactions > 0
            )
            SELECT categoryid, itemid
            FROM ranked_items
            WHERE rn <= 30
            ORDER BY categoryid, rn
        """, conn)
        
        # Group by category
        for cat_id in category_items['categoryid'].unique():
            cat_items = category_items[category_items['categoryid'] == cat_id]['itemid'].tolist()
            self.category_popular[int(cat_id)] = cat_items
        
        print(f"[OK] Loaded popular items for {len(self.category_popular)} categories")
        
        conn.close()
        
        print("\n" + "="*60)
        print("[SUCCESS] Trained!")
        print("="*60 + "\n")
    
    def recommend(self, user_id=None, n=10):
        """Generate recommendations"""
        
        if user_id is None:
            return self.popular_items[:n]
        
        # Convert numpy int64 to Python int
        user_id = int(user_id)
        
        # Get user's favorite category
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT favorite_category 
            FROM user_features 
            WHERE visitorid = %s
        """, (user_id,))
        
        result = cursor.fetchone()
        conn.close()
        
        if result and result[0] is not None and result[0] in self.category_popular:
            return self.category_popular[result[0]][:n]
        
        return self.popular_items[:n]
    
    def save_model(self, filepath="data/models/popularity_model.pkl"):
        """Save model"""
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        
        model_data = {
            'popular_items': self.popular_items,
            'category_popular': self.category_popular
        }
        
        with open(filepath, 'wb') as f:
            pickle.dump(model_data, f)
        
        print(f"[OK] Model saved to {filepath}")
    
    def load_model(self, filepath="data/models/popularity_model.pkl"):
        """Load model"""
        with open(filepath, 'rb') as f:
            model_data = pickle.load(f)
        
        self.popular_items = model_data['popular_items']
        self.category_popular = model_data['category_popular']

def main():
    model = PopularityRecommender()
    model.train()
    model.save_model()
    
    # Test
    print("\n" + "="*60)
    print("TESTING")
    print("="*60)
    
    print("\nTest 1: No user context")
    recs = model.recommend(n=5)
    print(f"Recommendations: {recs}")
    
    conn = psycopg2.connect(**DB_CONFIG)
    sample = pd.read_sql("SELECT visitorid FROM user_features LIMIT 1", conn)
    conn.close()
    
    if len(sample) > 0:
        user_id = sample['visitorid'].iloc[0]
        print(f"\nTest 2: User {user_id}")
        recs = model.recommend(user_id=user_id, n=5)
        print(f"Recommendations: {recs}")
    
    print("\n" + "="*60 + "\n")

if __name__ == "__main__":
    main()