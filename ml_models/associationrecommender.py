import pandas as pd
import psycopg2
from collections import defaultdict
from itertools import combinations
import pickle
from pathlib import Path
import os
from dotenv import load_dotenv
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

class AssociationRulesRecommender:
    """
    'Customers who bought X also bought Y'
    Based on co-occurrence patterns in purchase history
    """
    
    def __init__(self, min_support=3):
        self.item_pairs = {}  
        self.min_support = min_support
        
    def train(self):
        """Build item-item co-occurrence matrix"""
        print("TRAINING ASSOCIATION RULES")        
        conn = psycopg2.connect(**DB_CONFIG)
        
        # Get purchase transactions (group by user)
        print("[1/2] Loading purchase baskets...")
        transactions = pd.read_sql("""
            SELECT visitorid, itemid
            FROM events
            WHERE event = 'transaction'
            ORDER BY visitorid, timestamp
        """, conn)
        
        conn.close()
        
        # Group items by user (basket)
        baskets = transactions.groupby('visitorid')['itemid'].apply(list).tolist()
        print(f"[OK] Loaded {len(baskets):,} purchase baskets")
        
        # Count co-occurrences
        print("\n[2/2] Computing item associations...")
        pair_counts = defaultdict(int)
        
        for basket in baskets:
            if len(basket) < 2:
                continue
            # Get all pairs in this basket
            for item_a, item_b in combinations(basket, 2):
                # Store both directions
                pair_counts[(item_a, item_b)] += 1
                pair_counts[(item_b, item_a)] += 1
        
        # Filter by minimum support and organize
        for (item_a, item_b), count in pair_counts.items():
            if count >= self.min_support:
                if item_a not in self.item_pairs:
                    self.item_pairs[item_a] = []
                self.item_pairs[item_a].append((item_b, count))
        
        # Sort by frequency
        for item in self.item_pairs:
            self.item_pairs[item].sort(key=lambda x: x[1], reverse=True)
        
        print(f"[OK] Found associations for {len(self.item_pairs):,} items")
        print(f"[OK] Total item pairs: {len(pair_counts):,}")
        print("[SUCCESS] Trained!")
    
    def recommend(self, item_id, n=10):
        """Get items frequently bought with this item"""
        if item_id not in self.item_pairs:
            return []
        
        # Get top N associated items
        associated = self.item_pairs[item_id][:n]
        return [item for item, score in associated]
    
    def recommend_for_basket(self, basket_items, n=10):
        """Recommend based on multiple items (e.g., shopping cart)"""
        if not basket_items:
            return []
        
        # Aggregate scores from all items in basket
        item_scores = defaultdict(int)
        
        for item in basket_items:
            if item in self.item_pairs:
                for assoc_item, score in self.item_pairs[item]:
                    if assoc_item not in basket_items:  
                        item_scores[assoc_item] += score
        
        # Sort and return top N
        sorted_items = sorted(item_scores.items(), key=lambda x: x[1], reverse=True)
        return [item for item, score in sorted_items[:n]]
    
    def save_model(self, filepath="data/models/association_rules.pkl"):
        """Save model"""
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        
        model_data = {
            'item_pairs': dict(self.item_pairs),
            'min_support': self.min_support
        }
        
        with open(filepath, 'wb') as f:
            pickle.dump(model_data, f)
        
        print(f"[OK] Model saved to {filepath}")

def main():
    model = AssociationRulesRecommender(min_support=3)
    model.train()
    model.save_model()
    
    # Test
    print("TESTING")    
    # Test single item
    test_item = list(model.item_pairs.keys())[0]
    print(f"\nTest 1: Items frequently bought with {test_item}")
    recs = model.recommend(test_item, n=5)
    print(f"Recommendations: {recs}")
    
    # Test basket
    basket = list(model.item_pairs.keys())[:3]
    print(f"\nTest 2: Recommendations for basket {basket}")
    recs = model.recommend_for_basket(basket, n=5)
    print(f"Recommendations: {recs}")
if __name__ == "__main__":
    main()