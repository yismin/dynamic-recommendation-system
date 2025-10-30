import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix
from sklearn.metrics.pairwise import cosine_similarity
import psycopg2
import pickle
from pathlib import Path
import os
from dotenv import load_dotenv
import warnings
warnings.filterwarnings('ignore', category=UserWarning)

load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

class CategoryCollaborativeFiltering:
    """
    Collaborative Filtering on CATEGORIES instead of items
    
    Why this works when item CF failed:
    - 1,669 categories vs 417K items
    - Much denser user-category matrix
    - More stable patterns
    """
    
    def __init__(self):
        self.user_category_matrix = None
        self.user_similarity = None
        self.user_ids = None
        self.category_ids = None
        self.category_popular_items = {}
        
    def train(self, use_train_set=True):
        """
        Build category-based CF model
        
        Args:
            use_train_set: If True, use only train_set for proper evaluation
        """
        print("\n" + "="*60)
        print("CATEGORY-BASED COLLABORATIVE FILTERING")
        print("="*60 + "\n")
        
        conn = psycopg2.connect(**DB_CONFIG)
        
        # Choose data source
        table_name = "train_set" if use_train_set else "events"
        print(f"[INFO] Training on: {table_name}")
        
        # Get user-category interactions
        print("[1/5] Loading user-category interactions...")
        interactions = pd.read_sql(f"""
            SELECT 
                e.visitorid,
                ip.categoryid,
                COUNT(*) as interaction_count,
                SUM(CASE WHEN e.event = 'transaction' THEN 1 ELSE 0 END) as purchases,
                SUM(CASE WHEN e.event = 'addtocart' THEN 1 ELSE 0 END) as carts,
                SUM(CASE WHEN e.event = 'view' THEN 1 ELSE 0 END) as views
            FROM {table_name} e
            JOIN item_properties ip ON e.itemid = ip.itemid
            WHERE ip.categoryid IS NOT NULL
            GROUP BY e.visitorid, ip.categoryid
            HAVING COUNT(*) >= 2
        """, conn)
        
        print(f"[OK] Loaded {len(interactions):,} user-category pairs")
        
        # Calculate weighted interaction score
        print("\n[2/5] Calculating interaction scores...")
        interactions['score'] = (
            interactions['purchases'] * 5 + 
            interactions['carts'] * 3 + 
            interactions['views'] * 1
        )
        
        # Build user-category matrix
        print("\n[3/5] Building user-category matrix...")
        matrix = interactions.pivot(
            index='visitorid',
            columns='categoryid',
            values='score'
        ).fillna(0)
        
        self.user_ids = matrix.index.values
        self.category_ids = matrix.columns.values
        self.user_category_matrix = csr_matrix(matrix.values)
        
        sparsity = (1 - self.user_category_matrix.nnz / 
                   (self.user_category_matrix.shape[0] * self.user_category_matrix.shape[1])) * 100
        
        print(f"[OK] Matrix shape: {self.user_category_matrix.shape}")
        print(f"[OK] Users: {len(self.user_ids):,}, Categories: {len(self.category_ids):,}")
        print(f"[OK] Sparsity: {sparsity:.2f}% (vs 99.9% for item-based CF!)")
        
        # Calculate user similarities
        print("\n[4/5] Computing user similarities...")
        print("[INFO] This may take 2-3 minutes...")
        self.user_similarity = cosine_similarity(self.user_category_matrix, dense_output=False)
        
        print(f"[OK] Computed similarities for {len(self.user_ids):,} users")
        
        # Get popular items per category (from train set only)
        print("\n[5/5] Loading popular items per category...")
        for cat_id in self.category_ids:
            items = pd.read_sql(f"""
                SELECT e.itemid, COUNT(*) as popularity
                FROM {table_name} e
                JOIN item_properties ip ON e.itemid = ip.itemid
                WHERE ip.categoryid = {cat_id}
                  AND e.event = 'transaction'
                GROUP BY e.itemid
                ORDER BY popularity DESC
                LIMIT 30
            """, conn)
            
            if len(items) > 0:
                self.category_popular_items[int(cat_id)] = items['itemid'].tolist()
            else:
                # Fallback to views if no purchases
                items = pd.read_sql(f"""
                    SELECT e.itemid, COUNT(*) as popularity
                    FROM {table_name} e
                    JOIN item_properties ip ON e.itemid = ip.itemid
                    WHERE ip.categoryid = {cat_id}
                    GROUP BY e.itemid
                    ORDER BY popularity DESC
                    LIMIT 30
                """, conn)
                if len(items) > 0:
                    self.category_popular_items[int(cat_id)] = items['itemid'].tolist()
        
        print(f"[OK] Loaded top items for {len(self.category_popular_items)} categories")
        
        conn.close()
        
        print("\n" + "="*60)
        print("[SUCCESS] Category CF trained!")
        print("="*60 + "\n")
    
    def recommend(self, user_id, n=10):
        """
        Recommend items based on category preferences of similar users
        """
        
        if user_id not in self.user_ids:
            return []
        
        # Get user index
        user_idx = np.where(self.user_ids == user_id)[0][0]
        
        # Get similar users
        user_sims = self.user_similarity[user_idx].toarray().flatten()
        similar_user_indices = np.argsort(user_sims)[::-1][1:31]  # Top 30 similar (exclude self)
        
        # Get user's categories (to avoid recommending same category)
        user_categories = self.user_category_matrix[user_idx].toarray().flatten()
        user_top_categories = set(np.where(user_categories > 0)[0])
        
        # Score categories based on similar users
        category_scores = np.zeros(len(self.category_ids))
        
        for sim_user_idx in similar_user_indices:
            if user_sims[sim_user_idx] > 0:
                sim_user_categories = self.user_category_matrix[sim_user_idx].toarray().flatten()
                category_scores += sim_user_categories * user_sims[sim_user_idx]
        
        # Get top categories (excluding user's current favorites)
        category_scores_filtered = category_scores.copy()
        for cat_idx in user_top_categories:
            category_scores_filtered[cat_idx] = -1
        
        top_category_indices = np.argsort(category_scores_filtered)[::-1][:5]
        
        # Get items from recommended categories
        recommendations = []
        for cat_idx in top_category_indices:
            if category_scores_filtered[cat_idx] > 0:
                cat_id = self.category_ids[cat_idx]
                if cat_id in self.category_popular_items:
                    recommendations.extend(self.category_popular_items[cat_id][:10])
        
        # Remove duplicates while preserving order
        seen = set()
        final_recs = []
        for item in recommendations:
            if item not in seen:
                seen.add(item)
                final_recs.append(int(item))
        
        return final_recs[:n]
    
    def save_model(self, filepath="data/models/category_cf.pkl"):
        """Save trained model"""
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        
        with open(filepath, 'wb') as f:
            pickle.dump({
                'user_category_matrix': self.user_category_matrix,
                'user_similarity': self.user_similarity,
                'user_ids': self.user_ids,
                'category_ids': self.category_ids,
                'category_popular_items': self.category_popular_items
            }, f)
        
        print(f"[OK] Model saved to {filepath}")
    
    def load_model(self, filepath="data/models/category_cf.pkl"):
        """Load saved model"""
        with open(filepath, 'rb') as f:
            model_data = pickle.load(f)
        
        self.user_category_matrix = model_data['user_category_matrix']
        self.user_similarity = model_data['user_similarity']
        self.user_ids = model_data['user_ids']
        self.category_ids = model_data['category_ids']
        self.category_popular_items = model_data['category_popular_items']
        
        print(f"[OK] Model loaded from {filepath}")


def main():
    """Train and test Category CF"""
    
    model = CategoryCollaborativeFiltering()
    
    # IMPORTANT: Train on train_set only for proper evaluation
    model.train(use_train_set=True)
    model.save_model()
    
    # Test recommendations
    print("\n" + "="*60)
    print("TESTING RECOMMENDATIONS")
    print("="*60)
    
    if len(model.user_ids) > 0:
        test_user = model.user_ids[0]
        print(f"\nUser {test_user} recommendations:")
        recs = model.recommend(test_user, n=10)
        print(f"Items: {recs}")
        
        # Test a few more users
        for i in range(min(3, len(model.user_ids))):
            user = model.user_ids[i]
            recs = model.recommend(user, n=5)
            print(f"\nUser {user}: {recs}")
    
    print("\n" + "="*60 + "\n")


if __name__ == "__main__":
    main()