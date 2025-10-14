import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix
from sklearn.metrics.pairwise import cosine_similarity
import psycopg2
import os
from dotenv import load_dotenv
import pickle
from pathlib import Path

load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

class CollaborativeFilteringModel:
    def __init__(self):
        self.user_item_matrix = None
        self.user_similarity = None
        self.item_similarity = None
        self.user_ids = None
        self.item_ids = None
        
    def load_data(self):
        """Load ONLY active buyers with sampling"""
        conn = psycopg2.connect(**DB_CONFIG)
        
        query = """
        SELECT 
            e.visitorid,
            e.itemid,
            CASE 
                WHEN e.event = 'transaction' THEN 5
                WHEN e.event = 'addtocart' THEN 3
                WHEN e.event = 'view' THEN 1
            END as implicit_rating
        FROM events_train e
        INNER JOIN user_features uf ON e.visitorid = uf.visitorid
        WHERE uf.user_segment IN ('converter', 'power_user')
        """
        
        df = pd.read_sql(query, conn)
        
        # Additional sampling if needed
        n_users = df['visitorid'].nunique()
        print(f"[INFO] Loaded {n_users:,} active users")
        
        if n_users > 30000:
            print(f"[INFO] Sampling top 30,000 users...")
            top_users = df.groupby('visitorid').size().nlargest(30000).index
            df = df[df['visitorid'].isin(top_users)]
        
        return df
    
    def build_user_item_matrix(self, interactions_df):
        """Create user-item interaction matrix"""
        print("[INFO] Building user-item matrix...")
        
        # Aggregate interactions (sum ratings per user-item pair)
        user_item = interactions_df.groupby(['visitorid', 'itemid'])['implicit_rating'].sum().reset_index()
        
        # Pivot to matrix
        matrix = user_item.pivot(
            index='visitorid',
            columns='itemid',
            values='implicit_rating'
        ).fillna(0)
        
        self.user_ids = matrix.index.values
        self.item_ids = matrix.columns.values
        self.user_item_matrix = csr_matrix(matrix.values)
        
        sparsity = (1 - self.user_item_matrix.nnz / (self.user_item_matrix.shape[0] * self.user_item_matrix.shape[1])) * 100
        
        print(f"[OK] Matrix shape: {self.user_item_matrix.shape}")
        print(f"[OK] Users: {len(self.user_ids):,}, Items: {len(self.item_ids):,}")
        print(f"[OK] Sparsity: {sparsity:.2f}%")
    
    def train(self):
        """Train collaborative filtering model"""
        print("\n[INFO] Training item-based CF...")
        print("[INFO] Computing item similarities ...")
        
        # Item-based CF (faster and often better than user-based)
        self.item_similarity = cosine_similarity(self.user_item_matrix.T, dense_output=False)
        
        print("[OK] Model trained")
    
    def recommend(self, user_id, n_recommendations=10):
        """Generate recommendations for a user"""
        if user_id not in self.user_ids:
            return []  # Cold start - return empty (handle separately)
        
        user_idx = np.where(self.user_ids == user_id)[0][0]
        user_items_idx = np.where(self.user_item_matrix[user_idx].toarray().flatten() > 0)[0]
        
        if len(user_items_idx) == 0:
            return []  # User has no interactions
        
        # Score items based on similarity to user's items
        item_scores = np.zeros(len(self.item_ids))
        for user_item_idx in user_items_idx:
            item_sims = self.item_similarity[user_item_idx].toarray().flatten()
            item_scores += item_sims
        
        # Remove items user already interacted with
        item_scores[user_items_idx] = -1
        
        # Get top N
        top_items_idx = np.argsort(item_scores)[::-1][:n_recommendations]
        recommendations = [int(self.item_ids[idx]) for idx in top_items_idx if item_scores[idx] > 0]
        
        return recommendations
    
    def save_model(self):
        """Save trained model"""
        model_dir = Path("data/models")
        model_dir.mkdir(parents=True, exist_ok=True)
        
        model_data = {
            'user_item_matrix': self.user_item_matrix,
            'item_similarity': self.item_similarity,
            'user_ids': self.user_ids,
            'item_ids': self.item_ids
        }
        
        filepath = model_dir / "cf_model.pkl"
        with open(filepath, 'wb') as f:
            pickle.dump(model_data, f)
        
        print(f"[OK] Model saved to {filepath}")


def main():
    print("COLLABORATIVE FILTERING MODEL")
    
    # Initialize
    model = CollaborativeFilteringModel()
    
    # Load data
    interactions = model.load_data()
    
    # Build matrix
    model.build_user_item_matrix(interactions)
    
    # Train
    model.train()
    
    # Save
    model.save_model()
    
    # Test on sample users
    print("TESTING RECOMMENDATIONS")
    
    sample_users = model.user_ids[:5]
    for user_id in sample_users:
        recs = model.recommend(user_id, n_recommendations=5)
        print(f"User {user_id}: {recs}")
    
    print("[SUCCESS] Model trained and saved!")

if __name__ == "__main__":
    main()