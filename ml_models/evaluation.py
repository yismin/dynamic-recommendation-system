import pandas as pd
import numpy as np
import psycopg2
import pickle
from pathlib import Path
import os
from dotenv import load_dotenv
import warnings
warnings.filterwarnings('ignore')

load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

class ModelEvaluator:
    """
    Evaluate and compare all recommendation models
    
    Metrics:
    - Hit Rate@K: % of users where true item is in top K
    - Precision@K: % of recommendations that are relevant
    - Coverage: % of users that receive recommendations
    - A/B Test Simulation: Statistical significance of uplift
    """
    
    def __init__(self):
        self.models = {}
        self.test_data = None
        self.results = {}
        
    def load_test_data(self):
        """Load test set"""
        print("\n" + "="*60)
        print("LOADING TEST DATA")
        print("="*60 + "\n")
        
        conn = psycopg2.connect(**DB_CONFIG)
        
        self.test_data = pd.read_sql("""
            SELECT visitorid, itemid, event
            FROM test_set
            WHERE event IN ('addtocart', 'transaction')
            ORDER BY visitorid, timestamp
        """, conn)
        
        conn.close()
        
        print(f"[OK] Loaded {len(self.test_data):,} test interactions")
        print(f"[OK] Unique users: {self.test_data['visitorid'].nunique():,}")
        print(f"[OK] Unique items: {self.test_data['itemid'].nunique():,}")
        
    def load_models(self):
        """Load all trained models"""
        print("\n" + "="*60)
        print("LOADING MODELS")
        print("="*60 + "\n")
        
        model_files = {
            'Popularity': 'data/models/popularity_model.pkl',
            'Trending': 'data/models/trending_model.pkl',
            'Category_CF': 'data/models/category_cf.pkl'
        }
        
        for name, filepath in model_files.items():
            if Path(filepath).exists():
                with open(filepath, 'rb') as f:
                    self.models[name] = pickle.load(f)
                print(f"[OK] Loaded {name}")
            else:
                print(f"[SKIP] {name} not found at {filepath}")
        
        print(f"\n[OK] Loaded {len(self.models)} models")
    
    def hit_rate_at_k(self, recommendations, true_items, k=10):
        """Calculate hit rate@k"""
        if not recommendations:
            return 0
        
        top_k = recommendations[:k]
        return 1 if any(item in top_k for item in true_items) else 0
    
    def precision_at_k(self, recommendations, true_items, k=10):
        """Calculate precision@k"""
        if not recommendations:
            return 0
        
        top_k = recommendations[:k]
        hits = sum(1 for item in top_k if item in true_items)
        return hits / len(top_k)
    
    def evaluate_popularity(self, k=10):
        """Evaluate popularity recommender"""
        print("\n[1/3] Evaluating Popularity Model...")
        
        model = self.models.get('Popularity')
        if not model:
            return {}
        
        hits = 0
        precisions = []
        
        for user_id, group in self.test_data.groupby('visitorid'):
            true_items = group['itemid'].tolist()
            
            # Get recommendations (popularity is same for everyone)
            recs = model['popular_items'][:k]
            
            hits += self.hit_rate_at_k(recs, true_items, k)
            precisions.append(self.precision_at_k(recs, true_items, k))
        
        return {
            'hit_rate': hits / len(self.test_data['visitorid'].unique()),
            'precision': np.mean(precisions),
            'coverage': 1.0  # Works for all users
        }
    
    def evaluate_trending(self, k=10):
        """Evaluate trending model"""
        print("\n[2/3] Evaluating Trending Model...")
        
        model = self.models.get('Trending')
        if not model or not model['trending_items']:
            return {}
        
        hits = 0
        precisions = []
        
        for user_id, group in self.test_data.groupby('visitorid'):
            true_items = group['itemid'].tolist()
            
            recs = model['trending_items'][:k]
            
            hits += self.hit_rate_at_k(recs, true_items, k)
            precisions.append(self.precision_at_k(recs, true_items, k))
        
        return {
            'hit_rate': hits / len(self.test_data['visitorid'].unique()),
            'precision': np.mean(precisions),
            'coverage': 1.0  # Works for all users
        }
    
    def evaluate_category_cf(self, k=10):
        """Evaluate category CF"""
        print("\n[3/3] Evaluating Category CF...")
        
        model = self.models.get('Category_CF')
        if not model:
            print("[SKIP] Category CF model not loaded")
            return {}
        
        # Check if model has required data
        if 'user_ids' not in model or model['user_ids'] is None:
            print("[SKIP] Category CF model has no user data")
            return {}
        
        hits = 0
        precisions = []
        users_with_recs = 0
        total_users = 0
        
        for user_id, group in self.test_data.groupby('visitorid'):
            total_users += 1
            
            # Check if user exists in model
            if user_id not in model['user_ids']:
                continue
            
            users_with_recs += 1
            true_items = group['itemid'].tolist()
            
            # Get user index
            user_idx = np.where(model['user_ids'] == user_id)[0][0]
            
            # Get similar users (top 10)
            user_sims = model['user_similarity'][user_idx].toarray().flatten()
            similar_users = np.argsort(user_sims)[::-1][1:11]  # Exclude self
            
            # Aggregate category scores from similar users
            category_scores = np.zeros(len(model['category_ids']))
            for sim_idx in similar_users:
                if user_sims[sim_idx] > 0:
                    category_scores += model['user_category_matrix'][sim_idx].toarray().flatten() * user_sims[sim_idx]
            
            # Get top categories
            top_cat_indices = np.argsort(category_scores)[::-1][:5]
            
            # Get items from those categories
            recs = []
            for cat_idx in top_cat_indices:
                if category_scores[cat_idx] > 0:
                    cat_id = model['category_ids'][cat_idx]
                    if cat_id in model['category_popular_items']:
                        items = model['category_popular_items'][cat_id][:5]
                        recs.extend([int(item) for item in items])
            
            # Remove duplicates while preserving order
            seen = set()
            final_recs = []
            for item in recs:
                if item not in seen:
                    seen.add(item)
                    final_recs.append(item)
            
            final_recs = final_recs[:k]
            
            if final_recs:
                hits += self.hit_rate_at_k(final_recs, true_items, k)
                precisions.append(self.precision_at_k(final_recs, true_items, k))
        
        if users_with_recs == 0:
            print("[SKIP] No test users found in Category CF model")
            return {
                'hit_rate': 0,
                'precision': 0,
                'coverage': 0
            }
        
        coverage = users_with_recs / total_users
        
        print(f"[OK] Evaluated {users_with_recs}/{total_users} users ({coverage:.1%} coverage)")
        
        return {
            'hit_rate': hits / users_with_recs if users_with_recs > 0 else 0,
            'precision': np.mean(precisions) if precisions else 0,
            'coverage': coverage
        }
    
    def run_evaluation(self, k=10):
        """Run full evaluation"""
        print("\n" + "="*60)
        print("MODEL EVALUATION")
        print("="*60)
        
        self.load_test_data()
        self.load_models()
        
        print("\n" + "="*60)
        print("COMPUTING METRICS")
        print("="*60)
        
        self.results['Popularity'] = self.evaluate_popularity(k)
        self.results['Trending'] = self.evaluate_trending(k)
        self.results['Category_CF'] = self.evaluate_category_cf(k)
        
        self.print_results()
        self.save_results()
    
    def print_results(self):
        """Print comparison table"""
        print("\n" + "="*60)
        print("RESULTS COMPARISON")
        print("="*60 + "\n")
        
        print(f"{'Model':<20} {'Hit Rate@10':<15} {'Precision@10':<15} {'Coverage':<10}")
        print("-" * 60)
        
        for model_name, metrics in self.results.items():
            if metrics:
                hit_rate = metrics.get('hit_rate', 0)
                precision = metrics.get('precision', 0)
                coverage = metrics.get('coverage', 0)
                
                print(f"{model_name:<20} "
                      f"{hit_rate:>6.2%}          "
                      f"{precision:>6.2%}          "
                      f"{coverage:>6.2%}")
            else:
                print(f"{model_name:<20} {'N/A':<15} {'N/A':<15} {'N/A':<10}")
        
        print("\n" + "="*60)
        
        # Best model
        valid_results = {k: v for k, v in self.results.items() if v and v.get('hit_rate', 0) > 0}
        
        if valid_results:
            best_model = max(valid_results.items(), key=lambda x: x[1].get('hit_rate', 0))
            
            print(f"\n🏆 BEST MODEL: {best_model[0]}")
            print(f"   Hit Rate@10: {best_model[1]['hit_rate']:.2%}")
            print(f"   Precision@10: {best_model[1]['precision']:.2%}")
            print(f"   Coverage: {best_model[1]['coverage']:.2%}")
        else:
            print("\n⚠️  No valid results to compare")
    
    def save_results(self):
        """Save results to file"""
        output_file = "data/evaluation_results.json"
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        
        import json
        with open(output_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        print(f"\n[OK] Results saved to {output_file}")
    
    def simulate_ab_test(self):
        """
        Simulate A/B test: Popularity vs Category CF
        
        Measures: Click-through rate on recommendations
        """
        print("\n" + "="*60)
        print("A/B TEST SIMULATION")
        print("="*60 + "\n")
        
        print("Scenario: Popularity (Control) vs Category CF (Treatment)")
        print("Metric: Recommendation relevance (hit rate)\n")
        
        # Control: Popularity recommendations
        control_hits = 0
        control_total = 0
        
        # Treatment: Category CF recommendations
        treatment_hits = 0
        treatment_total = 0
        
        pop_model = self.models['Popularity']
        cf_model = self.models.get('Category_CF')
        
        if not cf_model:
            print("[ERROR] Category CF model not available")
            return
        
        # Simulate for users who exist in both
        for user_id, group in self.test_data.groupby('visitorid'):
            true_items = group['itemid'].tolist()
            
            # Control group: Popularity
            pop_recs = pop_model['popular_items'][:10]
            control_total += 1
            if any(item in pop_recs for item in true_items):
                control_hits += 1
            
            # Treatment group: Category CF (only for users in the model)
            if user_id in cf_model['user_ids']:
                treatment_total += 1
                
                # Get user index
                user_idx = np.where(cf_model['user_ids'] == user_id)[0][0]
                
                # Get similar users
                user_sims = cf_model['user_similarity'][user_idx].toarray().flatten()
                similar_users = np.argsort(user_sims)[::-1][1:11]
                
                # Get category scores
                category_scores = np.zeros(len(cf_model['category_ids']))
                for sim_idx in similar_users:
                    if user_sims[sim_idx] > 0:
                        category_scores += cf_model['user_category_matrix'][sim_idx].toarray().flatten()
                
                # Get top categories
                top_cats = np.argsort(category_scores)[::-1][:3]
                
                # Get recommendations
                cf_recs = []
                for cat_idx in top_cats:
                    if category_scores[cat_idx] > 0:
                        cat_id = cf_model['category_ids'][cat_idx]
                        if cat_id in cf_model['category_popular_items']:
                            cf_recs.extend(cf_model['category_popular_items'][cat_id][:5])
                
                cf_recs = list(dict.fromkeys(cf_recs))[:10]
                
                if any(item in cf_recs for item in true_items):
                    treatment_hits += 1
        
        # Calculate metrics
        control_hit_rate = control_hits / control_total if control_total > 0 else 0
        treatment_hit_rate = treatment_hits / treatment_total if treatment_total > 0 else 0
        
        # Calculate uplift
        uplift = ((treatment_hit_rate - control_hit_rate) / control_hit_rate * 100) if control_hit_rate > 0 else 0
        
        # Statistical significance (simple chi-square)
        from scipy import stats
        
        contingency_table = [
            [treatment_hits, treatment_total - treatment_hits],
            [control_hits, control_total - control_hits]
        ]
        chi2, p_value = stats.chi2_contingency(contingency_table)[:2]
        
        print(f"Sample Sizes:")
        print(f"  Control (Popularity): {control_total:,} users")
        print(f"  Treatment (Category CF): {treatment_total:,} users")
        
        print(f"\nControl (Popularity):")
        print(f"  Hit Rate: {control_hit_rate:.2%}")
        print(f"  Hits: {control_hits:,}/{control_total:,}")
        
        print(f"\nTreatment (Category CF):")
        print(f"  Hit Rate: {treatment_hit_rate:.2%}")
        print(f"  Hits: {treatment_hits:,}/{treatment_total:,}")
        
        print(f"\nResults:")
        print(f"  Absolute Lift: {(treatment_hit_rate - control_hit_rate)*100:+.2f} percentage points")
        print(f"  Relative Lift: {uplift:+.1f}%")
        print(f"  P-value: {p_value:.4f}")
        
        if p_value < 0.05:
            significance = "✅ STATISTICALLY SIGNIFICANT"
        else:
            significance = "⚠️  Not statistically significant"
        
        print(f"\n{significance}")
        
        if uplift > 0:
            print(f"\n🎉 Category CF wins! {uplift:.1f}% better than baseline")
            print(f"   Expected impact: If 1000 users see recommendations,")
            print(f"   Category CF will generate {int(uplift * 10)} more clicks")
        else:
            print(f"\n⚠️  Category CF underperforms by {abs(uplift):.1f}%")
        
        print("\n" + "="*60)


def main():
    evaluator = ModelEvaluator()
    evaluator.run_evaluation(k=10)
    evaluator.simulate_ab_test()


if __name__ == "__main__":
    main()