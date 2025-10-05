import os
from dotenv import load_dotenv 
import psycopg2

def test_connection():
    """Test PostgreSQL connection"""

    # Load environment variables from .env
    load_dotenv()

    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME", "recommender_db"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432")
        )

        print("Connection successful!")

        # Test query
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f" PostgreSQL version: {version[0]}")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"Connection failed: {e}")
        return False


if __name__ == "__main__":
    test_connection()
