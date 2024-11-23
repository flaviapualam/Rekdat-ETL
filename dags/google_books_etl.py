from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import requests
from dotenv import load_dotenv
import os

load_dotenv()

POSTGRES_CONN_ID = 'postgres_default'
API_KEY = os.getenv("google_api_key")
BASE_URL = "https://www.googleapis.com/books/v1/volumes"

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Query parameter to search books
QUERY = "philosophy"  

with DAG(dag_id='google_books_etl',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    @task()
    def extract_google_books():
        """Extract books data from Google Books API."""
        params = {
            'q': QUERY,
            'printType': 'books',
            'maxResults': 40,
            'key': API_KEY
        }
        response = requests.get(BASE_URL, params=params)

        if response.status_code == 200:
            books = []
            data = response.json()
            for item in data.get('items', []):
                volume_info = item.get('volumeInfo', {})

                # Collect relevant fields
                books.append({
                    'title': volume_info.get('title'),
                    'rating': volume_info.get('averageRating', 0),  # Default to 0 if no rating
                    'voters': volume_info.get('ratingsCount', 0)  # Default to 0 if no voters
                })

            return books
        else:
            raise Exception(f"Failed to fetch data from Google Books API: {response.status_code}")
    
    @task()
    def transform_google_books(extracted_data):
        """Transform the extracted books data."""
        transformed_data = []
        for book in extracted_data:
            transformed_book = {
                'title': book['title'].strip().title(),  # Clean and standardize title
                'rating': round(float(book['rating']), 2),  # Round rating to 2 decimal places
                'voters': int(book['voters'])  # Ensure voters is an integer
            }
            transformed_data.append(transformed_book)
        
        return transformed_data
    
    @task()
    def load_google_books(transformed_data):
        """Load books data into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS google_books (
            title TEXT,
            rating FLOAT,
            voters INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Insert books data 
        for book in transformed_data:
            cursor.execute("""
            INSERT INTO google_books (title, rating, voters)
            VALUES (%s, %s, %s)
            """, (
                book['title'],
                book['rating'],
                book['voters']
            ))

        conn.commit()
        cursor.close()

    # DAG workflow
    extracted_books = extract_google_books()
    transformed_books = transform_google_books(extracted_books)
    load_google_books(transformed_books)