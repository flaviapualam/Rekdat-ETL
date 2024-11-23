from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import requests
from bs4 import BeautifulSoup
import re
import html
from typing import List, Dict

# Constants
POSTGRES_CONN_ID = 'postgres_default'

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Define the DAG
with DAG(
    dag_id='goodreads_books_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    @task()
    def extract_goodreads_books() -> List[Dict]:
        """Extract books data from Goodreads website."""
        url = "https://www.goodreads.com/genres/new_releases/biography"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        # Perform HTTP request
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all <script> elements
        scripts = soup.find_all('script')

        # List to store book data
        books = []

        # Iterate through script elements
        for script in scripts:
            if script.string:  # Ensure <script> has content
                script_content = script.string

                # Extract book title
                title_match = re.search(r'readable bookTitle.*?>([^<]+)<', script_content)
                title = html.unescape(title_match.group(1)) if title_match else None

                # Extract rating
                rating_match = re.search(r'(\d+\.\d+)\savg rating', script_content)
                rating = html.unescape(rating_match.group(1)) if rating_match else None

                # Extract number of ratings
                ratings_count_match = re.search(r'(\d{1,3}(?:,\d{3})*)\sratings', script_content)
                ratings_count = html.unescape(ratings_count_match.group(1)) if ratings_count_match else None

                # Save book data if complete
                if title and rating and ratings_count:
                    books.append({
                        'title': title,
                        'rating': float(rating),
                        'ratings_count': int(ratings_count.replace(',', ''))
                    })

                # Stop if 100 books have been fetched
                if len(books) >= 100:
                    break

        if not books:
            raise Exception("No books were successfully extracted")

        return books

    @task()
    def transform_goodreads_books(extracted_data: List[Dict]) -> List[Dict]:
        """Transform the extracted books data."""
        transformed_data = []
        
        for book in extracted_data:
            transformed_book = {
                'title': book['title'].strip().title(),  # Clean and standardize title
                'rating': round(float(book['rating']), 2),  # Round rating to 2 decimal places
                'ratings_count': int(book['ratings_count']),  # Ensure ratings_count is an integer
            }
            transformed_data.append(transformed_book)
        
        return transformed_data

    @task()
    def load_goodreads_books(transformed_data: List[Dict]) -> None:
        """Load books data into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS goodreads_books (
            title TEXT,
            rating FLOAT,
            ratings_count INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Insert books data
        for book in transformed_data:
            cursor.execute("""
            INSERT INTO goodreads_books (title, rating, ratings_count)
            VALUES (%s, %s, %s)
            """, (
                book['title'],
                book['rating'],
                book['ratings_count']
            ))

        conn.commit()
        cursor.close()

    # Define the DAG workflow
    extracted_books = extract_goodreads_books()
    transformed_books = transform_goodreads_books(extracted_books)
    load_goodreads_books(transformed_books)
