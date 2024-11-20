from datetime import datetime, timedelta
from airflow import DAG
import requests
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Function to fetch Google Books data (Extract + Transform)
def fetch_google_books(num_books, ti):
    API_KEY = "goggle_books_api_key"
    query = "data engineering"
    url = f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults={num_books}&key={API_KEY}"

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        books = []
        for item in data.get('items', []):
            volume_info = item.get('volumeInfo', {})
            books.append({
                "Title": volume_info.get('title', 'N/A'),
                "Authors": ', '.join(volume_info.get('authors', [])),
                "Description": volume_info.get('description', 'N/A'),
                "AverageRating": volume_info.get('averageRating', 'N/A'),
                "ReviewsCount": volume_info.get('ratingsCount', 0),
            })

        df = pd.DataFrame(books)
        ti.xcom_push(key='google_book_data', value=df.to_dict('records'))
    else:
        raise ValueError("Failed to fetch data from Google Books API.")

# Function to load data into PostgreSQL (Load)
def load_google_books_to_postgres(ti):
    book_data = ti.xcom_pull(key='google_book_data', task_ids='fetch_google_books')
    if not book_data:
        raise ValueError("No book data found.")

    postgres_hook = PostgresHook(postgres_conn_id='books_connection')
    insert_query = """
    INSERT INTO GoogleBooks (title, authors, description, average_rating, reviews_count)
    VALUES (%s, %s, %s, %s, %s);
    """
    for book in book_data:
        postgres_hook.run(insert_query, parameters=(book['Title'], book['Authors'], book['Description'], book['AverageRating'], book['ReviewsCount']))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'google_books_etl',
    default_args=default_args,
    description='ETL for Google books every hour',
    schedule_interval=timedelta(days=1),
)

fetch_google_books_task = PythonOperator(
    task_id='fetch_google_books',
    python_callable=fetch_google_books,
    op_args=[500],  # Number of books to fetch
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_google_books_table',
    postgres_conn_id='books_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS GoogleBooks (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        authors TEXT,
        description TEXT,
        average_rating TEXT,
        reviews_count INTEGER
    );
    """,
    dag=dag,
)

load_google_books_task = PythonOperator(
    task_id='load_google_books_to_postgres',
    python_callable=load_google_books_to_postgres,
    dag=dag,
)

fetch_google_books_task >> create_table_task >> load_google_books_task
