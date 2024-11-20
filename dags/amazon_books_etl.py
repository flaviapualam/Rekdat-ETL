from datetime import datetime, timedelta
from airflow import DAG
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Headers for web scraping
headers = {
    "Referer": 'https://www.amazon.com/',
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "macOS",
    'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
}

# Function to fetch Amazon books data (Extract + Transform)
def fetch_amazon_books(num_books, ti):
    base_url = "https://www.amazon.com/s?k=data+engineering+books"
    books = []
    seen_titles = set()
    page = 1

    while len(books) < num_books:
        url = f"{base_url}&page={page}"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            book_containers = soup.find_all("div", {"class": "s-result-item"})

            for book in book_containers:
                title = book.find("span", {"class": "a-text-normal"})
                author = book.find("a", {"class": "a-size-base"})
                price = book.find("span", {"class": "a-price-whole"})
                rating = book.find("span", {"class": "a-icon-alt"})

                if title and author and price and rating:
                    book_title = title.text.strip()
                    if book_title not in seen_titles:
                        seen_titles.add(book_title)
                        books.append({
                            "Title": book_title,
                            "Author": author.text.strip(),
                            "Price": price.text.strip(),
                            "Rating": rating.text.strip(),
                        })

            page += 1
        else:
            print("Failed to retrieve the page.")
            break

    books = books[:num_books]
    df = pd.DataFrame(books)
    df.drop_duplicates(subset="Title", inplace=True)
    ti.xcom_push(key='amazon_book_data', value=df.to_dict('records'))

# Function to load data into PostgreSQL (Load)
def load_amazon_books_to_postgres(ti):
    book_data = ti.xcom_pull(key='amazon_book_data', task_ids='fetch_amazon_books')
    if not book_data:
        raise ValueError("No book data found.")

    postgres_hook = PostgresHook(postgres_conn_id='books_connection')
    insert_query = """
    INSERT INTO AmazonBooks (title, authors, price, rating)
    VALUES (%s, %s, %s, %s);
    """
    for book in book_data:
        postgres_hook.run(insert_query, parameters=(book['Title'], book['Author'], book['Price'], book['Rating']))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'amazon_books_etl',
    default_args=default_args,
    description='ETL for Amazon books every hour',
    schedule_interval='@hourly',
)

fetch_amazon_books_task = PythonOperator(
    task_id='fetch_amazon_books',
    python_callable=fetch_amazon_books,
    op_args=[50],  # Number of books to fetch
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_amazon_books_table',
    postgres_conn_id='books_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS AmazonBooks (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        authors TEXT,
        price TEXT,
        rating TEXT
    );
    """,
    dag=dag,
)

load_amazon_books_task = PythonOperator(
    task_id='load_amazon_books_to_postgres',
    python_callable=load_amazon_books_to_postgres,
    dag=dag,
)

fetch_amazon_books_task >> create_table_task >> load_amazon_books_task
