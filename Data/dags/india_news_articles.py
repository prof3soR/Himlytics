from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.models.xcom_arg import XComArg

# Define DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    dag_id="india_news_article_pipeline",
    start_date=days_ago(1),
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["newsapi", "india", "postgres"]
) as dag:

    # Step 1: Create PostgreSQL Table if Not Exists
    @task
    def create_news_table():
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        CREATE_TABLE_SQL = """
        CREATE TABLE IF NOT EXISTS india_news_articles (
            id SERIAL PRIMARY KEY,
            title TEXT,
            description TEXT,
            url TEXT,
            published_at TIMESTAMP,
            source_name TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """
        pg_hook.run(CREATE_TABLE_SQL)

    # Step 2: Extract News API Data
    districts = [
        "Bilaspur", "Chamba", "Hamirpur", "Kangra", "Kinnaur", "Kullu", "Lahaul and Spiti",
        "Mandi", "Shimla", "Sirmaur", "Solan", "Una"
    ]

    # Build the query string with ORs
    query = "himachal tourism " + " OR ".join([f'"{district} tourism"' for district in districts])

    from_date = '2025-03-12'
    to_date = '2025-04-12'

    extract_news_data = SimpleHttpOperator(
        task_id="extract_news_data",
        http_conn_id="newsapi_conn",
        endpoint=(
            f"v2/everything?q={query}&from={from_date}&to={to_date}"
            f"&language=en&sortBy=publishedAt&apiKey={{{{ conn.newsapi_conn.extra_dejson.api_key }}}}"
        ),
        method="GET",
        response_filter=lambda response: response.json().get("articles", []),
        log_response=True,
    )

    # Step 3: Transform News Data
    @task
    def transform_news_data(response):
        transformed_data = []
        for article in response:
            transformed_data.append({
                'title': article.get("title", ''),
                'description': article.get("description", ''),
                'url': article.get("url", ''),
                'published_at': article.get("publishedAt", ''),
                'source_name': article.get("source", {}).get("name", '')
            })
        return transformed_data

    # Step 4: Load Transformed News to PostgreSQL
    @task
    def load_news_to_postgres(news_data_list):
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")

        insert_query = """
        INSERT INTO india_news_articles (title, description, url, published_at, source_name)
        VALUES (%s, %s, %s, %s, %s);
        """

        for news in news_data_list:
            pg_hook.run(insert_query, parameters=(
                news['title'],
                news['description'],
                news['url'],
                news['published_at'],
                news['source_name'],
            ))

    # Task Dependencies
    create_table_task = create_news_table()
    news_response = extract_news_data
    transformed_news = transform_news_data(XComArg(news_response))
    load_news_task = load_news_to_postgres(transformed_news)

    create_table_task >> news_response >> transformed_news >> load_news_task
