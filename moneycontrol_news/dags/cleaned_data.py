import asyncio
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import sys
import os
import logging
import traceback

# Add project root & utils folder to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)
sys.path.append(os.path.join(project_root, 'utils'))

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Import crawl_news from main.py
try:
    from main import crawl_news  # async function
    logger.info("Successfully imported crawl_news() from main.py")
except Exception as import_error:
    logger.error("Failed to import crawl_news(): %s", import_error)
    traceback.print_exc()
    raise

# Wrapper: run async crawl_news in sync context
def sync_crawl_news_wrapper():
    try:
        logger.info("Launching crawl_news()...")
        asyncio.run(crawl_news())
        logger.info("crawl_news() completed successfully.")
    except Exception as e:
        logger.error("Error in sync_crawl_news_wrapper(): %s", e)
        traceback.print_exc()
        raise

# Default args for DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
with DAG(
    dag_id="cleaned_data",
    default_args=default_args,
    description="Crawl Moneycontrol news incrementally, apply SCD Type 2, upload results to Azure Blob",
    schedule_interval="0 0,12 * * *",  # run at midnight & noon
    start_date=datetime(2025, 1, 1),   # safe static start date
    catchup=False,
    tags=["web_scraping", "scd2", "azure_blob"],
) as dag:

    # Task 1: Run the crawler
    run_crawler_task = PythonOperator(
        task_id="run_incremental_news_crawl",
        python_callable=sync_crawl_news_wrapper,
    )

    # Task 2: Trigger downstream processing DAG
    trigger_processing_dag = TriggerDagRunOperator(
        task_id="trigger_blob_processing_dag",
        trigger_dag_id="blob_csv_processing",  # Update with actual downstream DAG ID
        conf={"message": "Data crawled by cleaned_data DAG, ready for processing."},
    )

    # Define task order
    run_crawler_task >> trigger_processing_dag
