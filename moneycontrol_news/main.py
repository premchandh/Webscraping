import asyncio
import logging
import os
import hashlib
import pandas as pd
from azure.storage.blob import BlobServiceClient

from utils.data_utils import save_news_to_csv, load_news_from_csv
from utils.scraper_utils import (
    fetch_and_process_page,
    get_browser_config,
    get_llm_strategynew,
)
from crawl4ai import AsyncWebCrawler
from config import BASE_URL, CSS_SELECTOR, REQUIRED_KEYS

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Azure Blob Storage Configuration
def get_blob_container_client():
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME", "webnewscrawler")
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    container_client = blob_service_client.get_container_client(container_name)
    return container_client


async def crawl_news():
    logger.info("Initializing browser and crawling config...")
    browser_config = get_browser_config()
    llm_strategy = get_llm_strategynew()
    session_id = "news_crawl_session"

    page_number = 1
    new_crawled_news = []

    async with AsyncWebCrawler(config=browser_config) as crawler:
        while True:
            news, no_results_found = await fetch_and_process_page(
                crawler,
                page_number,
                BASE_URL,
                CSS_SELECTOR,
                llm_strategy,
                session_id,
                REQUIRED_KEYS,
                set(),
            )

            if no_results_found or not news:
                break

            new_crawled_news.extend(news)
            logger.info(f"Extracted {len(news)} items from page {page_number}")
            page_number += 1
            if page_number > 5:  # Optional page limit
                break
            await asyncio.sleep(2)

    if not new_crawled_news:
        logger.warning("No new news found, skipping save and upload.")
        return

    # Add 'name' column for consistency
    for news_item in new_crawled_news:
        news_item['name'] = news_item.get('title', '')

    new_df = pd.DataFrame(new_crawled_news)

    # Generate hash as primary key
    def calculate_hash(row):
        content = str(row.get('title', '')) + str(row.get('description', '')) + str(row.get('publishtime', ''))
        return hashlib.md5(content.encode('utf-8')).hexdigest()

    new_df['hash_value'] = new_df.apply(calculate_hash, axis=1)

    # Azure Blob setup
    container_client = get_blob_container_client()
    blob_name = "news/moneycontrol_news.csv"
    historical_df = pd.DataFrame()

    # Load existing file from Azure Blob if exists
    try:
        temp_file = "temp_moneycontrol_news.csv"
        blob_client = container_client.get_blob_client(blob_name)
        with open(temp_file, "wb") as f:
            download_stream = blob_client.download_blob()
            f.write(download_stream.readall())
        historical_df = load_news_from_csv(temp_file)
        os.remove(temp_file)
        logger.info(f"Loaded {len(historical_df)} historical records from Azure Blob.")
    except Exception as e:
        logger.warning(f"No historical file found or error reading it: {e}. Starting fresh.")

    # Filter out already existing records using hash_value
    if not historical_df.empty and 'hash_value' in historical_df.columns:
        existing_hashes = set(historical_df['hash_value'])
        new_df = new_df[~new_df['hash_value'].isin(existing_hashes)]
        logger.info(f"{len(new_df)} new unique articles to insert.")
    else:
        logger.info("No existing hashes found. All articles are considered new.")

    if new_df.empty:
        logger.info("No new articles to insert. Skipping file upload.")
        return

    # Append new data
    final_df = pd.concat([historical_df, new_df], ignore_index=True)

    # Save to CSV locally
    save_news_to_csv(final_df, "moneycontrol_news.csv")

    # Upload to Azure Blob (overwrite same file)
    try:
        blob_client = container_client.get_blob_client(blob_name)
        with open("moneycontrol_news.csv", "rb") as f:
            blob_client.upload_blob(f, overwrite=True)
        logger.info(f"Uploaded updated historical data with {len(final_df)} articles to Azure Blob at {blob_name}.")
    except Exception as e:
        logger.error(f"Failed to upload updated file to Azure Blob: {e}")

    # Show usage for LLM (optional)
    llm_strategy.show_usage()


async def main():
    await crawl_news()


if __name__ == "__main__":
    asyncio.run(main())
