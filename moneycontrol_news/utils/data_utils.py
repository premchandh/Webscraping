import pandas as pd
import os
import hashlib
import requests
from bs4 import BeautifulSoup
from typing import List
from models.mcnews import News

def is_duplicate_news(news_identifier: str, seen_identifiers: set) -> bool:
    return news_identifier in seen_identifiers

def fill_missing_fields(url: str, description: str, publishtime: str):
    try:
        html = requests.get(url, timeout=10).text
        soup = BeautifulSoup(html, "html.parser")

        if not description:
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc and meta_desc.get("content"):
                description = meta_desc["content"].strip()

        if not publishtime:
            meta_time = soup.find("meta", property="article:published_time")
            if meta_time and meta_time.get("content"):
                publishtime = meta_time["content"].strip()
    except Exception as e:
        print(f"Error fetching details for {url}: {e}")

    return description, publishtime

def is_complete_news(news: dict, required_keys: List[str]) -> bool:
    if ("description" in required_keys or "publishtime" in required_keys) and "url" in news:
        if not news.get("description") or not news.get("publishtime"):
            news["description"], news["publishtime"] = fill_missing_fields(
                news["url"], news.get("description"), news.get("publishtime")
            )
    for key in required_keys:
        if key not in news or not news[key]:
            return False
    return True

def calculate_content_hash(row: pd.Series) -> str:
    relevant_columns = [
        col for col in News.model_fields.keys() if col not in ['publishtime']
    ]
    concatenated = "".join(str(row.get(col, "")) for col in relevant_columns)
    return hashlib.md5(concatenated.encode('utf-8')).hexdigest()

def load_news_from_csv(filename: str) -> pd.DataFrame:
    expected_columns = list(News.model_fields.keys()) + ['start_date', 'end_date', 'is_current', 'hash_value']
    if not os.path.exists(filename) or os.stat(filename).st_size == 0:
        return pd.DataFrame(columns=expected_columns)
    try:
        df = pd.read_csv(filename)
        if 'hash_value' not in df.columns:
            df['hash_value'] = df.apply(calculate_content_hash, axis=1)
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None
        return df[expected_columns]
    except Exception:
        return pd.DataFrame(columns=expected_columns)

def save_news_to_csv(df: pd.DataFrame, filename: str):
    if df.empty:
        print("No news data to save.")
        return
    all_fields = list(News.model_fields.keys()) + ['start_date', 'end_date', 'is_current', 'hash_value']
    for field in all_fields:
        if field not in df.columns:
            df[field] = None
    df = df[all_fields]
    df.to_csv(filename, mode='w', index=False, encoding="utf-8")
    print(f"Saved {len(df)} news records to '{filename}'.")
