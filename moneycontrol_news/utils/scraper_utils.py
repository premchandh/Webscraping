import json
import os
from typing import List, Set, Tuple

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CacheMode,
    CrawlerRunConfig,
    LLMExtractionStrategy,
    LLMConfig
)

from models.mcnews import News
from utils.data_utils import is_complete_news, is_duplicate_news

# Use GEMINI API Key from environment (Airflow Variables / Key Vault)
os.environ["GEMINI_API_KEY"] = os.getenv("GEMINI_API_KEY", "")


def get_browser_config() -> BrowserConfig:
    return BrowserConfig(
        browser_type="chromium",
        headless=True,
        verbose=True,
    )


def get_llm_strategynew() -> LLMExtractionStrategy:
    llm_config = LLMConfig(
        provider="gemini/gemini-1.5-flash",
        api_token=os.getenv('GEMINI_API_KEY')
    )

    return LLMExtractionStrategy(
        llm_config=llm_config,
        schema=News.model_json_schema(),
        extraction_type="schema",
        instruction=(
            "Extract all news objects with 'title', 'description', 'url'. "
            "Extract 'publishtime' from span tag if available. "
            "'provider' is the name of the website."
        ),
        input_format="html",
        verbose=True,
    )


async def check_no_results(crawler: AsyncWebCrawler, url: str, session_id: str) -> bool:
    result = await crawler.arun(
        url=url,
        config=CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            session_id=session_id,
        ),
    )
    if result.success and "No Results Found" in result.cleaned_html:
        return True
    return False


async def fetch_and_process_page(
    crawler: AsyncWebCrawler,
    page_number: int,
    base_url: str,
    css_selector: str,
    llm_strategy: LLMExtractionStrategy,
    session_id: str,
    required_keys: List[str],
    seen_names: Set[str],
) -> Tuple[List[dict], bool]:

    url = f"{base_url}/page-{page_number}/"
    print(f"Loading page {page_number} → {url}")

    no_results = await check_no_results(crawler, url, session_id)
    if no_results:
        return [], True

    result = await crawler.arun(
        url=url,
        config=CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            extraction_strategy=llm_strategy,
            css_selector=css_selector,
            session_id=session_id,
        ),
    )

    if not (result.success and result.extracted_content):
        print(f"Error fetching page {page_number}: {result.error_message}")
        return [], False

    try:
        extracted_data = json.loads(result.extracted_content)
    except Exception as e:
        print(f"Failed to parse extracted content JSON: {e}")
        return [], False

    complete_news = []
    for news in extracted_data:
        if news.get("error") is False:
            news.pop("error", None)
        if not is_complete_news(news, required_keys):
            continue
        if is_duplicate_news(news.get("title"), seen_names):
            continue
        seen_names.add(news.get("title"))
        complete_news.append(news)

    return complete_news, False
