import datetime
import os
import pandas as pd
import requests

from src.data.cache import get_cache
from src.data.persistent_cache import get_persistent_cache
from src.data.models import (
    CompanyNews,
    CompanyNewsResponse,
    FinancialMetrics,
    FinancialMetricsResponse,
    Price,
    PriceResponse,
    LineItem,
    LineItemResponse,
    InsiderTrade,
    InsiderTradeResponse,
    CompanyFactsResponse,
)

# Global cache instances
_cache = get_cache()
_persistent_cache = get_persistent_cache()


def get_prices(ticker: str, start_date: str, end_date: str) -> list[Price]:
    """Fetch price data from cache or API."""
    # Create a cache key that includes all parameters to ensure exact matches
    cache_key = f"{ticker}_{start_date}_{end_date}"
    
    # Check memory cache first - fastest
    if cached_data := _cache.get_prices(cache_key):
        return [Price(**price) for price in cached_data]
    
    # Check persistent cache - second fastest
    if persistent_data := _persistent_cache.get_prices(ticker, start_date, end_date):
        # Load into memory cache for faster future access
        _cache.set_prices(cache_key, persistent_data)
        return [Price(**price) for price in persistent_data]

    # If not in cache, fetch from API
    headers = {}
    if api_key := os.environ.get("FINANCIAL_DATASETS_API_KEY"):
        headers["X-API-KEY"] = api_key

    url = f"https://api.financialdatasets.ai/prices/?ticker={ticker}&interval=day&interval_multiplier=1&start_date={start_date}&end_date={end_date}"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Error fetching data: {ticker} - {response.status_code} - {response.text}")

    # Parse response with Pydantic model
    price_response = PriceResponse(**response.json())
    prices = price_response.prices

    if not prices:
        return []

    # Cache the results in both memory and persistent cache
    price_data = [p.model_dump() for p in prices]
    _cache.set_prices(cache_key, price_data)
    _persistent_cache.set_prices(ticker, start_date, end_date, price_data)
    return prices


def get_financial_metrics(
    ticker: str,
    end_date: str,
    period: str = "ttm",
    limit: int = 10,
) -> list[FinancialMetrics]:
    """Fetch financial metrics from cache or API."""
    # Create a cache key that includes all parameters to ensure exact matches
    cache_key = f"{ticker}_{period}_{end_date}_{limit}"
    
    # Check memory cache first - fastest
    if cached_data := _cache.get_financial_metrics(cache_key):
        return [FinancialMetrics(**metric) for metric in cached_data]
    
    # Check persistent cache - second fastest
    if persistent_data := _persistent_cache.get_financial_metrics(ticker, period, end_date, limit):
        # Load into memory cache for faster future access
        _cache.set_financial_metrics(cache_key, persistent_data)
        return [FinancialMetrics(**metric) for metric in persistent_data]

    # If not in cache, fetch from API
    headers = {}
    if api_key := os.environ.get("FINANCIAL_DATASETS_API_KEY"):
        headers["X-API-KEY"] = api_key

    url = f"https://api.financialdatasets.ai/financial-metrics/?ticker={ticker}&report_period_lte={end_date}&limit={limit}&period={period}"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Error fetching data: {ticker} - {response.status_code} - {response.text}")

    # Parse response with Pydantic model
    metrics_response = FinancialMetricsResponse(**response.json())
    financial_metrics = metrics_response.financial_metrics

    if not financial_metrics:
        return []

    # Cache the results in both memory and persistent cache
    metrics_data = [m.model_dump() for m in financial_metrics]
    _cache.set_financial_metrics(cache_key, metrics_data)
    _persistent_cache.set_financial_metrics(ticker, period, end_date, limit, metrics_data)
    return financial_metrics


def search_line_items(
    ticker: str,
    line_items: list[str],
    end_date: str,
    period: str = "ttm",
    limit: int = 10,
) -> list[LineItem]:
    """Fetch line items from cache or API."""
    # Create a cache key that includes all parameters
    line_items_str = "_".join(sorted(line_items))  # Sort for consistent cache key
    cache_key = f"{ticker}_{period}_{end_date}_{limit}_{line_items_str}"
    
    # Check memory cache first - fastest
    if cached_data := _cache.get_line_items(cache_key):
        return [LineItem(**item) for item in cached_data]
    
    # Check persistent cache - second fastest
    if persistent_data := _persistent_cache.get_line_items(ticker, line_items, period, end_date, limit):
        # Load into memory cache for faster future access
        _cache.set_line_items(cache_key, persistent_data)
        return [LineItem(**item) for item in persistent_data]

    # If not in cache, fetch from API
    headers = {}
    if api_key := os.environ.get("FINANCIAL_DATASETS_API_KEY"):
        headers["X-API-KEY"] = api_key

    url = "https://api.financialdatasets.ai/financials/search/line-items"

    body = {
        "tickers": [ticker],
        "line_items": line_items,
        "end_date": end_date,
        "period": period,
        "limit": limit,
    }
    response = requests.post(url, headers=headers, json=body)
    if response.status_code != 200:
        raise Exception(f"Error fetching data: {ticker} - {response.status_code} - {response.text}")
    data = response.json()
    response_model = LineItemResponse(**data)
    search_results = response_model.search_results
    if not search_results:
        return []

    # Cache the results in both memory and persistent cache
    line_items_data = [item.model_dump() for item in search_results]
    _cache.set_line_items(cache_key, line_items_data)
    _persistent_cache.set_line_items(ticker, line_items, period, end_date, limit, line_items_data)
    return search_results[:limit]


def get_insider_trades(
    ticker: str,
    end_date: str,
    start_date: str | None = None,
    limit: int = 1000,
) -> list[InsiderTrade]:
    """Fetch insider trades from cache or API."""
    # Create a cache key that includes all parameters to ensure exact matches
    cache_key = f"{ticker}_{start_date or 'none'}_{end_date}_{limit}"
    
    # Check memory cache first - fastest
    if cached_data := _cache.get_insider_trades(cache_key):
        return [InsiderTrade(**trade) for trade in cached_data]
    
    # Check persistent cache - second fastest
    if persistent_data := _persistent_cache.get_insider_trades(ticker, start_date or '1900-01-01', end_date, limit):
        # Load into memory cache for faster future access
        _cache.set_insider_trades(cache_key, persistent_data)
        return [InsiderTrade(**trade) for trade in persistent_data]

    # If not in cache, fetch from API
    headers = {}
    if api_key := os.environ.get("FINANCIAL_DATASETS_API_KEY"):
        headers["X-API-KEY"] = api_key

    all_trades = []
    current_end_date = end_date

    while True:
        url = f"https://api.financialdatasets.ai/insider-trades/?ticker={ticker}&filing_date_lte={current_end_date}"
        if start_date:
            url += f"&filing_date_gte={start_date}"
        url += f"&limit={limit}"

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Error fetching data: {ticker} - {response.status_code} - {response.text}")

        data = response.json()
        response_model = InsiderTradeResponse(**data)
        insider_trades = response_model.insider_trades

        if not insider_trades:
            break

        all_trades.extend(insider_trades)

        # Only continue pagination if we have a start_date and got a full page
        if not start_date or len(insider_trades) < limit:
            break

        # Update end_date to the oldest filing date from current batch for next iteration
        current_end_date = min(trade.filing_date for trade in insider_trades).split("T")[0]

        # If we've reached or passed the start_date, we can stop
        if current_end_date <= start_date:
            break

    if not all_trades:
        return []

    # Cache the results in both memory and persistent cache
    trades_data = [trade.model_dump() for trade in all_trades]
    _cache.set_insider_trades(cache_key, trades_data)
    _persistent_cache.set_insider_trades(ticker, start_date or '1900-01-01', end_date, limit, trades_data)
    return all_trades


def get_company_news(
    ticker: str,
    end_date: str,
    start_date: str | None = None,
    limit: int = 1000,
) -> list[CompanyNews]:
    """Fetch company news from cache or API."""
    # Create a cache key that includes all parameters to ensure exact matches
    cache_key = f"{ticker}_{start_date or 'none'}_{end_date}_{limit}"
    
    # Check memory cache first - fastest
    if cached_data := _cache.get_company_news(cache_key):
        return [CompanyNews(**news) for news in cached_data]
    
    # Check persistent cache - second fastest
    if persistent_data := _persistent_cache.get_company_news(ticker, start_date or '1900-01-01', end_date, limit):
        # Load into memory cache for faster future access
        _cache.set_company_news(cache_key, persistent_data)
        return [CompanyNews(**news) for news in persistent_data]

    # If not in cache, fetch from API
    headers = {}
    if api_key := os.environ.get("FINANCIAL_DATASETS_API_KEY"):
        headers["X-API-KEY"] = api_key

    all_news = []
    current_end_date = end_date

    while True:
        url = f"https://api.financialdatasets.ai/news/?ticker={ticker}&end_date={current_end_date}"
        if start_date:
            url += f"&start_date={start_date}"
        url += f"&limit={limit}"

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Error fetching data: {ticker} - {response.status_code} - {response.text}")

        data = response.json()
        response_model = CompanyNewsResponse(**data)
        company_news = response_model.news

        if not company_news:
            break

        all_news.extend(company_news)

        # Only continue pagination if we have a start_date and got a full page
        if not start_date or len(company_news) < limit:
            break

        # Update end_date to the oldest date from current batch for next iteration
        current_end_date = min(news.date for news in company_news).split("T")[0]

        # If we've reached or passed the start_date, we can stop
        if current_end_date <= start_date:
            break

    if not all_news:
        return []

    # Cache the results in both memory and persistent cache
    news_data = [news.model_dump() for news in all_news]
    _cache.set_company_news(cache_key, news_data)
    _persistent_cache.set_company_news(ticker, start_date or '1900-01-01', end_date, limit, news_data)
    return all_news


# Cache management functions
def get_cache_stats():
    """Get statistics about both memory and persistent caches."""
    persistent_stats = _persistent_cache.get_cache_stats()
    
    return {
        'memory_cache': {
            'prices_count': len(_cache._prices_cache),
            'financial_metrics_count': len(_cache._financial_metrics_cache),
            'line_items_count': len(_cache._line_items_cache),
            'insider_trades_count': len(_cache._insider_trades_cache),
            'company_news_count': len(_cache._company_news_cache),
        },
        'persistent_cache': persistent_stats
    }


def clear_cache():
    """Clear all cached data."""
    # Clear memory cache
    _cache._prices_cache.clear()
    _cache._financial_metrics_cache.clear()
    _cache._line_items_cache.clear()
    _cache._insider_trades_cache.clear()
    _cache._company_news_cache.clear()
    
    # Clear persistent cache
    _persistent_cache.clear_all()


def clear_expired_cache():
    """Clear only expired cache entries."""
    return _persistent_cache.clear_expired()


def force_refresh_ticker(ticker: str):
    """Force refresh all cache entries for a specific ticker."""
    # Clear memory cache entries for the ticker
    keys_to_remove = []
    for key in _cache._prices_cache:
        if key.startswith(ticker):
            keys_to_remove.append(key)
    for key in keys_to_remove:
        _cache._prices_cache.pop(key, None)
        
    keys_to_remove = []
    for key in _cache._financial_metrics_cache:
        if key.startswith(ticker):
            keys_to_remove.append(key)
    for key in keys_to_remove:
        _cache._financial_metrics_cache.pop(key, None)
    
    keys_to_remove = []
    for key in _cache._line_items_cache:
        if key.startswith(ticker):
            keys_to_remove.append(key)
    for key in keys_to_remove:
        _cache._line_items_cache.pop(key, None)
    
    keys_to_remove = []
    for key in _cache._insider_trades_cache:
        if key.startswith(ticker):
            keys_to_remove.append(key)
    for key in keys_to_remove:
        _cache._insider_trades_cache.pop(key, None)
    
    keys_to_remove = []
    for key in _cache._company_news_cache:
        if key.startswith(ticker):
            keys_to_remove.append(key)
    for key in keys_to_remove:
        _cache._company_news_cache.pop(key, None)
    
    # Clear persistent cache entries for the ticker
    return _persistent_cache.force_refresh_ticker(ticker)


def get_market_cap(
    ticker: str,
    end_date: str,
) -> float | None:
    """Fetch market cap from the API."""
    # Check if end_date is today
    if end_date == datetime.datetime.now().strftime("%Y-%m-%d"):
        # Get the market cap from company facts API
        headers = {}
        if api_key := os.environ.get("FINANCIAL_DATASETS_API_KEY"):
            headers["X-API-KEY"] = api_key

        url = f"https://api.financialdatasets.ai/company/facts/?ticker={ticker}"
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Error fetching company facts: {ticker} - {response.status_code}")
            return None

        data = response.json()
        response_model = CompanyFactsResponse(**data)
        return response_model.company_facts.market_cap

    financial_metrics = get_financial_metrics(ticker, end_date)
    if not financial_metrics:
        return None

    market_cap = financial_metrics[0].market_cap

    if not market_cap:
        return None

    return market_cap


def prices_to_df(prices: list[Price]) -> pd.DataFrame:
    """Convert prices to a DataFrame."""
    df = pd.DataFrame([p.model_dump() for p in prices])
    df["Date"] = pd.to_datetime(df["time"])
    df.set_index("Date", inplace=True)
    numeric_cols = ["open", "close", "high", "low", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.sort_index(inplace=True)
    return df


# Update the get_price_data function to use the new functions
def get_price_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    prices = get_prices(ticker, start_date, end_date)
    return prices_to_df(prices)
