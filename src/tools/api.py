import logging
from typing import List, Optional
import pandas as pd

from src.data.cache import get_cache
from src.data.persistent_cache import get_persistent_cache
from src.data.data_config import get_data_config
from src.data.data_provider_factory import DataProviderFactory, DataProviderType, AbstractDataProvider
from src.data.models import (
    Price,
    FinancialMetrics,
    LineItem,
    InsiderTrade,
    CompanyNews,
    AggregatedFinancialInfo,
    TransactionType,
)
from src.utils.financial_utils import reconstruct_financial_metrics

logger = logging.getLogger(__name__)

def _convert_transaction_type(trade_data: dict) -> dict:
    """转换transaction_type为枚举类型"""
    if 'transaction_type' in trade_data and isinstance(trade_data['transaction_type'], str):
        transaction_type_str = trade_data['transaction_type']
        if transaction_type_str in ["Buy", "Purchase", "buy", "purchase", "增持", "购买"]:
            trade_data['transaction_type'] = TransactionType.BUY
        elif transaction_type_str in ["Sell", "Sale", "sell", "sale", "减持", "出售"]:
            trade_data['transaction_type'] = TransactionType.SELL
    return trade_data

# Global cache instances
_cache = get_cache()
_persistent_cache = get_persistent_cache()
_config = get_data_config()

# Global data provider instance
_data_provider: Optional[AbstractDataProvider] = None


def _get_data_provider() -> AbstractDataProvider:
    """获取数据提供商实例"""
    global _data_provider
    
    if _data_provider is None:
        try:
            # 从配置获取默认数据提供商
            default_provider = _config.get_default_data_provider()
            _data_provider = DataProviderFactory.get_provider_by_name(default_provider)
            logger.info(f"初始化数据提供商: {default_provider}")
        except Exception as e:
            logger.error(f"无法初始化数据提供商: {e}")
            # 回退到默认的FinancialDatasets提供商
            _data_provider = DataProviderFactory.create_provider(DataProviderType.FINANCIAL_DATASETS)
            logger.info("使用默认的FinancialDatasets提供商")
    
    return _data_provider


def switch_data_provider(provider_name: str, api_key: Optional[str] = None) -> bool:
    """切换数据提供商"""
    global _data_provider
    
    try:
        new_provider = DataProviderFactory.get_provider_by_name(provider_name, api_key)
        if new_provider.is_available():
            _data_provider = new_provider
            _config.set_default_data_provider(provider_name)
            logger.info(f"数据提供商已切换为: {provider_name}")
            return True
        else:
            logger.error(f"数据提供商 {provider_name} 不可用")
            return False
    except Exception as e:
        logger.error(f"切换数据提供商失败: {e}")
        return False


def get_current_provider_info() -> dict:
    """获取当前数据提供商信息"""
    try:
        provider = _get_data_provider()
        return {
            'name': provider.name,
            'type': provider.__class__.__name__,
            'available': provider.is_available()
        }
    except Exception as e:
        logger.error(f"获取数据提供商信息失败: {e}")
        return {'name': 'Unknown', 'type': 'Unknown', 'available': False}


def get_prices(ticker: str, start_date: str, end_date: str) -> List[Price]:
    """获取股价数据（支持缓存）"""
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

    # If not in cache, fetch from data provider
    try:
        provider = _get_data_provider()
        prices = provider.get_prices(ticker, start_date, end_date)
        
        if not prices:
            return []

        # Cache the results in both memory and persistent cache
        price_data = [p.model_dump() for p in prices]
        _cache.set_prices(cache_key, price_data)
        _persistent_cache.set_prices(ticker, start_date, end_date, price_data)
        return prices
    except Exception as e:
        logger.error(f"获取股价数据失败 {ticker}: {e}")
        return []


def get_financial_metrics(
    ticker: str,
    end_date: str,
    period: str = "ttm",
    limit: int = 10,
) -> List[FinancialMetrics]:
    """获取财务指标数据（支持缓存）"""
    # Create a cache key that includes all parameters to ensure exact matches
    cache_key = f"{ticker}_{period}_{end_date}_{limit}"
    
    provider = _get_data_provider()
    filtering_period = provider.convert_period(period)

    # Check memory cache first - fastest
    if cached_data := _cache.get_financial_metrics(cache_key):
        all_metrics = [FinancialMetrics(**metric) for metric in cached_data]
        # Apply filter and limit to cached data
        filtered_metrics = _filter_period_and_limit_number(all_metrics, filtering_period, limit)
        return filtered_metrics
    
    # Check persistent cache - second fastest
    if persistent_data := _persistent_cache.get_financial_metrics(ticker, period, end_date, limit):
        all_metrics = [FinancialMetrics(**metric) for metric in persistent_data]
        # Load into memory cache for faster future access
        _cache.set_financial_metrics(cache_key, persistent_data)
        # Apply filter and limit to cached data
        filtered_metrics = _filter_period_and_limit_number(all_metrics, filtering_period, limit)
        return filtered_metrics

    # If not in cache, fetch from data provider
    try:
        metrics = provider.get_financial_metrics(ticker, end_date, period, limit)
        
        if not metrics:
            return []

        # Cache the results in both memory and persistent cache
        metrics_data = [m.model_dump() for m in metrics]
        _cache.set_financial_metrics(cache_key, metrics_data)
        _persistent_cache.set_financial_metrics(ticker, period, end_date, limit, metrics_data)

        # Filter by period and apply limit
        filtered_metrics = _filter_period_and_limit_number(metrics, filtering_period, limit)
        return filtered_metrics
    except Exception as e:
        logger.error(f"获取财务指标失败 {ticker}: {e}")
        return []

def _filter_period_and_limit_number(items: List[LineItem], period: str, limit: int) -> List[LineItem]:
    """Filter line items by period and return the latest N items.
    
    Args:
        items: List of LineItem objects
        period: Period type to filter by ("annual", "quarter", etc.)
        limit: Maximum number of items to return
    
    Returns:
        Filtered and limited list of LineItem objects, sorted by report_period (latest first)
    """
    # Filter by period type
    logger.debug(f"_filter_and_limit_number: {period}, {limit}.")
    filtered_items = [item for item in items if item.period == period]
    
    # Sort by report_period in descending order (latest first)
    filtered_items.sort(key=lambda x: x.report_period, reverse=True)
    logger.debug(f"_filter_and_limit_number: input len : {len(items)}, after filtered len : {len(filtered_items[:limit])}")
    logger.debug(f"_filter_and_limit_number: after filtered periods : {[item.report_period for item in filtered_items[:limit]]}")
    
    # Apply limit to get the latest N items
    return filtered_items[:limit]


def search_line_items(
    ticker: str,
    line_items: List[str],
    end_date: str,
    period: str = "ttm",
    limit: int = 10,
) -> List[LineItem]:
    provider = _get_data_provider()
    filtering_period = provider.convert_period(period)

    """搜索财务报表项目（支持缓存）"""
    # Create a cache key without limit - cache complete data
    line_items_str = "_".join(sorted(line_items))  # Sort for consistent cache key
    cache_key = f"{ticker}_{period}_{end_date}_{line_items_str}"
    
    # Check memory cache first - fastest
    if cached_data := _cache.get_line_items(cache_key):
        try:
            all_items = [LineItem(**item) for item in cached_data]
            # Filter by period and apply limit
            filtered_items = _filter_period_and_limit_number(all_items, filtering_period, limit)
            return filtered_items
        except Exception as e:
            # If cached data format is invalid, clear it and fetch fresh
            logger.warning(f"Invalid cached data format for line_items {ticker}, clearing cache: {e}")
            _cache._line_items_cache.pop(cache_key, None)
    
    # Check persistent cache - second fastest
    # Use a large limit (1000) to get complete data from persistent cache
    if persistent_data := _persistent_cache.get_line_items(ticker, line_items, period, end_date, 1000):
        try:
            all_items = [LineItem(**item) for item in persistent_data]
            logger.debug(f"persistent_data: {len(all_items)} items loaded")
            # Load into memory cache for faster future access
            _cache.set_line_items(cache_key, persistent_data)
            # Filter by period and apply limit
            filtered_items = _filter_period_and_limit_number(all_items, filtering_period, limit)
            return filtered_items
        except Exception as e:
            # If persistent data format is invalid, ignore and fetch fresh
            logger.warning(f"Invalid persistent data format for line_items {ticker}: {e}")

    # If not in cache, fetch from data provider with large limit to get complete data
    try:
        # Fetch complete data (use 1000 as a large limit to get all available data)
        search_results = provider.search_line_items(ticker, line_items, end_date, period, limit)
        logger.debug(f"search_results: {len(search_results)} items fetched")

        if not search_results:
            return []

        # Cache the complete results in both memory and persistent cache
        line_items_data = [item.model_dump() for item in search_results]
        _cache.set_line_items(cache_key, line_items_data)
        _persistent_cache.set_line_items(ticker, line_items, period, end_date, 1000, line_items_data)
        
        # Filter by period and apply limit
        filtered_items = _filter_period_and_limit_number(search_results, filtering_period, limit)
        return filtered_items
    except Exception as e:
        logger.error(f"搜索财务报表项目失败 {ticker}: {e}")
        return []


def get_insider_trades(
    ticker: str,
    end_date: str,
    start_date: Optional[str] = None,
    limit: int = 1000,
) -> List[InsiderTrade]:
    """获取内部交易数据（支持缓存）"""
    # Create a cache key that includes all parameters to ensure exact matches
    cache_key = f"{ticker}_{start_date or 'none'}_{end_date}_{limit}"
    
    # Check memory cache first - fastest
    if cached_data := _cache.get_insider_trades(cache_key):
        return [InsiderTrade(**_convert_transaction_type(trade)) for trade in cached_data]
    
    # Check persistent cache - second fastest
    if persistent_data := _persistent_cache.get_insider_trades(ticker, start_date or '1900-01-01', end_date, limit):
        # Load into memory cache for faster future access
        _cache.set_insider_trades(cache_key, persistent_data)
        return [InsiderTrade(**_convert_transaction_type(trade)) for trade in persistent_data]

    # If not in cache, fetch from data provider
    try:
        provider = _get_data_provider()
        trades = provider.get_insider_trades(ticker, end_date, start_date, limit)
        
        if not trades:
            return []

        # Cache the results in both memory and persistent cache
        trades_data = [t.model_dump() for t in trades]
        _cache.set_insider_trades(cache_key, trades_data)
        _persistent_cache.set_insider_trades(ticker, start_date or '1900-01-01', end_date, limit, trades_data)
        return trades
    except Exception as e:
        logger.error(f"获取内部交易数据失败 {ticker}: {e}")
        return []


def get_company_news(
    ticker: str,
    end_date: str,
    start_date: Optional[str] = None,
    limit: int = 1000,
) -> List[CompanyNews]:
    """获取公司新闻（支持缓存）"""
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

    # If not in cache, fetch from data provider
    try:
        provider = _get_data_provider()
        news = provider.get_company_news(ticker, end_date, start_date, limit)
        
        if not news:
            return []

        # Cache the results in both memory and persistent cache
        news_data = [n.model_dump() for n in news]
        _cache.set_company_news(cache_key, news_data)
        _persistent_cache.set_company_news(ticker, start_date or '1900-01-01', end_date, limit, news_data)
        return news
    except Exception as e:
        logger.error(f"获取公司新闻失败 {ticker}: {e}")
        return []


def get_market_cap(ticker: str, end_date: str) -> Optional[float]:
    """获取市值"""
    try:
        provider = _get_data_provider()
        return provider.get_market_cap(ticker, end_date)
    except Exception as e:
        logger.error(f"获取市值失败 {ticker}: {e}")
        return None


def prices_to_df(prices: List[Price]) -> pd.DataFrame:
    """将价格数据转换为DataFrame"""
    if not prices:
        return pd.DataFrame()
    
    data = []
    for price in prices:
        data.append({
            'time': price.time,
            'open': price.open,
            'high': price.high,
            'low': price.low,
            'close': price.close,
            'volume': price.volume,
            'ticker': price.ticker
        })
    
    df = pd.DataFrame(data)
    df['time'] = pd.to_datetime(df['time'])
    return df.set_index('time').sort_index()


def get_price_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """获取价格数据并转换为DataFrame"""
    prices = get_prices(ticker, start_date, end_date)
    return prices_to_df(prices)


# 缓存管理函数
def get_cache_stats():
    """获取缓存统计信息"""
    return {
        'memory_cache': _cache.get_cache_stats() if hasattr(_cache, 'get_cache_stats') else {},
        'persistent_cache': _persistent_cache.get_cache_stats() if hasattr(_persistent_cache, 'get_cache_stats') else {},
        'current_provider': get_current_provider_info()
    }


def clear_cache():
    """清除所有缓存"""
    try:
        _cache.clear()
        logger.info("内存缓存已清除")
    except Exception as e:
        logger.error(f"清除内存缓存失败: {e}")
    
    try:
        _persistent_cache.clear()
        logger.info("持久化缓存已清除")
    except Exception as e:
        logger.error(f"清除持久化缓存失败: {e}")


def force_refresh_ticker(ticker: str):
    """强制刷新指定股票的缓存"""
    try:
        _cache.clear_ticker(ticker)
        _persistent_cache.clear_ticker(ticker)
        logger.info(f"已清除股票 {ticker} 的缓存")
    except Exception as e:
        logger.error(f"清除股票缓存失败 {ticker}: {e}")


def get_provider_status() -> dict:
    """获取所有数据提供商的状态"""
    return DataProviderFactory.get_available_providers()


def merge_financial_data(financial_metrics: list, financial_line_items: list) -> List[AggregatedFinancialInfo]:
    """
    合并财务指标和财务报表项目数据，按时间进行对齐
    
    Args:
        financial_metrics: 财务指标数据列表
        financial_line_items: 财务报表项目数据列表
        
    Returns:
        List[AggregatedFinancialInfo]: 合并后的财务数据，每个对象包含该期间的所有可用数据
    """
    
    logger.info(f"开始合并财务数据: {len(financial_metrics)} 条财务指标, {len(financial_line_items)} 条财务报表数据")
    
    # 直接构建AggregatedFinancialInfo对象字典，按报告期间分组
    aggregated_objects = {}
    
    # 处理财务指标数据 - 直接创建AggregatedFinancialInfo对象
    for metric in financial_metrics:
        period_key = metric.report_period
        try:
            # 使用财务指标数据创建AggregatedFinancialInfo对象
            aggregated_objects[period_key] = AggregatedFinancialInfo(**metric.model_dump())
        except Exception as e:
            logger.warning(f"从财务指标创建AggregatedFinancialInfo失败，期间 {period_key}: {e}")
            continue
    
    # 处理财务报表项目数据 - 仅更新现有对象
    for line_item in financial_line_items:
        period_key = line_item.report_period
        
        if period_key in aggregated_objects:
            # 更新现有对象 - 只添加不存在的字段，保持财务指标优先
            existing_obj = aggregated_objects[period_key]
            existing_data = existing_obj.model_dump()
            line_item_data = line_item.model_dump()
            
            # 合并数据，财务指标优先
            for k, v in line_item_data.items():
                if k not in existing_data or existing_data[k] is None:
                    existing_data[k] = v
            
            try:
                aggregated_objects[period_key] = AggregatedFinancialInfo(**existing_data)
            except Exception as e:
                logger.warning(f"更新AggregatedFinancialInfo失败，期间 {period_key}: {e}")
        else:
            # 不存在对应的财务指标数据，记录日志并跳过
            logger.info(f"跳过财务报表项目数据，期间 {period_key} 没有对应的财务指标数据, 有效的: {aggregated_objects.keys()}")

    # 更新组合指标
    for key, obj in aggregated_objects.items():
        aggregated_objects[key] = reconstruct_financial_metrics(obj)
        
    logger.debug(f"reconstructed financial data: {aggregated_objects}")
    # 按时间排序（最新的在前）并返回列表
    merged_data = [aggregated_objects[period] for period in sorted(aggregated_objects.keys(), reverse=True)]
    
    logger.info(f"合并完成，共 {len(merged_data)} 个期间的数据")
    return merged_data