import os
import requests
from typing import List, Optional
import logging

from src.data.abstract_data_provider import AbstractDataProvider
from src.data.models import (
    Price,
    PriceResponse,
    FinancialMetrics,
    FinancialMetricsResponse,
    LineItem,
    LineItemResponse,
    InsiderTrade,
    InsiderTradeResponse,
    CompanyNews,
    CompanyNewsResponse,
)
from src.utils.timeout_retry import with_http_timeout_retry

logger = logging.getLogger(__name__)

class FinancialDatasetsProvider(AbstractDataProvider):
    """FinancialDatasets.ai数据提供商实现"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("FinancialDatasets", api_key or os.environ.get("FINANCIAL_DATASETS_API_KEY"))
        self.base_url = "https://api.financialdatasets.ai"
    
    def _get_headers(self) -> dict:
        """获取请求头"""
        headers = {}
        if self.api_key:
            headers["X-API-KEY"] = self.api_key
        return headers
    
    def _make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """发送HTTP请求，添加统一的异常处理"""
        try:
            # 获取当前接口的超时配置
            from src.data.data_config import get_timeout_seconds
            # 从调用栈中推断当前使用的接口
            import inspect
            frame = inspect.currentframe()
            caller_name = None
            try:
                # 找到调用_make_request的方法名
                caller_frame = frame.f_back.f_back  # 跳过装饰器的frame
                if caller_frame:
                    caller_name = caller_frame.f_code.co_name
            finally:
                del frame
            
            # 根据调用者确定接口名称
            interface_mapping = {
                'get_prices': 'get_prices',
                'get_financial_metrics': 'get_financial_metrics',
                'search_line_items': 'search_line_items',
                'get_insider_trades': 'get_insider_trades',
                'get_company_news': 'get_company_news'
            }
            interface_name = interface_mapping.get(caller_name, 'get_prices')  # 默认使用get_prices配置
            timeout_seconds = get_timeout_seconds(interface_name)
            
            response = requests.request(
                method=method,
                url=url,
                headers=self._get_headers(),
                timeout=timeout_seconds,
                **kwargs
            )
            
            if response.status_code != 200:
                error_msg = f"API请求失败: {response.status_code}"
                try:
                    error_data = response.json()
                    if 'message' in error_data:
                        error_msg += f" - {error_data['message']}"
                except:
                    error_msg += f" - {response.text}"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            return response
            
        except requests.exceptions.Timeout:
            logger.error(f"请求超时: {url}")
            raise Exception(f"请求超时: {url}")
        except requests.exceptions.RequestException as e:
            logger.error(f"请求异常: {e}")
            raise Exception(f"请求异常: {e}")
    
    @with_http_timeout_retry("get_prices")
    def get_prices(
        self,
        ticker: str,
        start_date: str,
        end_date: str
    ) -> List[Price]:
        """获取股价数据"""
        try:
            url = f"{self.base_url}/prices/?ticker={ticker}&interval=day&interval_multiplier=1&start_date={start_date}&end_date={end_date}"
            response = self._make_request("GET", url)
            
            # 解析响应
            price_response = PriceResponse(**response.json())
            return price_response.prices or []
            
        except Exception as e:
            logger.error(f"获取股价数据失败 {ticker}: {e}")
            return []
    
    @with_http_timeout_retry("get_financial_metrics")
    def get_financial_metrics(
        self,
        ticker: str,
        end_date: str,
        period: str = "ttm",
        limit: int = 10,
    ) -> List[FinancialMetrics]:
        """获取财务指标数据"""
        try:
            url = f"{self.base_url}/financial-metrics/?ticker={ticker}&report_period_lte={end_date}&limit={limit}&period={period}"
            response = self._make_request("GET", url)
            
            # 解析响应
            metrics_response = FinancialMetricsResponse(**response.json())
            return metrics_response.financial_metrics or []
            
        except Exception as e:
            logger.error(f"获取财务指标失败 {ticker}: {e}")
            return []
    
    @with_http_timeout_retry("search_line_items")
    def search_line_items(
        self,
        ticker: str,
        line_items: List[str],
        end_date: str,
        period: str = "ttm",
        limit: int = 10,
    ) -> List[LineItem]:
        """搜索财务报表项目"""
        try:
            url = f"{self.base_url}/financials/search/line-items"
            body = {
                "tickers": [ticker],
                "line_items": line_items,
                "end_date": end_date,
                "period": period,
                "limit": limit,
            }
            logger.debug(f"url={url}, body={body}")
            response = self._make_request("POST", url, json=body)
            data = response.json()
            logger.debug(f"search_line_items result: search {url}, body {body}, result {data}")

            response_model = LineItemResponse(**data)
            search_results = response_model.search_results
            if not search_results:
                return []
            logger.debug(f"search_line_items: search results {search_results[:limit]}")

            # Cache the results
            return search_results[:limit]
            
        except Exception as e:
            logger.error(f"搜索财务报表项目失败 {ticker}: {e}")
            return []
    
    @with_http_timeout_retry("get_insider_trades")
    def get_insider_trades(
        self,
        ticker: str,
        end_date: str,
        start_date: Optional[str] = None,
        limit: int = 1000,
    ) -> List[InsiderTrade]:
        """获取内部交易数据"""
        try:
            all_trades = []
            current_end_date = end_date

            while True:
                url = f"{self.base_url}/insider-trades/?ticker={ticker}&filing_date_lte={current_end_date}"
                if start_date:
                    url += f"&filing_date_gte={start_date}"
                url += f"&limit={limit}"
                logger.debug(f"get_insider_trades url={url}")

                response = self._make_request("GET", url)
                data = response.json()
                
                # 解析响应
                insider_response = InsiderTradeResponse(**data)
                if not insider_response.insider_trades:
                    break

                all_trades.extend(insider_response.insider_trades)
                
                # 检查是否有更多数据
                if not start_date or len(insider_response.insider_trades) < limit:
                    break
                
                # 更新日期范围以获取下一页
                last_filing_date = insider_response.insider_trades[-1].filing_date
                # Extract only the date part (YYYY-MM-DD) from the datetime string
                current_end_date = last_filing_date.split("T")[0] if "T" in last_filing_date else last_filing_date
                if current_end_date <= start_date:
                    break

            return all_trades
            
        except Exception as e:
            logger.error(f"获取内部交易数据失败 {ticker}: {e}")
            return []
    
    @with_http_timeout_retry("get_company_news")
    def get_company_news(
        self,
        ticker: str,
        end_date: str,
        start_date: Optional[str] = None,
        limit: int = 1000,
    ) -> List[CompanyNews]:
        """获取公司新闻"""
        try:
            all_news = []
            current_end_date = end_date

            while True:
                url = f"{self.base_url}/news/?ticker={ticker}&end_date={current_end_date}"
                if start_date:
                    url += f"&start_date={start_date}"
                url += f"&limit={limit}"
                logger.debug(f"get_company_news url={url}")

                response = self._make_request("GET", url)
                data = response.json()
                
                # Transform the response to match the expected CompanyNewsResponse structure
                # The API returns {'news': [...]} but CompanyNewsResponse expects {'company_news': [...]}
                if 'news' in data and 'company_news' not in data:
                    data = {'company_news': data['news']}
                
                # 解析响应
                news_response = CompanyNewsResponse(**data)
                if not news_response.company_news:
                    break

                all_news.extend(news_response.company_news)
                
                # 检查是否有更多数据
                if not start_date or len(news_response.company_news) < limit:
                    break
                
                # 更新日期范围以获取下一页
                last_news_date = news_response.company_news[-1].date
                # Extract only the date part (YYYY-MM-DD) from the datetime string
                current_end_date = last_news_date.split("T")[0] if "T" in last_news_date else last_news_date
                if current_end_date <= start_date:
                    break

            return all_news
            
        except Exception as e:
            logger.error(f"获取公司新闻失败 {ticker}: {e}")
            return []
    
    def get_market_cap(
        self,
        ticker: str,
        end_date: str,
    ) -> Optional[float]:
        """获取市值"""
        try:
            # 通过财务指标获取市值
            metrics = self.get_financial_metrics(ticker, end_date, period="ttm", limit=1)
            if metrics and hasattr(metrics[0], 'market_cap') and metrics[0].market_cap:
                return float(metrics[0].market_cap)
            
            # 如果没有市值数据，尝试通过股价和股份数计算
            prices = self.get_prices(ticker, end_date, end_date)
            if not prices:
                return None
            
            # 这里可以进一步实现通过股价和股份数计算市值的逻辑
            return None
            
        except Exception as e:
            logger.error(f"获取市值失败 {ticker}: {e}")
            return None
    
    def is_available(self) -> bool:
        """检查数据提供商是否可用"""
        try:
            # 检查API密钥是否存在
            if not self.api_key:
                return False
            
            # 尝试一个简单的API调用来验证可用性
            # 使用一个常见的股票代码进行测试
            test_url = f"{self.base_url}/prices/?ticker=AAPL&interval=day&interval_multiplier=1&start_date=2024-01-01&end_date=2024-01-02&limit=1"
            response = requests.get(test_url, headers=self._get_headers(), timeout=10)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"FinancialDatasets可用性检查失败: {e}")
            return False

    def convert_period(self, period: str) -> str:
        return period