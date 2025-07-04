from abc import ABC, abstractmethod
from typing import List, Optional
from datetime import datetime

from src.data.models import (
    Price,
    FinancialMetrics,
    LineItem,
    FinancialProfile,
    InsiderTrade,
    CompanyNews,
)


class AbstractDataProvider(ABC):
    """抽象数据提供商基类，定义统一的数据接口"""
    
    def __init__(self, name: str, api_key: Optional[str] = None):
        self.name = name
        self.api_key = api_key
    
    @abstractmethod
    def get_prices(
        self,
        tickers: List[str],
        start_date: str,
        end_date: str,
        freq: str = '1d'  # e.g., '1m', '5m', '1d', '1w', '1M'
    ) -> List[Price]:
        """获取股价数据"""
        pass
    
    @abstractmethod
    def get_financial_metrics(
        self,
        ticker: str,
        end_date: str,
        period: str = "ttm",
        limit: int = 10,
    ) -> List[FinancialMetrics]:
        """获取财务指标数据"""
        pass
    
    @abstractmethod
    def search_line_items(
        self,
        ticker: str,
        line_items: List[str],
        end_date: str,
        period: str = "ttm",
        limit: int = 10,
    ) -> List[LineItem]:
        """搜索财务报表项目"""
        pass

    @abstractmethod
    def get_financial_profile(
        self,
        ticker: str,
        end_date: str,
        period: str = "annual",
        limit: int = 1
    ) -> List[FinancialProfile]:
        """
        Retrieves the financial profile for a given ticker.
        """
        pass

    @abstractmethod
    def get_insider_trades(
        self,
        ticker: str,
        end_date: str,
        start_date: Optional[str] = None,
        limit: int = 1000,
    ) -> List[InsiderTrade]:
        """获取内部交易数据"""
        pass
    
    @abstractmethod
    def get_company_news(
        self,
        ticker: str,
        end_date: str,
        start_date: Optional[str] = None,
        limit: int = 1000,
    ) -> List[CompanyNews]:
        """获取公司新闻"""
        pass
    
    @abstractmethod
    def get_market_cap(
        self,
        ticker: str,
        end_date: str,
    ) -> Optional[float]:
        """获取市值"""
        pass
    
    @abstractmethod
    def is_available(self) -> bool:
        """检查数据提供商是否可用"""
        pass

    @abstractmethod
    def convert_period(self, period: str) -> str:
        """转换period, for ttm"""
        pass

    def __str__(self):
        return f"{self.__class__.__name__}({self.name})" 