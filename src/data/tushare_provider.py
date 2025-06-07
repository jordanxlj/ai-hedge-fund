import os
import pandas as pd
from typing import List, Optional
import logging
from datetime import datetime, timedelta
import tushare as ts

from src.data.abstract_data_provider import AbstractDataProvider
from src.data.models import (
    Price,
    FinancialMetrics,
    LineItem,
    InsiderTrade,
    CompanyNews,
)

logger = logging.getLogger(__name__)


class TushareProvider(AbstractDataProvider):
    """Tushare数据提供商实现"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("Tushare", api_key or os.environ.get("TUSHARE_API_KEY"))
        self.pro = None
        if self.api_key:
            try:
                ts.set_token(self.api_key)
                self.pro = ts.pro_api()
            except Exception as e:
                logger.error(f"Tushare初始化失败: {e}")
                self.pro = None
    
    def _convert_ticker(self, ticker: str) -> str:
        """转换股票代码格式 (如 AAPL -> 000001.SZ)"""
        # 这里需要根据实际需求来转换股票代码格式
        # 示例：如果是美股代码，可能需要映射到A股代码
        # 或者保持原有格式，取决于具体使用场景
        
        # 简单的转换逻辑：如果是6位数字，认为是A股代码
        if len(ticker) == 6 and ticker.isdigit():
            # 判断是上交所还是深交所
            if ticker.startswith(('60', '68')):
                return f"{ticker}.SH"
            else:
                return f"{ticker}.SZ"
        
        # 如果已经有后缀，直接返回
        if '.' in ticker:
            return ticker
            
        # 对于其他格式，暂时直接返回
        return ticker
    
    def _convert_date_format(self, date_str: str) -> str:
        """转换日期格式 (YYYY-MM-DD -> YYYYMMDD)"""
        try:
            return date_str.replace('-', '')
        except:
            return date_str
    
    def get_prices(
        self,
        ticker: str,
        start_date: str,
        end_date: str
    ) -> List[Price]:
        """获取股价数据"""
        try:
            if self.pro is None:
                logger.error("Tushare API未初始化")
                return []
                
            ts_code = self._convert_ticker(ticker)
            start_date_ts = self._convert_date_format(start_date)
            end_date_ts = self._convert_date_format(end_date)
            
            # 获取日K线数据
            df = self.pro.daily(
                ts_code=ts_code,
                start_date=start_date_ts,
                end_date=end_date_ts
            )
            
            if df is None or df.empty:
                return []
            
            prices = []
            for _, row in df.iterrows():
                price = Price(
                    time=row['trade_date'][:4] + '-' + row['trade_date'][4:6] + '-' + row['trade_date'][6:8],
                    open=float(row['open']),
                    high=float(row['high']),
                    low=float(row['low']),
                    close=float(row['close']),
                    volume=int(row['vol'] * 100) if pd.notna(row['vol']) else 0,  # 转换为股数
                    ticker=ticker
                )
                prices.append(price)
            
            # 按时间排序
            prices.sort(key=lambda x: x.time)
            return prices
            
        except Exception as e:
            logger.error(f"获取股价数据失败 {ticker}: {e}")
            return []
    
    def get_financial_metrics(
        self,
        ticker: str,
        end_date: str,
        period: str = "ttm",
        limit: int = 10,
    ) -> List[FinancialMetrics]:
        """获取财务指标数据"""
        try:
            ts_code = self._convert_ticker(ticker)
            
            # 获取财务指标数据
            df = self.pro.fina_indicator(
                ts_code=ts_code,
                end_date=self._convert_date_format(end_date),
                fields='ts_code,end_date,roe,roa,current_ratio,quick_ratio,debt_to_assets,eps,pe,pb'
            )
            
            if df is None or df.empty:
                return []
            
            metrics = []
            for _, row in df.iterrows():
                metric = FinancialMetrics(
                    ticker=ticker,
                    report_period=row['end_date'][:4] + '-' + row['end_date'][4:6] + '-' + row['end_date'][6:8],
                    period=period,
                    roe=float(row['roe']) if pd.notna(row['roe']) else None,
                    roa=float(row['roa']) if pd.notna(row['roa']) else None,
                    current_ratio=float(row['current_ratio']) if pd.notna(row['current_ratio']) else None,
                    quick_ratio=float(row['quick_ratio']) if pd.notna(row['quick_ratio']) else None,
                    debt_to_equity=float(row['debt_to_assets']) if pd.notna(row['debt_to_assets']) else None,
                    eps=float(row['eps']) if pd.notna(row['eps']) else None,
                    pe_ratio=float(row['pe']) if pd.notna(row['pe']) else None,
                    pb_ratio=float(row['pb']) if pd.notna(row['pb']) else None,
                )
                metrics.append(metric)
            
            return metrics[:limit]
            
        except Exception as e:
            logger.error(f"获取财务指标失败 {ticker}: {e}")
            return []
    
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
            ts_code = self._convert_ticker(ticker)
            
            # 获取财务报表数据 - 利润表
            income_df = self.pro.income(
                ts_code=ts_code,
                end_date=self._convert_date_format(end_date),
                fields='ts_code,end_date,revenue,operate_profit,total_profit,n_income'
            )
            
            # 获取财务报表数据 - 资产负债表
            balance_df = self.pro.balancesheet(
                ts_code=ts_code,
                end_date=self._convert_date_format(end_date),
                fields='ts_code,end_date,total_assets,total_liab,total_equity'
            )
            
            line_items_data = []
            
            # 处理利润表数据
            if income_df is not None and not income_df.empty:
                for _, row in income_df.iterrows():
                    for item_name in line_items:
                        if item_name.lower() in ['revenue', '营业收入'] and pd.notna(row['revenue']):
                            line_item = LineItem(
                                ticker=ticker,
                                report_period=row['end_date'][:4] + '-' + row['end_date'][4:6] + '-' + row['end_date'][6:8],
                                period=period,
                                line_item=item_name,
                                value=float(row['revenue']),
                                unit="CNY"
                            )
                            line_items_data.append(line_item)
                        elif item_name.lower() in ['net_income', '净利润'] and pd.notna(row['n_income']):
                            line_item = LineItem(
                                ticker=ticker,
                                report_period=row['end_date'][:4] + '-' + row['end_date'][4:6] + '-' + row['end_date'][6:8],
                                period=period,
                                line_item=item_name,
                                value=float(row['n_income']),
                                unit="CNY"
                            )
                            line_items_data.append(line_item)
            
            # 处理资产负债表数据
            if balance_df is not None and not balance_df.empty:
                for _, row in balance_df.iterrows():
                    for item_name in line_items:
                        if item_name.lower() in ['total_assets', '总资产'] and pd.notna(row['total_assets']):
                            line_item = LineItem(
                                ticker=ticker,
                                report_period=row['end_date'][:4] + '-' + row['end_date'][4:6] + '-' + row['end_date'][6:8],
                                period=period,
                                line_item=item_name,
                                value=float(row['total_assets']),
                                unit="CNY"
                            )
                            line_items_data.append(line_item)
            
            return line_items_data[:limit]
            
        except Exception as e:
            logger.error(f"搜索财务报表项目失败 {ticker}: {e}")
            return []
    
    def get_insider_trades(
        self,
        ticker: str,
        end_date: str,
        start_date: Optional[str] = None,
        limit: int = 1000,
    ) -> List[InsiderTrade]:
        """获取内部交易数据 (在Tushare中可能是股东减持数据)"""
        try:
            ts_code = self._convert_ticker(ticker)
            
            # 获取股东减持数据
            df = self.pro.share_float(
                ts_code=ts_code,
                end_date=self._convert_date_format(end_date) if end_date else None
            )
            
            if df is None or df.empty:
                return []
            
            trades = []
            for _, row in df.iterrows():
                trade = InsiderTrade(
                    ticker=ticker,
                    filing_date=row['end_date'][:4] + '-' + row['end_date'][4:6] + '-' + row['end_date'][6:8] if pd.notna(row['end_date']) else end_date,
                    transaction_type="减持",
                    shares=float(row['float_share']) if pd.notna(row['float_share']) else 0,
                    price=0.0,  # Tushare减持数据中通常没有具体价格
                    value=0.0
                )
                trades.append(trade)
            
            return trades[:limit]
            
        except Exception as e:
            logger.error(f"获取内部交易数据失败 {ticker}: {e}")
            return []
    
    def get_company_news(
        self,
        ticker: str,
        end_date: str,
        start_date: Optional[str] = None,
        limit: int = 1000,
    ) -> List[CompanyNews]:
        """获取公司新闻 (Tushare可能没有新闻数据，返回空列表)"""
        try:
            # Tushare主要提供财务数据，新闻数据可能需要其他数据源
            logger.warning(f"Tushare暂不支持公司新闻数据: {ticker}")
            return []
            
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
            ts_code = self._convert_ticker(ticker)
            
            # 获取基本信息
            df = self.pro.daily_basic(
                ts_code=ts_code,
                trade_date=self._convert_date_format(end_date)
            )
            
            if df is None or df.empty:
                return None
            
            # 总市值 (万元)
            if pd.notna(df.iloc[0]['total_mv']):
                return float(df.iloc[0]['total_mv']) * 10000  # 转换为元
            
            return None
            
        except Exception as e:
            logger.error(f"获取市值失败 {ticker}: {e}")
            return None
    
    def is_available(self) -> bool:
        """检查数据提供商是否可用"""
        try:
            if not self.api_key or self.pro is None:
                return False
            
            # 简单测试，获取交易日历
            cal_df = self.pro.trade_cal(
                exchange='SSE',
                start_date='20240101',
                end_date='20240102',
                fields='cal_date,is_open'
            )
            return cal_df is not None and not cal_df.empty
            
        except Exception as e:
            logger.error(f"Tushare可用性检查失败: {e}")
            return False 