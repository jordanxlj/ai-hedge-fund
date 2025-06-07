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
        """转换股票代码格式，支持A股和港股"""
        # A股代码转换
        if len(ticker) == 6 and ticker.isdigit():
            # 判断是上交所还是深交所
            if ticker.startswith(('60', '68')):
                return f"{ticker}.SH"
            else:
                return f"{ticker}.SZ"
        
        # 港股代码转换 (如 00700 -> 00700.HK)
        if len(ticker) == 5 and ticker.isdigit():
            return f"{ticker}.HK"
        
        # 港股代码转换 (如 700 -> 00700.HK)
        if len(ticker) <= 4 and ticker.isdigit():
            ticker_padded = ticker.zfill(5)  # 补齐到5位
            return f"{ticker_padded}.HK"
        
        # 如果已经有后缀，直接返回
        if '.' in ticker:
            return ticker
            
        # 对于其他格式，暂时直接返回
        return ticker
    
    def _is_hk_stock(self, ticker: str) -> bool:
        """判断是否为港股代码"""
        return ticker.endswith('.HK') or (len(ticker) <= 5 and ticker.isdigit())
    
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
        """获取股价数据，支持A股和港股"""
        try:
            if self.pro is None:
                logger.error("Tushare API未初始化")
                return []
                
            ts_code = self._convert_ticker(ticker)
            start_date_ts = self._convert_date_format(start_date)
            end_date_ts = self._convert_date_format(end_date)
            
            # 根据股票类型选择不同的API
            if self._is_hk_stock(ticker):
                # 港股使用hk_daily
                df = self.pro.hk_daily(
                    ts_code=ts_code,
                    start_date=start_date_ts,
                    end_date=end_date_ts
                )
            else:
                # A股使用daily
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
    
    def get_financial_metrics(self, ticker: str, period: str = "annual", 
                            start_date: Optional[str] = None, end_date: Optional[str] = None, 
                            limit: int = 10) -> List[FinancialMetrics]:
        """
        获取财务指标，支持A股和港股
        """
        if not self.is_available():
            return []
        
        try:
            tushare_ticker = self._convert_ticker(ticker)
            
            # 港股暂不支持财务指标数据
            if self._is_hk_stock(ticker):
                print(f"Tushare暂不支持港股财务指标数据: {ticker}")
                return []
            
            # A股使用fina_indicator
            df = self.pro.fina_indicator(ts_code=tushare_ticker, 
                                        start_date=start_date.replace('-', '') if start_date else None,
                                        end_date=end_date.replace('-', '') if end_date else None)
            
            if df.empty:
                print(f"财务指标数据为空 {ticker}")
                return []
            
            print(f"获取财务指标 {ticker}: {len(df)} 条记录，字段: {list(df.columns)}")
            
            metrics = []
            for _, row in df.iterrows():
                metric = FinancialMetrics(
                    ticker=ticker,
                    report_period=row['end_date'][:4] + '-' + row['end_date'][4:6] + '-' + row['end_date'][6:8],
                    period=period,
                    roe=float(row['roe']) if 'roe' in row and pd.notna(row['roe']) else None,
                    roa=float(row['roa']) if 'roa' in row and pd.notna(row['roa']) else None,
                    current_ratio=float(row['current_ratio']) if 'current_ratio' in row and pd.notna(row['current_ratio']) else None,
                    quick_ratio=float(row['quick_ratio']) if 'quick_ratio' in row and pd.notna(row['quick_ratio']) else None,
                    debt_to_equity=float(row['debt_to_assets']) if 'debt_to_assets' in row and pd.notna(row['debt_to_assets']) else None,
                    eps=float(row['eps']) if 'eps' in row and pd.notna(row['eps']) else None,
                    pe_ratio=self._safe_get_float(row, 'pe'),
                    pb_ratio=self._safe_get_float(row, 'pb'),
                )
                metrics.append(metric)
            
            return metrics[:limit]
            
        except Exception as e:
            print(f"获取财务指标失败 {ticker}: {e}")
            return []
    
    def _safe_get_float(self, row, field: str) -> Optional[float]:
        """安全获取浮点数字段"""
        try:
            if field in row and pd.notna(row[field]):
                return float(row[field])
        except (KeyError, ValueError, TypeError):
            pass
        return None
    
    def search_line_items(
        self,
        ticker: str,
        line_items: List[str],
        end_date: str,
        period: str = "ttm",
        limit: int = 10,
    ) -> List[LineItem]:
        """搜索财务报表项目，支持A股和港股"""
        try:
            ts_code = self._convert_ticker(ticker)
            
            # 港股暂不支持财务报表数据
            if self._is_hk_stock(ticker):
                print(f"Tushare暂不支持港股财务报表数据: {ticker}")
                return []
            
            # A股财务数据
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
                                unit="HKD" if self._is_hk_stock(ticker) else "CNY",
                                currency="HKD" if self._is_hk_stock(ticker) else "CNY"
                            )
                            line_items_data.append(line_item)
                        # 处理净利润字段（A股使用n_income，港股使用net_profit）
                        net_income_field = 'net_profit' if self._is_hk_stock(ticker) else 'n_income'
                        if (item_name.lower() in ['net_income', '净利润'] and 
                            net_income_field in row and pd.notna(row[net_income_field])):
                            line_item = LineItem(
                                ticker=ticker,
                                report_period=row['end_date'][:4] + '-' + row['end_date'][4:6] + '-' + row['end_date'][6:8],
                                period=period,
                                line_item=item_name,
                                value=float(row[net_income_field]),
                                unit="HKD" if self._is_hk_stock(ticker) else "CNY",
                                currency="HKD" if self._is_hk_stock(ticker) else "CNY"
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
                                unit="HKD" if self._is_hk_stock(ticker) else "CNY",
                                currency="HKD" if self._is_hk_stock(ticker) else "CNY"
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
            
            # 获取股东减持数据 - 使用更简单的API
            df = self.pro.top_list(
                trade_date=self._convert_date_format(end_date) if end_date else None,
                ts_code=ts_code
            )
            
            if df is None or df.empty:
                return []
            
            trades = []
            for _, row in df.iterrows():
                trade = InsiderTrade(
                    ticker=ticker,
                    filing_date=row['trade_date'][:4] + '-' + row['trade_date'][4:6] + '-' + row['trade_date'][6:8] if pd.notna(row['trade_date']) else end_date,
                    transaction_type="减持",
                    shares=float(row['amount']) if pd.notna(row['amount']) else 0,
                    price=float(row['close']) if pd.notna(row['close']) else 0.0,
                    value=float(row['amount']) * float(row['close']) if pd.notna(row['amount']) and pd.notna(row['close']) else 0.0
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
    
    def get_market_cap(self, ticker: str, end_date: str) -> Optional[float]:
        """获取市值数据"""
        if not self.is_available():
            return None
            
        try:
            tushare_ticker = self._convert_ticker(ticker)
            
            # 首先尝试获取最近的交易日
            trade_date = end_date.replace('-', '')
            
            # 港股暂不支持市值数据
            if self._is_hk_stock(ticker):
                print(f"Tushare暂不支持港股市值数据: {ticker}")
                return None
            
            # A股使用daily_basic获取市值数据
            df = self.pro.daily_basic(ts_code=tushare_ticker, 
                                     trade_date=trade_date,
                                     fields='ts_code,trade_date,total_mv')
            
            if df.empty:
                # 如果指定日期没有数据，尝试获取最近一个月的数据
                from datetime import datetime, timedelta
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                start_dt = end_dt - timedelta(days=30)
                start_date_str = start_dt.strftime('%Y%m%d')
                
                df = self.pro.daily_basic(ts_code=tushare_ticker, 
                                         start_date=start_date_str,
                                         end_date=trade_date,
                                         fields='ts_code,trade_date,total_mv')
                
                if df.empty:
                    print(f"市值数据为空 {ticker} 在日期 {end_date}")
                    return None
                
                # 取最近的有数据的日期
                df = df.sort_values('trade_date', ascending=False)
            
            # A股total_mv字段，单位是万元
            if pd.notna(df.iloc[0]['total_mv']):
                market_cap = float(df.iloc[0]['total_mv']) * 10000
                print(f"获取市值成功 {ticker}: {market_cap:,.0f} 人民币 (日期: {df.iloc[0]['trade_date']})")
                return market_cap
            else:
                print(f"市值字段为空 {ticker}")
                return None
            
        except Exception as e:
            print(f"获取市值失败 {ticker}: {e}")
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