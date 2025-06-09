import os
import pandas as pd
from pathlib import Path
from typing import List, Optional
import logging
from datetime import datetime, timedelta
import tushare as ts

from src.data.abstract_data_provider import AbstractDataProvider
from src.utils.config_utils import load_yaml_config
from src.data.models import (
    Price,
    FinancialMetrics,
    LineItem,
    InsiderTrade,
    CompanyNews,
)
from src.utils.timeout_retry import with_timeout_retry

logger = logging.getLogger(__name__)


class TushareProvider(AbstractDataProvider):
    """Tushare数据提供商实现"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("Tushare", api_key or os.environ.get("TUSHARE_API_KEY"))
        self.pro = None
        # 初始化时加载H股到A股映射
        self.h2a_mapping = self._load_h2a_mapping('conf/H2A_mapping.yaml')
        
        if self.api_key:
            try:
                ts.set_token(self.api_key)
                self.pro = ts.pro_api()
            except Exception as e:
                logger.error(f"Tushare初始化失败: {e}")
                self.pro = None
    
    def _convert_ticker(self, ticker: str) -> str:
        """转换股票代码格式，支持A股和港股"""
        # 如果已经有后缀，直接返回
        if '.' in ticker:
            return ticker
        
        # A股代码转换（6位数字）
        if len(ticker) == 6 and ticker.isdigit():
            # 判断是上交所还是深交所
            if ticker.startswith(('60', '68')):
                return f"{ticker}.SH"
            else:
                return f"{ticker}.SZ"
        
        # 港股代码转换 (5位数字，如 00700)
        if len(ticker) == 5 and ticker.isdigit():
            return f"{ticker}.HK"
        
        # 港股代码转换 (1-4位数字，如 700，补齐到5位)
        if len(ticker) <= 4 and ticker.isdigit():
            ticker_padded = ticker.zfill(5)  # 补齐到5位
            return f"{ticker_padded}.HK"
        
        # 对于其他格式，直接返回
        return ticker
    
    def _is_hk_stock(self, ticker: str) -> bool:
        """判断是否为港股代码"""
        return ticker.endswith('.HK')
    
    def _load_h2a_mapping(self, config_file) -> dict:
        """从YAML配置文件加载H股到A股的代码映射，在初始化时调用
        
        Returns:
            dict: H股代码到A股代码的映射字典，如果文件不存在或加载失败则返回空字典
        """
        try:
            # 获取配置文件路径 - 使用pathlib
            config_path = Path(config_file)
            
            # 检查文件是否存在
            if not config_path.exists():
                logger.info(f"H2A_mapping.yaml 文件不存在: {config_path}，将不使用H股到A股映射")
                return {}
            
            # 使用配置工具加载YAML文件
            config = load_yaml_config(config_path, {})
            
            # 获取映射数据
            hk_to_a_mapping = config.get('h2a_mapping', {})
            
            logger.info(f"成功加载H股到A股映射配置，共 {len(hk_to_a_mapping)} 个映射关系")
            return hk_to_a_mapping
            
        except Exception as e:
            logger.warning(f"加载H股到A股映射配置失败: {e}，将不使用H股到A股映射")
            return {}
    
    def reload_h2a_mapping(self) -> bool:
        """重新加载H股到A股映射配置
        
        Returns:
            bool: 加载是否成功
        """
        try:
            old_count = len(self.h2a_mapping)
            self.h2a_mapping = self._load_h2a_mapping()
            new_count = len(self.h2a_mapping)
            logger.info(f"重新加载H股到A股映射配置成功，映射关系从 {old_count} 个更新为 {new_count} 个")
            return True
        except Exception as e:
            logger.error(f"重新加载H股到A股映射配置失败: {e}")
            return False
    
    def _get_corresponding_a_stock(self, hk_ticker: str) -> str:
        """获取H股对应的A股代码
        
        Args:
            hk_ticker: H股代码 (如 "00700.HK")
            
        Returns:
            str: 对应的A股代码，如果没有对应则返回原H股代码
        """
        if not self._is_hk_stock(hk_ticker):
            return hk_ticker
        
        # 使用初始化时加载的映射数据
        a_stock_code = self.h2a_mapping.get(hk_ticker)
        
        if a_stock_code:
            logger.info(f"H股 {hk_ticker} 找到对应A股代码: {a_stock_code}")
            return a_stock_code
        else:
            logger.info(f"H股 {hk_ticker} 未找到对应A股代码，将使用原H股代码")
            return hk_ticker
    
    def _convert_date_format(self, date_str: str) -> str:
        """转换日期格式 (YYYY-MM-DD -> YYYYMMDD)"""
        try:
            return date_str.replace('-', '')
        except:
            return date_str
    
    @with_timeout_retry("get_prices")
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
    
    @with_timeout_retry("get_financial_metrics")
    def get_financial_metrics(
        self,
        ticker: str,
        end_date: str,
        period: str = "ttm",
        limit: int = 10,
    ) -> List[FinancialMetrics]:
        """
        获取财务指标，支持A股和港股
        对于H股，如果有对应的A股代码，则使用A股接口查询基本面信息
        """
        if not self.is_available():
            return []
        
        try:
            tushare_ticker = self._convert_ticker(ticker)
            original_ticker = ticker  # 保存原始ticker用于返回结果
            
            # 如果是H股，尝试获取对应的A股代码
            if self._is_hk_stock(ticker):
                corresponding_a_stock = self._get_corresponding_a_stock(tushare_ticker)
                if corresponding_a_stock != tushare_ticker:
                    # 找到了对应的A股代码，使用A股接口查询
                    print(f"H股 {ticker} 使用对应A股代码 {corresponding_a_stock} 查询基本面信息")
                    tushare_ticker = corresponding_a_stock
                else:
                    # 没有找到对应的A股代码，H股暂不支持财务指标数据
                    print(f"H股 {ticker} 未找到对应A股代码，Tushare暂不支持纯港股财务指标数据")
                    return []
            
            # A股使用fina_indicator获取主要财务指标
            df = self.pro.fina_indicator(
                ts_code=tushare_ticker, 
                end_date=end_date.replace('-', '') if end_date else None
            )
            
            # 获取PE、PB等估值指标（从daily_basic表）
            valuation_df = None
            try:
                # 尝试获取最近交易日的估值数据
                trade_date = end_date.replace('-', '')
                valuation_df = self.pro.daily_basic(
                    ts_code=tushare_ticker,
                    trade_date=trade_date,
                    fields='ts_code,trade_date,pe,pb,ps'
                )
                if valuation_df.empty:
                    # 如果当日没有数据，尝试获取最近一个月的数据
                    from datetime import datetime, timedelta
                    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                    start_dt = end_dt - timedelta(days=30)
                    start_date_str = start_dt.strftime('%Y%m%d')
                    
                    valuation_df = self.pro.daily_basic(
                        ts_code=tushare_ticker,
                        start_date=start_date_str,
                        end_date=trade_date,
                        fields='ts_code,trade_date,pe,pb,ps'
                    )
                    if not valuation_df.empty:
                        valuation_df = valuation_df.sort_values('trade_date', ascending=False)
            except Exception as e:
                print(f"获取估值指标失败 {ticker}: {e}")
                valuation_df = None
            
            if df.empty:
                print(f"财务指标数据为空 {ticker}")
                return []
            
            print(f"获取财务指标 {ticker}: {len(df)} 条记录，字段: {list(df.columns)}")
            
            metrics = []
            for _, row in df.iterrows():
                # 获取对应期间的估值指标
                pe_ratio = None
                pb_ratio = None
                ps_ratio = None
                if valuation_df is not None and not valuation_df.empty:
                    # 使用最近的估值数据
                    val_row = valuation_df.iloc[0]
                    pe_ratio = self._safe_get_float(val_row, 'pe')
                    pb_ratio = self._safe_get_float(val_row, 'pb') 
                    ps_ratio = self._safe_get_float(val_row, 'ps')
                
                # 安全地获取并转换数值字段
                def safe_percentage_to_decimal(row, field_name):
                    """安全地将百分比数值转换为小数"""
                    value = self._safe_get_float(row, field_name)
                    return value / 100 if value is not None else None
                
                def safe_get_value(row, field_name):
                    """安全地获取数值"""
                    return self._safe_get_float(row, field_name)
                
                metric = FinancialMetrics(
                    ticker=original_ticker,
                    report_period=row['end_date'][:4] + '-' + row['end_date'][4:6] + '-' + row['end_date'][6:8],
                    period=period,
                    # 基础字段映射
                    return_on_equity=safe_percentage_to_decimal(row, 'roe_waa'),  # ROE使用加权平均值转换为小数
                    return_on_assets=safe_percentage_to_decimal(row, 'roa_yearly'),  # ROA转换为小数
                    current_ratio=safe_get_value(row, 'current_ratio'),
                    quick_ratio=safe_get_value(row, 'quick_ratio'),
                    debt_to_assets=safe_percentage_to_decimal(row, 'debt_to_assets'),  # 负债率转换为小数
                    debt_to_equity=safe_get_value(row, 'debt_to_eqt'),
                    # 盈利能力指标
                    gross_margin=safe_percentage_to_decimal(row, 'grossprofit_margin'),  # 毛利率
                    operating_margin=safe_percentage_to_decimal(row, 'op_of_gr'),  # 营业利润率
                    net_margin=safe_percentage_to_decimal(row, 'netprofit_margin'),  # 净利率
                    # 每股指标
                    earnings_per_share=safe_get_value(row, 'eps'),
                    book_value_per_share=safe_get_value(row, 'bps'),
                    free_cash_flow_per_share=safe_get_value(row, 'fcff_ps'),
                    # 增长率指标
                    revenue_growth=safe_percentage_to_decimal(row, 'or_yoy'),  # 营收增长率
                    earnings_growth=safe_percentage_to_decimal(row, 'netprofit_yoy'),  # 净利润增长率
                    book_value_growth=safe_percentage_to_decimal(row, 'bps_yoy'),  # 每股净资产增长率
                    # 估值指标 (从daily_basic获取)
                    price_to_earnings_ratio=pe_ratio,
                    price_to_book_ratio=pb_ratio,
                    price_to_sales_ratio=ps_ratio,
                    # Tushare兼容字段  
                    roe=safe_get_value(row, 'roe_waa'),
                    roa=safe_get_value(row, 'roa_yearly'),
                    eps=safe_get_value(row, 'eps'),
                    pe_ratio=pe_ratio,
                    pb_ratio=pb_ratio,
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
    
    @with_timeout_retry("search_line_items")
    def search_line_items(
        self,
        ticker: str,
        line_items: List[str],
        end_date: str,
        period: str = "ttm",
        limit: int = 10,
    ) -> List[LineItem]:
        """搜索财务报表项目，支持A股和港股
        对于H股，如果有对应的A股代码，则使用A股接口查询财务报表数据"""
        try:
            ts_code = self._convert_ticker(ticker)
            original_ticker = ticker  # 保存原始ticker用于返回结果
            
            # 如果是H股，尝试获取对应的A股代码
            if self._is_hk_stock(ticker):
                corresponding_a_stock = self._get_corresponding_a_stock(ts_code)
                if corresponding_a_stock != ts_code:
                    # 找到了对应的A股代码，使用A股接口查询
                    print(f"H股 {ticker} 使用对应A股代码 {corresponding_a_stock} 查询财务报表数据")
                    ts_code = corresponding_a_stock
                else:
                    # 没有找到对应的A股代码，H股暂不支持财务报表数据
                    print(f"H股 {ticker} 未找到对应A股代码，Tushare暂不支持纯港股财务报表数据")
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
    
    @with_timeout_retry("get_insider_trades")
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
    
    @with_timeout_retry("get_company_news")
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