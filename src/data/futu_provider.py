import futu as ft
from typing import List, Optional
from src.data.abstract_data_provider import AbstractDataProvider
from src.data.models import (
    Price,
    FinancialMetrics,
    LineItem,
    InsiderTrade,
    CompanyNews,
)
from src.utils.log_util import logger

class FutuDataProvider(AbstractDataProvider):
    def __init__(self, api_key: Optional[str] = None):
        super().__init__(name="Futu", api_key=api_key)
        self.quote_ctx = None

    def _connect(self):
        if self.quote_ctx is None:
            try:
                # Assuming the FutuOpenD client is running on localhost:11111
                self.quote_ctx = ft.OpenQuoteContext(host='127.0.0.1', port=11111)
            except Exception as e:
                logger.error(f"Failed to connect to FutuOpenD: {e}")
                raise

    def _disconnect(self):
        if self.quote_ctx:
            self.quote_ctx.close()
            self.quote_ctx = None

    def get_prices(self, ticker: str, start_date: str, end_date: str) -> List[Price]:
        self._connect()
        try:
            # Futu tickers are in the format 'MARKET.CODE', e.g., 'HK.00700' or 'US.AAPL'
            # We need to adapt the ticker format if it's not already in this format.
            futu_ticker = self._convert_ticker_format(ticker)
            
            ret, data, page_req_key = self.quote_ctx.request_history_kline(
                futu_ticker,
                start=start_date,
                end=end_date,
                ktype=ft.KLType.K_DAY,
                autype=ft.AuType.QFQ  # Qian Fu Quan (forward-adjusted)
            )

            if ret == ft.RET_OK:
                prices = []
                for index, row in data.iterrows():
                    prices.append(Price(
                        open=float(row['open']),
                        close=float(row['close']),
                        high=float(row['high']),
                        low=float(row['low']),
                        volume=int(row['volume']),
                        time=str(row['time_key']),
                        ticker=ticker
                    ))
                logger.info(f"Successfully fetched {len(prices)} price records for {ticker}")
                return prices
            else:
                logger.error(f"Futu API error for get_prices('{ticker}'): {data}")
                return []
        except Exception as e:
            logger.error(f"An exception occurred in get_prices('{ticker}'): {e}")
            return []
        finally:
            self._disconnect()
            
    def _convert_ticker_format(self, ticker: str) -> str:
        """Convert ticker to Futu format (MARKET.CODE)"""
        if '.' in ticker:
            return ticker.upper()
        
        # Try to guess the market based on ticker characteristics
        # This is a simple heuristic and may need refinement
        if ticker.isdigit() and len(ticker) in [4, 5]:
            # Likely Hong Kong stock (e.g., 0700, 00700)
            return f"HK.{ticker.zfill(5)}"
        elif len(ticker) >= 1 and ticker.isalpha():
            # Likely US stock (e.g., AAPL, TSLA)
            return f"US.{ticker.upper()}"
        else:
            # Default to HK market
            logger.warning(f"Cannot determine market for ticker '{ticker}', defaulting to HK market")
            return f"HK.{ticker}"

    def get_financial_metrics(self, ticker: str, end_date: str, period: str = "ttm", limit: int = 10) -> List[FinancialMetrics]:
        """
        使用get_stock_filter接口获取财务指标数据
        """
        self._connect()
        try:
            futu_ticker = self._convert_ticker_format(ticker)
            
            # 创建财务指标过滤器
            financial_filters = []
            
            # 定义需要获取的财务指标字段
            financial_fields = [
                # 估值指标
                ft.StockField.PE_ANNUAL,        # 市盈率(年报)
                ft.StockField.PE_TTM,           # 市盈率TTM
                ft.StockField.PB_RATE,          # 市净率
                ft.StockField.PS_TTM,           # 市销率TTM
                ft.StockField.PCF_TTM,          # 市现率TTM
                # 市值和股份
                ft.StockField.MARKET_VAL,       # 市值
                ft.StockField.TOTAL_SHARE,      # 总股本
                ft.StockField.FLOAT_SHARE,      # 流通股本
                ft.StockField.FLOAT_MARKET_VAL, # 流通市值
                # 盈利能力
                ft.StockField.NET_PROFIT,       # 净利润
                ft.StockField.NET_PROFIT_RATE,  # 净利率
                ft.StockField.GROSS_PROFIT_RATE,# 毛利率
                ft.StockField.RETURN_ON_EQUITY_RATE,  # ROE
                ft.StockField.ROA_TTM,          # ROA
                ft.StockField.ROIC,             # ROIC
                # 成长性
                ft.StockField.NET_PROFIX_GROWTH,# 净利润增长率
                ft.StockField.SUM_OF_BUSINESS,  # 营业收入
                ft.StockField.SUM_OF_BUSINESS_GROWTH,  # 营业收入增长率
                # 财务健康度
                ft.StockField.DEBT_ASSET_RATE,  # 资产负债率
                ft.StockField.CURRENT_RATIO,    # 流动比率
                ft.StockField.QUICK_RATIO,      # 速动比率
                # 运营效率
                ft.StockField.OPERATING_MARGIN_TTM, # 营业利润率TTM
                ft.StockField.EBIT_MARGIN,      # EBIT利润率
                ft.StockField.EBITDA_MARGIN,    # EBITDA利润率
                # 现金流
                ft.StockField.OPERATING_CASH_FLOW_TTM, # 经营现金流TTM
                # 其他指标
                ft.StockField.BASIC_EPS,        # 基本每股收益
                ft.StockField.DILUTED_EPS,      # 稀释每股收益
                ft.StockField.NOCF_PER_SHARE,   # 每股经营现金流
            ]
            
            # 为每个字段创建财务过滤器
            for field in financial_fields:
                financial_filter = ft.FinancialFilter()
                financial_filter.stock_field = field
                financial_filter.is_no_filter = True  # 不过滤，只获取数据
                # 设置季度，TTM通常使用年报数据
                if period.lower() == "ttm":
                    financial_filter.quarter = ft.FinancialQuarter.ANNUAL
                else:
                    financial_filter.quarter = ft.FinancialQuarter.ANNUAL
                financial_filters.append(financial_filter)
            
            # 根据ticker格式判断市场
            markets_to_try = []
            if futu_ticker.startswith('HK.'):
                markets_to_try.append(ft.Market.HK_Security)
            elif futu_ticker.startswith('US.'):
                markets_to_try.append(ft.Market.US_Security)
            else:
                # 默认先尝试港股，再尝试美股
                markets_to_try = [ft.Market.HK_Security, ft.Market.US_Security]
            
            for market in markets_to_try:
                ret, data = self.quote_ctx.get_stock_filter(
                    market=market,
                    filter_list=financial_filters,
                    begin=0,
                    num=100  # 增加查询数量，提高找到匹配股票的可能性
                )
                
                if ret == ft.RET_OK and data:
                    last_page, all_count, stock_list = data
                    
                    # 查找匹配的股票
                    target_stock = None
                    stock_code = futu_ticker.split('.')[-1]
                    
                    for stock in stock_list:
                        if stock.stock_code == stock_code:
                            target_stock = stock
                            break
                    
                    if target_stock:
                        return self._convert_to_financial_metrics(target_stock, ticker, end_date, period, market)
            
            logger.warning(f"未找到股票 {ticker} 的财务指标数据")
            return []
            
        except Exception as e:
            logger.error(f"获取财务指标失败 {ticker}: {e}")
            return []
        finally:
            self._disconnect()

    def _convert_to_financial_metrics(self, stock_data, ticker: str, end_date: str, period: str, market=None) -> List[FinancialMetrics]:
        """
        将futu股票筛选结果转换为FinancialMetrics对象
        """
        try:
            # 从股票数据中提取财务数据
            financial_data = {}
            
            # 根据市场判断货币类型
            if market == ft.Market.HK_Security:
                currency = "HKD"
            elif market == ft.Market.US_Security:
                currency = "USD" 
            else:
                # 根据ticker格式判断
                futu_ticker = self._convert_ticker_format(ticker)
                currency = "HKD" if futu_ticker.startswith('HK.') else "USD"
            
            # 遍历财务数据
            for financial_item in stock_data.financial_data_list:
                field = financial_item.field_name
                value = financial_item.value
                
                # 映射字段到FinancialMetrics字段
                if field == ft.StockField.PE_ANNUAL:
                    financial_data['price_to_earnings_ratio'] = value
                elif field == ft.StockField.PE_TTM:
                    financial_data['pe_ratio'] = value
                elif field == ft.StockField.PB_RATE:
                    financial_data['price_to_book_ratio'] = value
                    financial_data['pb_ratio'] = value
                elif field == ft.StockField.PS_TTM:
                    financial_data['price_to_sales_ratio'] = value / 100.0 if value else None  # 转换百分比
                elif field == ft.StockField.PCF_TTM:
                    financial_data['free_cash_flow_yield'] = value / 100.0 if value else None
                elif field == ft.StockField.MARKET_VAL:
                    financial_data['market_cap'] = value
                elif field == ft.StockField.TOTAL_SHARE:
                    financial_data['outstanding_shares'] = value
                elif field == ft.StockField.NET_PROFIT:
                    financial_data['net_income'] = value
                elif field == ft.StockField.NET_PROFIT_RATE:
                    financial_data['net_margin'] = value / 100.0 if value else None
                elif field == ft.StockField.GROSS_PROFIT_RATE:
                    financial_data['gross_margin'] = value / 100.0 if value else None
                elif field == ft.StockField.RETURN_ON_EQUITY_RATE:
                    financial_data['return_on_equity'] = value / 100.0 if value else None
                    financial_data['roe'] = value / 100.0 if value else None
                elif field == ft.StockField.ROA_TTM:
                    financial_data['return_on_assets'] = value / 100.0 if value else None
                    financial_data['roa'] = value / 100.0 if value else None
                elif field == ft.StockField.ROIC:
                    financial_data['return_on_invested_capital'] = value / 100.0 if value else None
                elif field == ft.StockField.NET_PROFIX_GROWTH:
                    financial_data['earnings_growth'] = value / 100.0 if value else None
                elif field == ft.StockField.SUM_OF_BUSINESS:
                    financial_data['revenue'] = value
                elif field == ft.StockField.SUM_OF_BUSINESS_GROWTH:
                    financial_data['revenue_growth'] = value / 100.0 if value else None
                elif field == ft.StockField.DEBT_ASSET_RATE:
                    financial_data['debt_to_assets'] = value / 100.0 if value else None
                elif field == ft.StockField.CURRENT_RATIO:
                    financial_data['current_ratio'] = value
                elif field == ft.StockField.QUICK_RATIO:
                    financial_data['quick_ratio'] = value
                elif field == ft.StockField.OPERATING_MARGIN_TTM:
                    financial_data['operating_margin'] = value / 100.0 if value else None
                elif field == ft.StockField.EBIT_MARGIN:
                    financial_data['ebitda_margin'] = value / 100.0 if value else None
                elif field == ft.StockField.OPERATING_CASH_FLOW_TTM:
                    financial_data['operating_cash_flow'] = value
                elif field == ft.StockField.BASIC_EPS:
                    financial_data['earnings_per_share'] = value
                    financial_data['eps'] = value
                elif field == ft.StockField.DILUTED_EPS:
                    financial_data['diluted_eps'] = value
                elif field == ft.StockField.NOCF_PER_SHARE:
                    financial_data['free_cash_flow_per_share'] = value
            
            # 创建FinancialMetrics对象
            metrics = FinancialMetrics(
                ticker=ticker,
                report_period=end_date,
                period=period,
                currency=currency,
                **financial_data
            )
            
            return [metrics]
            
        except Exception as e:
            logger.error(f"转换财务指标数据失败: {e}")
            return []

    def search_line_items(self, ticker: str, line_items: List[str], end_date: str, period: str = "ttm", limit: int = 10) -> List[LineItem]:
        # Futu API does not provide a direct way to search for arbitrary line items.
        # This would require fetching financial statements and parsing them.
        return []

    def get_insider_trades(self, ticker: str, end_date: str, start_date: Optional[str] = None, limit: int = 1000) -> List[InsiderTrade]:
        # Futu API does not seem to provide insider trade data directly.
        return []

    def get_company_news(self, ticker: str, end_date: str, start_date: Optional[str] = None, limit: int = 1000) -> List[CompanyNews]:
        # Futu API does not seem to provide company news directly.
        return []

    def get_market_cap(self, ticker: str, end_date: str) -> Optional[float]:
        self._connect()
        try:
            futu_ticker = self._convert_ticker_format(ticker)
            
            ret, data = self.quote_ctx.get_market_snapshot([futu_ticker])
            
            if ret == ft.RET_OK and not data.empty:
                # Market cap is usually calculated as shares_outstanding * current_price
                # Futu API provides market cap directly in some cases
                row = data.iloc[0]
                
                # Try to get market cap directly if available
                if 'market_cap' in row and row['market_cap'] is not None:
                    return float(row['market_cap'])
                
                # Calculate from shares outstanding and current price if available
                if 'shares_outstanding' in row and 'cur_price' in row:
                    shares = row['shares_outstanding']
                    price = row['cur_price']
                    if shares is not None and price is not None and shares > 0:
                        return float(shares * price)
                        
                logger.warning(f"Market cap data not available for {ticker}")
                return None
            else:
                logger.error(f"Futu API error for get_market_cap('{ticker}'): {data}")
                return None
                
        except Exception as e:
            logger.error(f"An exception occurred in get_market_cap('{ticker}'): {e}")
            return None
        finally:
            self._disconnect()

    def is_available(self) -> bool:
        try:
            self._connect()
            self._disconnect()
            return True
        except Exception:
            return False

    def convert_period(self, period: str) -> str:
        # Futu API uses different period designations
        if period.lower() == 'ttm':
            # This needs to be mapped to Futu's equivalent, if available
            return 'ANNUAL' 
        return period.upper() 