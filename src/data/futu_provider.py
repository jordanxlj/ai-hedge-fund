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

import logging
from datetime import datetime
import time
import os

logger = logging.getLogger(__name__)

class FutuDataProvider(AbstractDataProvider):
    class _DummyStockData:
        def __init__(self, data_dict):
            self.__dict__.update(data_dict)
        
        def __getattr__(self, name):
            return self.__dict__.get(name)

    def __init__(self, api_key: Optional[str] = None):
        self.host = os.getenv("FUTU_HOST", "127.0.0.1")
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
            
            # 尝试使用简单过滤器而不是财务过滤器
            # 使用当前价格作为过滤条件，设置一个很宽的范围
            simple_filter = ft.FinancialFilter ()
            simple_filter.stock_field = ft.StockField.BASIC_EPS
            simple_filter.filter_min = 0.01  # 最小价格
            simple_filter.filter_max = 10  # 最大价格，设置得很大以包含大部分股票
            simple_filter.is_no_filter = False  # 启用过滤
            simple_filter.quarter = ft.FinancialQuarter.ANNUAL

            financial_filters = [simple_filter]
            
            # 根据ticker格式判断市场
            markets_to_try = []
            if futu_ticker.startswith('HK.'):
                markets_to_try.append(ft.Market.HK)
            elif futu_ticker.startswith('US.'):
                markets_to_try.append(ft.Market.US)
            else:
                # 默认先尝试港股，再尝试美股
                markets_to_try = [ft.Market.HK, ft.Market.US]
            
            for market in markets_to_try:
                ret, data = self.quote_ctx.get_stock_filter(
                    market=market,
                    filter_list=financial_filters,
                    begin=0,
                    num=200  # 增加查询数量，提高找到匹配股票的可能性
                )
                print(f"ret = {ret}, data = {data}")
                
                if ret == ft.RET_OK and data:
                    last_page, all_count, stock_list = data
                    print(len(stock_list))
                    
                    # 查找匹配的股票
                    target_stock = None
                    
                    for stock in stock_list:
                        print(f"stock = {stock}, stock_code = {futu_ticker}")
                        # stock.stock_code 包含市场前缀，如 "HK.00700"
                        # futu_ticker 也包含市场前缀，如 "HK.00700"
                        if stock.stock_code == futu_ticker:
                            target_stock = stock
                            break
                        # 也尝试匹配不带前缀的代码部分
                        elif stock.stock_code.split('.')[-1] == futu_ticker.split('.')[-1]:
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

    def scrape_financial_metrics(self, market: str, quarter: str = "annual", end_date: Optional[str] = None) -> List[FinancialMetrics]:
        """
        Scrapes financial metrics for all stocks in a given market by iterating through each financial field one by one.
        NOTE: This process is very slow due to API limitations and rate limiting.
        """
        self._connect()
        try:
            if market.upper() == 'HK':
                ft_market = ft.Market.HK
            elif market.upper() == 'US':
                ft_market = ft.Market.US
            else:
                raise ValueError("Unsupported market. Please use 'HK' or 'US'.")

            if end_date is None:
                end_date = datetime.now().strftime('%Y-%m-%d')

            financial_fields_to_scrape = [
                ft.StockField.ACCOUNTS_RECEIVABLE, ft.StockField.BASIC_EPS, ft.StockField.CASH_AND_CASH_EQUIVALENTS,
                ft.StockField.CURRENT_ASSET_RATIO, ft.StockField.CURRENT_DEBT_RATIO, ft.StockField.CURRENT_RATIO,
                ft.StockField.DEBT_ASSET_RATE, ft.StockField.DILUTED_EPS, ft.StockField.EBIT_GROWTH_RATE,
                ft.StockField.EBIT_MARGIN, ft.StockField.EBIT_TTM, ft.StockField.EBITDA, ft.StockField.EBITDA_MARGIN,
                ft.StockField.EPS_GROWTH_RATE, ft.StockField.EQUITY_MULTIPLIER, ft.StockField.FINANCIAL_COST_RATE,
                ft.StockField.FIXED_ASSET_TURNOVER, ft.StockField.GROSS_PROFIT_RATE, ft.StockField.INVENTORY_TURNOVER,
                ft.StockField.NET_PROFIT, ft.StockField.NET_PROFIT_CASH_COVER_TTM, ft.StockField.NET_PROFIT_RATE,
                ft.StockField.NET_PROFIX_GROWTH, ft.StockField.NOCF_GROWTH_RATE, ft.StockField.NOCF_PER_SHARE,
                ft.StockField.NOCF_PER_SHARE_GROWTH_RATE, ft.StockField.OPERATING_CASH_FLOW_TTM,
                ft.StockField.OPERATING_MARGIN_TTM, ft.StockField.OPERATING_PROFIT_GROWTH_RATE,
                ft.StockField.OPERATING_PROFIT_TO_TOTAL_PROFIT, ft.StockField.OPERATING_PROFIT_TTM,
                ft.StockField.OPERATING_REVENUE_CASH_COVER, ft.StockField.PROFIT_BEFORE_TAX_GROWTH_RATE,
                ft.StockField.PROFIT_TO_SHAREHOLDERS_GROWTH_RATE, ft.StockField.PROPERTY_RATIO,
                ft.StockField.QUICK_RATIO, ft.StockField.RETURN_ON_EQUITY_RATE, ft.StockField.ROA_TTM,
                ft.StockField.ROE_GROWTH_RATE, ft.StockField.ROIC, ft.StockField.ROIC_GROWTH_RATE,
                ft.StockField.SHAREHOLDER_NET_PROFIT_TTM, ft.StockField.SUM_OF_BUSINESS,
                ft.StockField.SUM_OF_BUSINESS_GROWTH, ft.StockField.TOTAL_ASSET_TURNOVER,
                ft.StockField.TOTAL_ASSETS_GROWTH_RATE, ft.StockField.PE_ANNUAL, ft.StockField.PE_TTM,
                ft.StockField.PB_RATE, ft.StockField.PS_TTM, ft.StockField.PCF_TTM, ft.StockField.MARKET_VAL,
                ft.StockField.TOTAL_SHARE
            ]

            all_stocks_data = {}
            
            if quarter.lower() == 'annual':
                quarter_enum = ft.FinancialQuarter.ANNUAL
            elif quarter.lower() == 'q1':
                quarter_enum = ft.FinancialQuarter.Q1
            elif quarter.lower() == 'interim':
                quarter_enum = ft.FinancialQuarter.INTERIM
            elif quarter.lower() == 'q3':
                quarter_enum = ft.FinancialQuarter.Q3
            else:
                quarter_enum = ft.FinancialQuarter.ANNUAL

            for field in financial_fields_to_scrape:
                logger.info(f"Scraping data for field: {field.name}")
                begin_index = 0
                num_per_req = 100
                last_page = False

                while not last_page:
                    financial_filter = ft.FinancialFilter()
                    financial_filter.stock_field = field
                    financial_filter.is_no_filter = True
                    financial_filter.quarter = quarter_enum

                    ret, data = self.quote_ctx.get_stock_filter(
                        market=ft_market,
                        filter_list=[financial_filter],
                        begin=begin_index,
                        num=num_per_req
                    )
                    
                    time.sleep(3.1)

                    if ret != ft.RET_OK:
                        if "不支持该过滤字段" in str(data):
                             logger.warning(f"Field {field.name} is not supported by get_stock_filter. Skipping.")
                             break 
                        logger.error(f"Futu API error for field {field.name}: {data}")
                        break

                    page_info, all_count, stock_list_chunk = data
                    last_page = page_info

                    if not stock_list_chunk:
                        logger.warning(f"No stock data returned for field {field.name} on this page.")
                        break

                    for stock_data in stock_list_chunk:
                        stock_code = stock_data.stock_code
                        if stock_code not in all_stocks_data:
                            all_stocks_data[stock_code] = {'stock_code': stock_code, 'stock_name': stock_data.stock_name}
                        
                        attr_name = field.name.lower()
                        if hasattr(stock_data, attr_name):
                            value = getattr(stock_data, attr_name)
                            all_stocks_data[stock_code][attr_name] = value

                    begin_index += len(stock_list_chunk)
            
            final_metrics = []
            for stock_code, data_dict in all_stocks_data.items():
                dummy_stock_obj = self._DummyStockData(data_dict)
                parts = stock_code.split('.')
                if len(parts) == 2:
                    internal_ticker = f"{parts[1]}.{parts[0]}"
                    metrics = self._convert_to_financial_metrics(dummy_stock_obj, internal_ticker, end_date, quarter, ft_market)
                    final_metrics.extend(metrics)

            return final_metrics

        except Exception as e:
            logger.error(f"Failed to scrape financial metrics for market {market}: {e}")
            import traceback
            logger.error(f"Detailed error: {traceback.format_exc()}")
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
            if market == ft.Market.HK:
                currency = "HKD"
            elif market == ft.Market.US:
                currency = "USD" 
            else:
                # 根据ticker格式判断
                futu_ticker = self._convert_ticker_format(ticker)
                currency = "HKD" if futu_ticker.startswith('HK.') else "USD"
            
            # 根据futu API文档，FilterStockData对象直接包含财务指标字段
            # 直接从stock_data对象获取属性值
            
            # 调试：打印stock_data对象的所有属性
            print(f"stock_data类型: {type(stock_data)}")
            print(f"stock_data属性: {dir(stock_data)}")
            print(f"stock_data内容: {stock_data}")
            
            # 尝试访问一些可能的属性
            for attr in dir(stock_data):
                if not attr.startswith('_'):
                    try:
                        value = getattr(stock_data, attr)
                        print(f"{attr}: {value}")
                    except:
                        pass
            
            # 估值指标
            if hasattr(stock_data, 'pe_annual') and stock_data.pe_annual is not None:
                financial_data['price_to_earnings_ratio'] = stock_data.pe_annual
            if hasattr(stock_data, 'pe_ttm') and stock_data.pe_ttm is not None:
                financial_data['pe_ratio'] = stock_data.pe_ttm
            if hasattr(stock_data, 'pb_rate') and stock_data.pb_rate is not None:
                financial_data['price_to_book_ratio'] = stock_data.pb_rate
                financial_data['pb_ratio'] = stock_data.pb_rate
            if hasattr(stock_data, 'ps_ttm') and stock_data.ps_ttm is not None:
                financial_data['price_to_sales_ratio'] = stock_data.ps_ttm
            if hasattr(stock_data, 'pcf_ttm') and stock_data.pcf_ttm is not None:
                financial_data['pcf_ratio'] = stock_data.pcf_ttm
                
            # 市值和股份
            if hasattr(stock_data, 'market_val') and stock_data.market_val is not None:
                financial_data['market_cap'] = stock_data.market_val
            if hasattr(stock_data, 'total_share') and stock_data.total_share is not None:
                financial_data['outstanding_shares'] = stock_data.total_share
                
            # 盈利指标
            if hasattr(stock_data, 'net_profit') and stock_data.net_profit is not None:
                financial_data['net_income'] = stock_data.net_profit
            if hasattr(stock_data, 'net_profit_rate') and stock_data.net_profit_rate is not None:
                financial_data['net_margin'] = stock_data.net_profit_rate
            if hasattr(stock_data, 'gross_profit_rate') and stock_data.gross_profit_rate is not None:
                financial_data['gross_margin'] = stock_data.gross_profit_rate
            if hasattr(stock_data, 'ebitda') and stock_data.ebitda is not None:
                financial_data['ebitda'] = stock_data.ebitda
            if hasattr(stock_data, 'ebit_ttm') and stock_data.ebit_ttm is not None:
                financial_data['ebit'] = stock_data.ebit_ttm
            if hasattr(stock_data, 'shareholder_net_profit_ttm') and stock_data.shareholder_net_profit_ttm is not None:
                financial_data['net_income_to_shareholders'] = stock_data.shareholder_net_profit_ttm
                
            # 回报率指标
            if hasattr(stock_data, 'return_on_equity_rate') and stock_data.return_on_equity_rate is not None:
                financial_data['return_on_equity'] = stock_data.return_on_equity_rate
                financial_data['roe'] = stock_data.return_on_equity_rate
            if hasattr(stock_data, 'roa_ttm') and stock_data.roa_ttm is not None:
                financial_data['return_on_assets'] = stock_data.roa_ttm
                financial_data['roa'] = stock_data.roa_ttm
            if hasattr(stock_data, 'roic') and stock_data.roic is not None:
                financial_data['return_on_invested_capital'] = stock_data.roic
                
            # 增长率指标
            if hasattr(stock_data, 'net_profix_growth') and stock_data.net_profix_growth is not None:
                financial_data['earnings_growth'] = stock_data.net_profix_growth
            if hasattr(stock_data, 'sum_of_business') and stock_data.sum_of_business is not None:
                financial_data['revenue'] = stock_data.sum_of_business
            if hasattr(stock_data, 'sum_of_business_growth') and stock_data.sum_of_business_growth is not None:
                financial_data['revenue_growth'] = stock_data.sum_of_business_growth
            if hasattr(stock_data, 'eps_growth_rate') and stock_data.eps_growth_rate is not None:
                financial_data['eps_growth'] = stock_data.eps_growth_rate
            if hasattr(stock_data, 'roe_growth_rate') and stock_data.roe_growth_rate is not None:
                financial_data['roe_growth'] = stock_data.roe_growth_rate
            if hasattr(stock_data, 'roic_growth_rate') and stock_data.roic_growth_rate is not None:
                financial_data['roic_growth'] = stock_data.roic_growth_rate
            if hasattr(stock_data, 'nocf_growth_rate') and stock_data.nocf_growth_rate is not None:
                financial_data['operating_cash_flow_growth'] = stock_data.nocf_growth_rate
            if hasattr(stock_data, 'nocf_per_share_growth_rate') and stock_data.nocf_per_share_growth_rate is not None:
                financial_data['free_cash_flow_per_share_growth'] = stock_data.nocf_per_share_growth_rate
            if hasattr(stock_data, 'operating_profit_growth_rate') and stock_data.operating_profit_growth_rate is not None:
                financial_data['operating_income_growth'] = stock_data.operating_profit_growth_rate
            if hasattr(stock_data, 'total_assets_growth_rate') and stock_data.total_assets_growth_rate is not None:
                financial_data['total_assets_growth'] = stock_data.total_assets_growth_rate
            if hasattr(stock_data, 'profit_to_shareholders_growth_rate') and stock_data.profit_to_shareholders_growth_rate is not None:
                financial_data['net_income_to_shareholders_growth'] = stock_data.profit_to_shareholders_growth_rate
            if hasattr(stock_data, 'profit_before_tax_growth_rate') and stock_data.profit_before_tax_growth_rate is not None:
                financial_data['pretax_income_growth'] = stock_data.profit_before_tax_growth_rate
            if hasattr(stock_data, 'ebit_growth_rate') and stock_data.ebit_growth_rate is not None:
                financial_data['ebit_growth'] = stock_data.ebit_growth_rate

            # 财务健康指标
            if hasattr(stock_data, 'debt_asset_rate') and stock_data.debt_asset_rate is not None:
                financial_data['debt_to_assets'] = stock_data.debt_asset_rate
            if hasattr(stock_data, 'current_ratio') and stock_data.current_ratio is not None:
                financial_data['current_ratio'] = stock_data.current_ratio
            if hasattr(stock_data, 'quick_ratio') and stock_data.quick_ratio is not None:
                financial_data['quick_ratio'] = stock_data.quick_ratio
            if hasattr(stock_data, 'property_ratio') and stock_data.property_ratio is not None:
                financial_data['debt_to_equity'] = stock_data.property_ratio
            if hasattr(stock_data, 'equity_multiplier') and stock_data.equity_multiplier is not None:
                financial_data['equity_multiplier'] = stock_data.equity_multiplier
            if hasattr(stock_data, 'current_asset_ratio') and stock_data.current_asset_ratio is not None:
                financial_data['current_assets_ratio'] = stock_data.current_asset_ratio
            if hasattr(stock_data, 'current_debt_ratio') and stock_data.current_debt_ratio is not None:
                financial_data['current_liabilities_ratio'] = stock_data.current_debt_ratio
            
            # 利润率指标
            if hasattr(stock_data, 'operating_margin_ttm') and stock_data.operating_margin_ttm is not None:
                financial_data['operating_margin'] = stock_data.operating_margin_ttm
            if hasattr(stock_data, 'ebit_margin') and stock_data.ebit_margin is not None:
                financial_data['ebit_margin'] = stock_data.ebit_margin
            if hasattr(stock_data, 'ebitda_margin') and stock_data.ebitda_margin is not None:
                financial_data['ebitda_margin'] = stock_data.ebitda_margin
                
            # 现金流指标
            if hasattr(stock_data, 'operating_cash_flow_ttm') and stock_data.operating_cash_flow_ttm is not None:
                financial_data['operating_cash_flow'] = stock_data.operating_cash_flow_ttm
            if hasattr(stock_data, 'net_profit_cash_cover_ttm') and stock_data.net_profit_cash_cover_ttm is not None:
                financial_data['cash_conversion_ratio'] = stock_data.net_profit_cash_cover_ttm
            if hasattr(stock_data, 'operating_revenue_cash_cover') and stock_data.operating_revenue_cash_cover is not None:
                financial_data['cash_from_operations_to_revenue_ratio'] = stock_data.operating_revenue_cash_cover
                
            # 每股指标
            if hasattr(stock_data, 'basic_eps') and stock_data.basic_eps is not None:
                financial_data['earnings_per_share'] = stock_data.basic_eps
                financial_data['eps'] = stock_data.basic_eps
            if hasattr(stock_data, 'diluted_eps') and stock_data.diluted_eps is not None:
                financial_data['diluted_eps'] = stock_data.diluted_eps
            if hasattr(stock_data, 'nocf_per_share') and stock_data.nocf_per_share is not None:
                financial_data['free_cash_flow_per_share'] = stock_data.nocf_per_share

            # 其他资产负债表/利润表/现金流量表项目
            if hasattr(stock_data, 'accounts_receivable') and stock_data.accounts_receivable is not None:
                financial_data['accounts_receivable'] = stock_data.accounts_receivable
            if hasattr(stock_data, 'cash_and_cash_equivalents') and stock_data.cash_and_cash_equivalents is not None:
                financial_data['cash_and_equivalents'] = stock_data.cash_and_cash_equivalents
            if hasattr(stock_data, 'operating_profit_ttm') and stock_data.operating_profit_ttm is not None:
                financial_data['operating_income'] = stock_data.operating_profit_ttm
            if hasattr(stock_data, 'operating_profit_to_total_profit') and stock_data.operating_profit_to_total_profit is not None:
                financial_data['operating_income_to_total_income_ratio'] = stock_data.operating_profit_to_total_profit
            
            # 周转率
            if hasattr(stock_data, 'inventory_turnover') and stock_data.inventory_turnover is not None:
                financial_data['inventory_turnover_ratio'] = stock_data.inventory_turnover
            if hasattr(stock_data, 'total_asset_turnover') and stock_data.total_asset_turnover is not None:
                financial_data['asset_turnover_ratio'] = stock_data.total_asset_turnover
            if hasattr(stock_data, 'fixed_asset_turnover') and stock_data.fixed_asset_turnover is not None:
                financial_data['fixed_asset_turnover_ratio'] = stock_data.fixed_asset_turnover

            # 成本率
            if hasattr(stock_data, 'financial_cost_rate') and stock_data.financial_cost_rate is not None:
                financial_data['financial_cost_to_revenue_ratio'] = stock_data.financial_cost_rate

            # 创建FinancialMetrics对象
            metrics = FinancialMetrics(
                ticker=ticker,
                report_period=end_date,
                period=period,
                currency=currency,
                **financial_data
            )
            print(metrics)
            
            return [metrics]
            
        except Exception as e:
            logger.error(f"转换财务指标数据失败: {e}")
            import traceback
            logger.error(f"详细错误信息: {traceback.format_exc()}")
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