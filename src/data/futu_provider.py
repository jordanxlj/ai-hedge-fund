import futu as ft
import os
from typing import List, Optional
from src.data.abstract_data_provider import AbstractDataProvider
from src.data.models import Price, FinancialMetrics, InsiderTrade, CompanyNews, LineItem
from src.data.futu_utils import convert_to_financial_metrics
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class FutuDataProvider(AbstractDataProvider):

    def __init__(self, api_key: Optional[str] = None):
        self.host = os.getenv("FUTU_HOST", "127.0.0.1")
        super().__init__(name="Futu", api_key=api_key)
        self.quote_ctx = None

    def _connect(self):
        if self.quote_ctx is None:
            try:
                self.quote_ctx = ft.OpenQuoteContext(host=self.host, port=11111)
            except Exception as e:
                logger.error(f"Failed to connect to Futu: {e}")
                raise

    def _disconnect(self):
        if self.quote_ctx:
            self.quote_ctx.close()
            self.quote_ctx = None

    def get_prices(self, ticker: str, start_date: str, end_date: str) -> List[Price]:
        self._connect()
        try:
            futu_ticker = self._convert_ticker_format(ticker)
            
            ret, data, _ = self.quote_ctx.request_history_kline(
                futu_ticker,
                start=start_date,
                end=end_date,
                ktype=ft.KLType.K_DAY,
                autype=ft.AuType.QFQ
            )

            if ret == ft.RET_OK:
                prices = []
                for _, row in data.iterrows():
                    prices.append(Price(
                        open=float(row['open']),
                        close=float(row['close']),
                        high=float(row['high']),
                        low=float(row['low']),
                        volume=int(row['volume']),
                        time=str(row['time_key']),
                        ticker=ticker
                    ))
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
        if '.' in ticker:
            parts = ticker.split('.')
            return f"{parts[1].upper()}.{parts[0]}" # HK.00700
        
        # Simple heuristic, might need improvement
        if all(c.isdigit() for c in ticker):
            return f"HK.{ticker.zfill(5)}"
        return f"US.{ticker.upper()}"

    def get_financial_metrics(self, ticker: str, end_date: str, period: str = "ttm", limit: int = 10) -> List[FinancialMetrics]:
        self._connect()
        try:
            futu_ticker = self._convert_ticker_format(ticker)
            
            simple_filter = ft.FinancialFilter()
            simple_filter.stock_field = ft.StockField.MARKET_VAL 
            simple_filter.filter_min = 1
            simple_filter.filter_max = 1e15
            simple_filter.is_no_filter = False
            simple_filter.quarter = ft.FinancialQuarter.ANNUAL

            markets_to_try = []
            if futu_ticker.startswith('HK.'):
                markets_to_try.append(ft.Market.HK)
            elif futu_ticker.startswith('US.'):
                markets_to_try.append(ft.Market.US)
            else:
                markets_to_try = [ft.Market.HK, ft.Market.US]
            
            for market in markets_to_try:
                ret, data = self.quote_ctx.get_stock_filter(
                    market=market,
                    filter_list=[simple_filter],
                    begin=0,
                    num=200
                )
                
                if ret == ft.RET_OK and data is not None:
                    _, _, stock_list = data
                    
                    target_stock = None
                    for stock in stock_list:
                        if stock.stock_code == futu_ticker:
                            target_stock = stock
                            break
                    
                    if target_stock:
                        return convert_to_financial_metrics(target_stock, ticker, end_date, period, market)
            
            logger.warning(f"Could not find financial metrics for stock {ticker}")
            return []
            
        except Exception as e:
            logger.error(f"Failed to get financial metrics for {ticker}: {e}")
            return []
        finally:
            self._disconnect()

    def search_line_items(self, ticker: str, line_items: List[str], end_date: str, period: str = "ttm", limit: int = 10) -> List[LineItem]:
        return []

    def get_insider_trades(self, ticker: str, end_date: str, start_date: Optional[str] = None, limit: int = 1000) -> List[InsiderTrade]:
        return []

    def get_company_news(self, ticker: str, end_date: str, start_date: Optional[str] = None, limit: int = 1000) -> List[CompanyNews]:
        return []

    def get_market_cap(self, ticker: str, end_date: str) -> Optional[float]:
        self._connect()
        try:
            futu_ticker = self._convert_ticker_format(ticker)
            ret, data = self.quote_ctx.get_market_snapshot([futu_ticker])
            
            if ret == ft.RET_OK and not data.empty:
                market_cap = data['market_cap'].iloc[0]
                return float(market_cap) if market_cap else None
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