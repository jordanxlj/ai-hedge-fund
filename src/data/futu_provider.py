import futu as ft
import os
from typing import List, Optional
from src.data.abstract_data_provider import AbstractDataProvider
from src.data.models import Price, FinancialMetrics, InsiderTrade, CompanyNews, LineItem
from src.data.futu_utils import convert_to_financial_metrics
import logging
from datetime import datetime
import duckdb

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
        """
        Retrieves financial metrics for a given stock from the local DuckDB database.
        """
        if not os.path.exists(self.db_path):
            logger.warning(f"DuckDB file '{self.db_path}' not found. Cannot fetch financial metrics.")
            return []
            
        try:
            with duckdb.connect(self.db_path, read_only=True) as conn:
                # Construct the query to get the most recent record on or before the end_date
                query = f"""
                SELECT *
                FROM financial_metrics
                WHERE ticker = ?
                  AND period = ?
                  AND report_period <= ?
                ORDER BY report_period DESC
                LIMIT ?
                """
                
                results = conn.execute(query, [ticker, period, end_date, limit]).fetchdf()
                
                if results.empty:
                    return []
                    
                # Convert DataFrame rows back to FinancialMetrics objects
                metrics_list = []
                for _, row in results.iterrows():
                    # Pydantic's `model_validate` is the correct, type-safe method for V2
                    metrics_list.append(FinancialMetrics.model_validate(row.to_dict()))
                
                return metrics_list

        except (duckdb.CatalogException, duckdb.BinderException) as e:
            logger.error(f"Error querying financial_metrics table: {e}. It may not exist or the schema may be incorrect. Try running the scraper first.")
            return []
        except Exception as e:
            logger.error(f"Failed to get financial metrics for {ticker} from DuckDB: {e}")
            return []

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