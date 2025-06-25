import futu as ft
import os
from typing import List, Optional
from src.data.abstract_data_provider import AbstractDataProvider
from src.data.models import Price, FinancialProfile, InsiderTrade, CompanyNews, LineItem
from src.data.futu_utils import get_report_period_date
import logging
from datetime import datetime, date
import duckdb
from pathlib import Path
from src.utils.timeout_retry import with_timeout_retry

logger = logging.getLogger(__name__)

class FutuDataProvider(AbstractDataProvider):

    def __init__(self, db_path: str = "data/futu_financials.duckdb"):
        super().__init__("Futu", api_key=None) # API key no longer needed for provider
        self.db_path = db_path
        self.quote_ctx = None

    def _connect(self):
        if self.quote_ctx is None:
            try:
                self.quote_ctx = ft.OpenQuoteContext(host=os.getenv("FUTU_HOST", "127.0.0.1"), port=11111)
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

    def _get_db_connection(self):
        """Creates and returns a connection to the DuckDB database."""
        try:
            return duckdb.connect(database=self.db_path, read_only=True)
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB database at {self.db_path}: {e}")
            return None

    @with_timeout_retry("get_financial_profile")
    def get_financial_metrics(
        self,
        ticker: str,
        end_date: str,
        period: str = "annual",
        limit: int = 10,
    ) -> List[FinancialProfile]:
        """
        从DuckDB数据库获取财务指标。
        """
        report_date = get_report_period_date(datetime.strptime(end_date, "%Y-%m-%d").date(), period)
        table_name = f"financial_profile_{report_date.strftime('%Y_%m_%d')}"

        con = self._get_db_connection()
        if not con:
            return []

        try:
            # Check if table exists
            res = con.execute(f"SELECT 1 FROM pg_tables WHERE tablename = '{table_name}'").fetchone()
            if res is None:
                logger.warning(f"Table '{table_name}' does not exist in the database.")
                return []

            query = f"SELECT * FROM {table_name} WHERE ticker = ? ORDER BY report_period DESC LIMIT ?"
            df = con.execute(query, [ticker, limit]).fetch_df()
            
            if df.empty:
                return []
            
            profiles = [FinancialProfile(**row) for _, row in df.iterrows()]
            return profiles

        except Exception as e:
            logger.error(f"Failed to get financial metrics for {ticker} from {table_name}: {e}")
            return []
        finally:
            if con:
                con.close()

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
        """检查数据提供商是否可用（检查数据库文件是否存在）。"""
        return Path(self.db_path).exists()

    def convert_period(self, period: str) -> str:
        # Futu API uses different period designations
        if period.lower() == 'ttm':
            # This needs to be mapped to Futu's equivalent, if available
            return 'ANNUAL' 
        return period.upper() 