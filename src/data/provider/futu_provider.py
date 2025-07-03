import futu as ft
import os
from typing import List, Optional, TYPE_CHECKING
import logging
from datetime import datetime
from futu import KLType

from src.utils.timeout_retry import with_timeout_retry
from src.data.provider.abstract_data_provider import AbstractDataProvider
from src.data.models import (
    Price,
    FinancialProfile,
    InsiderTrade,
    CompanyNews,
    LineItem
)
from src.data.futu_utils import get_report_period_date
from src.data.db.base import DatabaseAPI
from src.data.db.duckdb_impl import DuckDBAPI

if TYPE_CHECKING:
    from src.data.db.base import DatabaseAPI

logger = logging.getLogger(__name__)

class FutuDataProvider(AbstractDataProvider):

    def __init__(self, db_api: Optional[DatabaseAPI] = None):
        super().__init__("Futu", api_key=None)
        if db_api is None:
            self.db_api: DatabaseAPI = DuckDBAPI(db_path="data/futu_financials.duckdb")
        else:
            self.db_api: DatabaseAPI = db_api
        self.quote_ctx = None

    def _connect(self):
        if self.quote_ctx is None:
            try:
                self.quote_ctx = ft.OpenQuoteContext(host=os.getenv("FUTU_HOST", "127.0.0.1"), port=11111)
                self.db_api.connect()
            except Exception as e:
                logger.error(f"Failed to connect to Futu or database: {e}")
                raise

    def _disconnect(self):
        if self.quote_ctx:
            self.quote_ctx.close()
            self.quote_ctx = None
        self.db_api.close()

    def get_prices(
        self,
        tickers: List[str],
        start_date: str,
        end_date: str,
        freq: str = '1d'
    ) -> List[Price]:
        self._connect()
        all_prices = []
        try:
            for ticker in tickers:
                try:
                    futu_ticker = self._convert_ticker_format(ticker)
                    
                    # Map freq to Futu's KLType
                    ktype = self._map_freq_to_kltype(freq)
                    if not ktype:
                        logger.warning(f"Unsupported frequency '{freq}' for Futu provider. Skipping ticker {ticker}.")
                        continue

                    ret, data, _ = self.quote_ctx.request_history_kline(
                        futu_ticker,
                        start=start_date,
                        end=end_date,
                        ktype=ktype
                    )

                    if ret == ft.RET_OK:
                        prices = [
                            Price(
                                open=float(row['open']),
                                close=float(row['close']),
                                high=float(row['high']),
                                low=float(row['low']),
                                volume=int(row['volume']),
                                time=str(row['time_key']),
                                ticker=ticker  # Use the original ticker
                            )
                            for _, row in data.iterrows()
                        ]
                        all_prices.extend(prices)
                    else:
                        logger.error(f"Futu API error for get_prices('{ticker}'): {data}")
                except Exception as e:
                    logger.error(f"An exception occurred in get_prices for ticker '{ticker}': {e}", exc_info=True)
            
            return all_prices

        except Exception as e:
            logger.error(f"An exception occurred in get_prices for tickers '{tickers}': {e}", exc_info=True)
            return []
        finally:
            # self.close() # Usually, we don't close the connection here to allow reuse.
            pass

    def _map_freq_to_kltype(self, freq: str):
        mapping = {
            '1m': KLType.K_1M,
            '5m': KLType.K_5M,
            '15m': KLType.K_15M,
            '30m': KLType.K_30M,
            '60m': KLType.K_60M,
            '1d': KLType.K_DAY,
            '1w': KLType.K_WEEK,
            '1M': KLType.K_MON,
        }
        return mapping.get(freq)

    def _convert_ticker_format(self, ticker: str) -> str:
        if '.' in ticker:
            parts = ticker.split('.')
            return f"{parts[1].upper()}.{parts[0]}" # HK.00700
        
        # Simple heuristic, might need improvement
        if all(c.isdigit() for c in ticker):
            return f"HK.{ticker.zfill(5)}"
        return f"US.{ticker.upper()}"

    @with_timeout_retry("get_financial_profile")
    def get_financial_metrics(
        self,
        ticker: str,
        end_date: str,
        period: str = "annual",
        limit: int = 10,
    ) -> List[FinancialProfile]:
        """
        从数据库获取财务指标。
        """
        report_date = get_report_period_date(datetime.strptime(end_date, "%Y-%m-%d").date(), period)
        table_name = f"financial_profile_{report_date.strftime('%Y_%m_%d')}"

        try:
            self.db_api.connect()
            if not self.db_api.table_exists(table_name):
                logger.warning(f"Table '{table_name}' does not exist in the database.")
                return []

            query = f"SELECT * FROM {table_name} WHERE ticker = ? ORDER BY report_period DESC LIMIT ?"
            
            profiles = self.db_api.query_to_models(query, FinancialProfile, params=[ticker, limit])
            return profiles

        except Exception as e:
            logger.error(f"Failed to get financial metrics for {ticker} from {table_name}: {e}")
            return []
        finally:
            self.db_api.close()

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
        """检查数据提供商是否可用（通过尝试连接数据库）。"""
        try:
            self.db_api.connect()
            self.db_api.close()
            return True
        except Exception:
            return False

    def convert_period(self, period: str) -> str:
        # Futu API uses different period designations
        if period.lower() == 'ttm':
            # This needs to be mapped to Futu's equivalent, if available
            return 'ANNUAL' 
        return period.upper()
 