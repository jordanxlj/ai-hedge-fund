import logging
from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd
import yfinance as yf
from yfinance.exceptions import YFTzMissingError, YFPricesMissingError

from src.data.models import (
    Price,
    FinancialMetrics,
    LineItem,
    InsiderTrade,
    CompanyNews,
)
from src.data.provider.abstract_data_provider import AbstractDataProvider
from src.utils.timeout_retry import with_timeout_retry

logger = logging.getLogger(__name__)


class YFinanceProvider(AbstractDataProvider):
    """
    Provides historical stock data using the yfinance library.
    """

    def __init__(self):
        super().__init__("yfinance")

    @with_timeout_retry("get_prices")
    def get_prices(self, ticker: str, start_date: str, end_date: str, freq: str = '1d') -> List[Price]:
        """
        Fetches historical stock data for a given ticker and date range.
        Handles chunking for 1-minute data which is limited to 7-day fetches.
        """
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            # yfinance 'end' is exclusive, so add one day for daily, but not for minute chunks
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")

            all_data = []

            if freq == '1m':
                current_start = start_dt
                while current_start <= end_dt:
                    chunk_end = current_start + timedelta(days=6)
                    if chunk_end > end_dt:
                        chunk_end = end_dt
                    
                    # For yfinance, the end of the interval is exclusive
                    fetch_end_date = chunk_end + timedelta(days=1)

                    data_chunk = self._download_chunk(ticker, current_start, fetch_end_date, freq)
                    if data_chunk is not None and not data_chunk.empty:
                        all_data.append(data_chunk)
                    
                    current_start = chunk_end + timedelta(days=1)
            else:
                fetch_end_date = end_dt + timedelta(days=1)
                data = self._download_chunk(ticker, start_dt, fetch_end_date, freq)
                if data is not None and not data.empty:
                    all_data.append(data)

            if not all_data:
                logger.warning(f"No data returned for {ticker} for freq '{freq}'")
                return []
            
            df = pd.concat(all_data)
            df.reset_index(inplace=True)
            
            prices = [
                Price(
                    ticker=ticker,
                    time=str(row.Datetime),
                    open=row.Open,
                    close=row.Close,
                    high=row.High,
                    low=row.Low,
                    volume=int(row.Volume)
                )
                for row in df.itertuples() if pd.notna(row.Open) # Ensure row contains valid data
            ]
            
            prices.sort(key=lambda x: x.time)
            logger.info(f"Successfully fetched {len(prices)} data points for {ticker}.")
            return prices

        except Exception as e:
            logger.error(f"Error fetching data for {ticker} from yfinance: {e}", exc_info=True)
            return []

    def _download_chunk(self, ticker: str, start: datetime, end: datetime, interval: str) -> pd.DataFrame:
        logger.debug(f"yf downloading: {ticker}, start={start}, end={end}, interval={interval}")
        try:
            return yf.download(
                tickers=ticker, start=start, end=end, interval=interval,
                progress=False, auto_adjust=True, ignore_tz=True
            )
        except YFTzMissingError:
            logger.warning(f"Could not download data for ticker '{ticker}'. It may be an invalid ticker or delisted.")
            return pd.DataFrame()
        except YFPricesMissingError:
            logger.warning(f"Could not download price data for '{ticker}' for the requested range. For minute data, yfinance only supports fetching data within the last 30 days.")
            return pd.DataFrame()
        except Exception:
            # For other unexpected errors during download, log it and return empty.
            logger.error(f"An unexpected error occurred while downloading data for {ticker}.", exc_info=True)
            return pd.DataFrame()

    # --- Dummy implementations for other abstract methods ---
    def get_financial_metrics(self, ticker: str, end_date: str, period: str = "ttm", limit: int = 10) -> List[FinancialMetrics]:
        logger.warning("get_financial_metrics is not implemented for YFinanceProvider.")
        return []

    def search_line_items(self, ticker: str, line_items: List[str], end_date: str, period: str = "ttm", limit: int = 10) -> List[LineItem]:
        logger.warning("search_line_items is not implemented for YFinanceProvider.")
        return []

    def get_insider_trades(self, ticker: str, end_date: str, start_date: Optional[str] = None, limit: int = 1000) -> List[InsiderTrade]:
        logger.warning("get_insider_trades is not implemented for YFinanceProvider.")
        return []

    def get_company_news(self, ticker: str, end_date: str, start_date: Optional[str] = None, limit: int = 1000) -> List[CompanyNews]:
        logger.warning("get_company_news is not implemented for YFinanceProvider.")
        return []

    def get_market_cap(self, ticker: str, end_date: str) -> Optional[float]:
        logger.warning("get_market_cap is not implemented for YFinanceProvider.")
        return None

    def convert_period(self, period: str) -> str:
        # This method is simple and can be implemented directly
        return "annual" if period == 'ttm' else period

    def is_available(self) -> bool:
        return True