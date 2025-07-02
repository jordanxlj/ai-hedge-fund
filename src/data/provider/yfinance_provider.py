import logging
from datetime import datetime, timedelta, date
from typing import List, Optional

import pandas as pd
import yfinance as yf
from yfinance.exceptions import YFTzMissingError, YFPricesMissingError
from pandas.api.types import is_list_like

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
        Proactively checks if the request for 1m data is within the 30-day limit.
        """
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()

            # Proactive check for 1-minute data limitation
            if freq == '1m':
                thirty_days_ago = date.today() - timedelta(days=30)
                if start_dt < thirty_days_ago:
                    logger.warning(
                        f"Request for 1-minute data for {ticker} starts at {start_date}, "
                        f"which is more than 30 days ago. yfinance does not support this. Skipping."
                    )
                    return []
                # Adjust start_date if it's within the valid window but the user requested an older date
                if end_dt < thirty_days_ago:
                     logger.warning(
                        f"Request for 1-minute data for {ticker} ends at {end_date}, "
                        f"which is more than 30 days ago. No data will be fetched."
                    )
                     return []


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
            
            # Handle both MultiIndex and simple Index columns
            df.reset_index(inplace=True)
            
            if isinstance(df.columns, pd.MultiIndex):
                # Case 1: MultiIndex columns, e.g., ('Open', '0700.HK')
                # Find the correct ticker case from the columns
                actual_ticker = next((col[1] for col in df.columns if is_list_like(col) and len(col) > 1 and col[0] == 'Open'), ticker)
                
                prices = [
                    Price(
                        ticker=ticker, # Store with the original requested ticker
                        time=str(row[('Datetime', '')]),
                        open=row[('Open', actual_ticker)],
                        close=row[('Close', actual_ticker)],
                        high=row[('High', actual_ticker)],
                        low=row[('Low', actual_ticker)],
                        volume=int(row[('Volume', actual_ticker)])
                    )
                    for _, row in df.iterrows() if pd.notna(row.get(('Open', actual_ticker)))
                ]
            else:
                # Case 2: Simple Index columns, e.g., 'Open'
                df.columns = [str(col) for col in df.columns] # Ensure all columns are strings
                
                prices = [
                    Price(
                        ticker=ticker,
                        time=str(row['Datetime']),
                        open=row['Open'],
                        close=row['Close'],
                        high=row['High'],
                        low=row['Low'],
                        volume=int(row['Volume'])
                    )
                    for _, row in df.iterrows() if pd.notna(row.get('Open'))
                ]

            prices.sort(key=lambda x: x.time)
            logger.info(f"Successfully processed {len(prices)} data points for {ticker}.")
            return prices

        except Exception as e:
            logger.error(f"Error processing data for {ticker} from yfinance: {e}", exc_info=True)
            return []

    def _download_chunk(self, ticker: str, start: datetime, end: datetime, interval: str) -> pd.DataFrame:
        logger.debug(f"yf downloading: {ticker}, start={start}, end={end}, interval={interval}")
        try:
            # Don't use group_by='ticker' as it can cause issues with single ticker downloads
            return yf.download(
                tickers=ticker, start=start, end=end, interval=interval,
                progress=False, auto_adjust=True, ignore_tz=True
            )
        except YFTzMissingError:
            logger.warning(f"Could not download data for ticker '{ticker}'. It may be an invalid ticker or delisted.")
            return pd.DataFrame()
        except YFPricesMissingError:
            logger.warning(f"Could not download price data for '{ticker}' for the requested range. "
                           f"For minute data, yfinance only supports fetching data within the last 30 days.")
            return pd.DataFrame()
        except Exception:
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
        return "annual" if period == 'ttm' else period

    def is_available(self) -> bool:
        return True
