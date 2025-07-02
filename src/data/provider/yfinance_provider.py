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
    def get_prices(self, tickers: List[str], start_date: str, end_date: str, freq: str = '1d') -> List[Price]:
        """
        Fetches historical stock data for a given list of tickers and date range.
        Handles chunking for 1-minute data which is limited to 7-day fetches.
        Proactively checks if the request for 1m data is within the 30-day limit.
        """
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()

            all_dataframes = []

            if freq == '1m':
                thirty_days_ago = date.today() - timedelta(days=30)
                if start_dt < thirty_days_ago:
                    ticker_str = ", ".join(tickers)
                    logger.warning(
                        f"Request for 1-minute data for {ticker_str} starts at {start_date}, "
                        f"which is more than 30 days ago. yfinance does not support this. Skipping."
                    )
                    return []

                # Chunking logic for ALL tickers together
                current_start = start_dt
                while current_start <= end_dt:
                    chunk_end = min(current_start + timedelta(days=6), end_dt)
                    fetch_end_date = chunk_end + timedelta(days=1)
                    
                    data_chunk = self._download_data(tickers, current_start, fetch_end_date, freq)
                    if data_chunk is not None and not data_chunk.empty:
                        all_dataframes.append(data_chunk)
                    
                    # Correctly advance the loop to prevent infinite loops
                    current_start = chunk_end + timedelta(days=1)
            else:
                fetch_end_date = end_dt + timedelta(days=1)
                data = self._download_data(tickers, start_dt, fetch_end_date, freq)
                if data is not None and not data.empty:
                    all_dataframes.append(data)

            if not all_dataframes:
                logger.warning(f"No data returned for {tickers} for freq '{freq}'")
                return []

            df = pd.concat(all_dataframes)
            df.reset_index(inplace=True)
            
            prices = []
            # The time column is reliably the first column after reset_index()
            time_col = df.columns[0]

            # Multi-ticker downloads result in a MultiIndex DataFrame
            if isinstance(df.columns, pd.MultiIndex):
                for ticker in tickers:
                    if ('Open', ticker) in df.columns and df[('Open', ticker)].notna().any():
                        ticker_df = df[df[('Open', ticker)].notna()]
                        for _, row in ticker_df.iterrows():
                            prices.append(Price(
                                ticker=ticker,
                                time=str(row[time_col]),
                                open=row[('Open', ticker)],
                                close=row[('Close', ticker)],
                                high=row[('High', ticker)],
                                low=row[('Low', ticker)],
                                volume=int(row[('Volume', ticker)])
                            ))
            # Single-ticker downloads result in a standard DataFrame
            else:
                if len(tickers) == 1:
                    ticker = tickers[0]
                    for _, row in df.iterrows():
                        if pd.notna(row.get('Open')):
                            prices.append(Price(
                                ticker=ticker,
                                time=str(row[time_col]),
                                open=row['Open'],
                                close=row['Close'],
                                high=row['High'],
                                low=row['Low'],
                                volume=int(row['Volume'])
                            ))
                else:
                    logger.warning(
                        "YFinanceProvider received a non-multi-index dataframe for a multi-ticker request. "
                        "This can happen if only one of the tickers was valid. Data cannot be reliably assigned."
                    )

            prices.sort(key=lambda x: x.time)
            logger.info(f"Successfully processed {len(prices)} total data points for {len(tickers)} tickers.")
            return prices

        except Exception as e:
            logger.error(f"Error processing data for tickers {tickers} from yfinance: {e}", exc_info=True)
            return []

    def _download_data(self, tickers: List[str], start: datetime, end: datetime, interval: str) -> Optional[pd.DataFrame]:
        ticker_str = " ".join(tickers)
        logger.debug(f"yf downloading: {ticker_str}, start={start}, end={end}, interval={interval}")
        try:
            data = yf.download(
                tickers=ticker_str, start=start, end=end, interval=interval,
                progress=False, auto_adjust=True, ignore_tz=True, group_by='column'
            )
            if data.empty:
                return None
            return data
        except YFTzMissingError as e:
            logger.warning(f"Could not download data for a ticker in '{ticker_str}'. It may be an invalid ticker or delisted. Error: {e}")
            return None
        except YFPricesMissingError as e:
            logger.warning(f"Could not download price data for a ticker in '{ticker_str}' for the requested range. "
                           f"For minute data, yfinance only supports fetching data within the last 30 days. Error: {e}")
            return None
        except Exception:
            logger.error(f"An unexpected error occurred while downloading data for {ticker_str}.", exc_info=True)
            return None

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
