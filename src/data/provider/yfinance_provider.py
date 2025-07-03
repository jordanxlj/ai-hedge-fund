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
    FinancialProfile,
    LineItem,
    InsiderTrade,
    CompanyNews,
)
from src.data.provider.abstract_data_provider import AbstractDataProvider
from src.utils.timeout_retry import with_timeout_retry
from src.utils.financial_utils import reconstruct_financial_metrics

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
        """
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()

            dataframes = self._fetch_data_for_period(tickers, start_dt, end_dt, freq)

            if not dataframes:
                logger.warning(f"No data returned for {tickers} for freq '{freq}'")
                return []

            df = pd.concat(dataframes)
            prices = self._process_dataframe(df, tickers)
            
            prices.sort(key=lambda x: x.time)
            logger.info(f"Successfully processed {len(prices)} total data points for {len(tickers)} tickers.")
            return prices

        except Exception as e:
            logger.error(f"Error processing data for tickers {tickers} from yfinance: {e}", exc_info=True)
            return []

    def _fetch_data_for_period(self, tickers: List[str], start_dt: date, end_dt: date, freq: str) -> List[pd.DataFrame]:
        """Fetches data, handling 1-minute frequency chunking."""
        dataframes = []
        if freq == '1m':
            thirty_days_ago = date.today() - timedelta(days=30)
            if start_dt < thirty_days_ago:
                ticker_str = ", ".join(tickers)
                logger.warning(
                    f"Request for 1-minute data for {ticker_str} starts at {start_dt}, "
                    f"which is more than 30 days ago. yfinance does not support this. Skipping."
                )
                return []

            current_start = start_dt
            while current_start <= end_dt:
                chunk_end = min(current_start + timedelta(days=6), end_dt)
                fetch_end_date = chunk_end + timedelta(days=1)
                
                data_chunk = self._download_data(tickers, current_start, fetch_end_date, freq)
                if data_chunk is not None and not data_chunk.empty:
                    dataframes.append(data_chunk)
                
                current_start = chunk_end + timedelta(days=1)
        else:
            fetch_end_date = end_dt + timedelta(days=1)
            data = self._download_data(tickers, start_dt, fetch_end_date, freq)
            if data is not None and not data.empty:
                dataframes.append(data)
        return dataframes

    def _process_dataframe(self, df: pd.DataFrame, tickers: List[str]) -> List[Price]:
        """Processes the downloaded DataFrame into a list of Price objects."""
        prices = []
        df.reset_index(inplace=True)
        time_col = df.columns[0]

        if isinstance(df.columns, pd.MultiIndex):
            for ticker in tickers:
                if ('Open', ticker) in df.columns and df[('Open', ticker)].notna().any():
                    # Extract all columns for the current ticker and create a new DataFrame
                    ticker_df = df[[time_col, ('Open', ticker), ('Close', ticker), ('High', ticker), ('Low', ticker), ('Volume', ticker)]].copy()
                    # Rename columns for easier access
                    ticker_df.columns = [time_col, 'Open', 'Close', 'High', 'Low', 'Volume']
                    for _, row in ticker_df.iterrows():
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
        return prices

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
    def get_financial_profile(self, ticker: str, end_date: str, period: str = "annual", limit: int = 1) -> List[FinancialProfile]:
        """
        Retrieves a comprehensive financial profile for a given ticker.
        """
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            # Fetching data based on the period
            if period == "annual":
                income_stmt = stock.income_stmt.T
                balance_sheet = stock.balance_sheet.T
                cash_flow = stock.cash_flow.T
            else: # quarterly
                income_stmt = stock.quarterly_income_stmt.T
                balance_sheet = stock.quarterly_balance_sheet.T
                cash_flow = stock.quarterly_cash_flow.T

            if income_stmt.empty or balance_sheet.empty or cash_flow.empty:
                logger.warning(f"No financial data available for {ticker} for the specified period.")
                return []

            # Combine all financial data into a single DataFrame
            financials_df = pd.concat([income_stmt, balance_sheet, cash_flow], axis=1)
            financials_df = financials_df.loc[:, ~financials_df.columns.duplicated()] # Remove duplicate columns
            financials_df.index = pd.to_datetime(financials_df.index)
            
            # Filter by end_date and limit
            logger.debug(f"end_date: {end_date}, limit: {limit}, financials_df.index: {financials_df.index}")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            financials_df = financials_df[financials_df.index <= end_dt].head(limit)

            if financials_df.empty:
                logger.info(f"No financial data found for {ticker} before or on {end_date}.")
                return []

            # Calculate growth metrics
            df_sorted = financials_df.sort_index(ascending=True)
            growth_mapping = {
                'revenue_growth': 'Total Revenue', 'earnings_growth': 'Net Income',
                'operating_income_growth': 'Operating Income', 'ebitda_growth': 'EBITDA',
                'total_assets_growth': 'Total Assets', 'book_value_growth': 'Stockholders Equity',
                'earnings_per_share_growth': 'Basic EPS'
            }
            for key, col in growth_mapping.items():
                if col in df_sorted.columns:
                    financials_df[key] = df_sorted[col].pct_change()

            profiles = []
            for index, row in financials_df.iterrows():
                data = row.to_dict()

                # Discard data with excessive NaN values
                nan_count = sum(1 for v in data.values() if pd.isna(v))
                if len(data) > 0 and (nan_count / len(data)) > 0.5:
                    logger.warning(
                        f"Discarding data for {ticker} for period {index.strftime('%Y-%m-%d')} "
                        f"due to excessive NaN values ({nan_count}/{len(data)})."
                    )
                    continue
                
                # Start with fundamental data
                profile_data = {
                    'ticker': ticker, 'name': info.get('longName', ''),
                    'report_period': index.strftime('%Y-%m-%d'), 'period': period,
                    'currency': info.get('currency', 'USD'),
                    'revenue': data.get('Total Revenue'), 'gross_profit': data.get('Gross Profit'),
                    'operating_income': data.get('Operating Income'), 'operating_expense': data.get('Operating Expense'),
                    'selling_expenses': data.get('Selling General And Administration'),
                    'research_and_development': data.get('Research And Development'),
                    'net_income': data.get('Net Income'), 'ebit': data.get('EBIT'), 'ebitda': data.get('EBITDA'),
                    'profit_before_tax': data.get('Pretax Income'), 'income_tax_expense': data.get('Tax Provision'),
                    'total_assets': data.get('Total Assets'), 'current_assets': data.get('Current Assets'),
                    'total_liabilities': data.get('Total Liab'), 'current_liabilities': data.get('Current Liabilities'),
                    'working_capital': data.get('Working Capital'), 'shareholders_equity': data.get('Stockholders Equity'),
                    'goodwill': data.get('Goodwill'), 'intangible_assets': data.get('Other Intangible Assets'),
                    'goodwill_and_intangible_assets': data.get('Goodwill And Other Intangible Assets'),
                    'inventories': data.get('Inventory'), 'accounts_receivable': data.get('Net Receivables'),
                    'cash_and_equivalents': data.get('Cash And Cash Equivalents'),
                    'operating_cash_flow': data.get('Total Cash From Operating Activities'),
                    'capital_expenditure': data.get('Capital Expenditures'),
                    'depreciation_and_amortization': data.get('Depreciation And Amortization'),
                    'issuance_or_purchase_of_equity_shares': data.get('Net Common Stock Issuance'),
                    'earnings_per_share': data.get('Basic EPS'), 'tax_rate': data.get('Tax Rate For Calcs'),
                    'short_term_debt': data.get('Current Debt'), 'long_term_debt': data.get('Long Term Debt'),
                    'total_debt': data.get('Total Debt'), 'invested_capital': data.get('Invested Capital'),
                }
                
                # Add growth metrics
                for key in growth_mapping:
                    profile_data[key] = data.get(key)

                # Use directly provided Free Cash Flow if available, otherwise calculate it
                if 'Free Cash Flow' in data and pd.notna(data['Free Cash Flow']):
                    profile_data['free_cash_flow'] = data.get('Free Cash Flow')
                else:
                    op_cash = data.get('Total Cash From Operating Activities', 0)
                    cap_ex = data.get('Capital Expenditures', 0)
                    profile_data['free_cash_flow'] = (op_cash or 0) + (cap_ex or 0)
                
                # Create initial profile and reconstruct it to get calculated ratios
                temp_profile = reconstruct_financial_metrics(FinancialProfile(**profile_data))
                
                # Add metrics from stock.info and per-share calculations
                market_cap = info.get('marketCap')
                shares_outstanding = info.get('sharesOutstanding')
                
                temp_profile.market_cap = market_cap
                temp_profile.outstanding_shares = shares_outstanding
                temp_profile.enterprise_value = info.get('enterpriseValue')
                temp_profile.payout_ratio = info.get('payoutRatio')
                temp_profile.peg_ratio = info.get('pegRatio')
                temp_profile.price_to_book_ratio = info.get('priceToBook')
                temp_profile.price_to_sales_ratio = info.get('priceToSalesTrailing12Months')
                temp_profile.enterprise_value_to_ebitda_ratio = info.get('enterpriseToEbitda')
                temp_profile.enterprise_value_to_revenue_ratio = info.get('enterpriseToRevenue')
                temp_profile.price_to_earnings_ratio = info.get('trailingPE')

                if shares_outstanding and shares_outstanding > 0:
                    if temp_profile.shareholders_equity is not None:
                        temp_profile.book_value_per_share = temp_profile.shareholders_equity / shares_outstanding
                    if temp_profile.free_cash_flow is not None:
                        temp_profile.free_cash_flow_per_share = temp_profile.free_cash_flow / shares_outstanding

                if market_cap and temp_profile.operating_cash_flow and temp_profile.operating_cash_flow > 0:
                    temp_profile.price_to_cashflow_ratio = market_cap / temp_profile.operating_cash_flow

                profiles.append(temp_profile)

            logger.info(f"Successfully retrieved {len(profiles)} financial profiles for {ticker}.")
            return profiles

        except Exception as e:
            logger.error(f"Error fetching financial profile for {ticker} from yfinance: {e}", exc_info=True)
            return []

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

