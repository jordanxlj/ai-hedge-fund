import futu as ft
import os
import time
import logging
from datetime import datetime, date
from typing import List, Optional
from collections import deque
import asyncio

import pandas as pd
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type, RetryCallState

from src.data.models import FinancialProfile, StockPlateMapping, Market
from src.data.futu_utils import FutuDummyStockData, futu_data_to_financial_profile, get_report_period_date
from src.utils.log_util import logger_setup as _init_logging
from src.data.db import get_database_api, DatabaseAPI

# Initialise logging first so subsequent imports are covered
_init_logging()

logger = logging.getLogger(__name__)

# Fields that require using SimpleFilter based on Futu API docs
SIMPLE_FILTER_FIELDS = {
    ft.StockField.MARKET_VAL, ft.StockField.PE_ANNUAL, ft.StockField.PE_TTM,
    ft.StockField.PB_RATE, ft.StockField.PS_TTM, ft.StockField.PCF_TTM, ft.StockField.TOTAL_SHARE,
    ft.StockField.FLOAT_SHARE, ft.StockField.FLOAT_MARKET_VAL
}

FINANCIAL_FILTER_FIELDS = [
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
    ft.StockField.TOTAL_ASSETS_GROWTH_RATE,
]

# Combine all fields, ensuring no duplicates
ALL_FIELDS_TO_SCRAPE = list(set(FINANCIAL_FILTER_FIELDS + list(SIMPLE_FILTER_FIELDS)))

class FutuNetworkError(Exception):
    """Custom exception for retryable network errors."""
    pass

class FutuRateLimitError(Exception):
    """Custom exception for Futu rate limit errors."""
    pass

def wait_based_on_error(retry_state: RetryCallState) -> float:
    """
    A synchronous wait strategy for tenacity that calculates wait time based on error type.
    It does not perform the sleep itself; it returns the number of seconds to wait.
    """
    exc = retry_state.outcome.exception()
    wait_seconds = 1.0  # Default wait time
    if isinstance(exc, FutuRateLimitError):
        wait_seconds = 31.0
        logger.warning(f"Rate limit error detected. Waiting {wait_seconds} seconds before retry...")
    elif isinstance(exc, FutuNetworkError):
        # Exponential backoff
        wait_seconds = float(2 ** retry_state.attempt_number)
        logger.warning(f"Network error detected. Waiting {wait_seconds} seconds before retry...")

    return wait_seconds

class FutuScraper:
    """Asynchronously scrapes financial data and plate mappings from Futu OpenD API."""
    def __init__(self, db_path: str = "data/futu_financials.duckdb"):
        self.host = os.getenv("FUTU_HOST", "127.0.0.1")
        self.port = 11111
        self.db_path = db_path
        self.quote_ctx: Optional[ft.OpenQuoteContext] = None
        self.db_api: Optional[DatabaseAPI] = None  # Initialize as None
        self.req_freq = 3.1
        self.rate_limit_timestamps = deque(maxlen=10)

    async def __aenter__(self):
        """Asynchronous context manager entry point. Connects to resources."""
        await self._connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Asynchronous context manager exit point. Closes connections."""
        self.close()

    async def _connect(self):
        """Asynchronously connects to the Futu API and the database."""
        if not self.quote_ctx:
            try:
                self.quote_ctx = ft.OpenQuoteContext(host=self.host, port=self.port)
                logger.info("Futu API connected successfully.")
            except Exception as e:
                logger.error(f"Failed to connect to Futu API: {e}")
                raise

        if not self.db_api:
            try:
                self.db_api = get_database_api("duckdb", db_path=self.db_path)
                self.db_api.connect()
            except UnicodeDecodeError as e:
                logger.error(f"Failed to connect to DuckDB due to a database file path openning issue: {e}")
                logger.error(f"Problematic path: {self.db_path}")
                raise
            except Exception as e:
                logger.error(f"An unexpected error occurred while connecting to the database: {e}")
                raise

    def _disconnect(self):
        """Disconnects from the Futu API and the database."""
        if self.quote_ctx:
            self.quote_ctx.close()
            self.quote_ctx = None
        if self.db_api:
            self.db_api.close()
            self.db_api = None

    async def scrape_and_store_financials(self, market: str, quarter: str = "annual"):
        """Scrapes financial data for a specific report period and stores it."""
        report_date = get_report_period_date(date.today(), quarter)
        report_date_str = report_date.strftime('%Y-%m-%d')
        
        scraped_profiles = await self.scrape_financial_profile(market, quarter, report_date_str)
        if not scraped_profiles:
            logger.info("No financial data was scraped, nothing to store.")
            return
            
        table_name = f"financial_profile_{report_date.strftime('%Y_%m_%d')}"
        logger.info(f"Scraped {len(scraped_profiles)} records for period {report_date_str}, storing to table '{table_name}'...")

        primary_keys = ["ticker", "report_period", "period"]
        self.db_api.create_table_from_model(table_name, FinancialProfile, primary_keys)
        self.db_api.upsert_data_from_models(table_name, scraped_profiles, primary_keys)
        logger.info(f"Successfully stored/updated {len(scraped_profiles)} records in '{table_name}'.")

    async def scrape_financial_profile(self, market: str, quarter: str, end_date: str) -> List[FinancialProfile]:
        """Asynchronously scrapes financial profiles for all stocks in a given market."""
        await self._connect()
        try:
            ft_market = self._validate_market(market)
            logger.info(f"Using report period end date: {end_date} for quarter '{quarter}'")

            all_stocks_data = {}
            tasks = [self._scrape_field(field, ft_market, quarter, all_stocks_data) for field in ALL_FIELDS_TO_SCRAPE]
            await asyncio.gather(*tasks)

            return self._process_scraped_data(all_stocks_data, end_date, quarter)
        except Exception as e:
            logger.error(f"Failed to scrape financial profiles for market {market}: {e}", exc_info=True)
            return []

    async def _scrape_field(self, field, ft_market, quarter, all_stocks_data):
        """Asynchronously scrapes data for a single financial field."""
        logger.info(f"Scraping field: {field}")
        begin_index = 0
        num_per_req = 200
        quarter_enum = self._get_quarter_enum(quarter)

        while True:
            filter_instance = self._create_filter(field, quarter_enum)
            ret, data = self.quote_ctx.get_stock_filter(market=ft_market, filter_list=[filter_instance], begin=begin_index, num=num_per_req)
            await asyncio.sleep(self.req_freq)

            if ret != ft.RET_OK:
                logger.error(f"Futu API error for field {field}: {data}")
                break
            if data is None:
                break

            is_last_page, _, stock_list_chunk = data
            for stock_data in stock_list_chunk:
                stock_code = stock_data.stock_code.split('.')[1]
                stock_vars = vars(stock_data)
                value = None

                if stock_code not in all_stocks_data:
                    all_stocks_data[stock_code] = {'ticker': stock_code}
                    all_stocks_data[stock_code]['stock_name'] = stock_data.stock_name

                # FinancialFilter returns a tuple key, SimpleFilter returns a string key
                if field in SIMPLE_FILTER_FIELDS:
                    value = stock_vars.get(field.lower())
                else:
                    value = stock_vars.get((field.lower(), quarter))

                all_stocks_data[stock_code][field.lower()] = value

            if is_last_page:
                break
            begin_index += num_per_req

    def _validate_market(self, market: str) -> ft.Market:
        """Validates and returns the Futu API market enum."""
        market_map = {'HK': ft.Market.HK, 'US': ft.Market.US}
        ft_market = market_map.get(market.upper())
        if not ft_market:
            raise ValueError(f"Unsupported market: {market}. Please use 'HK' or 'US'.")
        return ft_market

    def _get_quarter_enum(self, quarter: str) -> ft.FinancialQuarter:
        """Gets the Futu API quarter enum."""
        quarter_map = {"annual": ft.FinancialQuarter.ANNUAL, "q1": ft.FinancialQuarter.FIRST_QUARTER, "interim": ft.FinancialQuarter.INTERIM, "q3": ft.FinancialQuarter.THIRD_QUARTER}
        return quarter_map.get(quarter.lower(), ft.FinancialQuarter.ANNUAL)

    def _create_filter(self, field, quarter_enum):
        """Creates an API request filter instance."""
        filter_instance = ft.SimpleFilter() if field in SIMPLE_FILTER_FIELDS else ft.FinancialFilter()
        if isinstance(filter_instance, ft.FinancialFilter):
            filter_instance.quarter = quarter_enum
        filter_instance.stock_field = field
        filter_instance.filter_min = -1e15
        filter_instance.filter_max = 1e15
        filter_instance.is_no_filter = False
        return filter_instance

    def _process_scraped_data(self, all_stocks_data: dict, end_date: str, quarter: str) -> List[FinancialProfile]:
        """Processes raw scraped data into a list of FinancialProfile objects."""
        logger.info(f"Finished scraping. Total unique stocks found: {len(all_stocks_data)}")
        return futu_data_to_financial_profile(all_stocks_data, end_date, quarter)

    async def scrape_stock_plate_mappings(self, market: Market = Market.HK):
        """Scrapes and stores stock-to-plate mappings."""
        await self._connect()
        try:
            plate_list = self._get_plate_list(market)
            if not plate_list:
                logger.warning(f"No plates found for market: {market.value}")
                return

            all_mappings = []
            for plate in plate_list:
                stocks_df = await self._get_plate_stock_with_retry(plate['code'])
                if stocks_df is not None and not stocks_df.empty:
                    for _, stock in stocks_df.iterrows():
                        ticker = stock['code'].split('.')[1] if '.' in stock['code'] else stock['code']
                        all_mappings.append(StockPlateMapping(ticker=ticker, stock_name=stock['stock_name'], plate_code=plate['plate_id'], plate_name=plate['plate_name'], market=market.value))

            if all_mappings:
                table_name = "stock_plate_mappings"
                primary_keys = ["ticker", "plate_code"]
                self.db_api.create_table_from_model(table_name, StockPlateMapping, primary_keys)
                self.db_api.upsert_data_from_models(table_name, all_mappings, primary_keys)
                logger.info(f"Upserted {len(all_mappings)} stock-plate mappings.")
        finally:
            self.close()

    def _get_plate_list(self, market: Market):
        """Fetches the list of all plates for a given market."""
        ft_market = self._validate_market(market.value)
        ret, data = self.quote_ctx.get_plate_list(ft_market, ft.Plate.ALL)
        if ret == ft.RET_OK:
            return data.to_dict('records')
        logger.error(f"Failed to get plate list for {market.value}: {data}")
        return []

    @retry(stop=stop_after_attempt(5), wait=wait_based_on_error, retry=retry_if_exception_type((FutuNetworkError, FutuRateLimitError)))
    async def _get_plate_stock_with_retry(self, plate_code: str) -> Optional[pd.DataFrame]:
        """A retry-enabled async method to fetch stocks in a given plate."""
        await self._connect()
        logger.info(f"Fetching stocks for plate: {plate_code}")
        ret, data = self.quote_ctx.get_plate_stock(plate_code)
        
        error_msg = str(data)
        if ret != ft.RET_OK:
            if "频率太高" in error_msg: raise FutuRateLimitError(error_msg)
            if "NN_ProtoRet_ByDisConnOrCacnel" in error_msg or "网络中断" in error_msg: raise FutuNetworkError(error_msg)
            logger.error(f"Non-retryable error for plate {plate_code}: {error_msg}")
            return None
        return data

    def close(self):
        """Closes both Futu and database connections."""
        self._disconnect()

if __name__ == "__main__":
    scraper = FutuScraper()
    asyncio.run(scraper.scrape_and_store_financials("HK", "annual"))
    asyncio.run(scraper.scrape_stock_plate_mappings(Market.HK))
    scraper.close()