import futu as ft
import os
import logging
from datetime import datetime, date
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type, RetryCallState

from src.data.models import FinancialProfile, StockPlateMapping, Market
from src.data.futu_utils import futu_data_to_financial_profile, get_report_period_date
from src.utils.log_util import logger_setup as _init_logging
from src.data.db import get_database_api, DatabaseAPI
from src.utils.api_executor import FutuAPIExecutor

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
    ft.StockField.DEBT_ASSET_RATE, ft.StockField.DILUTED_EPS, ft.StockField.EBITDA, ft.StockField.EBITDA_MARGIN,
    ft.StockField.EBIT_GROWTH_RATE, ft.StockField.EBIT_MARGIN, ft.StockField.EBIT_TTM, ft.StockField.EPS_GROWTH_RATE,
    ft.StockField.EQUITY_MULTIPLIER, ft.StockField.FINANCIAL_COST_RATE, ft.StockField.FIXED_ASSET_TURNOVER,
    ft.StockField.GROSS_PROFIT_RATE, ft.StockField.INVENTORY_TURNOVER, ft.StockField.NET_PROFIT,
    ft.StockField.NET_PROFIT_CASH_COVER_TTM, ft.StockField.NET_PROFIT_RATE, ft.StockField.NET_PROFIX_GROWTH,
    ft.StockField.NOCF_GROWTH_RATE, ft.StockField.NOCF_PER_SHARE, ft.StockField.NOCF_PER_SHARE_GROWTH_RATE,
    ft.StockField.OPERATING_CASH_FLOW_TTM, ft.StockField.OPERATING_MARGIN_TTM, ft.StockField.OPERATING_PROFIT_GROWTH_RATE,
    ft.StockField.OPERATING_PROFIT_TO_TOTAL_PROFIT, ft.StockField.OPERATING_PROFIT_TTM, ft.StockField.OPERATING_REVENUE_CASH_COVER,
    ft.StockField.PROFIT_BEFORE_TAX_GROWTH_RATE, ft.StockField.PROFIT_TO_SHAREHOLDERS_GROWTH_RATE, ft.StockField.PROPERTY_RATIO,
    ft.StockField.QUICK_RATIO, ft.StockField.RETURN_ON_EQUITY_RATE, ft.StockField.ROA_TTM, ft.StockField.ROE_GROWTH_RATE,
    ft.StockField.ROIC, ft.StockField.ROIC_GROWTH_RATE, ft.StockField.SHAREHOLDER_NET_PROFIT_TTM, ft.StockField.SUM_OF_BUSINESS,
    ft.StockField.SUM_OF_BUSINESS_GROWTH, ft.StockField.TOTAL_ASSETS_GROWTH_RATE, ft.StockField.TOTAL_ASSET_TURNOVER
]

# Combine all fields, ensuring no duplicates
ALL_FIELDS_TO_SCRAPE = list(set(FINANCIAL_FILTER_FIELDS + list(SIMPLE_FILTER_FIELDS)))

class FutuScraper:
    """Synchronously scrapes financial data using a multi-threaded executor."""
    def __init__(self, db_path: str = "data/futu_financials.duckdb"):
        self.host = os.getenv("FUTU_HOST", "127.0.0.1")
        self.port = 11111
        self.db_path = db_path
        self.quote_ctx: Optional[ft.OpenQuoteContext] = None
        self.db_api: Optional[DatabaseAPI] = None
        self.api_executor = FutuAPIExecutor(max_workers=1)

    def __enter__(self):
        """Context manager entry point. Connects to resources."""
        self._connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point. Closes connections."""
        self.close()

    def _connect(self):
        """Connects to the Futu API and the database."""
        if not self.quote_ctx:
            try:
                self.quote_ctx = ft.OpenQuoteContext(host=self.host, port=self.port)
                logger.info("Futu API connected successfully.")
            except Exception as e:
                logger.error(f"Failed to connect to Futu API: {e}", exc_info=True)
                raise
        if not self.db_api:
            try:
                self.db_api = get_database_api("duckdb", db_path=self.db_path)
                self.db_api.connect()
            except Exception as e:
                logger.error(f"An unexpected error occurred while connecting to the database: {e}", exc_info=True)
                raise

    def close(self):
        """Closes both Futu and database connections."""
        if self.quote_ctx:
            self.quote_ctx.close()
            self.quote_ctx = None
        if self.db_api:
            self.db_api.close()
            self.db_api = None

    def scrape_and_store_financials(self, market: str, quarter: str = "annual"):
        """Scrapes financial data and stores it."""
        self._connect()

        report_date = get_report_period_date(date.today(), quarter)
        report_date_str = report_date.strftime('%Y-%m-%d')
        
        scraped_profiles = self.scrape_financial_profile(market, quarter, report_date_str)
        if not scraped_profiles:
            logger.info("No financial data was scraped, nothing to store.")
            return
            
        table_name = f"financial_profile_{report_date.strftime('%Y_%m_%d')}"
        logger.info(f"Scraped {len(scraped_profiles)} records for period {report_date_str}, storing to table '{table_name}'...")

        primary_keys = ["ticker", "report_period", "period"]
        self.db_api.create_table_from_model(table_name, FinancialProfile, primary_keys)
        self.db_api.upsert_data_from_models(table_name, scraped_profiles, primary_keys)
        logger.info(f"Successfully stored/updated {len(scraped_profiles)} records in '{table_name}'.")
        self.close()

    def scrape_financial_profile(self, market: str, quarter: str, end_date: str) -> List[FinancialProfile]:
        """Concurrently scrapes financial profiles for all stocks in a given market."""
        try:
            ft_market = self._validate_market(market)
            logger.info(f"Using report period end date: {end_date} for quarter '{quarter}'")

            all_stocks_data = {}
            data_lock = Lock()
            currency = self._get_currency(ft_market)

            with ThreadPoolExecutor(max_workers=self.api_executor.max_workers) as executor:
                futures = {
                    executor.submit(
                        self._scrape_field_worker,
                        field,
                        ft_market,
                        quarter,
                        currency,
                        all_stocks_data,
                        data_lock
                    ): field for field in ALL_FIELDS_TO_SCRAPE
                }
                for future in futures:
                    future.result()

            return self._process_scraped_data(all_stocks_data, end_date, quarter)
        except Exception as e:
            logger.error(f"Failed to scrape financial profiles for market {market}: {e}", exc_info=True)
            return []

    def _scrape_field_worker(self, field, ft_market, quarter, currency, all_stocks_data, lock):
        """Worker function to scrape all pages for a single financial field."""
        logger.info(f"Starting scrape for field: {field}")
        begin_index = 0
        num_per_req = 200
        quarter_enum = self._get_quarter_enum(quarter)

        while True:
            filter_instance = self._create_filter(field, quarter_enum)

            ret, data = self.api_executor.execute(
                "get_stock_filter",
                self.quote_ctx.get_stock_filter,
                market=ft_market, filter_list=[filter_instance], begin=begin_index, num=num_per_req
            )

            if ret != ft.RET_OK:
                logger.error(f"Futu API error for field {field.name}: {data}")
                break
            if data is None:
                break

            is_last_page, stock_num, stock_list_chunk = data
            logger.debug(f"scrape field: {field}, total stocks: {stock_num}, is_last_page: {is_last_page}")
            with lock:
                for stock_data in stock_list_chunk:
                    stock_code = stock_data.stock_code
                    stock_code = stock_code.split('.')[1] if '.' in stock_code else stock_code
                    stock_vars = vars(stock_data)
                    value = None

                    if stock_code not in all_stocks_data:
                        all_stocks_data[stock_code] = {'ticker': stock_code}
                        all_stocks_data[stock_code]['name'] = stock_data.stock_name
                        all_stocks_data[stock_code]['currency'] = currency

                    # FinancialFilter returns a tuple key, SimpleFilter returns a string key
                    if field in SIMPLE_FILTER_FIELDS:
                        value = stock_vars.get(field.lower())
                    else:
                        value = stock_vars.get((field.lower(), quarter))
                    all_stocks_data[stock_code][field.lower()] = value

            if is_last_page:
                break
            begin_index += num_per_req
        logger.info(f"Finished scrape for field: {field}")
    
    def _validate_market(self, market: str) -> ft.Market:
        """Validates and returns the Futu API market enum."""
        market_map = {'HK': ft.Market.HK, 'US': ft.Market.US}
        ft_market = market_map.get(market.upper())
        if not ft_market:
            raise ValueError(f"Unsupported market: {market}. Please use 'HK' or 'US'.")
        return ft_market

    def _get_currency(self, market: ft.Market) -> str:
        # Determine currency from market
        if market == ft.Market.HK:
            return "HKD"
        elif market == ft.Market.US:
            return "USD"
        return "HKD" #default

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

    def scrape_stock_plate_mappings(self, market: Market = Market.HK):
        """Scrapes and stores stock-to-plate mappings synchronously."""
        self._connect()
        try:
            plate_list = self._get_plate_list(market)
            if not plate_list:
                logger.warning(f"No plates found for market: {market.value}")
                return

            all_mappings = []
            for plate in plate_list:
                stocks_df = self._get_plate_stock(plate['code'])
                if stocks_df is not None and not stocks_df.empty:
                    for _, stock in stocks_df.iterrows():
                        # The ticker in the plate data does not have a market prefix
                        ticker = stock['code']
                        ticker = ticker.split('.')[1] if '.' in ticker else ticker
                        all_mappings.append(StockPlateMapping(ticker=ticker, stock_name=stock['stock_name'], plate_code=plate['plate_id'], plate_name=plate['plate_name'], market=market.value))

            if all_mappings:
                table_name = "stock_plate_mappings"
                primary_keys = ["ticker", "plate_code"]
                self.db_api.create_table_from_model(table_name, StockPlateMapping, primary_keys)
                self.db_api.upsert_data_from_models(table_name, all_mappings, primary_keys)
                logger.info(f"Upserted {len(all_mappings)} stock-plate mappings.")
        except Exception as e:
            logger.error(f"Failed to scrape stock plate mappings: {e}", exc_info=True)

    def _get_plate_list(self, market: Market):
        """Fetches the list of all plates for a given market."""
        ft_market = self._validate_market(market.value)
        ret, data = self.api_executor.execute("get_plate_list", self.quote_ctx.get_plate_list, ft_market, ft.Plate.ALL)
        if ret == ft.RET_OK:
            return data.to_dict('records')
        logger.error(f"Failed to get plate list for {market.value}: {data}")
        return []

    def _get_plate_stock(self, plate_code: str) -> Optional[pd.DataFrame]:
        """Fetches stocks in a given plate using the API executor."""
        self._connect()
        logger.info(f"Fetching stocks for plate: {plate_code}")
        ret, data = self.api_executor.execute("get_plate_stock", self.quote_ctx.get_plate_stock, plate_code)
        
        if ret != ft.RET_OK:
            logger.error(f"API error for plate {plate_code}: {data}")
            return None
        return data

if __name__ == "__main__":
    logger.info("------------- ai hedge fund start ---------------")
    with FutuScraper(db_path="data/test.duckdb") as scraper:
        scraper.scrape_and_store_financials("HK", "annual")
        scraper.scrape_stock_plate_mappings(Market.HK)
    logger.info("------------- ai hedge fund finish --------------")