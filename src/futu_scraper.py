import futu as ft
import os
import time
import logging
from datetime import datetime, date
from typing import List, Optional
from collections import deque

import pandas as pd
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_fixed

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

class FutuScraper:
    """
    A scraper for fetching comprehensive financial profile from Futu OpenD.
    """
    def __init__(self, db_path: str = "data/futu_financials.duckdb"):
        self.host = os.getenv("FUTU_HOST", "127.0.0.1")
        self.port = 11111
        self.quote_ctx: Optional[ft.OpenQuoteContext] = None
        self.db_api: DatabaseAPI = get_database_api("duckdb", db_path=db_path)
        self.req_freq = 3.1
        self.rate_limit_timestamps = deque(maxlen=10)

    def _connect(self):
        """Connects to both Futu and the database."""
        if self.quote_ctx is None:
            try:
                self.quote_ctx = ft.OpenQuoteContext(host=self.host, port=self.port)
            except Exception as e:
                logger.error(f"Failed to connect to Futu: {e}")
                raise
        self.db_api.connect()

    def _disconnect(self):
        """Disconnects from Futu and the database."""
        if self.quote_ctx:
            self.quote_ctx.close()
            self.quote_ctx = None
        self.db_api.close()

    def _ensure_connection(self):
        """Ensures the Futu API connection is active."""
        if self.quote_ctx is None or self.quote_ctx.status() != ft.RET_OK:
            self._connect()

    def scrape_and_store(self, market: str, quarter: str = "annual"):
        """
        Scrapes financial profile for a specific report period and stores them
        in a dedicated table named after that period in a DB.
        """
        # Determine the target report period date
        report_date = get_report_period_date(date.today(), quarter)
        report_date_str = report_date.strftime('%Y-%m-%d')

        scraped_profile = self.scrape_financial_profile(market, quarter, report_date_str)
        if not scraped_profile:
            logger.info("No profile were scraped. Nothing to store.")
            return

        table_name = f"financial_profile_{report_date.strftime('%Y_%m_%d')}"
        logger.info(f"Scraped {len(scraped_profile)} records for period {report_date_str}. Storing to DB table '{table_name}'...")

        primary_keys = ["ticker", "report_period", "period"]
        self.db_api.create_table_from_model(table_name, FinancialProfile, primary_keys)

        if scraped_profile:
            self.db_api.upsert_data_from_models(table_name, scraped_profile, primary_keys)
            logger.info(f"Successfully stored/updated {len(scraped_profile)} records in table '{table_name}'.")
        else:
            logger.info("No data to store.")

    def scrape_financial_profile(self, market: str, quarter: str = "annual", end_date: Optional[str] = None) -> List[FinancialProfile]:
        """
        Scrapes financial profile for all stocks in a given market by iterating through each financial field one by one.
        NOTE: This process is very slow due to API limitations and rate limiting.
        """
        self._connect()
        try:
            if market.upper() == 'HK':
                ft_market = ft.Market.HK
            elif market.upper() == 'US':
                ft_market = ft.Market.US
            else:
                raise ValueError("Unsupported market. Please use 'HK' or 'US'.")

            if end_date is None:
                today = date.today()
                current_year = today.year

                if quarter == 'annual':
                    end_date = date(current_year - 1, 12, 31).strftime('%Y-%m-%d')
                elif quarter == 'q1':
                    year = current_year if today.month >= 4 else current_year - 1
                    end_date = date(year, 3, 31).strftime('%Y-%m-%d')
                elif quarter == 'interim':
                    year = current_year if today.month >= 7 else current_year - 1
                    end_date = date(year, 6, 30).strftime('%Y-%m-%d')
                elif quarter == 'q3':
                    year = current_year if today.month >= 10 else current_year - 1
                    end_date = date(year, 9, 30).strftime('%Y-%m-%d')
                else: # Fallback to today if quarter is not recognized
                    end_date = today.strftime('%Y-%m-%d')

            logger.info(f"Using report period end date: {end_date} for quarter '{quarter}'")

            all_stocks_data = {}

            quarter_map = {
                "annual": ft.FinancialQuarter.ANNUAL,
                "q1": ft.FinancialQuarter.FIRST_QUARTER,
                "interim": ft.FinancialQuarter.INTERIM,
                "q3": ft.FinancialQuarter.THIRD_QUARTER,
            }
            quarter_enum = quarter_map.get(quarter.lower(), ft.FinancialQuarter.ANNUAL)

            for field in ALL_FIELDS_TO_SCRAPE:
                logger.info(f"Scraping data for field: {field}")
                begin_index = 0
                num_per_req = 200
                last_page = False

                while not last_page:
                    # Choose filter type based on the field
                    if field in SIMPLE_FILTER_FIELDS:
                        filter_instance = ft.SimpleFilter()
                    else:
                        filter_instance = ft.FinancialFilter()
                        filter_instance.quarter = quarter_enum

                    filter_instance.stock_field = field
                    filter_instance.filter_min = -1e15
                    filter_instance.filter_max = 1e15
                    filter_instance.is_no_filter = False

                    ret, data = self.quote_ctx.get_stock_filter(
                        market=ft_market,
                        filter_list=[filter_instance],
                        begin=begin_index,
                        num=num_per_req
                    )
                    print(f"data: {data}")
                    time.sleep(self.req_freq)

                    if ret != ft.RET_OK:
                        if "不支持该过滤字段" in str(data):
                            logger.warning(f"Field {field} is not supported. Skipping.")
                            break
                        logger.error(f"Error fetching data for field {field}: {data}")
                        break

                    if data is None or data.empty:
                        last_page = True
                        continue

                    for index, row in data.iterrows():
                        stock_code = row['stock_code']
                        if stock_code not in all_stocks_data:
                            all_stocks_data[stock_code] = {'ticker': stock_code}

                        field_value = row[field.lower()]
                        all_stocks_data[stock_code][field.lower()] = field_value

                    if data.iloc[0]['is_last_page']:
                        last_page = True
                    else:
                        begin_index += num_per_req

            logger.info(f"Finished scraping. Total unique stocks found: {len(all_stocks_data)}")

            # Convert dict to FinancialProfile objects
            profile_list = futu_data_to_financial_profile(all_stocks_data, end_date, quarter)

            return profile_list

        finally:
            self._disconnect()

    def scrape_stock_plate_mappings(self, market: Market = Market.HK):
        """
        Scrapes stock-plate mappings for a given market and stores them in the database.
        """
        self._ensure_connection()
        plate_list = self._get_plate_list(market)
        if not plate_list:
            logger.warning(f"No plates found for market: {market.value}")
            return

        all_mappings = []
        for plate in plate_list:
            stocks_df = self._get_plate_stock_with_retry(plate['code'])
            if stocks_df is not None and not stocks_df.empty:
                for stock in stocks_df.to_dict('records'):
                    ticker = stock['code']
                    # Clean up ticker to remove market prefix
                    if '.' in ticker:
                        ticker = ticker.split('.')[1]

                    all_mappings.append(StockPlateMapping(
                        ticker=ticker,
                        stock_name=stock['stock_name'],
                        plate_code=plate['code'],
                        plate_name=plate['plate_name'],
                        market=market
                    ))
        
        if all_mappings:
            table_name = "stock_plate_mappings"
            primary_keys = ["ticker", "plate_code"]
            self.db_api.create_table_from_model(table_name, StockPlateMapping, primary_keys)
            self.db_api.upsert_data_from_models(table_name, all_mappings, primary_keys)
            logger.info(f"Upserted {len(all_mappings)} stock-plate mappings.")

    def _get_plate_list(self, market: Market = Market.HK):
        """Fetches the list of all plates for a given market."""
        self._ensure_connection()

        if market == Market.HK:
            futu_market = ft.Market.HK
        elif market == Market.US:
            futu_market = ft.Market.US
        else:
            raise ValueError("Unsupported market")

        ret, data = self.quote_ctx.get_plate_list(futu_market, ft.Plate.ALL)
        if ret == ft.RET_OK:
            return data.to_dict('records')
        else:
            logger.error(f"Failed to get plate list: {data}")
            return []

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(31))
    def _get_plate_stock_with_retry(self, plate_code: str) -> Optional[pd.DataFrame]:
        """
        A retry-enabled method to fetch stocks in a given plate.
        """
        self._ensure_connection()
        logger.info(f"Fetching stocks for plate: {plate_code}")
        ret, data = self.quote_ctx.get_plate_stock(plate_code)

        # Rate limit handling
        now = time.time()
        if len(self.rate_limit_timestamps) == 10:
            if now - self.rate_limit_timestamps[0] < 30:
                sleep_time = 30 - (now - self.rate_limit_timestamps[0])
                logger.warning(f"Approaching rate limit, sleeping for {sleep_time:.1f}s")
                time.sleep(sleep_time)
        self.rate_limit_timestamps.append(now)

        if ret == ft.RET_OK:
            return data
        else:
            logger.error(f"Failed to get stocks for plate {plate_code} after multiple retries. Error: {data}")
            return None

    def close(self):
        """Closes both Futu and database connections."""
        self._disconnect()

# Example usage:
if __name__ == '__main__':
    scraper = FutuScraper()
    # scraper.scrape_and_store(market='HK', quarter='annual')
    # scraper.scrape_and_store(market='HK', quarter='interim')
    # scraper.scrape_and_store(market='HK', quarter='q1')
    scraper.scrape_stock_plate_mappings(market=Market.HK)
    scraper.close()