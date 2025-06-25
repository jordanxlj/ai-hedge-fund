import futu as ft
import os
import time
import logging
from datetime import datetime, date
from typing import List, Optional, get_type_hints, get_origin, get_args

import duckdb
from pydantic import BaseModel
import pandas as pd
from collections import deque
from tenacity import retry, stop_after_attempt, wait_fixed

from src.data.models import FinancialProfile, StockPlateMapping, Market
from src.data.futu_utils import FutuDummyStockData, futu_data_to_financial_profile, get_report_period_date
from src.utils.log_util import logger_setup as _init_logging

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
        self.quote_ctx = None
        self.db_path = db_path
        self.req_freq = 3.1
        self.rate_limit_timestamps = deque(maxlen=10)

        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

    def _connect(self):
        if self.quote_ctx is None:
            try:
                self.quote_ctx = ft.OpenQuoteContext(host=self.host, port=self.port)
            except Exception as e:
                logger.error(f"Failed to connect to Futu: {e}")
                raise

    def _disconnect(self):
        if self.quote_ctx:
            self.quote_ctx.close()
            self.quote_ctx = None

    def _ensure_connection(self):
        """Ensures the Futu API connection is active."""
        if self.quote_ctx is None or self.quote_ctx.status() != ft.RET_OK:
            self._connect()

    def _get_pydantic_sql_type(self, field_type) -> str:
        """Maps Pydantic/Python types to DuckDB SQL types."""
        origin = get_origin(field_type)
        if origin is None:  # Simple type
            if field_type is str: return "VARCHAR"
            if field_type is float: return "DOUBLE"
            if field_type is int: return "BIGINT"
            if field_type is bool: return "BOOLEAN"
            if field_type is datetime: return "TIMESTAMP"
        
        # Handle Optional[T] types
        args = get_args(field_type)
        if len(args) == 2 and args[1] is type(None):
            return self._get_pydantic_sql_type(args[0])

        return "VARCHAR" # Default for complex types like list/dict

    def _create_table_from_model(self, conn: duckdb.DuckDBPyConnection, model: BaseModel, table_name: str, primary_keys: List[str]):
        """Creates a DuckDB table based on a Pydantic model."""
        fields = get_type_hints(model)
        
        columns_sql = []
        for name, field_type in fields.items():
            if name == 'model_config': continue
            sql_type = self._get_pydantic_sql_type(field_type)
            columns_sql.append(f'"{name}" {sql_type}')
        
        pk_sql = f"PRIMARY KEY ({', '.join(primary_keys)})"
        columns_sql.append(pk_sql)
        
        create_table_sql = f"CREATE TABLE IF NOT EXISTS \"{table_name}\" ({', '.join(columns_sql)})"
        
        conn.execute(create_table_sql)
        logger.info(f"Table '{table_name}' is ready in DuckDB.")

    def scrape_and_store(self, market: str, quarter: str = "annual"):
        """
        Scrapes financial profile for a specific report period and stores them 
        in a dedicated table named after that period in a DuckDB database.
        """
        # Determine the target report period date
        report_date = get_report_period_date(date.today(), quarter)
        report_date_str = report_date.strftime('%Y-%m-%d')
        
        scraped_profile = self.scrape_financial_profile(market, quarter, report_date_str)
        if not scraped_profile:
            logger.info("No profile were scraped. Nothing to store.")
            return
            
        table_name = f"financial_profile_{report_date.strftime('%Y_%m_%d')}"
        logger.info(f"Scraped {len(scraped_profile)} records for period {report_date_str}. Storing to DuckDB table '{table_name}' at {self.db_path}...")

        with duckdb.connect(self.db_path) as conn:
            primary_keys = ["ticker", "report_period", "period"]
            self._create_table_from_model(conn, FinancialProfile, table_name, primary_keys)
            
            # Use pandas DataFrame for easier insertion
            profile_dicts = [m.model_dump(exclude_none=True) for m in scraped_profile]
            if not profile_dicts:
                logger.warning("After processing, no metric dictionaries to insert.")
                return

            df = pd.DataFrame(profile_dicts)
            
            # Get the definitive list of model fields in order
            ordered_cols = list(FinancialProfile.model_fields.keys())

            # Ensure all model fields exist as columns in the dataframe, and in the correct order
            for col in ordered_cols:
                if col not in df.columns:
                    df[col] = None # DuckDB handles None as NULL
            
            # Reorder df columns to match table schema precisely and drop any extra columns
            df = df[ordered_cols]
            
            # Upsert into the main table
            conn.register('profile_df', df)
            
            update_set_sql = ", ".join([f'"{col}" = excluded."{col}"' for col in df.columns if col not in primary_keys])
            
            upsert_sql = f"""
            INSERT INTO "{table_name}"
            SELECT * FROM profile_df
            ON CONFLICT ({', '.join(primary_keys)}) DO UPDATE SET {update_set_sql};
            """
            
            conn.execute(upsert_sql)

        logger.info(f"Successfully stored/updated {len(df)} records in DuckDB table '{table_name}'.")

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
                    time.sleep(3.1) # Adjusted sleep time

                    if ret != ft.RET_OK:
                        if "不支持该过滤字段" in str(data):
                             logger.warning(f"Field {field} is not supported by get_stock_filter. Skipping.")
                             break 
                        logger.error(f"Futu API error for field {field}: {data}")
                        break

                    page_info, _, stock_list_chunk = data
                    last_page = page_info

                    if not stock_list_chunk:
                        break

                    for stock_data in stock_list_chunk:
                        stock_code = stock_data.ticker
                        if stock_code not in all_stocks_data:
                            all_stocks_data[stock_code] = {'stock_name': stock_data.stock_name}
                        
                        stock_vars = vars(stock_data)
                        attr_name = field.lower()
                        value = None

                        # FinancialFilter returns a tuple key, SimpleFilter returns a string key
                        if field in SIMPLE_FILTER_FIELDS:
                            value = stock_vars.get(attr_name)
                        else:
                            tuple_key = (attr_name, quarter)
                            value = stock_vars.get(tuple_key)
                        
                        if value is not None:
                            all_stocks_data[stock_code][attr_name] = value

                    begin_index += len(stock_list_chunk)
            
            final_profile = []
            for stock_code, data_dict in all_stocks_data.items():
                dummy_stock_obj = FutuDummyStockData(data_dict)
                parts = stock_code.split('.')
                if len(parts) == 2:
                    internal_ticker = f"{parts[1]}.{parts[0]}"
                    profile = futu_data_to_financial_profile(dummy_stock_obj, internal_ticker, data_dict['stock_name'], end_date, quarter, ft_market)
                    final_profile.extend(profile)

            return final_profile

        except Exception as e:
            logger.error(f"Failed to scrape financial profile for market {market}: {e}")
            import traceback
            logger.error(f"Detailed error: {traceback.format_exc()}")
            return []
        finally:
            self._disconnect()

    def scrape_stock_plate_mappings(self, market: Market = Market.HK):
        """
        Scrapes stock-plate mappings for a given market and stores them in DuckDB.
        """
        self._ensure_connection()

        # Convert our Market enum to the futu Market enum
        ft_market = ft.Market.HK if market == Market.HK else ft.Market.US

        table_name = "stock_plate_mappings"
        try:
            logger.info(f"Starting to scrape stock plate mappings for market: {market}")
            with duckdb.connect(self.db_path) as con:
                primary_keys = ["ticker", "plate_code"]
                self._create_table_from_model(con, StockPlateMapping, table_name, primary_keys)

                # 1. Get all plates for the market
                ret, plate_list_df = self.quote_ctx.get_plate_list(market=ft_market, plate_class=ft.Plate.ALL)
                if ret != ft.RET_OK:
                    logger.error(f"Failed to get plate list for market {market}: {plate_list_df}")
                    return

                if plate_list_df.empty:
                    logger.warning(f"No plates found for market {market}. Exiting.")
                    return

                plate_list = plate_list_df.to_dict('records')
                logger.info(f"Successfully retrieved {len(plate_list)} plates for market {market}.")

                all_mappings = []
                for i, plate_info in enumerate(plate_list):
                    plate_code = plate_info['code']
                    plate_name = plate_info['plate_name']
                    
                    logger.debug(f"Processing plate {i+1}/{len(plate_list)}: {plate_name} ({plate_code})")
                    
                    # 2. Get all stocks in the plate
                    ret, stock_list_df = self._get_plate_stock_with_retry(plate_code)

                    if ret != ft.RET_OK:
                        logger.warning(f"Could not get stocks for plate {plate_name} ({plate_code}): {stock_list_df}")
                        continue
                    
                    if stock_list_df.empty:
                        logger.debug(f"Plate {plate_name} ({plate_code}) has no stocks.")
                        continue
                        
                    logger.debug(f"Found {len(stock_list_df)} stocks in plate {plate_name}.")

                    for stock_info in stock_list_df.to_dict('records'):
                        ticker = stock_info['code']
                        # Clean up ticker: remove market prefix/suffix like .HK or .US
                        if '.' in ticker:
                            ticker = ticker.split('.')[1]  # Take the second part (stock symbol)

                        mapping = StockPlateMapping(
                            ticker=ticker,
                            stock_name=stock_info['stock_name'],
                            plate_code=plate_code,
                            plate_name=plate_name,
                            market=market
                        )
                        all_mappings.append(mapping)
                
                if all_mappings:
                    self._upsert_data(con, table_name, all_mappings, primary_keys)
                    logger.info(f"Successfully upserted {len(all_mappings)} stock-plate mappings for market {market} into '{table_name}'.")
                else:
                    logger.info(f"No new stock-plate mappings to insert for market {market}.")

        except Exception as e:
            logger.error(f"Failed to scrape stock plate mappings for market {market}: {e}")
            import traceback
            logger.error(f"Detailed error: {traceback.format_exc()}")
        finally:
            self._disconnect()

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(31))
    def _get_plate_stock_with_retry(self, plate_code: str):
        ret, stock_list_df = self.quote_ctx.get_plate_stock(plate_code)
        return ret, stock_list_df

    def _upsert_data(self, conn: duckdb.DuckDBPyConnection, table_name: str, data: List[BaseModel], primary_keys: List[str]):
        """
        Generic method to upsert a list of Pydantic models into a DuckDB table.
        """
        if not data:
            logger.info("No data provided to upsert.")
            return

        model = data[0].__class__
        model_fields = list(model.model_fields.keys())

        # Convert list of Pydantic objects to a list of dictionaries
        data_dicts = [m.model_dump() for m in data]
        
        # Create a DataFrame from the dictionaries, ensuring column order
        df = pd.DataFrame(data_dicts, columns=model_fields)

        # Upsert into the main table
        # Use a temporary table for the upsert operation to handle it atomically
        temp_table_name = f"temp_{table_name}_{int(time.time())}"
        conn.register(temp_table_name, df)
        
        # Ensure column names are quoted to handle special characters or keywords
        quoted_cols = [f'"{col}"' for col in model_fields]
        quoted_pk = [f'"{pk}"' for pk in primary_keys]
        
        # Prepare the SET clause for the DO UPDATE part
        update_set_sql = ", ".join([f'{col} = excluded.{col}' for col in quoted_cols if col not in quoted_pk])
        
        # If there are no columns to update (all are primary keys), the upsert is just an INSERT...ON CONFLICT DO NOTHING
        if not update_set_sql:
            on_conflict_sql = "DO NOTHING"
        else:
            on_conflict_sql = f"DO UPDATE SET {update_set_sql}"

        upsert_sql = f"""
        INSERT INTO "{table_name}" ({', '.join(quoted_cols)})
        SELECT {', '.join(quoted_cols)} FROM {temp_table_name}
        ON CONFLICT ({', '.join(quoted_pk)}) {on_conflict_sql};
        """
        
        try:
            conn.execute(upsert_sql)
            logger.info(f"Successfully upserted {len(df)} records into '{table_name}'.")
        finally:
            conn.unregister(temp_table_name)

    def close(self):
        self._disconnect()