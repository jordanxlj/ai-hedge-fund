import futu as ft
import os
import time
import logging
from datetime import datetime
from typing import List, Optional, get_type_hints, get_origin, get_args

import duckdb
from pydantic import BaseModel
import pandas as pd

from src.data.models import FinancialMetrics
from src.data.futu_utils import FutuDummyStockData, convert_to_financial_metrics

logger = logging.getLogger(__name__)


class FutuScraper:
    """
    A scraper for fetching comprehensive financial metrics from Futu OpenD.
    """
    def __init__(self, db_path: str = "data/futu_financials.duckdb"):
        self.host = os.getenv("FUTU_HOST", "127.0.0.1")
        self.port = 11111
        self.quote_ctx = None
        self.db_path = db_path
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

    def scrape_and_store(self, market: str, quarter: str = "annual", end_date: Optional[str] = None):
        """
        Scrapes financial metrics and stores them in a DuckDB database.
        """
        scraped_metrics = self.scrape_financial_metrics(market, quarter, end_date)
        if not scraped_metrics:
            logger.info("No metrics were scraped. Nothing to store.")
            return
            
        logger.info(f"Scraped {len(scraped_metrics)} records. Storing to DuckDB at {self.db_path}...")
        
        with duckdb.connect(self.db_path) as conn:
            table_name = "financial_metrics"
            primary_keys = ["ticker", "report_period", "period"]
            self._create_table_from_model(conn, FinancialMetrics, table_name, primary_keys)
            
            # Use pandas DataFrame for easier insertion
            metrics_dicts = [m.model_dump(exclude_none=True) for m in scraped_metrics]
            if not metrics_dicts:
                logger.warning("After processing, no metric dictionaries to insert.")
                return

            df = pd.DataFrame(metrics_dicts)
            
            # Get the definitive list of model fields in order
            ordered_cols = list(FinancialMetrics.model_fields.keys())

            # Ensure all model fields exist as columns in the dataframe, and in the correct order
            for col in ordered_cols:
                if col not in df.columns:
                    df[col] = None # DuckDB handles None as NULL
            
            # Reorder df columns to match table schema precisely and drop any extra columns
            df = df[ordered_cols]
            
            # Upsert into the main table
            conn.register('metrics_df', df)
            
            update_set_sql = ", ".join([f'"{col}" = excluded."{col}"' for col in df.columns if col not in primary_keys])
            
            upsert_sql = f"""
            INSERT INTO "{table_name}"
            SELECT * FROM metrics_df
            ON CONFLICT ({', '.join(primary_keys)}) DO UPDATE SET {update_set_sql};
            """
            
            conn.execute(upsert_sql)

        logger.info(f"Successfully stored/updated {len(df)} records in DuckDB.")

    def scrape_financial_metrics(self, market: str, quarter: str = "annual", end_date: Optional[str] = None) -> List[FinancialMetrics]:
        """
        Scrapes financial metrics for all stocks in a given market by iterating through each financial field one by one.
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
                end_date = datetime.now().strftime('%Y-%m-%d')

            financial_fields_to_scrape = [
                # A selection of fields for demonstration
                #ft.StockField.MARKET_VAL, ft.StockField.PE_TTM, ft.StockField.PB_RATE,
                ft.StockField.RETURN_ON_EQUITY_RATE, ft.StockField.NET_PROFIT, ft.StockField.SUM_OF_BUSINESS
            ]

            all_stocks_data = {}
            
            quarter_map = {
                "annual": ft.FinancialQuarter.ANNUAL,
                "q1": ft.FinancialQuarter.FIRST_QUARTER,
                "q2": ft.FinancialQuarter.INTERIM,
                "q3": ft.FinancialQuarter.THIRD_QUARTER,
            }
            quarter_enum = quarter_map.get(quarter.lower(), ft.FinancialQuarter.ANNUAL)

            for field in financial_fields_to_scrape:
                logger.info(f"Scraping data for field: {field}")
                begin_index = 0
                num_per_req = 200
                last_page = False

                while not last_page:
                    financial_filter = ft.FinancialFilter()
                    financial_filter.stock_field = field
                    financial_filter.filter_min = -1e15
                    financial_filter.filter_max = 1e15
                    financial_filter.is_no_filter = False
                    financial_filter.quarter = quarter_enum

                    ret, data = self.quote_ctx.get_stock_filter(
                        market=ft_market,
                        filter_list=[financial_filter],
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
                        stock_code = stock_data.stock_code
                        if stock_code not in all_stocks_data:
                            all_stocks_data[stock_code] = {'stock_code': stock_code, 'stock_name': stock_data.stock_name}
                        
                        stock_vars = vars(stock_data)
                        tuple_key = (field.lower(), quarter)
                        if tuple_key in stock_vars:
                            value = stock_vars[tuple_key]
                            attr_name = field.lower()
                            all_stocks_data[stock_code][attr_name] = value

                    begin_index += len(stock_list_chunk)
            
            final_metrics = []
            for stock_code, data_dict in all_stocks_data.items():
                dummy_stock_obj = FutuDummyStockData(data_dict)
                parts = stock_code.split('.')
                if len(parts) == 2:
                    internal_ticker = f"{parts[1]}.{parts[0]}"
                    metrics = convert_to_financial_metrics(dummy_stock_obj, internal_ticker, end_date, quarter, ft_market)
                    final_metrics.extend(metrics)

            return final_metrics

        except Exception as e:
            logger.error(f"Failed to scrape financial metrics for market {market}: {e}")
            import traceback
            logger.error(f"Detailed error: {traceback.format_exc()}")
            return []
        finally:
            self._disconnect() 