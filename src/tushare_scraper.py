import argparse
import logging
import time

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed
from typing import Optional
from dotenv import load_dotenv

from src.data.db import get_database_api, DatabaseAPI
from src.data.provider.tushare_provider import TushareProvider
from src.utils.log_util import logger_setup as _init_logging
from src.data.models import Price

_init_logging()
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

class TushareScraper:
    def __init__(self, db_api: DatabaseAPI):
        self.db = db_api
        self.provider = TushareProvider()

    def __enter__(self):
        """Context manager entry point. Connects to the database."""
        self.db.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point. Closes the database connection."""
        self.db.close()

    def get_hk_stock_tickers(self) -> list[str]:
        """
        Retrieves a list of unique Hong Kong stock tickers from the stock_plate_mappings table.
        Tushare requires tickers in the format 'XXXXX.HK'.
        """
        try:
            # The tickers in stock_plate_mappings are like 'HK.00700'
            query = """
            SELECT DISTINCT ticker 
            FROM stock_plate_mappings 
            """
            df = self.db.query_to_dataframe(query)
            if df.empty:
                logger.warning("No Hong Kong stock tickers found in 'stock_plate_mappings' table.")
                return []
            
            # Convert 'HK.00700' to '00700.HK' for Tushare
            tickers = [f"{row['ticker']}" for _, row in df.iterrows()]
            logger.info(f"Found {len(tickers)} Hong Kong stock tickers.")
            return tickers
        except Exception as e:
            logger.error(f"Error fetching HK stock tickers: {e}")
            return []

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def fetch_minute_data_for_ticker(self, ts_code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """
        Fetches minute-level data for a single stock using the Tushare provider.
        """
        logger.debug(f"Fetching minute data for {ts_code} from {start_date} to {end_date}")
        df = self.provider.get_stock_minute(ts_code, start_date=start_date, end_date=end_date)
        return df

    def run(self, start_date: str, end_date: str):
        """
        Fetches minute-level price data for all HK stocks for a given date range and stores it in the database.
        """
        tickers = self.get_hk_stock_tickers()
        if not tickers:
            logger.error("Could not retrieve stock tickers. Aborting.")
            return

        table_name = "hk_stock_minute_price"
        primary_keys = ["ticker", "time"]
        # Ensure table exists before processing
        self.db.create_table_from_model(table_name, Price, primary_keys)

        total_tickers = len(tickers)
        for i, ticker in enumerate(tickers):
            try:
                logger.info(f"Processing {ticker} ({i + 1}/{total_tickers})...")
                
                minute_df = self.fetch_minute_data_for_ticker(ticker, start_date, end_date)

                if minute_df is None or minute_df.empty:
                    logger.warning(f"No minute data found for {ticker} in the given date range.")
                    continue
                
                # Convert DataFrame to a list of Price models
                price_objects = [
                    Price(
                        ticker=row.ts_code,
                        time=row.trade_time,
                        open=row.open,
                        close=row.close,
                        high=row.high,
                        low=row.low,
                        volume=int(row.vol)
                    )
                    for row in minute_df.itertuples()
                ]
                
                self.db.upsert_data_from_models(table_name, price_objects, primary_keys)
                logger.info(f"Successfully stored {len(price_objects)} records for {ticker}.")

            except Exception as e:
                logger.error(f"Failed to process {ticker}: {e}")
            
            time.sleep(0.2) # Respect Tushare API rate limits

        logger.info("Tushare minute data scraping finished.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape minute-level stock data from Tushare.")
    parser.add_argument("--start_date", type=str, required=True, help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end_date", type=str, required=True, help="End date in YYYY-MM-DD format.")
    parser.add_argument("--db_path", type=str, default="data/futu_financials.duckdb", help="Path to the database file.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    db_api = get_database_api("duckdb", db_path=args.db_path)
    
    with TushareScraper(db_api) as scraper:
        scraper.run(args.start_date, args.end_date) 