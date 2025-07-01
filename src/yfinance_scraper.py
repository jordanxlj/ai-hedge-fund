import argparse
import logging
import time
from typing import Optional, List
from dotenv import load_dotenv

from src.data.db import get_database_api, DatabaseAPI
from src.data.provider.yfinance_provider import YFinanceProvider
from src.utils.log_util import logger_setup as _init_logging
from src.data.models import Price
from tenacity import retry, stop_after_attempt, wait_fixed

_init_logging()
logger = logging.getLogger(__name__)

load_dotenv()

class YFinanceScraper:
    def __init__(self, db_api: DatabaseAPI):
        self.db = db_api
        self.provider = YFinanceProvider()

    def __enter__(self):
        self.db.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()

    def get_hk_stock_tickers(self) -> list[str]:
        try:
            query = "SELECT DISTINCT ticker FROM stock_plate_mappings"
            df = self.db.query_to_dataframe(query)
            if df.empty:
                logger.warning("No HK stock tickers found in 'stock_plate_mappings'.")
                return []
            
            tickers = []
            for _, row in df.iterrows():
                try:
                    ticker_num_str = row['ticker']
                    ticker_num_int = int(ticker_num_str)
                    yfinance_ticker = f"{ticker_num_int:04d}.HK"
                    tickers.append(yfinance_ticker)
                except (ValueError, IndexError) as e:
                    logger.warning(f"Could not parse ticker {row['ticker']}: {e}")

            logger.info(f"Found {len(tickers)} HK tickers.")
            return tickers
        except Exception as e:
            logger.error(f"Error fetching HK stock tickers: {e}")
            return []

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def fetch_price_data(self, ticker: str, start_date: str, end_date: str) -> Optional[List[Price]]:
        logger.debug(f"Fetching minute data for {ticker} from {start_date} to {end_date}")
        return self.provider.get_prices(ticker, start_date=start_date, end_date=end_date, freq='1m')

    def run(self, start_date: str, end_date: str):
        tickers = self.get_hk_stock_tickers()
        if not tickers:
            logger.error("Could not retrieve stock tickers. Aborting.")
            return
        tickers = ['AAPL']

        table_name = "hk_stock_minute_price"
        primary_keys = ["ticker", "time"]
        self.db.create_table_from_model(table_name, Price, primary_keys)

        total_tickers = len(tickers)
        for i, ticker in enumerate(tickers):
            try:
                logger.info(f"Processing {ticker} ({i + 1}/{total_tickers})...")
                
                price_objects = self.fetch_price_data(ticker, start_date, end_date)

                if not price_objects:
                    logger.warning(f"No data found for {ticker}.")
                    continue
                
                self.db.upsert_data_from_models(table_name, price_objects, primary_keys)
                logger.info(f"Successfully stored {len(price_objects)} records for {ticker}.")

            except Exception as e:
                logger.error(f"Failed to process {ticker}: {e}", exc_info=True)
            
            time.sleep(1)

        logger.info("YFinance minute data scraping finished.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape minute-level stock data from yfinance.")
    parser.add_argument("--start_date", required=True, help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end_date", required=True, help="End date in YYYY-MM-DD format.")
    parser.add_argument("--db_path", default="data/test.duckdb", help="Path to database.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    db_api = get_database_api("duckdb", db_path=args.db_path)
    
    with YFinanceScraper(db_api) as scraper:
        scraper.run(args.start_date, args.end_date)