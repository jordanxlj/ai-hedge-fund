import argparse
import logging
import time
from typing import Optional, List, Tuple
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

    def get_hk_stock_tickers(self) -> List[Tuple[str, str]]:
        """
        Retrieves HK stock tickers from DB and formats them for yfinance.
        Strips leading zeros for the query ticker but keeps the original for DB consistency.
        
        Returns:
            A list of tuples: [(original_ticker, yfinance_query_ticker), ...]
            e.g., [('00700', '0700.HK')]
        """
        try:
            query = "SELECT DISTINCT ticker FROM stock_plate_mappings"
            df = self.db.query_to_dataframe(query)
            if df.empty:
                logger.warning("No HK stock tickers found in 'stock_plate_mappings'.")
                return []
            
            ticker_map = []
            for _, row in df.iterrows():
                original_ticker = row['ticker']
                try:
                    # '00700' -> '0700'
                    yfinance_ticker_base = original_ticker[1:] if original_ticker.startswith('0') else original_ticker
                    yfinance_ticker = f"{yfinance_ticker_base}.HK"
                    ticker_map.append((original_ticker, yfinance_ticker))
                except (ValueError, IndexError) as e:
                    logger.warning(f"Could not parse ticker {original_ticker}: {e}")

            logger.info(f"Found and formatted {len(ticker_map)} HK tickers.")
            return ticker_map
        except Exception as e:
            logger.error(f"Error fetching HK stock tickers: {e}")
            return []

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def fetch_price_data(self, tickers: List[str], start_date: str, end_date: str) -> Optional[List[Price]]:
        logger.debug(f"Fetching minute data for {tickers} from {start_date} to {end_date}")
        return self.provider.get_prices(tickers, start_date=start_date, end_date=end_date, freq='1m')

    def run(self, start_date: str, end_date: str):
        ticker_map = self.get_hk_stock_tickers()
        if not ticker_map:
            logger.error("Could not retrieve tickers. Aborting.")
            return

        table_name = "hk_stock_minute_price"
        primary_keys = ["ticker", "time"]
        self.db.create_table_from_model(table_name, Price, primary_keys)

        original_tickers = [item[0] for item in ticker_map]
        query_tickers = [item[1] for item in ticker_map]

        try:
            logger.info(f"Processing {len(query_tickers)} tickers...")

            price_objects = self.fetch_price_data(query_tickers, start_date, end_date)

            if not price_objects:
                logger.warning(f"No data found for any tickers.")
                return

            # Create a mapping from query_ticker (e.g., '0700.HK') back to original_ticker (e.g., '00700')
            query_to_original_map = {qt: ot for ot, qt in ticker_map}

            # Set the ticker to the original DB format before saving
            for p in price_objects:
                if p.ticker in query_to_original_map:
                    p.ticker = query_to_original_map[p.ticker]
                else:
                    logger.warning(f"Could not find original ticker for {p.ticker}. Using it as is.")


            self.db.upsert_data_from_models(table_name, price_objects, primary_keys)
            logger.info(f"Successfully stored {len(price_objects)} records for {len(original_tickers)} tickers.")

        except Exception as e:
            logger.error(f"Failed to process tickers: {e}", exc_info=True)

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