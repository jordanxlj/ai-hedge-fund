import argparse
import logging
import time
from typing import Optional, List, Tuple
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime

from src.data.db import get_database_api, DatabaseAPI
from src.data.provider.yfinance_provider import YFinanceProvider
from src.utils.log_util import logger_setup as _init_logging
from src.data.models import Price, FinancialProfile
from tenacity import retry, stop_after_attempt, wait_fixed

_init_logging()
logger = logging.getLogger(__name__)

load_dotenv()

class YFinanceScraper:
    def __init__(self, db_api: DatabaseAPI, provider: YFinanceProvider):
        self.db_api = db_api
        self.provider = provider
        if not self.provider.is_available():
            raise ConnectionError("YFinance provider is not available.")
        
        # Ensure tables are created
        self.db_api.create_table_from_model("hk_stock_minute_price", Price, ["ticker", "time"])
        self.db_api.create_table_from_model("financial_profile", FinancialProfile, ["ticker", "report_period", "period"])

    def __enter__(self):
        self.db_api.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db_api.close()

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
            df = self.db_api.query_to_dataframe(query)
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
            logger.error(f"Error fetching HK stock tickers: {e}", exc_info=True)
            return []

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def _scrape_prices(self, tickers: List[str], start_date: str, end_date: str, freq: str = '1m'):
        logger.debug(f"Fetching minute data for {tickers} from {start_date} to {end_date}")
        return self.provider.get_prices(tickers, start_date, end_date, freq)

    def _get_ticker_name_map(self) -> dict:
        try:
            query = "SELECT ticker, stock_name FROM stock_plate_mappings"
            df = self.db_api.query_to_dataframe(query)
            return dict(zip(df['ticker'], df['stock_name']))
        except Exception as e:
            logger.error(f"Error fetching ticker-name map: {e}", exc_info=True)
            return {}

    def scrape_financial_profiles(self, tickers: List[str], end_date: str, period: str = "annual", limit: int = 10):
        """
        Scrapes financial profiles for a list of tickers in batch and stores them in the database.
        """
        logger.info(f"Starting batch scrape for {len(tickers)} financial profiles. Period: {period}, Limit: {limit}")
        
        all_profiles = []

        for ticker in tickers:
            try:
                profiles = self.provider.get_financial_profile(ticker, end_date, period, limit)
                if profiles:
                    all_profiles.extend(profiles)
                    logger.debug(f"Successfully fetched financial profile for {ticker}.")
                else:
                    logger.warning(f"No profiles returned for {ticker}.")
            except Exception as exc:
                logger.error(f"Ticker {ticker} generated an exception: {exc}", exc_info=True)
        return all_profiles

    def run(self, scrape_type: str, start_date: str, end_date: str, batch_size: int = 50, **kwargs):
        logger.info("Starting price scraping run...")
        all_tickers = self.get_hk_stock_tickers()
        if not all_tickers:
            logger.warning("No tickers found to scrape. Exiting.")
            return

        ticker_name_map = self._get_ticker_name_map()

        for i in range(0, len(all_tickers), batch_size):
            batch = all_tickers[i:i + batch_size]
            batch_tickers = [item[0] for item in batch]
            logger.info(f"Processing batch {i//batch_size + 1}: {len(batch_tickers)} tickers")

            if scrape_type == 'price':
                price_objects = self._scrape_prices(batch_tickers, start_date, end_date)
                
                if price_objects:
                    # Map ticker names back if needed
                    for p in price_objects:
                        p.name = ticker_name_map.get(p.ticker.split('.')[0], p.ticker)
                    
                    self.db_api.upsert_data_from_models("hk_stock_minute_price", price_objects, ["ticker", "time"])
                    logger.info(f"Successfully stored {len(price_objects)} records for batch {i//batch_size + 1}.")
                else:
                    logger.warning(f"No price data returned for batch {i//batch_size + 1}.")
                logger.info("Price scraping run completed.")
            elif scrape_type == 'financials':
                batch_tickers = ['1810.HK', '0700.HK']
                all_profiles = self.scrape_financial_profiles(batch_tickers, end_date, kwargs['period'], kwargs['limit'])
                if all_profiles:
                    # Map ticker names back if needed
                    for p in all_profiles:
                        p.name = ticker_name_map.get(p.ticker.split('.')[0], p.ticker)

                    self.db_api.upsert_data_from_models("financial_profile", all_profiles, ["ticker", "report_period", "period"])
                    logger.info(f"Successfully stored {len(all_profiles)} records for batch {i//batch_size + 1}.")
                else:
                    logger.warning(f"No financial profile data returned for batch {i//batch_size + 1}.")
            else:
                logger.error(f"Invalid scrape_type: '{scrape_type}'. Choose 'price' or 'financials'.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="YFinance Scraper for stock data.")
    parser.add_argument("--db_path", type=str, required=True, help="Path to the DuckDB database file.")
    parser.add_argument("--scrape_type", type=str, required=True, choices=['price', 'financials'], help="Type of data to scrape.")
    parser.add_argument("--start_date", type=str, default=(datetime.date.today() - datetime.timedelta(days=7)).strftime('%Y-%m-%d'), help="Start date for price scraping (YYYY-MM-DD).")
    parser.add_argument("--end_date", type=str, default=datetime.date.today().strftime('%Y-%m-%d'), help="End date for price scraping (YYYY-MM-DD).")
    parser.add_argument("--batch_size", type=int, default=100, help="Batch size for fetching price data.")
    parser.add_argument("--tickers", nargs='+', default=["AAPL", "MSFT", "GOOGL"], help="List of tickers for financial profile scraping.")
    parser.add_argument("--period", type=str, default="annual", choices=['annual', 'quarterly'], help="Period for financials (annual or quarterly).")
    parser.add_argument("--limit", type=int, default=5, help="Number of past periods for financials.")
    
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    db_api = get_database_api("duckdb", db_path=args.db_path)
    yf_provider = YFinanceProvider()

    with db_api:
        scraper = YFinanceScraper(db_api, yf_provider)
        if args.scrape_type == 'financials':
            scraper.run(
                scrape_type=args.scrape_type, 
                start_date=None, 
                end_date=args.end_date,
                tickers=args.tickers, 
                period=args.period, 
                limit=args.limit
            )
        else: # price
            scraper.run(
                scrape_type=args.scrape_type, 
                start_date=args.start_date, 
                end_date=args.end_date, 
                batch_size=args.batch_size
            )