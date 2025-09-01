import argparse
import logging
import time

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed
from typing import Optional
from dotenv import load_dotenv
import hashlib

from src.data.db import get_database_api, DatabaseAPI
from src.data.provider.tushare_provider import TushareProvider
from src.utils.log_util import logger_setup as _init_logging
from src.data.models import Price, CompanyFacts, FinancialProfile

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
        self.db.connect(read_only=False)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point. Closes the database connection."""
        self.db.close()

    def scrape_stock_basic(self, exchange: str):
        """Fetches stock basic information from Tushare and stores it in the database."""
        logger.info(f"Fetching stock basic information for exchange: {exchange}")
        df = self.provider.get_stock_basic(exchange)
        if df is None or df.empty:
            logger.warning(f"No stock basic data retrieved for exchange: {exchange}")
            return

        # Build or extend industry -> industry_code (5-digit, unique) mapping
        industry_to_code: dict[str, str] = {}
        used_codes: set[str] = set()
        try:
            existing = self.db.query_to_dataframe(
                "SELECT DISTINCT industry, industry_code FROM cn_company_facts WHERE industry IS NOT NULL AND industry_code IS NOT NULL"
            )
            if existing is not None and not existing.empty:
                for _, row in existing.iterrows():
                    ind = str(row["industry"]).strip()
                    code = str(row["industry_code"]).strip()
                    if ind and code:
                        industry_to_code[ind] = code
                        used_codes.add(code)
        except Exception:
            # Table may not exist yet; start with empty mapping
            pass

        # Determine codes for any new industries
        unique_industries = sorted({str(x).strip() for x in df["industry"].dropna().unique() if str(x).strip()})

        def compute_code_for_industry(industry_name: str) -> str:
            # Stable base using md5 hashed to 5 digits
            digest = hashlib.md5(industry_name.encode("utf-8")).hexdigest()
            base_num = int(digest[:8], 16) % 100000
            candidate = base_num
            # Linear probe to avoid conflicts; keep 5-digit zero-padded
            for _ in range(100000):
                code_str = f"{candidate:05d}"
                if code_str not in used_codes:
                    return code_str
                candidate = (candidate + 1) % 100000
            # Fallback (should never hit)
            return f"{base_num:05d}"

        for ind in unique_industries:
            if ind in industry_to_code:
                continue
            code = compute_code_for_industry(ind)
            industry_to_code[ind] = code
            used_codes.add(code)

        company_facts_objects = [
            CompanyFacts(
                ticker=row.ts_code,
                name=row.name,
                industry=row.industry,
                industry_code=industry_to_code.get(str(row.industry).strip()) if pd.notna(row.industry) else None,
                market=row.market,
                listing_date=row.list_date,
                location=row.area,
            )
            for row in df.itertuples()
        ]

        table_name = "cn_company_facts"
        primary_keys = ["ticker"]
        self.db.create_table_from_model(table_name, CompanyFacts, primary_keys)
        self.db.upsert_data_from_models(table_name, company_facts_objects, primary_keys)
        logger.info(f"Successfully stored {len(company_facts_objects)} records for exchange: {exchange}.")

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

    def run(self, start_date: str, end_date: str, fetch_basic=False, fetch_plates=False):
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

    def scrape_financial_profile(self, ticker: str, end_date: str, period: str = "annual", limit: int = 10):
        """Fetch financial profile for a ticker and store it in the database."""
        try:
            logger.info(f"Fetching financial profile for {ticker} (end_date={end_date}, period={period}, limit={limit})")
            profiles = self.provider.get_financial_profile(ticker=ticker, end_date=end_date, period=period, limit=limit)
            if not profiles:
                logger.warning(f"No financial profile data returned for {ticker}.")
                return

            table_name = "cn_financial_profiles"
            primary_keys = ["ticker", "report_period"]
            self.db.create_table_from_model(table_name, FinancialProfile, primary_keys)
            self.db.upsert_data_from_models(table_name, profiles, primary_keys)
            logger.info(f"Stored {len(profiles)} financial profile records for {ticker}.")
        except Exception as e:
            logger.error(f"Failed to fetch/store financial profile for {ticker}: {e}")

    def scrape_all_financial_profiles(self, end_date: str, period: str = "annual", limit: int = 1, batch_size: int = 2000):
        """Fetch all stocks' financial profiles via bulk VIP endpoints and upsert in batches."""
        try:
            logger.info(f"Fetching ALL financial profiles (period={period}, end_date={end_date}, limit={limit}) via VIP bulk endpoints...")
            profiles = self.provider.get_all_financial_profiles(end_date=end_date, period=period, limit=limit)
            if not profiles:
                logger.warning("No financial profiles returned from bulk VIP endpoints.")
                return

            table_name = "cn_financial_profile"
            primary_keys = ["ticker", "report_period"]
            self.db.create_table_from_model(table_name, FinancialProfile, primary_keys)

            total = len(profiles)
            start = 0
            while start < total:
                end = min(start + batch_size, total)
                batch = profiles[start:end]
                self.db.upsert_data_from_models(table_name, batch, primary_keys)
                logger.info(f"Upserted {start + 1}-{end} of {total} financial profile records.")
                start = end
            logger.info("Completed bulk upsert for ALL financial profiles.")
        except Exception as e:
            logger.error(f"Failed to fetch/store ALL financial profiles: {e}")

    def get_all_cn_company_tickers(self) -> list[str]:
        """Retrieve all tickers from cn_company_facts table."""
        try:
            query = "SELECT DISTINCT ticker FROM cn_company_facts"
            df = self.db.query_to_dataframe(query)
            if df.empty:
                logger.warning("No tickers found in 'cn_company_facts'.")
                return []
            return [str(row["ticker"]) for _, row in df.iterrows()]
        except Exception as e:
            logger.error(f"Error fetching tickers from 'cn_company_facts': {e}")
            return []


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape data from Tushare.")
    parser.add_argument("--start_date", type=str, help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end_date", type=str, help="End date in YYYY-MM-DD format.")
    parser.add_argument("--db_path", type=str, default="data/futu_financials.duckdb", help="Path to the database file.")
    parser.add_argument("--fetch_stock_basic", action="store_true", help="Fetch stock basic info.")
    parser.add_argument("--exchange", type=str, default="", help="The exchange to fetch stock basic info from (e.g., 'HK', 'SSE', 'SZSE').")
    parser.add_argument("--fetch_financial_profile", action="store_true", help="Fetch financial profile for a ticker.")
    parser.add_argument("--ticker", type=str, help="Ticker code (e.g., 600519.SH or 00700.HK). For multiple, separate by comma.")
    parser.add_argument("--period", type=str, default="annual", help="Period for financial profile: 'annual' or 'quarter'.")
    parser.add_argument("--limit", type=int, default=10, help="Max number of profile records to fetch.")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    db_api = get_database_api("duckdb", db_path=args.db_path)
    
    with TushareScraper(db_api) as scraper:
        if args.fetch_stock_basic:
            scraper.scrape_stock_basic(args.exchange)
        elif args.fetch_financial_profile and args.ticker and args.end_date:
            if args.ticker.strip().lower() == "all":
                scraper.scrape_all_financial_profiles(args.end_date, args.period, limit=args.limit)
            else:
                tickers = [t.strip() for t in args.ticker.split(",") if t.strip()]
                for t in tickers:
                    scraper.scrape_financial_profile(t, args.end_date, args.period, args.limit)
        elif args.start_date and args.end_date:
            scraper.run(args.start_date, args.end_date)
        else:
            parser.print_help() 