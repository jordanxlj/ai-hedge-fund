import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import duckdb
import os

from src.tools.futu_scraper import FutuScraper
from src.data.models import StockPlateMapping, FinancialProfile
from futu import Market

# Sample data for mocking Futu API responses
SAMPLE_PLATE_LIST = pd.DataFrame([
    {'code': 'PLT.001', 'plate_name': 'Tech Giants'},
    {'code': 'PLT.002', 'plate_name': 'Finance Leaders'},
])

SAMPLE_STOCK_LIST_TECH = pd.DataFrame([
    {'code': 'US.AAPL', 'stock_name': 'Apple Inc.'},
    {'code': 'US.GOOG', 'stock_name': 'Alphabet Inc.'},
])

SAMPLE_STOCK_LIST_FINANCE = pd.DataFrame([
    {'code': 'US.JPM', 'stock_name': 'JPMorgan Chase & Co.'},
])

@pytest.fixture
def scraper_instance():
    """Fixture to create a FutuScraper instance with a test database."""
    test_db_path = "tests/test_futu_data.duckdb"
    # Ensure the test db is clean before each test
    if os.path.exists(test_db_path):
        os.remove(test_db_path)
    
    scraper = FutuScraper(db_path=test_db_path)
    # Mock the Futu OpenQuoteContext
    scraper.quote_ctx = MagicMock()
    
    yield scraper
    
    # Teardown: clean up the test database
    scraper.close()
    if os.path.exists(test_db_path):
        os.remove(test_db_path)

class TestFutuScraper:
    @patch('duckdb.connect')
    def test_scrape_stock_plate_mappings(self, mock_db_connect, scraper_instance):
        """Test the scraping and storing of stock-plate mappings."""
        # --- Setup Mocks ---
        # Mock the database connection and cursor
        mock_con = MagicMock()
        mock_db_connect.return_value = mock_con

        # Mock the Futu API calls
        def get_plate_stock_side_effect(plate_code, *args, **kwargs):
            if plate_code == 'PLT.001':
                return (0, SAMPLE_STOCK_LIST_TECH)
            elif plate_code == 'PLT.002':
                return (0, SAMPLE_STOCK_LIST_FINANCE)
            return (0, pd.DataFrame()) # Default empty response

        scraper_instance.quote_ctx.get_plate_list.return_value = (0, SAMPLE_PLATE_LIST)
        scraper_instance.quote_ctx.get_plate_stock.side_effect = get_plate_stock_side_effect

        # --- Execute the Method ---
        scraper_instance.scrape_stock_plate_mappings(market=Market.US)

        # --- Assertions ---
        # 1. Check if the table was created with the correct model
        # We can't directly check the model, but we can check if the execute call was made
        # A more robust test would inspect the CREATE TABLE SQL statement
        assert any("CREATE OR REPLACE TABLE stock_plate_mappings" in str(call) for call in mock_con.execute.call_args_list)

        # 2. Check that data was inserted
        # Total stocks = 2 (Tech) + 1 (Finance) = 3
        insert_calls = [call for call in mock_con.execute.call_args_list if "INSERT INTO stock_plate_mappings" in call[0][0]]
        assert len(insert_calls) == 3

        # 3. Inspect a sample insert call to verify data cleaning
        # Example: ('US.AAPL' -> 'AAPL')
        aapl_insert_args = insert_calls[0][0][1]
        assert aapl_insert_args[0] == 'AAPL'  # Ticker cleaned
        assert aapl_insert_args[1] == 'Apple Inc.'
        assert aapl_insert_args[2] == 'PLT.001' # Plate code
        assert aapl_insert_args[4] == 'US' # Market
        
        # 4. Check if the rate limiter was respected (mocked sleep)
        # In this simple test, we don't need to check time.sleep, but in a more complex scenario you might
        assert scraper_instance.quote_ctx.get_plate_list.call_count == 1
        assert scraper_instance.quote_ctx.get_plate_stock.call_count == 2 