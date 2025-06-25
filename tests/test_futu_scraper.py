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
        
        # Important: Set up the context manager behavior
        # When 'with duckdb.connect() as con:' is used, con should be mock_con
        mock_con.__enter__.return_value = mock_con
        mock_con.__exit__.return_value = None
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

        # Save a reference to the mocked quote_ctx before it gets cleaned up
        mock_quote_ctx = scraper_instance.quote_ctx

        # --- Execute the Method ---
        scraper_instance.scrape_stock_plate_mappings(market=Market.US)

        # --- Assertions ---
        # 1. Check if the table was created correctly
        if mock_con.execute.call_args_list:
            create_table_call = mock_con.execute.call_args_list[0]
            assert 'CREATE TABLE IF NOT EXISTS "stock_plate_mappings"' in str(create_table_call)
        else:
            # If no execute calls were made, the method likely exited early
            # Let's check if it was due to empty plate list or other issues
            assert False, "No database execute calls were made. Check if the method exited early."

        # 2. Check that the data was prepared and registered as a DataFrame
        # The `register` method is called on the connection object `mock_con`
        if mock_con.register.call_count > 0:
            mock_con.register.assert_called_once()
            # call_args gives a tuple of (args, kwargs), we need the first arg of the args tuple
            registered_df_name, registered_df = mock_con.register.call_args[0]
            
            assert isinstance(registered_df, pd.DataFrame)
            # Total stocks = 2 (Tech) + 1 (Finance) = 3
            assert len(registered_df) == 3

            # 3. Verify the contents of the registered DataFrame
            # Check that tickers were cleaned (e.g., 'US.AAPL' -> 'AAPL')
            assert 'AAPL' in registered_df['ticker'].values
            assert 'GOOG' in registered_df['ticker'].values
            assert 'JPM' in registered_df['ticker'].values
            # Check a specific row for correctness
            aapl_row = registered_df[registered_df['ticker'] == 'AAPL'].iloc[0]
            assert aapl_row['stock_name'] == 'Apple Inc.'
            assert aapl_row['plate_code'] == 'PLT.001'
            assert aapl_row['market'] == 'US'

            # 4. Check that a single upsert SQL command was executed
            # The first call is CREATE TABLE, the second should be the INSERT
            insert_calls = [call for call in mock_con.execute.call_args_list if "INSERT INTO" in str(call)]
            assert len(insert_calls) == 1, f"Expected 1 INSERT call, but found {len(insert_calls)}"
            assert "ON CONFLICT (\"ticker\", \"plate_code\") DO UPDATE" in str(insert_calls[0])
        else:
            assert False, "No DataFrame was registered. Check if the upsert method was called."

        # 5. Check API call counts to ensure no unnecessary requests
        assert mock_quote_ctx.get_plate_list.call_count == 1
        assert mock_quote_ctx.get_plate_stock.call_count == 2 