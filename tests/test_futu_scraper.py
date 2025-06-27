import pytest
import pandas as pd
import futu as ft
from unittest.mock import patch, MagicMock

from src.futu_scraper import FutuScraper, ALL_FIELDS_TO_SCRAPE
from src.data.models import Market

# Sample Data in DataFrame format for the synchronous scraper
SAMPLE_FINANCIAL_DATA_PAGE1 = pd.DataFrame({
    'stock_code': ['US.MSFT', 'US.GOOG'],
    'stock_name': ['Mcrosoft', 'Google'],
    'pe_ttm': [30.5, 25.1],
    'is_last_page': [False, False]
})
SAMPLE_FINANCIAL_DATA_PAGE2 = pd.DataFrame({
    'stock_code': ['US.AMZN'],
    'stock_name': ["Amazon"],
    'pe_ttm': [55.2],
    'is_last_page': [True]
})
# Empty dataframe to signify the end of pagination
EMPTY_DF = pd.DataFrame(columns=['stock_code', 'stock_name', 'pe_ttm', 'is_last_page'])


@pytest.fixture
def scraper_instance():
    """Synchronous fixture for the scraper, mocking the API executor."""
    with patch('futu.OpenQuoteContext'):
        scraper = FutuScraper(db_path="test_scraper.db")
        scraper.quote_ctx = MagicMock()
        scraper.db_api = MagicMock()
        scraper.api_executor = MagicMock()
        # Configure max_workers to be an integer to prevent TypeError
        scraper.api_executor.max_workers = 1 
        scraper.close = MagicMock()
        yield scraper

class TestFutuScraper:
    @patch('src.futu_scraper.FutuScraper._scrape_field_worker')
    def test_scrape_and_store_financial_profile(self, mock_worker, monkeypatch, scraper_instance):
        """Test the orchestration of financial profile scraping, mocking the worker function."""
        # We only need to test with one field since we're mocking the worker
        monkeypatch.setattr('src.futu_scraper.ALL_FIELDS_TO_SCRAPE', [ft.StockField.PE_TTM])

        # Define a side effect for the mock worker to simulate populating the data dict
        def worker_side_effect(field, ft_market, quarter, all_stocks_data, lock):
            with lock:
                all_stocks_data['US.MSFT'] = {'ticker': 'US.MSFT', 'stock_name': 'Microsoft', 'pe_ttm': 30.5}
                all_stocks_data['US.GOOG'] = {'ticker': 'US.GOOG', 'stock_name': 'Google', 'pe_ttm': 25.1}
        
        mock_worker.side_effect = worker_side_effect
        
        scraper_instance.scrape_and_store_financials("US", "annual")

        # Check that the worker was called once for our single test field
        mock_worker.assert_called_once()
        
        scraper_instance.db_api.create_table_from_model.assert_called_once()
        
        upsert_args, _ = scraper_instance.db_api.upsert_data_from_models.call_args
        _, models_list, _ = upsert_args
        
        assert len(models_list) == 2
        assert {m.ticker for m in models_list} == {'US.MSFT', 'US.GOOG'}

    def test_unsupported_market_validation(self, scraper_instance):
        """Test that _validate_market raises ValueError for an unsupported market."""
        with pytest.raises(ValueError, match="Unsupported market: XE"):
            scraper_instance._validate_market("XE")

    @patch('src.futu_scraper.ft')
    def test_connect_failure(self, mock_ft):
        """Test that an exception during Futu connection is handled."""
        # Configure the mock on the patched module
        mock_ft.OpenQuoteContext.side_effect = Exception("Connection Failed")
        
        scraper = FutuScraper()
        with pytest.raises(Exception, match="Connection Failed"):
            scraper._connect()
