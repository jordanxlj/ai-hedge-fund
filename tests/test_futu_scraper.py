import pytest
import pandas as pd
import futu as ft
from unittest.mock import patch, MagicMock
from datetime import date

from src.futu_scraper import FutuScraper, ALL_FIELDS_TO_SCRAPE
from src.data.models import Market, FinancialProfile
from src.data.futu_utils import get_report_period_date

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
    @patch('src.futu_scraper.FutuScraper.scrape_financial_profile')
    def test_scrape_and_store_financial_profile(self, mock_scrape_financial_profile, scraper_instance):
        """Test the orchestration of financial profile scraping, mocking the scrape_financial_profile method."""
        report_date = get_report_period_date(date.today(), "annual")
        report_date_str = report_date.strftime('%Y-%m-%d')

        # Configure the mock to return some data
        mock_profiles = [
            FinancialProfile(ticker='US.MSFT', name='Microsoft', pe_ttm=30.5, currency='USD', report_period=report_date_str, period='annual'),
            FinancialProfile(ticker='US.GOOG', name='Google', pe_ttm=25.1, currency='USD', report_period=report_date_str, period='annual')
        ]
        mock_scrape_financial_profile.return_value = mock_profiles

        scraper_instance.scrape_and_store_financials("US", "annual")

        # Check that scrape_financial_profile was called correctly
        mock_scrape_financial_profile.assert_called_once_with("US", "annual", report_date_str)

        # Now, this assertion should pass
        scraper_instance.db_api.create_table_from_model.assert_called_once()
        
        # Check that upsert is called with the correct data
        scraper_instance.db_api.upsert_data_from_models.assert_called_once()
        upsert_args, _ = scraper_instance.db_api.upsert_data_from_models.call_args
        _, models_list, primary_keys = upsert_args
        
        assert len(models_list) == 2
        assert {m.ticker for m in models_list} == {'US.MSFT', 'US.GOOG'}
        assert models_list == mock_profiles
        assert primary_keys == ["ticker", "report_period", "period"]

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
