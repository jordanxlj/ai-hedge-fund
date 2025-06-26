import pytest
import pandas as pd
import futu as ft
from unittest.mock import patch, MagicMock, call

from src.futu_scraper import FutuScraper, ALL_FIELDS_TO_SCRAPE
from src.data.models import Market

# --- Sample Data ---

SAMPLE_PLATE_LIST = pd.DataFrame([
    {'code': 'PLT.001', 'plate_name': 'Tech Giants', 'plate_id': 'P1'},
    {'code': 'PLT.002', 'plate_name': 'Financials', 'plate_id': 'P2'},
])

SAMPLE_STOCK_LIST_TECH = pd.DataFrame([
    {'code': 'US.AAPL', 'stock_name': 'Apple Inc.'},
    {'code': 'US.GOOG', 'stock_name': 'Alphabet Inc.'},
])

SAMPLE_STOCK_LIST_FINANCE = pd.DataFrame([
    {'code': 'US.JPM', 'stock_name': 'JPMorgan Chase & Co.'},
])

# Sample data for financial profile scraping
SAMPLE_FINANCIAL_DATA_PAGE1 = pd.DataFrame({
    'stock_code': ['US.MSFT', 'US.GOOG'],
    'pe_ttm': [30.5, 25.1],
    'is_last_page': [False, False]
})
SAMPLE_FINANCIAL_DATA_PAGE2 = pd.DataFrame({
    'stock_code': ['US.AMZN'],
    'pe_ttm': [55.2],
    'is_last_page': [True]
})


@pytest.fixture
def scraper_instance():
    """Fixture to create a FutuScraper instance with a mocked quote_ctx and db_api."""
    with patch('futu.OpenQuoteContext') as mock_futu_connect:
        # Create an instance of the scraper
        scraper = FutuScraper(db_path="test_scraper.db")

        # Mock the quote_ctx and db_api on the instance
        scraper.quote_ctx = MagicMock()
        scraper.db_api = MagicMock()

        yield scraper

        # Teardown
        scraper.close()


class TestFutuScraper:
    def test_scrape_stock_plate_mappings(self, scraper_instance):
        """Test the scraping and storing of stock-plate mappings."""
        # --- Setup Mocks ---
        def get_plate_stock_side_effect(plate_code, *args, **kwargs):
            if plate_code == 'PLT.001':
                return (ft.RET_OK, SAMPLE_STOCK_LIST_TECH)
            elif plate_code == 'PLT.002':
                return (ft.RET_OK, SAMPLE_STOCK_LIST_FINANCE)
            return (ft.RET_OK, pd.DataFrame())

        scraper_instance.quote_ctx.get_plate_list.return_value = (ft.RET_OK, SAMPLE_PLATE_LIST)
        scraper_instance.quote_ctx.get_plate_stock.side_effect = get_plate_stock_side_effect

        # --- Execute the Method ---
        scraper_instance.scrape_stock_plate_mappings(market=Market.US)

        # --- Assertions ---
        scraper_instance.db_api.create_table_from_model.assert_called_once()
        scraper_instance.db_api.upsert_data_from_models.assert_called_once()
        assert scraper_instance.quote_ctx.get_plate_list.call_count == 1
        assert scraper_instance.quote_ctx.get_plate_stock.call_count == 2

    @patch('time.sleep', return_value=None)
    @patch('src.futu_scraper.ALL_FIELDS_TO_SCRAPE', [ft.StockField.PE_TTM])
    def test_scrape_and_store_financial_profile(self, scraper_instance):
        """Test the financial profile scraping and storing workflow."""
        # --- Setup Mocks ---
        scraper_instance.quote_ctx.get_stock_filter.side_effect = [
            (ft.RET_OK, SAMPLE_FINANCIAL_DATA_PAGE1),
            (ft.RET_OK, SAMPLE_FINANCIAL_DATA_PAGE2),
        ]

        # --- Execute the Method ---
        scraper_instance.scrape_and_store(market='US', quarter='annual')

        # --- Assertions ---
        scraper_instance.db_api.create_table_from_model.assert_called_once()
        scraper_instance.db_api.upsert_data_from_models.assert_called_once()
        
        upsert_args, _ = scraper_instance.db_api.upsert_data_from_models.call_args
        _, models_list, _ = upsert_args
        
        assert len(models_list) == 3
        assert {m.ticker for m in models_list} == {'US.MSFT', 'US.GOOG', 'US.AMZN'}
        
        # Verify that a value was correctly parsed and mapped
        msft_model = next(m for m in models_list if m.ticker == 'US.MSFT')
        assert msft_model.price_to_earnings_ratio == 30.5

    @patch('time.sleep', return_value=None)
    @patch('src.futu_scraper.ALL_FIELDS_TO_SCRAPE', [ft.StockField.PE_TTM])
    def test_scrape_and_store_no_data(self, scraper_instance):
        """Test the scrape_and_store method when no financial data is returned."""
        scraper_instance.quote_ctx.get_stock_filter.return_value = (ft.RET_OK, pd.DataFrame())
        
        scraper_instance.scrape_and_store(market='US')
        
        # The scraper should log and exit gracefully without calling db methods
        scraper_instance.db_api.create_table_from_model.assert_not_called()
        scraper_instance.db_api.upsert_data_from_models.assert_not_called()

    def test_get_plate_list_error(self, scraper_instance):
        """Test error handling when fetching the plate list fails."""
        scraper_instance.quote_ctx.get_plate_list.return_value = (ft.RET_ERROR, "API error")
        result = scraper_instance._get_plate_list(Market.US)
        assert result == []

    def test_get_plate_stock_error(self, scraper_instance):
        """Test error handling when fetching stocks for a plate fails."""
        scraper_instance.quote_ctx.get_plate_stock.return_value = (ft.RET_ERROR, "API error")
        result = scraper_instance._get_plate_stock_with_retry("PLT.001")
        assert result is None

    def test_unsupported_market_scrape_profile(self, scraper_instance):
        """Test that an unsupported market raises a ValueError."""
        with pytest.raises(ValueError, match="Unsupported market"):
            scraper_instance.scrape_financial_profile(market="XE")

    @patch('futu.OpenQuoteContext', side_effect=Exception("Connection Failed"))
    def test_connect_failure(self, mock_futu_connect):
        """Test that an exception during Futu connection is handled."""
        scraper = FutuScraper()
        with pytest.raises(Exception, match="Connection Failed"):
            scraper._connect()
