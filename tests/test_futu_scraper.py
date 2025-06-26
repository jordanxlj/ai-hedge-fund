import pytest
import pandas as pd
import futu as ft
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock

from src.futu_scraper import FutuScraper, ALL_FIELDS_TO_SCRAPE, FutuNetworkError, FutuRateLimitError
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
    'stock_name': ["Amazon"],
    'pe_ttm': [55.2],
    'is_last_page': [True]
})


@pytest.fixture
def scraper_instance():
    """
    Synchronous fixture. Mocks the scraper's dependencies and key methods
    to prevent teardown race conditions in async tests.
    """
    with patch('futu.OpenQuoteContext'):
        scraper = FutuScraper(db_path="test_scraper.db")
        scraper.quote_ctx = MagicMock()
        scraper.db_api = MagicMock()
        scraper._connect = AsyncMock()
        # Mock the close method itself to prevent it from being called during
        # the fixture's teardown phase, which avoids race conditions with
        # the async test execution.
        scraper.close = MagicMock()
        yield scraper
        # No 'scraper.close()' is called here anymore.

@pytest.mark.asyncio
class TestFutuScraper:
    async def test_scrape_stock_plate_mappings(self, scraper_instance):
        """Test the async scraping and storing of stock-plate mappings."""
        # --- Mocks ---
        scraper_instance.quote_ctx.get_plate_list.return_value = (ft.RET_OK, SAMPLE_PLATE_LIST)
        scraper_instance.quote_ctx.get_plate_stock.side_effect = [
            (ft.RET_OK, SAMPLE_STOCK_LIST_TECH),
            (ft.RET_OK, SAMPLE_STOCK_LIST_FINANCE)
        ]
        
        # --- Execute ---
        await scraper_instance.scrape_stock_plate_mappings(market=Market.US)

        # --- Assertions ---
        scraper_instance.db_api.create_table_from_model.assert_called_once()
        scraper_instance.db_api.upsert_data_from_models.assert_called_once()
        assert scraper_instance.quote_ctx.get_plate_list.call_count == 1
        assert scraper_instance.quote_ctx.get_plate_stock.call_count == 2

    async def test_scrape_and_store_financial_profile(self, monkeypatch, scraper_instance):
        """Test the async financial profile scraping and storing workflow."""
        monkeypatch.setattr('src.futu_scraper.ALL_FIELDS_TO_SCRAPE', [ft.StockField.PE_TTM])
        
        # --- Mocks ---
        # Simulate pagination: first page has more data, second page is the last one.
        scraper_instance.quote_ctx.get_stock_filter.side_effect = [
            (ft.RET_OK, SAMPLE_FINANCIAL_DATA_PAGE1),
            (ft.RET_OK, SAMPLE_FINANCIAL_DATA_PAGE2)
        ]
        monkeypatch.setattr('src.futu_scraper.asyncio.sleep', AsyncMock()) # Use AsyncMock for sleep

        # --- Execute ---
        await scraper_instance.scrape_and_store_financials("US", "annual")

        # --- Assertions ---
        scraper_instance.db_api.create_table_from_model.assert_called_once()
        upsert_args, _ = scraper_instance.db_api.upsert_data_from_models.call_args
        _, models_list, _ = upsert_args
        assert len(models_list) == 3
        assert {m.ticker for m in models_list} == {'MSFT', 'GOOG', 'AMZN'}

    async def test_scrape_and_store_no_data(self, scraper_instance):
        """Test async scrape_and_store when no financial data is returned."""
        scraper_instance.quote_ctx.get_stock_filter.return_value = (ft.RET_OK, pd.DataFrame())
        await scraper_instance.scrape_and_store_financials('US')
        scraper_instance.db_api.create_table_from_model.assert_not_called()

    def test_get_plate_list_error(self, scraper_instance):
        """Test error handling for sync _get_plate_list."""
        scraper_instance.quote_ctx.get_plate_list.return_value = (ft.RET_ERROR, "API error")
        assert scraper_instance._get_plate_list(Market.US) == []

    def test_unsupported_market_validation(self, scraper_instance):
        """Test that _validate_market raises ValueError for an unsupported market."""
        with pytest.raises(ValueError, match="Unsupported market: XE"):
            scraper_instance._validate_market("XE")

    @patch('futu.OpenQuoteContext', side_effect=Exception("Connection Failed"))
    async def test_connect_failure(self, mock_futu_connect):
        """Test exception handling in async _connect."""
        scraper = FutuScraper()
        with pytest.raises(Exception, match="Connection Failed"):
            await scraper._connect()

    async def test_retry_on_network_error(self, monkeypatch, scraper_instance):
        """Test async retry on a recoverable network error."""
        scraper_instance.quote_ctx.get_plate_stock.side_effect = [
            (ft.RET_ERROR, "NN_ProtoRet_ByDisConnOrCacnel"),
            (ft.RET_OK, SAMPLE_STOCK_LIST_TECH)
        ]
        mock_sleep = AsyncMock() # Use AsyncMock
        monkeypatch.setattr('src.futu_scraper.asyncio.sleep', mock_sleep)

        result = await scraper_instance._get_plate_stock_with_retry("PLT.001")

        assert scraper_instance.quote_ctx.get_plate_stock.call_count == 2
        mock_sleep.assert_awaited_once_with(2)
        assert not result.empty

    async def test_retry_on_rate_limit_error(self, monkeypatch, scraper_instance):
        """Test async retry on a rate limit error."""
        scraper_instance.quote_ctx.get_plate_stock.side_effect = [
            (ft.RET_ERROR, "频率太高"),
            (ft.RET_OK, SAMPLE_STOCK_LIST_FINANCE)
        ]
        mock_sleep = AsyncMock() # Use AsyncMock
        monkeypatch.setattr('src.futu_scraper.asyncio.sleep', mock_sleep)

        result = await scraper_instance._get_plate_stock_with_retry("PLT.002")

        assert scraper_instance.quote_ctx.get_plate_stock.call_count == 2
        mock_sleep.assert_awaited_once_with(31)
        assert not result.empty

    async def test_no_retry_on_other_error(self, monkeypatch, scraper_instance):
        """Test no async retry on a non-specified error."""
        scraper_instance.quote_ctx.get_plate_stock.return_value = (ft.RET_ERROR, "Some other unknown error")
        mock_sleep = AsyncMock() # Use AsyncMock
        monkeypatch.setattr('src.futu_scraper.asyncio.sleep', mock_sleep)

        result = await scraper_instance._get_plate_stock_with_retry("PLT.003")

        assert result is None
        scraper_instance.quote_ctx.get_plate_stock.assert_called_once()
        mock_sleep.assert_not_called()
