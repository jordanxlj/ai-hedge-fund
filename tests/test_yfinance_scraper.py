
import pytest
from unittest.mock import MagicMock, patch
from src.yfinance_scraper import YFinanceScraper
from src.data.models import Price

@pytest.fixture
def mock_db_api():
    """Fixture for a mocked DatabaseAPI."""
    db_api = MagicMock()
    db_api.connect = MagicMock()
    db_api.close = MagicMock()
    db_api.query_to_dataframe = MagicMock()
    db_api.create_table_from_model = MagicMock()
    db_api.upsert_data_from_models = MagicMock()
    return db_api

@pytest.fixture
def scraper(mock_db_api):
    """Fixture for YFinanceScraper with a mocked DB API."""
    with patch('src.yfinance_scraper.YFinanceProvider'):
        scraper = YFinanceScraper(mock_db_api)
        return scraper

def test_run_batching(scraper, mock_db_api):
    """
    Tests that the scraper's run method correctly batches ticker processing.
    """
    # Arrange
    ticker_map = [('00001', '1.HK'), ('00002', '2.HK'), ('00003', '3.HK'), ('00004', '4.HK'), ('00005', '5.HK')]
    scraper.get_hk_stock_tickers = MagicMock(return_value=ticker_map)
    
    # Mock the fetch_price_data to return some data
    mock_prices = [Price(ticker='1.HK', time='2025-01-01 10:00:00', open=100, close=100, high=100, low=100, volume=1000)]
    scraper.fetch_price_data = MagicMock(return_value=mock_prices)

    # Act
    scraper.run(start_date='2025-01-01', end_date='2025-01-01', batch_size=2)

    # Assert
    assert scraper.fetch_price_data.call_count == 3
    assert mock_db_api.upsert_data_from_models.call_count == 3

    # Check the tickers passed in each call to fetch_price_data
    call_args_list = scraper.fetch_price_data.call_args_list
    assert call_args_list[0].args[0] == ['1.HK', '2.HK']
    assert call_args_list[1].args[0] == ['3.HK', '4.HK']
    assert call_args_list[2].args[0] == ['5.HK']
