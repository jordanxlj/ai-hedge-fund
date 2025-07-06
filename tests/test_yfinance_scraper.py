
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
    with patch('src.yfinance_scraper.YFinanceProvider') as mock_provider:
        scraper = YFinanceScraper(mock_db_api, mock_provider)
        return scraper

def test_run_batching(scraper, mock_db_api):
    """
    Tests that the scraper's run method correctly batches ticker processing.
    """
    # Arrange
    ticker_map = [('00001', '1.HK'), ('00002', '2.HK'), ('00003', '3.HK'), ('00004', '4.HK'), ('00005', '5.HK')]
    scraper.get_hk_stock_tickers = MagicMock(return_value=ticker_map)
    
    # Mock the _scrape_prices to return some data
    mock_prices = [Price(ticker='1.HK', time='2025-01-01 10:00:00', open=100, close=100, high=100, low=100, volume=1000)]
    scraper._scrape_prices = MagicMock(return_value=mock_prices)

    # Act
    scraper.run(scrape_type='kline1m', start_date='2025-01-01', end_date='2025-01-01', batch_size=2)

    # Assert
    assert scraper._scrape_prices.call_count == 3
    assert mock_db_api.upsert_data_from_models.call_count == 3

    # Check the tickers passed in each call to _scrape_prices
    call_args_list = scraper._scrape_prices.call_args_list
    assert call_args_list[0].args[0] == ['1.HK', '2.HK']
    assert call_args_list[1].args[0] == ['3.HK', '4.HK']
    assert call_args_list[2].args[0] == ['5.HK']


def test_run_kline_scraping(scraper, mock_db_api):
    """
    Tests that the scraper's run method correctly calls the scrape_prices method for kline data.
    """
    # Arrange
    ticker_map = [('00001', '1.HK')]
    scraper.get_hk_stock_tickers = MagicMock(return_value=ticker_map)
    
    mock_prices = [Price(ticker='1.HK', time='2025-01-01', open=100, close=100, high=100, low=100, volume=1000)]
    scraper.scrape_prices = MagicMock(return_value=mock_prices)

    # Act
    scraper.run(scrape_type='kline1d', start_date='2025-01-01', end_date='2025-01-01')

    # Assert
    scraper.scrape_prices.assert_called_once_with(['1.HK'], '2025-01-01', '2025-01-01')
    mock_db_api.upsert_data_from_models.assert_called_once_with("hk_stock_daily_price", mock_prices, ["ticker", "time"])


def test_run_financials_scraping(scraper, mock_db_api):
    """
    Tests that the scraper's run method correctly calls the financial profile scraping.
    """
    # Arrange
    ticker_map = [('00001', '1.HK'), ('00002', '2.HK')]
    scraper.get_hk_stock_tickers = MagicMock(return_value=ticker_map)
    
    mock_profiles = [MagicMock()]
    scraper.scrape_financial_profiles = MagicMock(return_value=mock_profiles)

    # Act
    scraper.run(scrape_type='financial', start_date=None, end_date='2025-01-01', period='annual', limit=1)

    # Assert
    scraper.scrape_financial_profiles.assert_called_once_with(['1.HK', '2.HK'], '2025-01-01', 'annual', 1, 10)
    mock_db_api.upsert_data_from_models.assert_called_once_with("financial_profile", mock_profiles, ["ticker", "report_period", "period"])


def test_run_invalid_scrape_type(scraper, mock_db_api):
    """
    Tests that the scraper's run method logs an error for an invalid scrape_type.
    """
    # Arrange
    scraper._scrape_prices = MagicMock()
    scraper.scrape_financial_profiles = MagicMock()

    # Act
    scraper.run(scrape_type='invalid', start_date=None, end_date='2025-01-01')

    # Assert
    scraper._scrape_prices.assert_not_called()
    scraper.scrape_financial_profiles.assert_not_called()

def test_get_hk_stock_tickers_empty(scraper, mock_db_api):
    """
    Tests that get_hk_stock_tickers returns an empty list when the DB query is empty.
    """
    # Arrange
    mock_db_api.query_to_dataframe.return_value = MagicMock(empty=True)

    # Act
    result = scraper.get_hk_stock_tickers()

    # Assert
    assert result == []
    mock_db_api.query_to_dataframe.assert_called_once_with("SELECT DISTINCT ticker FROM stock_plate_mappings")

@patch('src.yfinance_scraper.argparse.ArgumentParser')
@patch('src.yfinance_scraper.get_database_api')
@patch('src.yfinance_scraper.YFinanceProvider')
@patch('src.yfinance_scraper.YFinanceScraper')
def test_main(mock_scraper, mock_provider, mock_db_api, mock_argparse):
    """
    Tests the main execution block of the scraper.
    """
    # Arrange
    mock_args = MagicMock()
    mock_args.scrape_type = 'financial'
    mock_args.db_path = 'test.db'
    mock_args.end_date = '2025-01-01'
    mock_args.period = 'annual'
    mock_args.limit = 1
    mock_args.max_workers = 5
    mock_argparse.return_value.parse_args.return_value = mock_args

    # Act
    from src.yfinance_scraper import main
    main()

    # Assert
    mock_db_api.assert_called_once_with("duckdb", db_path='test.db')
    mock_provider.assert_called_once()
    mock_scraper.assert_called_once()
    mock_scraper.return_value.run.assert_called_once_with(
        scrape_type='financial',
        start_date=None,
        end_date='2025-01-01',
        period='annual',
        limit=1,
        max_workers=5
    )

def test_context_manager(scraper, mock_db_api):
    """
    Tests the context manager functionality of the scraper.
    """
    with scraper as s:
        assert s == scraper
        mock_db_api.connect.assert_called_once()
    mock_db_api.close.assert_called_once()

