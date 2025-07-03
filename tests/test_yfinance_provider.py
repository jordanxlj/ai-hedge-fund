
import pandas as pd
import pytest
from datetime import datetime, date, timedelta
from unittest.mock import MagicMock, patch

from src.data.provider.yfinance_provider import YFinanceProvider
from src.data.models import Price

@pytest.fixture
def provider():
    """Provides a YFinanceProvider instance for testing."""
    return YFinanceProvider()

def create_mock_daily_df(ticker='AAPL', start_date_str='2025-01-01'):
    """Creates a mock DataFrame simulating a yfinance daily download."""
    data = {
        'Open': [150.0, 151.0],
        'High': [152.0, 152.5],
        'Low': [149.0, 150.5],
        'Close': [151.5, 152.0],
        'Volume': [1000000, 1200000]
    }
    index = pd.to_datetime([start_date_str, (datetime.strptime(start_date_str, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')]).tz_localize('UTC')
    df = pd.DataFrame(data, index=index)
    df.index.name = 'Datetime'
    return df

def create_mock_multi_ticker_df():
    """Creates a mock DataFrame for a multi-ticker download."""
    headers = pd.MultiIndex.from_product([['Open', 'Close', 'High', 'Low', 'Volume'], ['AAPL', 'GOOG']],
                                         names=['Price', 'Ticker'])
    data = [
        [150.0, 2800.0, 151.5, 2810.0, 152.0, 2815.0, 149.5, 2795.0, 1000000, 500000],
        [151.0, 2810.0, 152.5, 2820.0, 153.0, 2825.0, 150.5, 2805.0, 1200000, 600000]
    ]
    index = pd.to_datetime(['2025-01-01', '2025-01-02']).tz_localize('UTC')
    mock_df = pd.DataFrame(data, index=index, columns=headers)
    mock_df.index.name = 'Datetime'
    return mock_df

def test_get_prices_daily_success(provider, monkeypatch):
    """Tests successful fetching of daily price data."""
    mock_df = create_mock_daily_df()
    mock_download = MagicMock(return_value=mock_df)
    monkeypatch.setattr(provider, '_download_data', mock_download)

    prices = provider.get_prices(tickers=['AAPL'], start_date='2025-01-01', end_date='2025-01-02', freq='1d')

    assert len(prices) == 2
    assert prices[0].ticker == 'AAPL'
    mock_download.assert_called_once()

def test_get_prices_minute_chunking(provider, monkeypatch):
    """Tests that a date range longer than 7 days for minute data results in multiple API calls."""
    today = date.today()
    start_date_obj = today - timedelta(days=10)
    end_date_obj = today - timedelta(days=2)
    
    mock_df = create_mock_daily_df() # Re-using daily df for simplicity
    mock_download = MagicMock(return_value=mock_df)
    monkeypatch.setattr(provider, '_download_data', mock_download)

    provider.get_prices(
        tickers=['AAPL'],
        start_date=start_date_obj.strftime('%Y-%m-%d'),
        end_date=end_date_obj.strftime('%Y-%m-%d'),
        freq='1m'
    )
    
    assert mock_download.call_count == 2

def test_process_dataframe_single_ticker(provider):
    """Tests the _process_dataframe method with a single ticker."""
    df = create_mock_daily_df(ticker='AAPL')
    prices = provider._process_dataframe(df, ['AAPL'])
    
    assert len(prices) == 2
    assert prices[0].ticker == 'AAPL'
    assert prices[0].close == 151.5

def test_process_dataframe_multi_ticker(provider):
    """Tests the _process_dataframe method with multiple tickers."""
    df = create_mock_multi_ticker_df()
    prices = provider._process_dataframe(df, ['AAPL', 'GOOG'])

    assert len(prices) == 4
    aapl_prices = [p for p in prices if p.ticker == 'AAPL']
    goog_prices = [p for p in prices if p.ticker == 'GOOG']
    assert len(aapl_prices) == 2
    assert len(goog_prices) == 2
    assert aapl_prices[0].close == 151.5
    assert goog_prices[1].close == 2820.0

def test_process_dataframe_one_valid_ticker(provider, monkeypatch):
    """Tests processing when only one of multiple tickers returns data."""
    # yfinance may return a single-ticker format df in this case
    mock_df = create_mock_daily_df(ticker='AAPL')
    
    with patch('src.data.provider.yfinance_provider.logger') as mock_logger:
        prices = provider._process_dataframe(mock_df, ['AAPL', 'INVALIDTICKER'])
        assert len(prices) == 0 # No prices should be returned as assignment is ambiguous
        mock_logger.warning.assert_called_once()

def test_get_prices_minute_outside_30_day_window(provider, monkeypatch):
    """Tests that requesting minute data older than 30 days returns an empty list."""
    start_date = (date.today() - timedelta(days=40)).strftime('%Y-%m-%d')
    end_date = (date.today() - timedelta(days=35)).strftime('%Y-%m-%d')
    mock_download = MagicMock()
    monkeypatch.setattr(provider, '_download_data', mock_download)

    with patch('src.data.provider.yfinance_provider.logger') as mock_logger:
        prices = provider.get_prices(tickers=['AAPL'], start_date=start_date, end_date=end_date, freq='1m')
        
        assert prices == []
        mock_download.assert_not_called()
        mock_logger.warning.assert_any_call(
            f"Request for 1-minute data for AAPL starts at {start_date}, "
            f"which is more than 30 days ago. yfinance does not support this. Skipping."
        )

def test_download_data_returns_none(provider, monkeypatch):
    """Tests that get_prices handles _download_data returning None."""
    mock_download = MagicMock(return_value=None)
    monkeypatch.setattr(provider, '_download_data', mock_download)
    prices = provider.get_prices(tickers=['AAPL'], start_date='2025-01-01', end_date='2025-01-02')
    assert prices == []

def test_get_financial_profile_success(provider, monkeypatch):
    """Tests successful fetching of financial profile data."""
    mock_ticker = MagicMock()
    mock_ticker.info = {'longName': 'Apple Inc.', 'currency': 'USD'}
    
    # Mock financial statements
    mock_income_stmt = pd.DataFrame({
        'Total Revenue': [383285000000],
        'Gross Profit': [169148000000],
        'Operating Income': [114301000000],
        'Net Income': [96995000000],
        'EBIT': [115301000000],
        'EBITDA': [125821000000],
        'Basic EPS': [6.16]
    }, index=[pd.to_datetime('2023-09-30')])
    
    mock_balance_sheet = pd.DataFrame({
        'Total Assets': [352583000000],
        'Total Liab': [290437000000],
        'Stockholders Equity': [62146000000],
        'Cash': [29965000000]
    }, index=[pd.to_datetime('2023-09-30')])
    
    mock_cash_flow = pd.DataFrame({
        'Total Cash From Operating Activities': [110543000000],
        'Capital Expenditures': [-10959000000]
    }, index=[pd.to_datetime('2023-09-30')])

    mock_earnings = pd.DataFrame({
        'Revenue': [383285000000],
        'Earnings': [96995000000]
    }, index=[pd.to_datetime('2023-09-30')])

    mock_ticker.income_stmt = mock_income_stmt.T
    mock_ticker.balance_sheet = mock_balance_sheet.T
    mock_ticker.cash_flow = mock_cash_flow.T
    mock_ticker.earnings = mock_earnings.T
    
    mock_yfinance_ticker = MagicMock(return_value=mock_ticker)
    monkeypatch.setattr('yfinance.Ticker', mock_yfinance_ticker)

    profiles = provider.get_financial_profile(ticker='AAPL', end_date='2025-01-01', period='annual', limit=1)

    assert len(profiles) == 1
    profile = profiles[0]
    assert profile.ticker == 'AAPL'
    assert profile.name == 'Apple Inc.'
    assert profile.revenue == 383285000000
    assert profile.net_income == 96995000000
    assert profile.free_cash_flow == 99584000000


def test_dummy_methods(provider):
    """Tests the dummy methods for coverage."""
    assert provider.get_financial_metrics("AAPL", "2025-01-01") == []
    assert provider.search_line_items("AAPL", ["revenue"], "2025-01-01") == []
    assert provider.get_insider_trades("AAPL", "2025-01-01") == []
    assert provider.get_company_news("AAPL", "2025-01-01") == []
    assert provider.get_market_cap("AAPL", "2025-01-01") is None
    assert provider.convert_period("ttm") == "annual"
    assert provider.is_available() is True
