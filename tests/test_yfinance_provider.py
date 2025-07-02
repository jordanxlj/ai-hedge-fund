import pandas as pd
import pytest
from datetime import datetime, date, timedelta
from unittest.mock import MagicMock, patch

from src.data.provider.yfinance_provider import YFinanceProvider
from src.data.models import Price
from yfinance.exceptions import YFTzMissingError


@pytest.fixture
def provider():
    """Provides a YFinanceProvider instance for testing."""
    return YFinanceProvider()


def create_mock_daily_df(ticker='AAPL'):
    """Creates a mock DataFrame simulating a yfinance daily download."""
    data = {
        'Open': [150.0, 151.0],
        'High': [152.0, 152.5],
        'Low': [149.0, 150.5],
        'Close': [151.5, 152.0],
        'Volume': [1000000, 1200000]
    }
    index = pd.to_datetime(['2025-01-01', '2025-01-02']).tz_localize('UTC')
    df = pd.DataFrame(data, index=index)
    df.index.name = 'Datetime'
    return df


def test_get_prices_daily_success(provider, monkeypatch):
    """
    Tests successful fetching of daily price data.
    """
    mock_df = create_mock_daily_df()
    mock_download = MagicMock(return_value=mock_df)
    monkeypatch.setattr("yfinance.download", mock_download)

    prices = provider.get_prices(ticker='AAPL', start_date='2025-01-01', end_date='2025-01-02', freq='1d')

    assert len(prices) == 2
    assert isinstance(prices[0], Price)
    assert prices[0].ticker == 'AAPL'
    assert prices[0].close == 151.5
    assert prices[1].volume == 1200000
    
    # Verify yf.download was called correctly
    mock_download.assert_called_once()
    args, kwargs = mock_download.call_args
    assert kwargs['tickers'] == 'AAPL'
    assert kwargs['interval'] == '1d' 

def create_mock_minute_df(ticker='0700.HK', start_date_str='2025-06-02'):
    """Creates a mock DataFrame with MultiIndex columns, simulating a yfinance minute download."""
    headers = pd.MultiIndex.from_tuples(
        [('Datetime', ''), ('Open', ticker), ('High', ticker), ('Low', ticker), ('Close', ticker), ('Volume', ticker)],
    )
    data = [
        [datetime.fromisoformat(f'{start_date_str}T09:30:00'), 493.0, 493.2, 490.6, 491.2, 762732],
        [datetime.fromisoformat(f'{start_date_str}T09:31:00'), 491.2, 492.8, 490.8, 492.0, 851500]
    ]
    df = pd.DataFrame(data, columns=headers)
    df = df.set_index([('Datetime', '')])
    df.index.name = 'Datetime'
    return df


def test_get_prices_minute_success(provider, monkeypatch):
    """
    Tests successful fetching of minute-level price data with MultiIndex columns.
    """
    today = date.today()
    start_date = (today - timedelta(days=5)).strftime('%Y-%m-%d')
    end_date = (today - timedelta(days=5)).strftime('%Y-%m-%d')
    
    mock_df = create_mock_minute_df(ticker='0700.HK', start_date_str=start_date)
    mock_download = MagicMock(return_value=mock_df)
    monkeypatch.setattr("yfinance.download", mock_download)

    prices = provider.get_prices(ticker='0700.HK', start_date=start_date, end_date=end_date, freq='1m')

    assert len(prices) == 2
    assert prices[0].open == 493.0
    assert prices[1].volume == 851500
    

def test_get_prices_minute_chunking(provider, monkeypatch):
    """
    Tests that a date range longer than 7 days for minute data results in multiple API calls.
    """
    today = date.today()
    start_date_obj = today - timedelta(days=10)
    end_date_obj = today - timedelta(days=2)
    
    # Mock will be called twice
    mock_df_1 = create_mock_minute_df(ticker='0700.HK', start_date_str=(start_date_obj).strftime('%Y-%m-%d'))
    mock_df_2 = create_mock_minute_df(ticker='0700.HK', start_date_str=(start_date_obj + timedelta(days=7)).strftime('%Y-%m-%d'))
    
    mock_download = MagicMock(side_effect=[mock_df_1, mock_df_2])
    monkeypatch.setattr("yfinance.download", mock_download)

    prices = provider.get_prices(
        ticker='0700.HK',
        start_date=start_date_obj.strftime('%Y-%m-%d'),
        end_date=end_date_obj.strftime('%Y-%m-%d'),
        freq='1m'
    )
    
    assert mock_download.call_count == 2
    assert len(prices) == 4 # 2 from each call


def test_get_prices_minute_outside_30_day_window(provider, monkeypatch):
    """
    Tests that requesting minute data older than 30 days returns an empty list and logs a warning.
    """
    start_date = (date.today() - timedelta(days=40)).strftime('%Y-%m-%d')
    end_date = (date.today() - timedelta(days=35)).strftime('%Y-%m-%d')
    mock_download = MagicMock()
    monkeypatch.setattr("yfinance.download", mock_download)

    with patch('src.data.provider.yfinance_provider.logger') as mock_logger:
        prices = provider.get_prices(ticker='0700.HK', start_date=start_date, end_date=end_date, freq='1m')
        
        assert prices == []
        # Proactive check should prevent any API call
        mock_download.assert_not_called()
        # Verify a warning was logged
        assert mock_logger.warning.call_count > 0
        mock_logger.warning.assert_any_call(
            f"Request for 1-minute data for 0700.HK starts at {start_date}, "
            f"which is more than 30 days ago. yfinance does not support this. Skipping."
        )


def test_get_prices_invalid_ticker(provider, monkeypatch):
    """
    Tests that an invalid ticker raises YFTzMissingError which is handled gracefully.
    """
    today = date.today()
    start_date = (today - timedelta(days=5)).strftime('%Y-%m-%d')
    end_date = (today - timedelta(days=5)).strftime('%Y-%m-%d')
    
    mock_download = MagicMock(return_value=pd.DataFrame())
    monkeypatch.setattr("yfinance.download", mock_download)

    with patch('src.data.provider.yfinance_provider.logger') as mock_logger:
        prices = provider.get_prices(ticker='INVALID.HK', start_date=start_date, end_date=end_date, freq='1m')
        assert prices == []
        mock_logger.warning.assert_called_once_with(
            "No data returned for INVALID.HK for freq '1m'"
        )