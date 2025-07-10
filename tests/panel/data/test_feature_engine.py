
import pytest
import pandas as pd
import numpy as np
from src.panel.data.feature_engine import FeatureEngine

@pytest.fixture
def sample_panel_data():
    """Creates a sample DataFrame for testing."""
    data = {
        'time': pd.to_datetime(['2025-01-01', '2025-01-02', '2025-01-03'] * 2),
        'ticker': ['AAPL'] * 3 + ['GOOG'] * 3,
        'open': [150, 152, 151, 2800, 2810, 2805],
        'high': [155, 156, 154, 2820, 2830, 2815],
        'low': [149, 151, 150, 2790, 2800, 2800],
        'close': [153, 155, 152, 2810, 2825, 2810],
        'volume': [1000, 1200, 1100, 500, 600, 550]
    }
    return pd.DataFrame(data)

@pytest.fixture
def feature_engine():
    """Fixture for the FeatureEngine class."""
    return FeatureEngine()

def test_add_moving_average(feature_engine, sample_panel_data):
    """Tests the add_moving_average method."""
    df = feature_engine.add_moving_average(sample_panel_data, window=2, ma_type='sma', price_col='close')
    assert 'SMA_2' in df.columns
    assert df[df['ticker'] == 'AAPL']['SMA_2'].iloc[-1] == 153.5
    assert df[df['ticker'] == 'GOOG']['SMA_2'].iloc[-1] == 2817.5

def test_add_volatility_std(feature_engine, sample_panel_data):
    """Tests the add_volatility method with standard deviation."""
    df = feature_engine.add_volatility(sample_panel_data, window=2, vol_type='std', price_col='close')
    assert 'vol_std_close_2' in df.columns
    assert not df['vol_std_close_2'].isnull().all()

def test_add_volatility_atr(feature_engine, sample_panel_data):
    """Tests the add_volatility method with ATR."""
    df = feature_engine.add_volatility(sample_panel_data, window=2, vol_type='atr')
    assert 'ATRr_2' in df.columns
    assert not df['ATRr_2'].isnull().all()

def test_add_rsi(feature_engine, sample_panel_data):
    """Tests the add_rsi method."""
    df = feature_engine.add_rsi(sample_panel_data, window=2)
    assert 'RSI_2' in df.columns
    assert not df['RSI_2'].isnull().all()

def test_add_macd(feature_engine, sample_panel_data):
    """Tests the add_macd method."""
    df = feature_engine.add_macd(sample_panel_data, fast=2, slow=3, signal=1)
    assert 'MACD_2_3_1' in df.columns
    assert 'MACDh_2_3_1' in df.columns
    assert 'MACDs_2_3_1' in df.columns

def test_add_bollinger_bands(feature_engine, sample_panel_data):
    """Tests the add_bollinger_bands method."""
    df = feature_engine.add_bollinger_bands(sample_panel_data, window=2)
    assert 'BBL_2_2.0' in df.columns
    assert 'BBM_2_2.0' in df.columns
    assert 'BBU_2_2.0' in df.columns

def test_add_relative_strength(feature_engine, sample_panel_data):
    """Tests the add_relative_strength method."""
    # Add a benchmark ticker
    benchmark_data = {
        'time': pd.to_datetime(['2025-01-01', '2025-01-02', '2025-01-03']),
        'ticker': ['SPY'] * 3,
        'open': [400, 401, 402],
        'high': [402, 403, 404],
        'low': [399, 400, 401],
        'close': [401, 402, 403],
        'volume': [10000, 12000, 11000]
    }
    df_with_benchmark = pd.concat([sample_panel_data, pd.DataFrame(benchmark_data)], ignore_index=True)
    df = feature_engine.add_relative_strength(df_with_benchmark, benchmark_ticker='SPY')
    assert 'relative_strength_vs_SPY' in df.columns
    assert not df[df['ticker'] == 'AAPL']['relative_strength_vs_SPY'].isnull().all()

def test_invalid_price_column(feature_engine, sample_panel_data):
    """Tests that a ValueError is raised for an invalid price column."""
    with pytest.raises(ValueError):
        feature_engine.add_moving_average(sample_panel_data, window=2, price_col='non_existent')
