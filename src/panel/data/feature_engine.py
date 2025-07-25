import pandas as pd
import pandas_ta as ta
import argparse
import numpy as np
from src.panel.data.data_loader import DataLoader
from src.panel.viz.plotter import Plotter
from src.data.db import get_database_api
import logging

logger = logging.getLogger(__name__)

class FeatureEngine:
    """
    A class for engineering features on financial panel data.
    This class leverages the pandas-ta library to provide a rich set of technical indicators.
    """

    def __init__(self):
        pass

    def add_moving_average(self, df: pd.DataFrame, window: int, ma_type: str = 'sma', price_col: str = 'close') -> pd.DataFrame:
        """
        Adds a moving average column to the DataFrame using a robust transform.
        """
        if price_col not in df.columns:
            raise ValueError(f"Price column '{price_col}' not found in DataFrame.")

        ma_func = {
            'sma': ta.sma,
            'ema': ta.ema,
            'wma': ta.wma
        }.get(ma_type.lower())

        if not ma_func:
            raise ValueError(f"Invalid moving average type: {ma_type}")

        # Use transform for robust, index-aligned single-column feature creation
        feature_name = f"{ma_type.upper()}_{window}"
        df[feature_name] = df.groupby('ticker', group_keys=False)[price_col].transform(lambda x: ma_func(x, length=window))
        return df

    def add_volatility(self, df: pd.DataFrame, window: int, vol_type: str = 'std', price_col: str = 'close') -> pd.DataFrame:
        """
        Adds a volatility column to the DataFrame.
        """
        if vol_type.lower() == 'std':
            if price_col not in df.columns:
                raise ValueError(f"Price column '{price_col}' not found for 'std' calculation.")
            feature_name = f'vol_std_{price_col}_{window}'
            df[feature_name] = df.groupby('ticker', group_keys=False)[price_col].transform(lambda x: x.pct_change().rolling(window=window).std())
        elif vol_type.lower() == 'atr':
            if not all(col in df.columns for col in ['high', 'low', 'close']):
                raise ValueError("'high', 'low', and 'close' columns are required for ATR calculation.")
            # Use the pandas-ta accessor
            df.ta.atr(length=window, append=True)
        else:
            raise ValueError(f"Invalid volatility type: {vol_type}")
        return df

    def add_atr(self, df: pd.DataFrame, period: int) -> pd.DataFrame:
        """
        Adds the Average True Range (ATR) to the DataFrame.
        """
        return self.add_volatility(df, window=period, vol_type='atr')

    def add_rsi(self, df: pd.DataFrame, window: int = 14, price_col: str = 'close') -> pd.DataFrame:
        """
        Adds the Relative Strength Index (RSI) to the DataFrame.
        """
        feature_name = f"RSI_{window}"
        df[feature_name] = df.groupby('ticker', group_keys=False)[price_col].transform(lambda x: ta.rsi(x, length=window))
        return df

    def add_macd(self, df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9, price_col: str = 'close') -> pd.DataFrame:
        """
        Adds the Moving Average Convergence Divergence (MACD) to the DataFrame.
        """
        macd_df = df.groupby('ticker', group_keys=False).apply(lambda x: ta.macd(x[price_col], fast=fast, slow=slow, signal=signal))
        return df.join(macd_df)

    def add_bollinger_bands(self, df: pd.DataFrame, window: int = 20, std: int = 2, price_col: str = 'close') -> pd.DataFrame:
        """
        Adds Bollinger Bands to the DataFrame.
        """
        bbands = ta.bbands(df[price_col], length=window, std=std, mamode='sma')
        if bbands is not None and not bbands.empty:
            for col in bbands.columns:
                if col not in df.columns:
                    df[col] = bbands[col]
        return df

    def add_supertrend(self, df: pd.DataFrame, period: int = 7, multiplier: float = 3.0) -> pd.DataFrame:
        """
        Adds the Super Trend indicator to the DataFrame.
        """
        # Check if supertrend columns already exist
        supertrend_col = f'SUPERT_{period}_{multiplier}'
        if supertrend_col in df.columns:
            return df

        supertrend_df = df.groupby('ticker', group_keys=False).apply(lambda x: ta.supertrend(x['high'], x['low'], x['close'], length=period, multiplier=multiplier))
        return df.join(supertrend_df)

    def add_pivot_point_super_trend(self, df: pd.DataFrame, pivot_period: int = 2, atr_factor: float = 3.0, atr_period: int = 10) -> pd.DataFrame:
        """
        Adds the Pivot Point Super Trend indicator to the DataFrame.
        """
        df = self.add_atr(df, period=atr_period)
        
        # Pine Script pivothigh/pivotlow equivalent
        df['ph'] = df['high'].rolling(window=pivot_period*2+1, center=True).max().shift(-pivot_period)
        df['pl'] = df['low'].rolling(window=pivot_period*2+1, center=True).min().shift(-pivot_period)
        
        df['center'] = np.nan
        df['last_pp'] = np.where(df['ph'] == df['high'], df['high'], np.where(df['pl'] == df['low'], df['low'], np.nan))
        df['last_pp'] = df['last_pp'].ffill()
        
        for i in range(1, len(df)):
            if not np.isnan(df['last_pp'].iloc[i]):
                if np.isnan(df['center'].iloc[i-1]):
                    df.loc[df.index[i], 'center'] = df['last_pp'].iloc[i]
                else:
                    df.loc[df.index[i], 'center'] = (df['center'].iloc[i-1] * 2 + df['last_pp'].iloc[i]) / 3
            else:
                df.loc[df.index[i], 'center'] = df['center'].iloc[i-1]

        df['upper_band'] = df['center'] - (atr_factor * df[f'ATRr_{atr_period}'])
        df['lower_band'] = df['center'] + (atr_factor * df[f'ATRr_{atr_period}'])

        df['trend_up'] = np.nan
        df['trend_down'] = np.nan
        df['trend'] = 1 # Default to 1 as in nz(Trend[1], 1)

        for i in range(1, len(df)):
            df.loc[df.index[i], 'trend_up'] = max(df['upper_band'].iloc[i], df['trend_up'].iloc[i-1]) if df['close'].iloc[i-1] > df['trend_up'].iloc[i-1] else df['upper_band'].iloc[i]
            df.loc[df.index[i], 'trend_down'] = min(df['lower_band'].iloc[i], df['trend_down'].iloc[i-1]) if df['close'].iloc[i-1] < df['trend_down'].iloc[i-1] else df['lower_band'].iloc[i]
            
            if df['close'].iloc[i] > df['trend_down'].iloc[i-1]:
                df.loc[df.index[i], 'trend'] = 1
            elif df['close'].iloc[i] < df['trend_up'].iloc[i-1]:
                df.loc[df.index[i], 'trend'] = -1
            else:
                df.loc[df.index[i], 'trend'] = df['trend'].iloc[i-1]

        df['trailing_sl'] = np.where(df['trend'] == 1, df['trend_up'], df['trend_down'])
        
        return df

    def _wavetrend(self, high: pd.Series, low: pd.Series, close: pd.Series, channel_length: int = 10, average_length: int = 21, sma_length: int = 4):
        ap = (high + low + close) / 3
        esa = ap.ewm(span=channel_length, adjust=False).mean()
        d = (ap - esa).abs().ewm(span=channel_length, adjust=False).mean()
        ci = (ap - esa) / (0.015 * d)
        wt1 = ci.ewm(span=average_length, adjust=False).mean()
        wt2 = wt1.rolling(window=sma_length).mean()
        wt_hist = wt1 - wt2

        # Set initial unstable values to NaN
        wt1.iloc[:channel_length + average_length] = np.nan
        wt2.iloc[:channel_length + average_length + sma_length] = np.nan
        wt_hist.iloc[:channel_length + average_length] = np.nan

        return pd.DataFrame({'WT1': wt1, 'WT2': wt2, 'WT_Hist': wt_hist})

    def add_wavetrend(self, df: pd.DataFrame, channel_length: int = 10, average_length: int = 21, sma_length: int = 4) -> pd.DataFrame:
        """
        Adds the Wave Trend Oscillator to the DataFrame.
        """
        wavetrend_df = df.groupby('ticker', group_keys=False).apply(lambda x: self._wavetrend(x['high'], x['low'], x['close'], channel_length=channel_length, average_length=average_length, sma_length=sma_length))
        return df.join(wavetrend_df)

    def add_relative_strength(self, df: pd.DataFrame, benchmark_ticker: str, price_col: str = 'close') -> pd.DataFrame:
        """
        Adds a relative strength column compared to a benchmark ticker.
        """
        if benchmark_ticker not in df['ticker'].unique():
            raise ValueError(f"Benchmark ticker '{benchmark_ticker}' not found in DataFrame.")
        
        benchmark_returns = df[df['ticker'] == benchmark_ticker].set_index('time')[price_col].pct_change()
        df_merged = df.join(benchmark_returns.rename('benchmark_returns'), on='time')
        
        def calculate_rs(x):
            asset_returns = x[price_col].pct_change()
            return asset_returns - x['benchmark_returns']

        feature_name = f'relative_strength_vs_{benchmark_ticker}'
        df[feature_name] = df_merged.groupby('ticker', group_keys=False).apply(calculate_rs)
        df.drop(columns=['benchmark_returns'], inplace=True, errors='ignore')
        return df

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Feature Engineering and Visualization for Financial Data.")
    parser.add_argument("--db_path", type=str, required=True, help="Path to the DuckDB database file.")
    parser.add_argument("--ticker", type=str, required=True, help="Ticker symbol to visualize.")
    parser.add_argument("--start_date", type=str, help="Start date for data loading (YYYY-MM-DD).")
    parser.add_argument("--end_date", type=str, help="End date for data loading (YYYY-MM-DD).")
    parser.add_argument("--chart_type", type=str, default="candlestick", choices=["candlestick", "line"], help="Type of chart to display.")

    args = parser.parse_args()

    db_api = get_database_api("duckdb", db_path=args.db_path)
    with db_api:
        data_loader = DataLoader(db_api)
        df = data_loader.load_daily_prices(tickers=[args.ticker], start_date=args.start_date, end_date=args.end_date)

        if not df.empty:
            feature_engine = FeatureEngine()
            df = feature_engine.add_moving_average(df, window=20)
            df = feature_engine.add_bollinger_bands(df, window=20)
            df = feature_engine.add_rsi(df, window=14)
            df = feature_engine.add_macd(df)
            df = feature_engine.add_wavetrend(df)

            plotter = Plotter()
            if args.chart_type == 'candlestick':
                plotter.plot_candlestick(df, ticker=args.ticker)
            elif args.chart_type == 'line':
                plotter.plot_line(df, tickers=[args.ticker])
        else:
            print(f"No data found for ticker {args.ticker}")
