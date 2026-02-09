import pandas as pd
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
    This class provides a small set of common technical indicators.

    Notes:
    - The project previously depended on `pandas-ta`. That dependency became hard to resolve
      (older versions were removed from PyPI; newer versions require newer Python/numpy).
    - To keep installs reproducible, we implement the indicators we use in tests directly.
    """

    def __init__(self):
        pass

    @staticmethod
    def _sma(x: pd.Series, length: int) -> pd.Series:
        return x.rolling(window=length, min_periods=length).mean()

    @staticmethod
    def _ema(x: pd.Series, length: int) -> pd.Series:
        return x.ewm(span=length, adjust=False, min_periods=length).mean()

    @staticmethod
    def _wma(x: pd.Series, length: int) -> pd.Series:
        weights = np.arange(1, length + 1, dtype=float)

        def _calc(arr: np.ndarray) -> float:
            return float(np.dot(arr, weights) / weights.sum())

        return x.rolling(window=length, min_periods=length).apply(lambda a: _calc(np.asarray(a)), raw=False)

    @staticmethod
    def _rsi(x: pd.Series, length: int) -> pd.Series:
        delta = x.diff()
        gain = delta.clip(lower=0.0)
        loss = (-delta).clip(lower=0.0)
        avg_gain = gain.rolling(window=length, min_periods=length).mean()
        avg_loss = loss.rolling(window=length, min_periods=length).mean()
        rs = avg_gain / avg_loss.replace(0.0, np.nan)
        rsi = 100.0 - (100.0 / (1.0 + rs))
        return rsi

    @staticmethod
    def _macd(x: pd.Series, fast: int, slow: int, signal: int) -> pd.DataFrame:
        fast_ema = x.ewm(span=fast, adjust=False, min_periods=fast).mean()
        slow_ema = x.ewm(span=slow, adjust=False, min_periods=slow).mean()
        macd = fast_ema - slow_ema
        macds = macd.ewm(span=signal, adjust=False, min_periods=signal).mean()
        macdh = macd - macds
        return pd.DataFrame(
            {
                f"MACD_{fast}_{slow}_{signal}": macd,
                f"MACDh_{fast}_{slow}_{signal}": macdh,
                f"MACDs_{fast}_{slow}_{signal}": macds,
            },
            index=x.index,
        )

    @staticmethod
    def _bbands(x: pd.Series, length: int, std: float) -> pd.DataFrame:
        mid = x.rolling(window=length, min_periods=length).mean()
        s = x.rolling(window=length, min_periods=length).std()
        upper = mid + std * s
        lower = mid - std * s
        std_str = str(float(std))
        return pd.DataFrame(
            {
                f"BBL_{length}_{std_str}": lower,
                f"BBM_{length}_{std_str}": mid,
                f"BBU_{length}_{std_str}": upper,
            },
            index=x.index,
        )

    @staticmethod
    def _atr(high: pd.Series, low: pd.Series, close: pd.Series, length: int) -> pd.DataFrame:
        prev_close = close.shift(1)
        tr = pd.concat(
            [
                (high - low).abs(),
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        atr = tr.rolling(window=length, min_periods=length).mean()
        # "ATRr" is treated as "relative ATR" (ATR divided by close) for compatibility with existing code/tests.
        atrr = atr / close.replace(0.0, np.nan)
        return pd.DataFrame({f"ATRr_{length}": atrr}, index=close.index)

    def add_moving_average(self, df: pd.DataFrame, window: int, ma_type: str = 'sma', price_col: str = 'close') -> pd.DataFrame:
        """
        Adds a moving average column to the DataFrame using a robust transform.
        """
        if price_col not in df.columns:
            raise ValueError(f"Price column '{price_col}' not found in DataFrame.")

        ma_func = {
            'sma': self._sma,
            'ema': self._ema,
            'wma': self._wma,
        }.get(ma_type.lower())

        if not ma_func:
            raise ValueError(f"Invalid moving average type: {ma_type}")

        # Use transform for robust, index-aligned single-column feature creation
        feature_name = f"{ma_type.upper()}_{window}"
        df[feature_name] = df.groupby('ticker', group_keys=False)[price_col].transform(lambda x: ma_func(x, window))
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
            atr_df = df.groupby('ticker', group_keys=False).apply(
                lambda x: self._atr(x['high'], x['low'], x['close'], length=window)
            )
            df = df.join(atr_df)
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
        df[feature_name] = df.groupby('ticker', group_keys=False)[price_col].transform(lambda x: self._rsi(x, window))
        return df

    def add_macd(self, df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9, price_col: str = 'close') -> pd.DataFrame:
        """
        Adds the Moving Average Convergence Divergence (MACD) to the DataFrame.
        """
        macd_df = df.groupby('ticker', group_keys=False).apply(lambda x: self._macd(x[price_col], fast=fast, slow=slow, signal=signal))
        return df.join(macd_df)

    def add_bollinger_bands(self, df: pd.DataFrame, window: int = 20, std: int = 2, price_col: str = 'close') -> pd.DataFrame:
        """
        Adds Bollinger Bands to the DataFrame.
        """
        bbands_df = df.groupby('ticker', group_keys=False).apply(lambda x: self._bbands(x[price_col], length=window, std=float(std)))
        return df.join(bbands_df)

    def add_supertrend(self, df: pd.DataFrame, period: int = 7, multiplier: float = 3.0) -> pd.DataFrame:
        """
        Adds the Super Trend indicator to the DataFrame.
        """
        # Check if supertrend columns already exist
        supertrend_col = f'SUPERT_{period}_{multiplier}'
        if supertrend_col in df.columns:
            return df

        def _supertrend(x: pd.DataFrame) -> pd.DataFrame:
            atr = self._atr(x['high'], x['low'], x['close'], length=period)[f"ATRr_{period}"] * x['close']
            hl2 = (x['high'] + x['low']) / 2.0
            upperband = hl2 + (multiplier * atr)
            lowerband = hl2 - (multiplier * atr)

            final_upper = upperband.copy()
            final_lower = lowerband.copy()
            trend = pd.Series(index=x.index, dtype=float)

            for i in range(1, len(x)):
                idx = x.index[i]
                prev = x.index[i - 1]

                if upperband.loc[idx] < final_upper.loc[prev] or x['close'].loc[prev] > final_upper.loc[prev]:
                    final_upper.loc[idx] = upperband.loc[idx]
                else:
                    final_upper.loc[idx] = final_upper.loc[prev]

                if lowerband.loc[idx] > final_lower.loc[prev] or x['close'].loc[prev] < final_lower.loc[prev]:
                    final_lower.loc[idx] = lowerband.loc[idx]
                else:
                    final_lower.loc[idx] = final_lower.loc[prev]

                if x['close'].loc[idx] > final_upper.loc[prev]:
                    trend.loc[idx] = 1.0
                elif x['close'].loc[idx] < final_lower.loc[prev]:
                    trend.loc[idx] = -1.0
                else:
                    trend.loc[idx] = trend.loc[prev] if pd.notna(trend.loc[prev]) else 1.0

            st = pd.Series(index=x.index, dtype=float)
            for i in range(len(x)):
                idx = x.index[i]
                if pd.isna(trend.loc[idx]):
                    st.loc[idx] = np.nan
                elif trend.loc[idx] > 0:
                    st.loc[idx] = final_lower.loc[idx]
                else:
                    st.loc[idx] = final_upper.loc[idx]

            return pd.DataFrame({supertrend_col: st}, index=x.index)

        supertrend_df = df.groupby('ticker', group_keys=False).apply(_supertrend)
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
