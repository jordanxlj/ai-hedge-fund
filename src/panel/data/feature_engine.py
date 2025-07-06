import pandas as pd
import pandas_ta as ta

class FeatureEngine:
    """
    A class for engineering features on financial panel data.
    This class leverages the pandas-ta library to provide a rich set of technical indicators.
    """

    def __init__(self):
        pass

    def _process_in_groups(self, df: pd.DataFrame, func, **kwargs) -> pd.DataFrame:
        """Helper to apply a function to each ticker group."""
        return df.groupby('ticker', group_keys=False).apply(func, **kwargs)

    def add_moving_average(self, df: pd.DataFrame, window: int, ma_type: str = 'sma', price_col: str = 'close') -> pd.DataFrame:
        """
        Adds a moving average column to the DataFrame.

        :param df: DataFrame with financial data.
        :param window: The rolling window size.
        :param ma_type: Type of moving average ('sma', 'ema', 'wma').
        :param price_col: The price column to use.
        :return: DataFrame with the added moving average column.
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

        ma_series = self._process_in_groups(df, lambda x: ma_func(x[price_col], length=window))
        df[f'{ma_type.lower()}_{price_col}_{window}'] = ma_series
        return df

    def add_volatility(self, df: pd.DataFrame, window: int, vol_type: str = 'std', price_col: str = 'close') -> pd.DataFrame:
        """
        Adds a volatility column to the DataFrame.

        :param df: DataFrame with financial data.
        :param window: The rolling window size.
        :param vol_type: Type of volatility ('std' for standard deviation, 'atr' for Average True Range).
        :param price_col: The price column to use for 'std'.
        :return: DataFrame with the added volatility column.
        """
        if vol_type.lower() == 'std':
            if price_col not in df.columns:
                raise ValueError(f"Price column '{price_col}' not found for 'std' calculation.")
            vol_series = self._process_in_groups(df, lambda x: x[price_col].pct_change().rolling(window=window).std())
            df[f'vol_std_{price_col}_{window}'] = vol_series
        elif vol_type.lower() == 'atr':
            if not all(col in df.columns for col in ['high', 'low', 'close']):
                raise ValueError("'high', 'low', and 'close' columns are required for ATR calculation.")
            atr_series = self._process_in_groups(df, lambda x: ta.atr(x['high'], x['low'], x['close'], length=window))
            df[f'atr_{window}'] = atr_series
        else:
            raise ValueError(f"Invalid volatility type: {vol_type}")
        return df

    def add_rsi(self, df: pd.DataFrame, window: int = 14, price_col: str = 'close') -> pd.DataFrame:
        """
        Adds the Relative Strength Index (RSI) to the DataFrame.
        """
        rsi_series = self._process_in_groups(df, lambda x: ta.rsi(x[price_col], length=window))
        df[f'rsi_{price_col}_{window}'] = rsi_series
        return df

    def add_macd(self, df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9, price_col: str = 'close') -> pd.DataFrame:
        """
        Adds the Moving Average Convergence Divergence (MACD) to the DataFrame.
        """
        macd_df = self._process_in_groups(df, lambda x: ta.macd(x[price_col], fast=fast, slow=slow, signal=signal))
        df = df.join(macd_df)
        return df

    def add_bollinger_bands(self, df: pd.DataFrame, window: int = 20, std: int = 2, price_col: str = 'close') -> pd.DataFrame:
        """
        Adds Bollinger Bands to the DataFrame.
        """
        bbands_df = self._process_in_groups(df, lambda x: ta.bbands(x[price_col], length=window, std=std))
        df = df.join(bbands_df)
        return df

    def add_relative_strength(self, df: pd.DataFrame, benchmark_ticker: str, price_col: str = 'close') -> pd.DataFrame:
        """
        Adds a relative strength column compared to a benchmark ticker.
        """
        if benchmark_ticker not in df['ticker'].unique():
            raise ValueError(f"Benchmark ticker '{benchmark_ticker}' not found in DataFrame.")
        
        benchmark = df[df['ticker'] == benchmark_ticker].set_index('time')[price_col].pct_change().rename('benchmark_returns')
        df_merged = df.join(benchmark, on='time')
        
        def calculate_rs(x):
            asset_returns = x[price_col].pct_change()
            relative_strength = asset_returns - x['benchmark_returns']
            return relative_strength

        rs_series = self._process_in_groups(df_merged, calculate_rs)
        df[f'relative_strength_vs_{benchmark_ticker}'] = rs_series
        df.drop(columns=['benchmark_returns'], inplace=True, errors='ignore')
        return df