import pandas as pd
import numpy as np
import argparse
from src.panel.strategy.base import Strategy
from src.panel.data.data_loader import DataLoader
from src.panel.data.feature_engine import FeatureEngine
from src.data.db import get_database_api

class MACrossoverStrategy(Strategy):
    """
    A simple moving average crossover strategy.
    """
    def __init__(self, short_window: int = 9, long_window: int = 18):
        self.short_window = short_window
        self.long_window = long_window

    def generate_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Generate trading signals based on moving average crossover.

        :param data: A DataFrame with historical data, including a 'close' column.
        :return: A DataFrame with a 'signal' column.
        """
        df = data.copy()
        # The feature engine already calculates the moving averages
        df['position'] = 0
        df.iloc[self.short_window:, df.columns.get_loc('position')] = np.where(
            df[f'SMA_{self.short_window}'].iloc[self.short_window:] > df[f'SMA_{self.long_window}'].iloc[self.short_window:], 1, 0
        )
        df['signal'] = df['position'].diff().fillna(0)
        return df

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Moving Average Crossover Strategy Test")
    parser.add_argument("--db_path", type=str, required=True, help="Path to the DuckDB database file.")
    parser.add_argument("--ticker", type=str, required=True, help="Ticker symbol to test.")
    parser.add_argument("--start_date", type=str, help="Start date for data loading (YYYY-MM-DD).")
    parser.add_argument("--end_date", type=str, help="End date for data loading (YYYY-MM-DD).")
    parser.add_argument("--short_window", type=int, default=9, help="Short moving average window.")
    parser.add_argument("--long_window", type=int, default=18, help="Long moving average window.")

    args = parser.parse_args()

    db_api = get_database_api("duckdb", db_path=args.db_path)
    with db_api:
        data_loader = DataLoader(db_api)
        df = data_loader.load_daily_prices(tickers=[args.ticker], start_date=args.start_date, end_date=args.end_date)

        if not df.empty:
            df = df.set_index('time')
            feature_engine = FeatureEngine()
            df = feature_engine.add_moving_average(df, window=args.short_window)
            df = feature_engine.add_moving_average(df, window=args.long_window)

            strategy = MACrossoverStrategy(short_window=args.short_window, long_window=args.long_window)
            strategy.visualize(df)
        else:
            print(f"No data found for ticker {args.ticker}")