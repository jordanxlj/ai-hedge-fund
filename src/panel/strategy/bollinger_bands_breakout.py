import pandas as pd
import numpy as np
import argparse
from src.panel.strategy.base import Strategy
from src.panel.data.data_loader import DataLoader
from src.panel.data.feature_engine import FeatureEngine
from src.panel.viz.plotter import Plotter
from src.data.db import get_database_api

class BollingerBandsBreakoutStrategy(Strategy):
    """
    Bollinger Bands Breakout Strategy based on the provided Pine Script.
    """
    def __init__(self, length: int = 20, mult: float = 2.0):
        self.length = length
        self.mult = mult

    def generate_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Generate trading signals based on Bollinger Bands breakout.

        :param data: A DataFrame with historical data, including a 'close' column.
        :return: A DataFrame with a 'signal' column (-1 for sell, 1 for buy, 0 for hold).
        """
        df = data.copy()
        upper_band = f'BBU_{self.length}_{self.mult}'
        lower_band = f'BBL_{self.length}_{self.mult}'

        df['position'] = np.nan
        df.loc[df['close'] < df[lower_band], 'position'] = 1
        df.loc[df['close'] > df[upper_band], 'position'] = 0

        df['position'] = df['position'].ffill().fillna(0)
        df['signal'] = df['position'].diff().fillna(0)

        return df

    def visualize_strategy(self, data: pd.DataFrame, plotter: Plotter):
        """
        Visualize the Bollinger Bands.

        :param data: A DataFrame with historical data.
        :param plotter: The plotter instance.
        """
        lower_band = f'BBL_{self.length}_{self.mult}'
        upper_band = f'BBU_{self.length}_{self.mult}'
        middle_band = f'BBM_{self.length}_{self.mult}'
        plotter.plot_line(data, lower_band, row=1, name='Lower Band', color='blue', fill='tonexty', fillcolor='rgba(0,0,255,0.1)')
        plotter.plot_line(data, upper_band, row=1, name='Upper Band', color='blue')
        plotter.plot_line(data, middle_band, row=1, name='Middle Band', color='blue', dash='dash')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Bollinger Bands Breakout Strategy Test")
    parser.add_argument("--db_path", type=str, required=True, help="Path to the DuckDB database file.")
    parser.add_argument("--ticker", type=str, required=True, help="Ticker symbol to test.")
    parser.add_argument("--start_date", type=str, help="Start date for data loading (YYYY-MM-DD).")
    parser.add_argument("--end_date", type=str, help="End date for data loading (YYYY-MM-DD).")
    parser.add_argument("--length", type=int, default=20, help="Bollinger Bands length.")
    parser.add_argument("--mult", type=float, default=2.0, help="Bollinger Bands multiplier.")

    args = parser.parse_args()

    db_api = get_database_api("duckdb", db_path=args.db_path)
    with db_api:
        data_loader = DataLoader(db_api)
        df = data_loader.load_daily_prices(tickers=[args.ticker], start_date=args.start_date, end_date=args.end_date)

        if not df.empty:
            df = df.set_index('time')
            feature_engine = FeatureEngine()
            df = feature_engine.add_bollinger_bands(df, window=args.length, std=args.mult)
            strategy = BollingerBandsBreakoutStrategy(length=args.length, mult=args.mult)
            strategy.visualize(df)
        else:
            print(f"No data found for ticker {args.ticker}")
