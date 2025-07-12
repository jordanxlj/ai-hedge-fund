import pandas as pd
import numpy as np
from scipy.signal import argrelextrema
from src.panel.strategy.base import Strategy
from src.panel.viz.plotter import Plotter

class GroundTruthStrategy(Strategy):
    """
    A strategy that identifies buy and sell points based on local minima and maxima of a smoothed closing price.
    """
    def __init__(self, smooth_window: int = 5, order: int = 5):
        self.smooth_window = smooth_window
        self.order = order

    def generate_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Generate trading signals based on local minima and maxima.

        :param data: A DataFrame with historical data, including a 'close' column.
        :return: A DataFrame with a 'signal' column (-1 for sell, 1 for buy, 0 for hold).
        """
        df = data.copy()
        df['Smoothed'] = df['close'].rolling(window=self.smooth_window, center=True).mean().bfill().ffill()

        min_idx = argrelextrema(df['Smoothed'].values, np.less, order=self.order)[0]
        max_idx = argrelextrema(df['Smoothed'].values, np.greater, order=self.order)[0]

        extrema_idx = np.sort(np.concatenate([min_idx, max_idx]))
        extrema_types = ['min' if i in min_idx else 'max' for i in extrema_idx]

        last_signal = None
        signals = []
        for i, idx in enumerate(extrema_idx):
            if extrema_types[i] == 'min' and last_signal != 'buy':
                signals.append((df.index[idx], 1))
                last_signal = 'buy'
            elif extrema_types[i] == 'max' and last_signal != 'sell':
                signals.append((df.index[idx], -1))
                last_signal = 'sell'

        df['signal'] = 0
        for date, signal in signals:
            df.loc[date, 'signal'] = signal

        return df

    def visualize_strategy(self, data: pd.DataFrame, plotter: Plotter):
        """
        Visualize the smoothed line and the buy/sell points.

        :param data: A DataFrame with historical data.
        :param plotter: The plotter instance.
        """
        df = data.copy()
        df['Smoothed'] = df['close'].rolling(window=self.smooth_window, center=True).mean().bfill().ffill()
        plotter.plot_line(df, 'Smoothed', row=1, name='Smoothed', color='purple', dash='dot')

if __name__ == '__main__':
    import argparse
    from src.panel.data.data_loader import DataLoader
    from src.data.db import get_database_api

    parser = argparse.ArgumentParser(description="Ground Truth Strategy Test")
    parser.add_argument("--db_path", type=str, required=True, help="Path to the DuckDB database file.")
    parser.add_argument("--ticker", type=str, required=True, help="Ticker symbol to test.")
    parser.add_argument("--start_date", type=str, help="Start date for data loading (YYYY-MM-DD).")
    parser.add_argument("--end_date", type=str, help="End date for data loading (YYYY-MM-DD).")
    parser.add_argument("--smooth_window", type=int, default=5, help="Smoothing window.")
    parser.add_argument("--order", type=int, default=5, help="Order for extrema detection.")

    args = parser.parse_args()

    db_api = get_database_api("duckdb", db_path=args.db_path)
    with db_api:
        data_loader = DataLoader(db_api)
        df = data_loader.load_daily_prices(tickers=[args.ticker], start_date=args.start_date, end_date=args.end_date)

        if not df.empty:
            df = df.set_index('time')
            strategy = GroundTruthStrategy(smooth_window=args.smooth_window, order=args.order)
            strategy.visualize(df)
        else:
            print(f"No data found for ticker {args.ticker}")
