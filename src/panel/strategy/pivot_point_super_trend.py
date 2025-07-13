
import pandas as pd
import numpy as np
import argparse
from src.panel.strategy.base import Strategy
from src.panel.data.data_loader import DataLoader
from src.panel.data.feature_engine import FeatureEngine
from src.data.db import get_database_api

class PivotPointSuperTrendStrategy(Strategy):
    """
    Pivot Point Super Trend strategy implementation.
    """
    def __init__(self, pivot_period: int = 2, atr_factor: float = 3.0, atr_period: int = 10):
        self.pivot_period = pivot_period
        self.atr_factor = atr_factor
        self.atr_period = atr_period

    def prepare_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare data for the strategy by adding the Pivot Point Super Trend indicator.

        :param data: A DataFrame with historical data.
        :return: A DataFrame with the Pivot Point Super Trend indicator.
        """
        df = data.copy()
        feature_engine = FeatureEngine()
        df = feature_engine.add_pivot_point_super_trend(
            df,
            pivot_period=self.pivot_period,
            atr_factor=self.atr_factor,
            atr_period=self.atr_period
        )
        return df

    def generate_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Generate trading signals based on the Pivot Point Super Trend indicator.

        :param data: A DataFrame with historical data, including the Pivot Point Super Trend indicator.
        :return: A DataFrame with a 'signal' column.
        """
        df = data.copy()
        df['signal'] = 0
        position = 0

        for i in range(1, len(df)):
            # Buy signal
            if df['trend'].iloc[i] == 1 and df['trend'].iloc[i-1] == -1 and position == 0:
                df.loc[df.index[i], 'signal'] = 1
                position = 1
            # Sell signal
            elif df['trend'].iloc[i] == -1 and df['trend'].iloc[i-1] == 1 and position == 1:
                df.loc[df.index[i], 'signal'] = -1
                position = 0
        
        return df

    def visualize_strategy(self, data: pd.DataFrame, plotter):
        """
        Visualize the Pivot Point Super Trend indicator.

        :param data: A DataFrame with historical data.
        :param plotter: The plotter instance.
        """
        plotter.plot_line(data, 'trailing_sl', subplot=1, name='Pivot Point Super Trend', color='purple', width=1.5)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Pivot Point Super Trend Strategy Test")
    parser.add_argument("--db_path", type=str, required=True, help="Path to the DuckDB database file.")
    parser.add_argument("--ticker", type=str, required=True, help="Ticker symbol to test.")
    parser.add_argument("--start_date", type=str, help="Start date for data loading (YYYY-MM-DD).")
    parser.add_argument("--end_date", type=str, help="End date for data loading (YYYY-MM-DD).")
    parser.add_argument("--pivot_period", type=int, default=2, help="Pivot Point Period.")
    parser.add_argument("--atr_factor", type=float, default=3.0, help="ATR Factor.")
    parser.add_argument("--atr_period", type=int, default=10, help="ATR Period.")

    args = parser.parse_args()

    db_api = get_database_api("duckdb", db_path=args.db_path)
    with db_api:
        data_loader = DataLoader(db_api)
        df = data_loader.load_daily_prices(tickers=[args.ticker], start_date=args.start_date, end_date=args.end_date)

        if not df.empty:
            df = df.set_index('time')
            strategy = PivotPointSuperTrendStrategy(
                pivot_period=args.pivot_period,
                atr_factor=args.atr_factor,
                atr_period=args.atr_period
            )
            strategy.visualize(df)
        else:
            print(f"No data found for ticker {args.ticker}")
