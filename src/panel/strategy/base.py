import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from src.panel.viz.plotter import Plotter

class Strategy(ABC):
    """
    Abstract base class for trading strategies.
    """

    @abstractmethod
    def generate_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Generate trading signals for the given data.

        :param data: A DataFrame with historical data, including a 'close' column.
        :return: A DataFrame with a 'signal' column (-1 for sell, 1 for buy, 0 for hold).
        """
        pass

    def prepare_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare data for the strategy by adding necessary indicators.

        :param data: A DataFrame with historical data.
        :return: A DataFrame with the necessary indicators.
        """
        return data

    def backtest(self, data: pd.DataFrame, initial_capital: float = 100000.0) -> dict:
        """
        Backtest the strategy and return performance metrics.

        :param data: A DataFrame with historical data.
        :param initial_capital: The initial capital for the backtest.
        :return: A dictionary with performance metrics.
        """
        df = self.prepare_data(data)
        signals = self.generate_signals(df)
        signals['position'] = signals['signal'].cumsum().clip(-1, 1)
        signals['returns'] = df['close'].pct_change()
        signals['strategy_returns'] = signals['position'].shift(1) * signals['returns']
        signals['cum_returns'] = (1 + signals['strategy_returns']).cumprod()
        signals['equity'] = initial_capital * signals['cum_returns'].fillna(1)

        total_return = signals['cum_returns'].iloc[-1] - 1 if not signals.empty else 0
        max_drawdown = (signals['cum_returns'] / signals['cum_returns'].cummax() - 1).min() if not signals.empty else 0
        win_rate = (signals['strategy_returns'] > 0).mean() if not signals.empty else 0

        return {
            'total_return': total_return,
            'max_drawdown': max_drawdown,
            'win_rate': win_rate,
            'equity_curve': signals['equity'],
            'signals': signals
        }

    def visualize_strategy(self, data: pd.DataFrame, plotter: Plotter):
        """
        Visualize the strategy's specific indicators.

        :param data: A DataFrame with historical data.
        :param plotter: The plotter instance.
        """
        pass

    def visualize(self, data: pd.DataFrame):
        """
        Visualize the strategy's signals and equity curve.

        :param data: A DataFrame with historical data.
        """
        df = self.prepare_data(data)
        signals = self.generate_signals(df)
        backtest_results = self.backtest(df)

        # Create a Plotter instance with three subplots
        plotter = Plotter(
            num_subplots=3, 
            subplot_titles=['Price and Signals', 'Equity Curve', 'Trade Log'], 
            row_heights=[0.6, 0.2, 0.2],
            specs=[[{"type": "xy"}], [{"type": "xy"}], [{"type": "domain"}]]
        )

        # Plot the candlestick chart on the first subplot
        plotter.plot_candlestick(df, subplot=1)

        # Overlay the trading signals on the first subplot
        plotter.plot_signals(signals, subplot=1)

        # Visualize the strategy's specific indicators
        self.visualize_strategy(df, plotter)

        # Plot the equity curve on the second subplot
        plotter.plot_line(backtest_results['equity_curve'].to_frame(name='equity'), 'equity', subplot=2, name='Equity Curve', color='blue', width=2)

        # Create and plot the trade log table
        trade_log = self.create_trade_log(backtest_results['signals'])
        if not trade_log.empty:
            plotter.plot_table(trade_log, subplot=3)

        # Show the figure
        plotter.show('Strategy Backtest', ['Price', 'Equity', 'Trades'])

    def create_trade_log(self, signals: pd.DataFrame) -> pd.DataFrame:
        """
        Create a trade log from the signals.

        :param signals: A DataFrame with a 'signal' column.
        :return: A DataFrame with the trade log.
        """
        trade_log = []
        buy_price = 0
        accumulated_profit = 0

        for i in range(len(signals)):
            if signals['signal'].iloc[i] == 1: # Buy signal
                buy_price = signals['close'].iloc[i]
                trade_log.append({
                    'Time': signals.index[i],
                    'Signal': 'BUY',
                    'Price': f"{buy_price:.2f}",
                    'Profit': '' # No profit on buy
                })
            elif signals['signal'].iloc[i] == -1 and buy_price != 0: # Sell signal
                sell_price = signals['close'].iloc[i]
                profit = (sell_price - buy_price) * 100 # Assuming 100 shares
                accumulated_profit += profit
                trade_log.append({
                    'Time': signals.index[i],
                    'Signal': 'SELL',
                    'Price': f"{sell_price:.2f}",
                    'Profit': f"{accumulated_profit:.2f}"
                })
                buy_price = 0 # Reset buy price
        
        return pd.DataFrame(trade_log)