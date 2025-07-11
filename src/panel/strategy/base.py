import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from abc import ABC, abstractmethod

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

    def backtest(self, data: pd.DataFrame, initial_capital: float = 100000.0) -> dict:
        """
        Backtest the strategy and return performance metrics.

        :param data: A DataFrame with historical data.
        :param initial_capital: The initial capital for the backtest.
        :return: A dictionary with performance metrics.
        """
        signals = self.generate_signals(data)
        signals['position'] = signals['signal'].cumsum().clip(-1, 1)
        signals['returns'] = data['close'].pct_change()
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
            'equity_curve': signals['equity']
        }

    def visualize(self, data: pd.DataFrame):
        """
        Visualize the strategy's signals and equity curve.

        :param data: A DataFrame with historical data.
        """
        signals = self.generate_signals(data)
        backtest_results = self.backtest(data)

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

        # Price and signals plot
        ax1.plot(data['close'], label='Close Price')
        buy_signals = signals[signals['signal'] == 1]
        sell_signals = signals[signals['signal'] == -1]
        ax1.scatter(buy_signals.index, buy_signals['close'], marker='^', color='g', label='Buy')
        ax1.scatter(sell_signals.index, sell_signals['close'], marker='v', color='r', label='Sell')
        ax1.set_title('Price and Signals')
        ax1.legend()

        # Equity curve plot
        ax2.plot(backtest_results['equity_curve'], label='Equity Curve', color='b')
        ax2.set_title('Equity Curve')
        ax2.legend()

        plt.tight_layout()
        plt.show()
