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
        """
        df = self.prepare_data(data)
        signals = self.generate_signals(df)

        # All-in strategy: dynamically calculate volume and track equity
        equity = initial_capital
        shares_held = 0
        trade_log = []
        equity_curve = []

        for i in range(len(df)):
            signal = signals['signal'].iloc[i]
            price = df['close'].iloc[i]
            time = df.index[i]

            if signal == 1 and equity > 0:  # Buy signal
                shares_to_buy = equity / price
                shares_held = shares_to_buy
                equity = 0  # All capital is now in the asset

                trade_log.append({
                    'Time': time,
                    'Signal': 'BUY',
                    'Price': f"{price:.2f}",
                    'Volume': f"{shares_held:.2f}",
                    'Profit': ''
                })

            elif signal == -1 and shares_held > 0:  # Sell signal
                sell_value = shares_held * price
                profit = sell_value - initial_capital # Simple profit calculation
                equity = sell_value
                shares_held = 0

                trade_log.append({
                    'Time': time,
                    'Signal': 'SELL',
                    'Price': f"{price:.2f}",
                    'Volume': f"{shares_held:.2f}",
                    'Profit': f"{profit:.2f}"
                })
            
            # Update equity curve
            current_value = (shares_held * price) + equity
            equity_curve.append(current_value)

        df['equity'] = equity_curve
        total_return = (df['equity'].iloc[-1] / initial_capital) - 1
        max_drawdown = (df['equity'] / df['equity'].cummax() - 1).min()
        win_rate = (pd.Series([float(t['Profit']) for t in trade_log if t['Profit']]) > 0).mean()

        # Add Total Assets to trade log
        for trade in trade_log:
            trade['Total Assets'] = f"{df.loc[trade['Time'], 'equity']:.2f}"

        return {
            'total_return': total_return,
            'max_drawdown': max_drawdown,
            'win_rate': win_rate,
            'equity_curve': df['equity'],
            'trade_log': pd.DataFrame(trade_log),
            'signals': signals
        }

    def visualize(self, data: pd.DataFrame):
        """
        Visualize the strategy's signals and equity curve.
        """
        df = self.prepare_data(data)
        backtest_results = self.backtest(df.copy(), initial_capital=100000.0)

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
        plotter.plot_signals(backtest_results['signals'], subplot=1)

        # Visualize the strategy's specific indicators
        self.visualize_strategy(df, plotter)

        # Plot the equity curve on the second subplot
        plotter.plot_line(backtest_results['equity_curve'].to_frame(name='equity'), 'equity', subplot=2, name='Equity Curve', color='blue', width=2)

        # Create and plot the trade log table
        if not backtest_results['trade_log'].empty:
            plotter.plot_table(backtest_results['trade_log'], subplot=3)

        # Show the figure
        plotter.show('Strategy Backtest', ['Price', 'Equity', 'Trades'])