import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

class Plotter:
    """
    A class for creating interactive financial plots using Plotly.
    Supports multiple subplots, including candlestick, bar, line, and signal plots.
    """

    def __init__(self, num_subplots: int, subplot_titles: list, row_heights: list):
        """
        Initializes the Plotter with a specified number of subplots, titles, and row heights.

        :param num_subplots: The number of subplots.
        :param subplot_titles: A list of titles for each subplot.
        :param row_heights: A list of row heights for each subplot.
        """
        self.fig = make_subplots(
            rows=num_subplots,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.02,
            subplot_titles=subplot_titles,
            row_heights=row_heights
        )

    def plot_candlestick(self, data: pd.DataFrame, row: int):
        """
        Adds a candlestick chart to a specified subplot.

        :param data: A DataFrame with 'open', 'high', 'low', and 'close' columns.
        :param row: The subplot row number (starting from 1).
        """
        self.fig.add_trace(
            go.Candlestick(
                x=data.index,
                open=data['open'],
                high=data['high'],
                low=data['low'],
                close=data['close'],
                name='OHLC'
            ),
            row=row,
            col=1
        )

    def plot_bar(self, data: pd.DataFrame, column: str, row: int, name: str = None):
        """
        Adds a bar chart to a specified subplot.

        :param data: A DataFrame containing the data.
        :param column: The name of the column to plot.
        :param row: The subplot row number (starting from 1).
        :param name: An optional legend name.
        """
        self.fig.add_trace(
            go.Bar(
                x=data.index,
                y=data[column],
                name=name or column
            ),
            row=row,
            col=1
        )

    def plot_line(self, data: pd.DataFrame, column: str, row: int, name: str = None):
        """
        Adds a line chart to a specified subplot.

        :param data: A DataFrame containing the data.
        :param column: The name of the column to plot.
        :param row: The subplot row number (starting from 1).
        :param name: An optional legend name.
        """
        self.fig.add_trace(
            go.Scatter(
                x=data.index,
                y=data[column],
                mode='lines',
                name=name or column
            ),
            row=row,
            col=1
        )

    def plot_signals(self, signals: pd.DataFrame, row: int):
        """
        Adds trading signal markers to a specified subplot.

        :param signals: A DataFrame with a 'signal' column (1 for buy, -1 for sell).
        :param row: The subplot row number (starting from 1).
        """
        buy_signals = signals[signals['signal'] == 1]
        sell_signals = signals[signals['signal'] == -1]

        # Add buy signal markers
        self.fig.add_trace(
            go.Scatter(
                x=buy_signals.index,
                y=buy_signals['low'] * 0.95,
                mode='markers',
                marker=dict(symbol='triangle-up', color='green', size=10),
                name='Buy Signal'
            ),
            row=row,
            col=1
        )

        # Add sell signal markers
        self.fig.add_trace(
            go.Scatter(
                x=sell_signals.index,
                y=sell_signals['high'] * 1.05,
                mode='markers',
                marker=dict(symbol='triangle-down', color='red', size=10),
                name='Sell Signal'
            ),
            row=row,
            col=1
        )

    def show(self):
        """
        Displays the figure.
        """
        self.fig.show()
