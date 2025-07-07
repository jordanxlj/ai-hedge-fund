import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

class Plotter:
    """
    A class for creating interactive financial plots using Plotly.
    """

    def plot_candlestick(self, df: pd.DataFrame, ticker: str, title: str = None, add_volume: bool = True):
        """
        Creates an interactive candlestick chart for a specific ticker with technical indicators.

        :param df: DataFrame containing the financial data and indicator columns.
        :param ticker: The ticker symbol to plot.
        :param title: The title of the chart.
        :param add_volume: Whether to add a volume subplot.
        """
        df_ticker = df[df['ticker'] == ticker].copy()
        if df_ticker.empty:
            raise ValueError(f"Ticker '{ticker}' not found in DataFrame.")

        if 'time' in df_ticker.columns:
            df_ticker.set_index('time', inplace=True)

        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                              vertical_spacing=0.03, subplot_titles=('OHLC', 'Volume'), 
                              row_width=[0.2, 0.7])

        # Candlestick trace
        fig.add_trace(go.Candlestick(x=df_ticker.index,
                                       open=df_ticker['open'],
                                       high=df_ticker['high'],
                                       low=df_ticker['low'],
                                       close=df_ticker['close'],
                                       name='OHLC'), row=1, col=1)

        # Add technical indicator lines
        for col in df_ticker.columns:
            if col.startswith(('sma_', 'ema_', 'wma_', 'BBM_', 'BBU_', 'BBL_')):
                fig.add_trace(go.Scatter(x=df_ticker.index, y=df_ticker[col], mode='lines', name=col), row=1, col=1)

        if add_volume:
            fig.add_trace(go.Bar(x=df_ticker.index, y=df_ticker['volume'], name='Volume'), row=2, col=1)

        fig.update_layout(
            title=title or f'{ticker} Candlestick Chart with Indicators',
            yaxis_title='Price',
            xaxis_rangeslider_visible=False,
            legend_title='Indicators'
        )
        fig.show()

    def plot_line(self, df: pd.DataFrame, tickers: list, price_col: str = 'close', title: str = None):
        """
        Creates an interactive line chart for one or more tickers.

        :param df: DataFrame containing the financial data.
        :param tickers: A list of ticker symbols to plot.
        :param price_col: The column to use for the line plot.
        :param title: The title of the chart.
        """
        fig = go.Figure()

        for ticker in tickers:
            df_ticker = df[df['ticker'] == ticker]
            if not df_ticker.empty:
                fig.add_trace(go.Scatter(x=df_ticker['time'], y=df_ticker[price_col], mode='lines', name=ticker))

        fig.update_layout(
            title=title or f'Line Chart: {price_col}',
            xaxis_title='Time',
            yaxis_title=f'Price ({price_col})',
            legend_title='Tickers'
        )
        fig.show()