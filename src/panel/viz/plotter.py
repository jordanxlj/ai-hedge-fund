import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import logging

logger = logging.getLogger(__name__)

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
            # Ensure the time column is in datetime format before setting as index
            df_ticker['time'] = pd.to_datetime(df_ticker['time'])
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
        legend_map = {
            'BBL': ('BB_Low', 'green'),
            'BBM': ('BB_Middle', 'blue'),
            'BBU': ('BB_High', 'red')
        }

        for col in df_ticker.columns:
            for prefix, (legend_name, color) in legend_map.items():
                if col.startswith(prefix):
                    fig.add_trace(go.Scatter(x=df_ticker.index, y=df_ticker[col], mode='lines', name=legend_name, line=dict(color=color)), row=1, col=1)
                    break # Move to next column once a match is found
            else: # No break
                if col.startswith(('sma_', 'ema_', 'wma_')):
                    fig.add_trace(go.Scatter(x=df_ticker.index, y=df_ticker[col], mode='lines', name=col), row=1, col=1)

        if add_volume:
            fig.add_trace(go.Bar(x=df_ticker.index, y=df_ticker['volume'], name='Volume'), row=2, col=1)

        # Custom X-axis formatting to mimic TradingView
        monthly_ticks = df_ticker.index.to_series().dt.to_period('M').unique()
        tick_vals = [df_ticker.index[df_ticker.index.to_series().dt.to_period('M') == p][0] for p in monthly_ticks]
        
        tick_text = []
        last_year = None
        for date in tick_vals:
            if date.year != last_year:
                tick_text.append(date.strftime('%Y'))
                last_year = date.year
            else:
                tick_text.append(date.strftime('%b'))

        # Custom X-axis formatting to mimic TradingView
        df_ticker.index = pd.to_datetime(df_ticker.index)
        monthly_ticks = df_ticker.index.to_series().dt.to_period('M').unique()
        tick_vals = [df_ticker.index[df_ticker.index.to_series().dt.to_period('M') == p][0] for p in monthly_ticks]
        
        tick_text = []
        last_year = None
        for date in tick_vals:
            if date.year != last_year:
                tick_text.append(date.strftime('%Y'))
                last_year = date.year
            else:
                tick_text.append(date.strftime('%b'))

        # Custom X-axis formatting to mimic TradingView
        df_ticker.index = pd.to_datetime(df_ticker.index)
        monthly_ticks = df_ticker.index.to_series().dt.to_period('M').unique()
        tick_vals = [df_ticker.index[df_ticker.index.to_series().dt.to_period('M') == p][0] for p in monthly_ticks]
        tick_text = []
        last_year = None
        for date in tick_vals:
            if date.year != last_year:
                tick_text.append(date.strftime('%Y'))
                last_year = date.year
            else:
                tick_text.append(date.strftime('%b'))

        fig.update_layout(
            title=title or f'{ticker} Candlestick Chart',
            yaxis_title='Price',
            xaxis_rangeslider_visible=False,
            legend_title='Indicators',
            xaxis_tickvals=tick_vals,
            xaxis_ticktext=tick_text
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