import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from abc import ABC, abstractmethod
import plotly.graph_objects as go
from plotly.subplots import make_subplots

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

        # Create a figure with 2 subplots
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                            vertical_spacing=0.05, subplot_titles=('Price and Signals', 'Equity Curve'),
                            specs=[[{"secondary_y": True}], [{"secondary_y": False}]],
                            row_heights=[0.7, 0.3])

        # Candlestick chart for price
        fig.add_trace(go.Candlestick(x=data.index, open=data['open'], high=data['high'], 
                                     low=data['low'], close=data['close'], name='Price',
                                     increasing_line_color='#2ca02c', decreasing_line_color='#ff0000',
                                     increasing_fillcolor='#2ca02c', decreasing_fillcolor='#ff0000'), row=1, col=1)

        # Add Bollinger Bands if they exist
        if 'BBU_20_2.0' in data.columns and 'BBL_20_2.0' in data.columns and 'BBM_20_2.0' in data.columns:
            fig.add_trace(go.Scatter(x=data.index, y=data['BBU_20_2.0'], mode='lines', line=dict(color='blue', width=1), name='Upper BB'), row=1, col=1)
            fig.add_trace(go.Scatter(x=data.index, y=data['BBL_20_2.0'], mode='lines', line=dict(color='blue', width=1), name='Lower BB', fill='tonexty', fillcolor='rgba(0,0,255,0.1)'), row=1, col=1)
            fig.add_trace(go.Scatter(x=data.index, y=data['BBM_20_2.0'], mode='lines', line=dict(color='blue', width=1, dash='dash'), name='Middle BB'), row=1, col=1)

        # Buy and sell signals
        buy_signals = signals[signals['signal'] == 1]
        sell_signals = signals[signals['signal'] == -1]

        # Add Buy labels below the low
        for idx, row in buy_signals.iterrows():
            fig.add_annotation(
                x=idx, y=row['low'] * 0.90,  # Adjusted to be further below to avoid overlap
                text='Buy',
                showarrow=False,
                font=dict(color='white', size=12),
                bgcolor='#2ca02c',
                bordercolor='#2ca02c',
                borderwidth=1,
                borderpad=4,
                opacity=0.8,
                row=1, col=1
            )

        # Add Sell labels above the high
        for idx, row in sell_signals.iterrows():
            fig.add_annotation(
                x=idx, y=row['high'] * 1.1,  # Adjusted to be further above to avoid overlap
                text='Sell',
                showarrow=False,
                font=dict(color='white', size=12),
                bgcolor='#ff0000',
                bordercolor='#ff0000',
                borderwidth=1,
                borderpad=4,
                opacity=0.8,
                row=1, col=1
            )

        # Equity curve plot
        fig.add_trace(go.Scatter(x=backtest_results['equity_curve'].index, y=backtest_results['equity_curve'].values,
                                 mode='lines', name='Equity Curve', line=dict(color='#9467bd', width=2)), row=2, col=1)

        # Update layout with enhanced styling
        fig.update_layout(
            title_text='Strategy Backtest',
            title_font=dict(size=24, color='#333333', family='Arial Black'),
            xaxis_rangeslider_visible=False,
            template='plotly_white',  # Clean white theme
            height=800,
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
            margin=dict(l=50, r=50, t=80, b=50),
            hovermode='x unified',
            plot_bgcolor='#f8f9fa',
            paper_bgcolor='#ffffff'
        )

        # Update axes
        fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#e0e0e0', zeroline=False)
        fig.update_yaxes(title_text="Price", row=1, col=1, showgrid=True, gridwidth=1, gridcolor='#e0e0e0')
        fig.update_yaxes(title_text="Equity", row=2, col=1, showgrid=True, gridwidth=1, gridcolor='#e0e0e0')

        # Add annotations for performance metrics
        metrics_text = f"Total Return: {backtest_results['total_return']*100:.2f}%<br>" \
                       f"Max Drawdown: {backtest_results['max_drawdown']*100:.2f}%<br>" \
                       f"Win Rate: {backtest_results['win_rate']*100:.2f}%"
        fig.add_annotation(
            text=metrics_text,
            xref="paper", yref="paper",
            x=0.98, y=0.98,
            showarrow=False,
            font=dict(size=12, color='#333333'),
            align='right',
            bgcolor='rgba(255,255,255,0.8)',
            bordercolor='#cccccc',
            borderwidth=1,
            borderpad=10
        )

        fig.show()
