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
            vertical_spacing=0.05,
            subplot_titles=subplot_titles,
            specs=[[{"secondary_y": True}], [{"secondary_y": False}]],
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
                name='OHLC',
                increasing_line_color='#2ca02c', decreasing_line_color='#ff0000',
                increasing_fillcolor='#2ca02c', decreasing_fillcolor='#ff0000'
            ),
            row=row,
            col=1
        )

    def plot_bar(self, data: pd.DataFrame, column: str, row: int, name: str = None, color: str = None, width: int = None):
        """
        Adds a bar chart to a specified subplot.

        :param data: A DataFrame containing the data.
        :param column: The name of the column to plot.
        :param row: The subplot row number (starting from 1).
        :param name: An optional legend name.
        :param color: An optional color for the bars.
        :param width: An optional width for the bars.
        """
        self.fig.add_trace(
            go.Bar(
                x=data.index,
                y=data[column],
                name=name or column,
                marker_color=color,
                width=width
            ),
            row=row,
            col=1
        )

    def plot_line(self, data: pd.DataFrame, column: str, row: int, name: str = None, color: str = None, width: int = None, dash: str = 'solid', fill: str = None, fillcolor: str = None):
        """
        Adds a line chart to a specified subplot.

        :param data: A DataFrame containing the data.
        :param column: The name of the column to plot.
        :param row: The subplot row number (starting from 1).
        :param name: An optional legend name.
        :param color: An optional color for the line.
        :param width: An optional width for the line.
        :param dash: An optional dash style for the line (e.g., 'solid', 'dash', 'dot').
        :param fill: An optional fill style for the area under the line (e.g., 'tozeroy', 'tonexty').
        :param fillcolor: An optional color for the fill.
        """
        self.fig.add_trace(
            go.Scatter(
                x=data.index,
                y=data[column],
                mode='lines',
                name=name,
                line=dict(color=color, width=width, dash=dash),
                fill=fill,
                fillcolor=fillcolor
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

        # Add Buy labels below the low
        for idx, row in buy_signals.iterrows():
            self.fig.add_annotation(
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
            self.fig.add_annotation(
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

    def show(self, title: str, yaxis_titles: list):
        """
        Displays the figure.
        """
        # Update layout with enhanced styling
        self.fig.update_layout(
            title_text=title,
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
        self.fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#e0e0e0', zeroline=False)
        for index, yaxis_title in enumerate(yaxis_titles):
            self.fig.update_yaxes(title_text=yaxis_title, row=index, col=1, showgrid=True, gridwidth=1, gridcolor='#e0e0e0')
        self.fig.show()

# 示例用法
if __name__ == "__main__":
    # 假设数据
    dates = pd.date_range('2023-01-01', periods=10, freq='D')
    data = pd.DataFrame({
        'open': [100, 102, 101, 103, 105, 107, 106, 108, 110, 109],
        'high': [102, 103, 102, 104, 106, 108, 107, 109, 111, 110],
        'low': [99, 101, 100, 102, 104, 106, 105, 107, 109, 108],
        'close': [101, 102, 101, 103, 105, 107, 106, 108, 110, 109],
        'sma': [101, 101.5, 101.7, 102, 103, 104, 105, 106, 107, 108]
    }, index=dates)
    signals = pd.DataFrame({
        'signal': [0, 1, 0, 0, -1, 0, 0, 1, 0, -1],
        'low': data['low'],
        'high': data['high']
    }, index=dates)

    # 创建 Plotter 实例
    plotter = Plotter(num_subplots=2, subplot_titles=['价格', '指标'], row_heights=[0.7, 0.3])

    # 在第一个子图上绘制K线图和信号
    plotter.plot_candlestick(data, row=1)
    plotter.plot_signals(signals, row=1)

    # 在第一个子图上叠加SMA曲线
    plotter.plot_line(data, 'sma', row=1, name='SMA')

    # 在第二个子图上绘制收盘价曲线
    plotter.plot_line(data, 'close', row=2, name='Close')

    # 显示图表
    plotter.show()

