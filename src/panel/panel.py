import argparse
import logging
import dash
import dash_bootstrap_components as dbc  # 新增：引入 Bootstrap Components
import os
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output, State
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from src.panel.data.data_loader import DataLoader
from src.data.db import get_database_api
from src.utils.log_util import logger_setup as _init_logging

_init_logging()
logger = logging.getLogger(__name__)

class Panel:
    def __init__(self, db_api):
        self.app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])  # 新增：使用 Bootstrap 主题
        self.db_api = db_api
        self.data_loader = DataLoader(self.db_api)
        self.app.config.suppress_callback_exceptions = True
        self._build_layout()
        self.register_callbacks()

    def _build_layout(self):
        self.app.layout = dbc.Container([
            dbc.Row([  # 新增：使用 Row 和 Col 布局
                dbc.Col(html.H1("Stock Panel", className="text-center mb-3"), width=12)  # 美化：居中标题，添加间距
            ]),
            dbc.Row([
                dbc.Col(dcc.RadioItems(
                    id='primary-view-selector',
                    options=[
                        {'label': '板块', 'value': 'plate'},
                        {'label': '个股', 'value': 'stock'},
                    ],
                    value='plate',
                    labelStyle={'display': 'inline-block', 'margin-right': '20px'},
                    className="mb-3"  # 美化：添加间距
                ), width=4),
                dbc.Col(dcc.RadioItems(
                    id='secondary-view-selector',
                    options=[
                        {'label': '热力图', 'value': 'heatmap'},
                        {'label': '列表', 'value': 'list'},
                    ],
                    value='heatmap',
                    labelStyle={'display': 'inline-block', 'margin-right': '20px'},
                    className="mb-3"
                ), width=4),
                dbc.Col(dcc.RadioItems(
                    id='period-selector',
                    options=[
                        {'label': 'Last Day', 'value': 1},
                        {'label': '5 Days', 'value': 5},
                        {'label': '10 Days', 'value': 10},
                        {'label': '30 Days', 'value': 30}
                    ],
                    value=1,
                    labelStyle={'display': 'inline-block', 'margin-right': '20px'},
                    className="mb-3"
                ), width=4),
            ]),
            dcc.Store(id='view-state-store', data={'view_mode': 'main', 'primary_view': 'plate', 'secondary_view': 'heatmap', 'days_back': 1, 'selected_plate': None}),
            html.Div(id='main-container', className="p-3 bg-light rounded shadow")  # 美化：添加背景、圆角和阴影
        ], fluid=True, className="p-4")  # 美化：整体容器添加 padding

    def __enter__(self):
        self.db_api.connect(read_only=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db_api.close()

    def calculate_plate_summary(self, df: pd.DataFrame, days_back: int) -> pd.DataFrame:
        """Calculates the plate summary from raw daily data over a period."""
        if df.empty:
            return pd.DataFrame(columns=['plate_name', 'avg_price_change', 'total_volume', 'total_volume_str'])

        df['time'] = pd.to_datetime(df['time'])
        df = df.sort_values(by=['ticker', 'time'])

        first_day = df.loc[df.groupby('ticker')['time'].idxmin()]
        last_day = df.loc[df.groupby('ticker')['time'].idxmax()]

        merged_df = pd.merge(
            first_day[['ticker', 'plate_name', 'close', 'market_cap']],
            last_day[['ticker', 'close']],
            on='ticker',
            suffixes=['_start', '_end']
        )

        merged_df['price_change'] = (merged_df['close_end'] - merged_df['close_start']) / merged_df['close_start']
        
        # For turnover, get the last N days of data from the df
        last_n_days_df = df.groupby('ticker').tail(days_back).copy()
        last_n_days_df['turnover'] = last_n_days_df['close'] * last_n_days_df['volume']
        total_turnover = last_n_days_df.groupby('ticker')['turnover'].sum().reset_index()

        final_df = pd.merge(merged_df, total_turnover, on='ticker')
        final_df.rename(columns={'turnover': 'total_volume'}, inplace=True)

        def weighted_avg(group):
            return (group['price_change'] * group['market_cap']).sum() / group['market_cap'].sum()

        plate_summary = final_df.groupby('plate_name').apply(lambda x: pd.Series({
            'avg_price_change': weighted_avg(x),
            'total_volume': x['total_volume'].sum()
        })).reset_index()

        plate_summary = plate_summary[plate_summary['total_volume'] >= 1e8]
        plate_summary = plate_summary.sort_values(by='total_volume', ascending=False).head(100)

        plate_summary['total_volume_str'] = (plate_summary['total_volume'] / 1e8).round(2).astype(str) + '亿'

        return plate_summary

    def calculate_stock_summary(self, df: pd.DataFrame, days_back: int) -> pd.DataFrame:
        """Calculates the stock summary from raw daily data over a period."""
        if df.empty:
            return pd.DataFrame(columns=['ticker', 'stock_name', 'price_change', 'total_volume', 'total_volume_str'])

        df['time'] = pd.to_datetime(df['time'])
        df = df.sort_values(by=['ticker', 'time'])

        first_day = df.loc[df.groupby('ticker')['time'].idxmin()]
        last_day = df.loc[df.groupby('ticker')['time'].idxmax()]

        merged_df = pd.merge(
            first_day[['ticker', 'stock_name', 'close']],
            last_day[['ticker', 'close']],
            on='ticker',
            suffixes=['_start', '_end']
        )

        merged_df['price_change'] = (merged_df['close_end'] - merged_df['close_start']) / merged_df['close_start']
        
        # For turnover, get the last N days of data from the df
        last_n_days_df = df.groupby('ticker').tail(days_back).copy()
        last_n_days_df['turnover'] = last_n_days_df['close'] * last_n_days_df['volume']
        total_turnover = last_n_days_df.groupby('ticker')['turnover'].sum().reset_index()

        final_df = pd.merge(merged_df, total_turnover, on='ticker')
        final_df.rename(columns={'turnover': 'total_volume'}, inplace=True)
        
        final_df = final_df.sort_values(by='total_volume', ascending=False).head(100)
        final_df['total_volume_str'] = (final_df['total_volume'] / 1e8).round(2).astype(str) + '亿'

        return final_df

    def register_callbacks(self):
        @self.app.callback(
            [Output('main-container', 'children'), Output('view-state-store', 'data')],
            [Input('primary-view-selector', 'value'),
             Input('secondary-view-selector', 'value'),
             Input('period-selector', 'value')]
        )
        def display_main_content(primary_view, secondary_view, days_back):
            children = []
            if primary_view == 'plate':
                raw_data = self.data_loader.get_plate_summary(days_back=days_back)
                plate_summary_data = self.calculate_plate_summary(raw_data, days_back)
                if secondary_view == 'heatmap':
                    children = dcc.Graph(id='plate-treemap', figure=self.create_treemap_figure(plate_summary_data, 'plate_name', 'avg_price_change'))
                elif secondary_view == 'list':
                    children = self.create_summary_datatable('plate-list-table', plate_summary_data, "板块名称", "plate_name", "平均涨跌幅(%)", "avg_price_change")
            elif primary_view == 'stock':
                raw_data = self.data_loader.get_stock_summary(days_back=days_back)
                stock_summary_data = self.calculate_stock_summary(raw_data, days_back)
                if secondary_view == 'heatmap':
                    children = dcc.Graph(id='stock-treemap', figure=self.create_treemap_figure(stock_summary_data, 'stock_name', 'price_change'))
                elif secondary_view == 'list':
                    children = self.create_summary_datatable('stock-list-table', stock_summary_data, "股票名称", "stock_name", "涨跌幅(%)", "price_change")
            
            new_state = {'view_mode': 'main', 'primary_view': primary_view, 'secondary_view': secondary_view, 'days_back': days_back, 'selected_plate': None}
            return children, new_state

        @self.app.callback(
            [Output('main-container', 'children', allow_duplicate=True), Output('view-state-store', 'data', allow_duplicate=True)],
            [Input('plate-treemap', 'clickData')],
            [State('view-state-store', 'data')],
            prevent_initial_call=True
        )
        def display_plate_details_from_heatmap(clickData, current_state):
            if clickData is None:
                return dash.no_update, dash.no_update
            plate_name = clickData['points'][0]['label']
            new_state = current_state.copy()
            new_state['view_mode'] = 'details'
            new_state['selected_plate'] = plate_name
            details_children = self.render_details_view(plate_name, current_state['days_back'])
            return details_children, new_state

        @self.app.callback(
            [Output('main-container', 'children', allow_duplicate=True), Output('view-state-store', 'data', allow_duplicate=True)],
            [Input('plate-list-table', 'active_cell')],
            [State('view-state-store', 'data'), State('plate-list-table', 'data')],
            prevent_initial_call=True
        )
        def display_plate_details_from_list(active_cell, current_state, table_data):
            if active_cell is None:
                return dash.no_update, dash.no_update
            row_index = active_cell['row']
            plate_name = table_data[row_index]['plate_name']
            new_state = current_state.copy()
            new_state['view_mode'] = 'details'
            new_state['selected_plate'] = plate_name
            details_children = self.render_details_view(plate_name, current_state['days_back'])
            return details_children, new_state

        @self.app.callback(
            Output('view-state-store', 'data', allow_duplicate=True),
            [Input('back-button', 'n_clicks')],
            [State('view-state-store', 'data')],
            prevent_initial_call=True
        )
        def go_back(n_clicks, current_state):
            if n_clicks > 0:
                new_state = current_state.copy()
                new_state['view_mode'] = 'main'
                new_state['selected_plate'] = None
                return new_state
            return dash.no_update

        @self.app.callback(
            Output('main-container', 'children', allow_duplicate=True),
            [Input('view-state-store', 'data')],
            prevent_initial_call=True
        )
        def render_based_on_state(state):
            if state['view_mode'] == 'main':
                # Re-generate main view using state values
                if state['primary_view'] == 'plate':
                    raw_data = self.data_loader.get_plate_summary(days_back=state['days_back'])
                    summary_data = self.calculate_plate_summary(raw_data, state['days_back'])
                    if state['secondary_view'] == 'heatmap':
                        return dcc.Graph(id='plate-treemap', figure=self.create_treemap_figure(summary_data, 'plate_name', 'avg_price_change'))
                    elif state['secondary_view'] == 'list':
                        return self.create_summary_datatable('plate-list-table', summary_data, "板块名称", "plate_name", "平均涨跌幅(%)", "avg_price_change")
                elif state['primary_view'] == 'stock':
                    raw_data = self.data_loader.get_stock_summary(days_back=state['days_back'])
                    summary_data = self.calculate_stock_summary(raw_data, state['days_back'])
                    if state['secondary_view'] == 'heatmap':
                        return dcc.Graph(id='stock-treemap', figure=self.create_treemap_figure(summary_data, 'stock_name', 'price_change'))
                    elif state['secondary_view'] == 'list':
                        return self.create_summary_datatable('stock-list-table', summary_data, "股票名称", "stock_name", "涨跌幅(%)", "price_change")
            elif state['view_mode'] == 'details':
                return self.render_details_view(state['selected_plate'], state['days_back'])
            return dash.no_update

    def create_treemap_figure(self, df, labels_col, colors_col):
        fixed_cmax = 0.03
        fixed_cmin = -0.03

        treemap_fig = go.Figure(go.Treemap(
            labels=df[labels_col],
            parents=["" for _ in df[labels_col]],
            values=df['total_volume'],
            customdata=df.apply(lambda row: [row[colors_col], row['total_volume_str']], axis=1),
            texttemplate="%{label}<br>%{customdata[0]:.2%}",
            hovertemplate='<b>%{label}</b><br>Change: %{customdata[0]:.2%}<br>Total Volume: %{customdata[1]}<extra></extra>',
            marker_colors=df[colors_col],
            marker_colorscale=[[0, '#2ca02c'], [0.4, '#006400'], [0.5, '#ffffff'], [0.6, '#8b0000'], [1, '#ff0000']],  # 美化：优化颜色渐变，更柔和
        ))
        treemap_fig.update_traces(marker_cmin=fixed_cmin, marker_cmax=fixed_cmax)
        treemap_fig.update_layout(
            yaxis_showgrid=False, yaxis_zeroline=False, yaxis_ticks='', yaxis_showticklabels=False,
            xaxis_showgrid=False, xaxis_zeroline=False, xaxis_ticks='', xaxis_showticklabels=False,
            plot_bgcolor='#f8f9fa',  # 美化：更柔和的背景色
            margin=dict(l=10, r=10, t=50, b=10)  # 美化：调整���距
        )
        return treemap_fig

    def create_summary_datatable(self, table_id, df, name_col_label, name_col_id, change_col_label, change_col_id):
        return dash_table.DataTable(
            id=table_id,
            columns=[
                {"name": name_col_label, "id": name_col_id},
                {"name": change_col_label, "id": change_col_id, "type": "numeric", "format": {"specifier": ".2%"}},
                {"name": "总成交额(亿)", "id": "total_volume_str"},
            ],
            data=df.to_dict('records'),
            sort_action="native",
            filter_action="native",
            style_header={
                'backgroundColor': '#343a40',  # 美化：深灰色头部
                'color': 'white',
                'fontWeight': 'bold',
                'textAlign': 'center'  # 美化：居中对齐
            },
            style_cell={
                'textAlign': 'left',
                'padding': '10px',  # 美化：增加间距
                'border': '1px solid #dee2e6',  # 美化：浅灰边框
                'fontSize': '14px'  # 美化：字体大小
            },
            style_data_conditional=[
                {
                    'if': {'row_index': 'odd'},
                    'backgroundColor': '#f8f9fa'
                },
                {
                    'if': {'filter_query': f'{{{change_col_id}}} > 0', 'column_id': change_col_id},
                    'color': '#28a745',
                    'fontWeight': 'bold'
                },
                {
                    'if': {'filter_query': f'{{{change_col_id}}} < 0', 'column_id': change_col_id},
                    'color': '#dc3545',
                    'fontWeight': 'bold'
                },
                {
                    'if': {'column_id': name_col_id},
                    'cursor': 'pointer',
                    'color': '#007bff',
                    'textDecoration': 'underline'
                }
            ],
            style_table={'border': '1px solid #dee2e6', 'borderRadius': '5px', 'overflow': 'hidden'}  # 美化：表格圆角和溢出隐藏
        )

    def render_details_view(self, plate_name, days_back):
        plate_details_df = self.data_loader.get_plate_details(plate_name, days_back)

        columns = [
            {"name": "代码", "id": "ticker"},
            {"name": "名称", "id": "name"},
            {"name": "现价(元)", "id": "price", "type": "numeric", "format": {"specifier": ".2f"}},
            {"name": "涨跌幅(%)", "id": "price_change_pct", "type": "numeric", "format": {"specifier": ".2%"}},
            {"name": "涨跌(元)", "id": "price_change", "type": "numeric", "format": {"specifier": ".2f"}},
            {"name": "成交额(亿)", "id": "turnover", "type": "numeric", "format": {"specifier": ".2f"}},
            {"name": "市盈率(TTM)", "id": "pe_ttm", "type": "numeric", "format": {"specifier": ".2f"}},
            {"name": "市净率(MRQ)", "id": "pb_mrq", "type": "numeric", "format": {"specifier": ".2f"}},
            {"name": "市值(亿)", "id": "market_cap", "type": "numeric", "format": {"specifier": ".2f"}},
            {"name": "ROE", "id": "roe", "type": "numeric", "format": {"specifier": ".2%"}},
            {"name": "ROIC", "id": "roic", "type": "numeric", "format": {"specifier": ".2%"}},
            {"name": "毛利率", "id": "gross_margin", "type": "numeric", "format": {"specifier": ".2%"}},
            {"name": "净利率", "id": "net_margin", "type": "numeric", "format": {"specifier": ".2%"}},
            {"name": "营收CAGR(3年)", "id": "revenue_cagr_3y", "type": "numeric", "format": {"specifier": ".2%"}},
            {"name": "净利润CAGR(3年)", "id": "net_income_cagr_3y", "type": "numeric", "format": {"specifier": ".2%"}},
            {"name": "是否最小板块", "id": "is_smallest_plate"},
        ]

        return html.Div([
            html.Button('Back to Main View', id='back-button', n_clicks=0, className="btn btn-primary mb-3"),  # 美化：使用 Bootstrap 按钮样式
            html.H2(f"Details for {plate_name}", className="text-primary"),  # 美化：蓝色标题
            dash_table.DataTable(
                columns=columns,
                data=plate_details_df.to_dict('records'),
                sort_action="native",
                filter_action="native",
                style_header={
                    'backgroundColor': '#343a40',
                    'color': 'white',
                    'fontWeight': 'bold',
                    'textAlign': 'center'
                },
                style_cell={
                    'textAlign': 'left',
                    'padding': '10px',
                    'border': '1px solid #dee2e6',
                    'fontSize': '14px'
                },
                style_data_conditional=[
                    {
                        'if': {'row_index': 'odd'},
                        'backgroundColor': '#f8f9fa'
                    },
                    {
                        'if': {'filter_query': '{price_change_pct} > 0', 'column_id': 'price_change_pct'},
                        'color': '#28a745',
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'filter_query': '{price_change_pct} < 0', 'column_id': 'price_change_pct'},
                        'color': '#dc3545',
                        'fontWeight': 'bold'
                    },
                    {
                        'if': {'filter_query': '{pe_ttm} < 15 and {pe_ttm} > 0', 'column_id': 'pe_ttm'},
                        'backgroundColor': 'rgba(255, 193, 7, 0.3)'  # 美化：黄色高亮
                    },
                    {
                        'if': {'filter_query': '{pb_mrq} < 1 and {pb_mrq} > 0', 'column_id': 'pb_mrq'},
                        'backgroundColor': 'rgba(255, 193, 7, 0.3)'
                    },
                    {
                        'if': {'filter_query': '{revenue_cagr_3y} > 0.15', 'column_id': 'revenue_cagr_3y'},
                        'backgroundColor': 'rgba(40, 167, 69, 0.3)'  # 美化：绿色高亮
                    },
                    {
                        'if': {'filter_query': '{net_income_cagr_3y} > 0.15', 'column_id': 'net_income_cagr_3y'},
                        'backgroundColor': 'rgba(40, 167, 69, 0.3)'
                    },
                    {
                        'if': {'filter_query': '{gross_margin} > 0.40', 'column_id': 'gross_margin'},
                        'backgroundColor': 'rgba(40, 167, 69, 0.3)'
                    },
                    {
                        'if': {'filter_query': '{net_margin} > 0.10', 'column_id': 'net_margin'},
                        'backgroundColor': 'rgba(40, 167, 69, 0.3)'
                    }
                ],
                style_table={'border': '1px solid #dee2e6', 'borderRadius': '5px', 'overflow': 'hidden'}
            )
        ], className="p-3 bg-white rounded shadow")  # 美化：细节视图容器添加白色背景、圆角和阴影

    # 其他方法和回调函数保持不变（register_callbacks 等）

    def run(self, debug=True):
        try:
            self.app.run(debug=debug)
        except Exception as e:
            logger.error(f"run panel exception: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Financial Panel.")
    parser.add_argument("--db_path", type=str, required=True, help="Path to the DuckDB database file.")

    args = parser.parse_args()
    abs_db_path = os.path.abspath(args.db_path)
    db_api = get_database_api("duckdb", db_path=abs_db_path)

    with db_api:
        panel = Panel(db_api)
        panel.run()
