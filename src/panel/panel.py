import argparse
import logging
import dash
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
        self.app = dash.Dash(__name__)
        self.db_api = db_api
        self.data_loader = DataLoader(self.db_api)
        self.app.config.suppress_callback_exceptions = True
        self._build_layout()
        self.register_callbacks()

    def _build_layout(self):
        self.app.layout = html.Div([
            html.H1("Stock Plate Dashboard", style={'margin-top': '0px', 'margin-bottom': '10px'}),
            html.Div([
                html.Div([
                    html.Button("热力图", id="heatmap-button", n_clicks=0, className="menu-button"),
                    html.Button("列表", id="list-button", n_clicks=0, className="menu-button"),
                ], style={'display': 'flex', 'margin-right': '20px'}),
                html.Div([
                    html.Button("Last Day", id="day-1-button", n_clicks=0, className="menu-button"),
                    html.Button("5 Days", id="day-5-button", n_clicks=0, className="menu-button"),
                    html.Button("10 Days", id="day-10-button", n_clicks=0, className="menu-button"),
                    html.Button("30 Days", id="day-30-button", n_clicks=0, className="menu-button"),
                ], style={'display': 'flex'}),
            ], style={'display': 'flex', 'align-items': 'center', 'padding': '5px', 'border': '1px solid #ddd', 'border-radius': '5px'}),
            dcc.Store(id='view-type-store', data='heatmap'),
            dcc.Store(id='period-days-store', data=1),
            html.Div(id='main-container', children=[])
        ])

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

    def display_main_view(self, view_type, days_back):
        raw_data = self.data_loader.get_plate_summary(days_back=days_back)
        plate_summary_data = self.calculate_plate_summary(raw_data, days_back)

        if view_type == 'heatmap':
            fixed_cmax = 0.03
            fixed_cmin = -0.03

            treemap_fig = go.Figure(go.Treemap(
                labels=plate_summary_data['plate_name'],
                parents=["" for _ in plate_summary_data['plate_name']],
                values=plate_summary_data['total_volume'],
                customdata=plate_summary_data.apply(lambda row: [row['avg_price_change'], row['total_volume_str']], axis=1),
                texttemplate="%{label}<br>%{customdata[0]:.2%}",
                hovertemplate='<b>%{label}</b><br>Avg. Change: %{customdata[0]:.2%}<br>Total Volume: %{customdata[1]}<extra></extra>',
                marker_colors=plate_summary_data['avg_price_change'],
                marker_colorscale=[[0, 'green'], [0.4, 'darkgreen'], [0.5, 'white'], [0.6, 'darkred'], [1, 'red']],
            ))
            treemap_fig.update_traces(marker_cmin=fixed_cmin, marker_cmax=fixed_cmax)
            treemap_fig.update_layout(
                yaxis_showgrid=False, yaxis_zeroline=False, yaxis_ticks='', yaxis_showticklabels=False,
                xaxis_showgrid=False, xaxis_zeroline=False, xaxis_ticks='', xaxis_showticklabels=False,
                plot_bgcolor='#f0f0f0'
            )
            return dcc.Graph(id='plate-treemap', figure=treemap_fig, style={'height': '95vh'})
        elif view_type == 'list':
            return dash_table.DataTable(
                id='plate-list-table',
                columns=[
                    {"name": "板块名称", "id": "plate_name"},
                    {"name": "平均涨跌幅(%)", "id": "avg_price_change", "type": "numeric", "format": {"specifier": ".2%"}},
                    {"name": "总成交额(亿)", "id": "total_volume_str"},
                ],
                data=plate_summary_data.to_dict('records'),
                sort_action="native",
                filter_action="native",
                style_header={
                    'backgroundColor': 'rgb(30, 30, 30)',
                    'color': 'white',
                    'fontWeight': 'bold'
                },
                style_cell={
                    'textAlign': 'left',
                    'padding': '5px',
                    'border': '1px solid grey'
                },
                style_data_conditional=[
                    {
                        'if': {'row_index': 'odd'},
                        'backgroundColor': 'rgb(248, 248, 248)'
                    },
                    {
                        'if': {'filter_query': '{avg_price_change} > 0', 'column_id': 'avg_price_change'},
                        'color': 'green'
                    },
                    {
                        'if': {'filter_query': '{avg_price_change} < 0', 'column_id': 'avg_price_change'},
                        'color': 'red'
                    }
                ],
                style_table={'border': '1px solid grey'}
            )

    def register_callbacks(self):
        @self.app.callback(
            Output('view-type-store', 'data'),
            [Input('heatmap-button', 'n_clicks'), Input('list-button', 'n_clicks')]
        )
        def update_view_type(heatmap_clicks, list_clicks):
            ctx = dash.callback_context
            if not ctx.triggered:
                return 'heatmap'
            button_id = ctx.triggered[0]['prop_id'].split('.')[0]
            if button_id == 'list-button':
                return 'list'
            else:
                return 'heatmap'

        @self.app.callback(
            Output('period-days-store', 'data'),
            [Input('day-1-button', 'n_clicks'), Input('day-5-button', 'n_clicks'), 
             Input('day-10-button', 'n_clicks'), Input('day-30-button', 'n_clicks')]
        )
        def update_period_days(day1, day5, day10, day30):
            ctx = dash.callback_context
            if not ctx.triggered:
                return 1
            button_id = ctx.triggered[0]['prop_id'].split('.')[0]
            if button_id == 'day-5-button':
                return 5
            elif button_id == 'day-10-button':
                return 10
            elif button_id == 'day-30-button':
                return 30
            else:
                return 1

        @self.app.callback(
            Output('main-container', 'children'),
            [Input('view-type-store', 'data'), Input('period-days-store', 'data')]
        )
        def display_main_view_callback(view_type, days_back):
            return self.display_main_view(view_type, days_back)

        @self.app.callback(
            Output('main-container', 'children', allow_duplicate=True),
            [Input('plate-treemap', 'clickData')],
            [State('period-days-store', 'data')],
            prevent_initial_call=True
        )
        def display_details_from_heatmap(clickData, days_back):
            if clickData is None:
                return dash.no_update

            plate_name = clickData['points'][0]['label']
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
                html.Button('Back to Main View', id='back-button', n_clicks=0),
                html.H2(f"Details for {plate_name}"),
                dash_table.DataTable(
                    columns=columns,
                    data=plate_details_df.to_dict('records'),
                    sort_action="native",
                    filter_action="native",
                    style_header={
                        'backgroundColor': 'rgb(30, 30, 30)',
                        'color': 'white',
                        'fontWeight': 'bold'
                    },
                    style_cell={
                        'textAlign': 'left',
                        'padding': '5px',
                        'border': '1px solid grey'
                    },
                    style_data_conditional=[
                        {
                            'if': {'row_index': 'odd'},
                            'backgroundColor': 'rgb(248, 248, 248)'
                        },
                        {
                            'if': {'filter_query': '{price_change_pct} > 0', 'column_id': 'price_change_pct'},
                            'color': 'green'
                        },
                        {
                            'if': {'filter_query': '{price_change_pct} < 0', 'column_id': 'price_change_pct'},
                            'color': 'red'
                        },
                        {
                            'if': {'filter_query': '{pe_ttm} < 15 and {pe_ttm} > 0', 'column_id': 'pe_ttm'},
                            'backgroundColor': 'rgba(255, 255, 0, 0.3)'
                        },
                        {
                            'if': {'filter_query': '{pb_mrq} < 1 and {pb_mrq} > 0', 'column_id': 'pb_mrq'},
                            'backgroundColor': 'rgba(255, 255, 0, 0.3)'
                        },
                        {
                            'if': {'filter_query': '{revenue_cagr_3y} > 0.15', 'column_id': 'revenue_cagr_3y'},
                            'backgroundColor': 'rgba(255, 255, 0, 0.3)'
                        },
                        {
                            'if': {'filter_query': '{net_income_cagr_3y} > 0.15', 'column_id': 'net_income_cagr_3y'},
                            'backgroundColor': 'rgba(255, 255, 0, 0.3)'
                        },
                        {
                            'if': {'filter_query': '{gross_margin} > 0.40', 'column_id': 'gross_margin'},
                            'backgroundColor': 'rgba(255, 255, 0, 0.3)'
                        },
                        {
                            'if': {'filter_query': '{net_margin} > 0.10', 'column_id': 'net_margin'},
                            'backgroundColor': 'rgba(255, 255, 0, 0.3)'
                        }
                    ],
                    style_table={'border': '1px solid grey'}
                )
            ])

        @self.app.callback(
            Output('main-container', 'children', allow_duplicate=True),
            [Input('back-button', 'n_clicks')],
            [State('view-type-store', 'data'), State('period-days-store', 'data')],
            prevent_initial_call=True
        )
        def go_back(n_clicks, view_type, days_back):
            if n_clicks > 0:
                return self.display_main_view(view_type, days_back)
            return dash.no_update

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