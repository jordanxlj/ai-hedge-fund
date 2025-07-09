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
            html.H1("Stock Plate Dashboard", style={'margin-top': '10px', 'margin-bottom': '10px'}),
            dcc.RadioItems(
                id='period-selector',
                options=[
                    {'label': 'Last Day', 'value': 1},
                    {'label': '5 Days', 'value': 5},
                    {'label': '10 Days', 'value': 10},
                    {'label': '30 Days', 'value': 30}
                ],
                value=1,
                labelStyle={'display': 'inline-block', 'margin-right': '20px'},
                style={'width': '50%', 'margin': 'auto'}
            ),
            html.Div(id='main-container', children=[
                dcc.Graph(id='plate-treemap', style={'height': '95vh'})
            ])
        ])

    def __enter__(self):
        self.db_api.connect()
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

    def register_callbacks(self):
        @self.app.callback(
            Output('main-container', 'children'),
            [Input('period-selector', 'value'), Input('plate-treemap', 'clickData')],
            [State('main-container', 'children')]
        )
        def display_content(days_back, clickData, current_children):
            ctx = dash.callback_context
            trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]

            if trigger_id == 'period-selector' or not clickData:
                raw_data = self.data_loader.get_plate_summary(days_back=days_back)
                plate_summary_data = self.calculate_plate_summary(raw_data, days_back)
                
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
                    title='Plate Heatmap',
                    yaxis_showgrid=False, yaxis_zeroline=False, yaxis_ticks='', yaxis_showticklabels=False,
                    xaxis_showgrid=False, xaxis_zeroline=False, xaxis_ticks='', xaxis_showticklabels=False,
                    plot_bgcolor='#f0f0f0'
                )
                return dcc.Graph(id='plate-treemap', figure=treemap_fig, style={'height': '95vh'})

            if trigger_id == 'plate-treemap':
                plate_name = clickData['points'][0]['label']
                plate_details_df = self.data_loader.get_plate_details(plate_name, days_back)

                columns = []
                for i in plate_details_df.columns:
                    if i == "涨跌幅":
                        columns.append({"name": "涨跌幅(%)", "id": i, "type": "numeric", "format": {"specifier": ".2%"}})
                    elif i == "涨跌":
                        columns.append({"name": "涨跌(元)", "id": i, "type": "numeric", "format": {"specifier": ".2f"}})
                    elif i == "现价":
                        columns.append({"name": "现价(元)", "id": i, "type": "numeric", "format": {"specifier": ".2f"}})
                    elif i == "成交额":
                        columns.append({"name": "成交额(亿)", "id": i, "type": "numeric", "format": {"specifier": ".2f"}})
                    elif i == "市盈率(TTM)" or i == "市净率(MRQ)":
                        columns.append({"name": i, "id": i, "type": "numeric", "format": {"specifier": ".2f"}})
                    else:
                        columns.append({"name": i, "id": i})

                detail_view = html.Div([
                    html.Button('Back to Treemap', id='back-button', n_clicks=0),
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
                                'if': {
                                    'filter_query': '{涨跌幅} > 0',
                                    'column_id': '涨跌幅'
                                },
                                'color': 'green'
                            },
                            {
                                'if': {
                                    'filter_query': '{涨跌幅} < 0',
                                    'column_id': '涨跌幅'
                                },
                                'color': 'red'
                            }
                        ],
                        style_table={'border': '1px solid grey'}
                    )
                ])
                return detail_view

            return current_children

        @self.app.callback(
            Output('main-container', 'children', allow_duplicate=True),
            [Input('back-button', 'n_clicks')],
            [State('period-selector', 'value')],
            prevent_initial_call=True
        )
        def go_back(n_clicks, days_back):
            if n_clicks > 0:
                raw_data = self.data_loader.get_plate_summary(days_back=days_back)
                plate_summary_data = self.calculate_plate_summary(raw_data, days_back)
                
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
                    title='Plate Heatmap',
                    yaxis_showgrid=False, yaxis_zeroline=False, yaxis_ticks='', yaxis_showticklabels=False,
                    xaxis_showgrid=False, xaxis_zeroline=False, xaxis_ticks='', xaxis_showticklabels=False,
                    plot_bgcolor='#f0f0f0'
                )
                return dcc.Graph(id='plate-treemap', figure=treemap_fig, style={'height': '95vh'})
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