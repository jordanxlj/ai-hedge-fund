import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output, State
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from src.panel.data.data_loader import DataLoader
from src.data.db import get_database_api

# Initialize the app and the data loader
app = dash.Dash(__name__)
db_api = get_database_api("duckdb", db_path="data/test.duckdb")
data_loader = DataLoader(db_api)

# Store the last processed clickData to avoid infinite loops
app.config.suppress_callback_exceptions = True  # Allow dynamic layout changes

def calculate_plate_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates the plate summary from raw daily data over a period."""
    if df.empty:
        return pd.DataFrame(columns=['plate_name', 'avg_price_change', 'total_volume', 'total_volume_str'])

    df['time'] = pd.to_datetime(df['time'])
    df = df.sort_values(by=['ticker', 'time'])

    # Get first and last entry for each ticker
    first_day = df.loc[df.groupby('ticker')['time'].idxmin()]
    last_day = df.loc[df.groupby('ticker')['time'].idxmax()]

    # Merge to get start and end prices in one row per ticker
    merged_df = pd.merge(
        first_day[['ticker', 'plate_name', 'close', 'market_cap']],
        last_day[['ticker', 'close']],
        on='ticker',
        suffixes=['_start', '_end']
    )

    # Calculate price change over the period
    merged_df['price_change'] = (merged_df['close_end'] - merged_df['close_start']) / merged_df['close_start']

    # Calculate total turnover for the period
    df['turnover'] = df['close'] * df['volume']
    total_turnover = df.groupby('ticker')['turnover'].sum().reset_index()

    # Merge turnover back
    final_df = pd.merge(merged_df, total_turnover, on='ticker')
    final_df.rename(columns={'turnover': 'total_volume'}, inplace=True)

    # Calculate weighted average price change for each plate
    def weighted_avg(group):
        # Use the most recent market cap for weighting
        return (group['price_change'] * group['market_cap']).sum() / group['market_cap'].sum()

    plate_summary = final_df.groupby('plate_name').apply(lambda x: pd.Series({
        'avg_price_change': weighted_avg(x),
        'total_volume': x['total_volume'].sum()
    })).reset_index()

    # Filter and format
    plate_summary = plate_summary[plate_summary['total_volume'] >= 1e8]
    plate_summary['total_volume_str'] = (plate_summary['total_volume'] / 1e8).round(2).astype(str) + '亿'

    return plate_summary

@app.callback(
    Output('plate-treemap', 'figure'),
    [Input('period-selector', 'value')])
def update_treemap(days_back=1):
    try:
        db_api.connect()
        raw_data = data_loader.get_plate_summary(days_back=days_back)
        plate_summary_data = calculate_plate_summary(raw_data)
    finally:
        db_api.close()

    fixed_cmax = 0.03  # Corresponds to +3%
    fixed_cmin = -0.03  # Corresponds to -3%

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
    return treemap_fig

@app.callback(
    [
        Output('treemap-container', 'style'),
        Output('plate-detail-view', 'style')
    ],
    [Input('plate-treemap', 'clickData')],
    [State('treemap-container', 'style'), State('plate-detail-view', 'style')])
def toggle_views(clickData, treemap_style, detail_style):
    ctx = dash.callback_context
    if not ctx.triggered:
        return treemap_style, detail_style

    if clickData:
        return {'width': '60%', 'display': 'inline-block', 'vertical-align': 'top'}, \
               {'width': '39%', 'display': 'inline-block', 'vertical-align': 'top'}
    return {'width': '100%', 'display': 'inline-block'}, {'display': 'none'}

@app.callback(
    [
        Output('plate-detail-title', 'children'),
        Output('ticker-fundamentals-table', 'children')
    ],
    [Input('plate-treemap', 'clickData')],
    [State('plate-detail-title', 'children')])
def update_plate_details(clickData, current_title):
    if clickData is None or (current_title and "Select a Plate" in current_title):
        return "Select a Plate", None

    plate_name = clickData['points'][0]['label']
        
    try:
        db_api.connect()
        plate_details_df = data_loader.get_plate_details(plate_name)
    finally:
        db_api.close()

    table = dash_table.DataTable(
        columns=[{"name": i, "id": i} for i in plate_details_df.columns],
        data=plate_details_df.to_dict('records'),
        sort_action="native",
        filter_action="native"
    )
    return f"Details for {plate_name}", table

# Layout definition
app.layout = html.Div([
    dcc.Dropdown(
        id='period-selector',
        options=[{'label': f'{i} days', 'value': i} for i in [7, 14, 30, 90]],
        value=7,
        style={'width': '30%'}
    ),
    html.Div(id='treemap-container', children=[
        dcc.Graph(id='plate-treemap', style={'height': '95vh'})
    ], style={'width': '100%', 'height': '95vh', 'display': 'inline-block', 'vertical-align': 'top'}),
    html.Div(id='plate-detail-view', children=[
        html.H3(id='plate-detail-title', children='Select a Plate'),
        html.Div(id='ticker-fundamentals-table')
    ])
], style={'padding': '20px'})

if __name__ == '__main__':
    app.run(debug=True)