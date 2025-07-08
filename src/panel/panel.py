
import dash
from dash import dash_table

app = dash.Dash(__name__)
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from src.panel.data.data_loader import DataLoader
from src.data.db import get_database_api

# Initialize the app and the data loader
app = dash.Dash(__name__)
db_api = get_database_api("duckdb", db_path="data/test.duckdb")
data_loader = DataLoader(db_api)

# Fetch and prepare data for the heatmap
try:
    db_api.connect()
    plate_summary_data = data_loader.get_plate_summary()
finally:
    db_api.close()

# Set a fixed color scale range for better visualization
fixed_cmax = 0.03  # Corresponds to +3%
fixed_cmin = -0.03 # Corresponds to -3%

# --- Create the Treemap ---
treemap_fig = go.Figure(go.Treemap(
    labels=plate_summary_data['plate_name'],
    parents=["" for _ in plate_summary_data['plate_name']], # All have the same parent
    values=plate_summary_data['total_volume'],
    customdata=plate_summary_data['avg_price_change'],
    texttemplate="%{label}<br>%{customdata:.2%}",
    marker_colors=plate_summary_data['avg_price_change'],
    marker_colorscale=[[0, 'green'], [0.4, 'darkgreen'], [0.5, 'white'], [0.6, 'darkred'], [1, 'red']],
))

treemap_fig.update_traces(marker_cmin=fixed_cmin, marker_cmax=fixed_cmax)

treemap_fig.update_layout(
)

app.layout = html.Div([
    html.H1("Stock Plate Dashboard", style={'margin-top': '10px', 'margin-bottom': '10px'}),
    html.Div(id='treemap-container', children=[
        dcc.Graph(id='plate-treemap', figure=treemap_fig, style={'height': '95vh'})
    ], style={'width': '100%', 'display': 'inline-block', 'vertical-align': 'top'}),
    html.Div(id='plate-detail-view', style={'display': 'none'}, children=[
        html.H2(id='plate-detail-title'),
        dcc.Graph(id='plate-detail-chart'),
        html.Div(id='ticker-fundamentals-table')
    ])
])

@app.callback(
    [
        Output('treemap-container', 'style'),
        Output('plate-detail-view', 'style')
    ],
    [Input('plate-treemap', 'clickData')]
)
def toggle_views(clickData):
    if clickData:
        # Shrink treemap and show details
        return {'width': '60%', 'display': 'inline-block', 'vertical-align': 'top'}, \
               {'width': '39%', 'display': 'inline-block', 'vertical-align': 'top'}
    else:
        # Full-width treemap, hide details
        return {'width': '100%', 'display': 'inline-block'}, {'display': 'none'}

@app.callback(
    [
        Output('plate-detail-title', 'children'),
        Output('ticker-fundamentals-table', 'children')
    ],
    [Input('plate-treemap', 'clickData')]
)
def update_plate_details(clickData):
    if clickData is None:
        return "Select a Plate", None

    plate_name = clickData['points'][0]['label']
    
    # Fetch details for the selected plate
    try:
        db_api.connect()
        plate_details_df = data_loader.get_plate_details(plate_name)
    finally:
        db_api.close()

    # Create the data table
    table = dash_table.DataTable(
        id='plate-detail-table',
        columns=[{"name": i, "id": i} for i in plate_details_df.columns],
        data=plate_details_df.to_dict('records'),
        sort_action="native",
        filter_action="native"
    )

    return f"Details for {plate_name}", table

if __name__ == '__main__':
    app.run(debug=True)
