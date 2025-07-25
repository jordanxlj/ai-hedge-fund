import argparse
import logging
import os

import dash
import dash_bootstrap_components as dbc
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
    """
    A Dash-based dashboard for stock plate analysis.
    """
    def __init__(self, db_api):
        self.app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
        self.db_api = db_api
        self.data_loader = DataLoader(self.db_api)
        self.app.config.suppress_callback_exceptions = True
        self._build_layout()
        self.register_callbacks()

    def _build_layout(self):
        """
        Builds the initial layout of the dashboard.
        """
        self.app.layout = dbc.Container([
            dbc.Row([
                dbc.Col(html.H1("Stock Panel", className="text-center mb-3"), width=12)
            ]),
            dbc.Row([
                dbc.Col(self._create_radio_items('primary-view-selector', [
                    {'label': '板块', 'value': 'plate'},
                    {'label': '个股', 'value': 'stock'},
                ], 'plate'), width=4),
                dbc.Col(self._create_radio_items('secondary-view-selector', [
                    {'label': '热力图', 'value': 'heatmap'},
                    {'label': '列表', 'value': 'list'},
                ], 'heatmap'), width=4),
                dbc.Col(self._create_radio_items('period-selector', [
                    {'label': 'Last Day', 'value': 1},
                    {'label': '5 Days', 'value': 5},
                    {'label': '10 Days', 'value': 10},
                    {'label': '30 Days', 'value': 30}
                ], 1), width=4),
            ]),
            dcc.Store(id='view-state-store', data={
                'view_mode': 'main',
                'primary_view': 'plate',
                'secondary_view': 'heatmap',
                'days_back': 1,
                'selected_plate': None
            }),
            html.Div(id='main-container', className="p-0 bg-light rounded shadow")
        ], fluid=True, className="p-2")

    def _create_radio_items(self, item_id, options, default_value):
        """
        Helper method to create radio items with consistent styling.
        """
        return dcc.RadioItems(
            id=item_id,
            options=options,
            value=default_value,
            labelStyle={'display': 'inline-block', 'margin-right': '20px'},
            className="mb-3"
        )

    def __enter__(self):
        self.db_api.connect(read_only=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db_api.close()

    def get_plate_cluster(self, plate_name: str) -> str:
        """Returns the plate cluster for a given plate name."""
        if plate_name in ['医疗设备及用品', '医疗及医学美容服务', '医药外包概念', '医疗保健', '中医药', '中医药概念', '药品', '药品分销', '生物技术', '生物医药', '生物医药B类股', '创新药概念', 'AI医疗概念股', '互联网医疗', '医美概念股']:
            return '医疗与健康'
        if plate_name in ['地产投资', '地产发展商', '楼宇建造', '内房股', '内地物业管理股', '物业服务及管理', '建筑材料', '建材水泥股', '地产代理', '房地产基金', '房地产投资信托', '养老概念']:
            return '地产与建筑'
        if plate_name in ['工业零件及器材', '重型机械', '重型机械股', '特殊化工用品', '钢铁', '其他金��及矿物', '铝', '铜', '煤炭股', '印刷及包装', '电力设备股', '半导体设备与材料']:
            return '工业与制造'
        if plate_name in ['油气设备与服务', '油气生产商', '石油与天然气', '新能源物料', '非传统/可再生能源', '风电股', '光伏太阳能股', '氢能源概念股', '电池', '能源储存装置', '环保', '环保工程', '水务', '水务股', '燃气供应', '燃气股', '有色金属', '石油股', '页岩气']:
            return '资源与环保'
        if plate_name in ['消费电子产品', '家具', '服装', '服装零售商', '纺织品及布料', '鞋类', '珠宝钟表', '奢侈品品牌股', '餐饮', '食品股', '包装食品', '食品添加剂', '农产品', '乳制品', '酒精饮料', '非酒精饮料', '啤酒', '超市及便利店', '百货业股', '其他零售商', '线上零售商', '国内零售股', '体育用品']:
            return '消费品与零售'
        if plate_name in ['OLED概念', 'LED', '电讯设备', '应用软件', '电脑及周边器材', '芯片股', '半导体', '5G概念', 'ChatGPT概念股', '元宇宙概念', '机器人概念股', '智能驾驶概念股', '小米概念', '苹果概念', '虚拟现实', 'DeepSeek概念股']:
            return '科技与创新'
        if plate_name in ['公共运输', '航运及港口', '港口运输股', '物流', '航空服务', '航空货运及物流', '公路及铁路股', '高铁基建股', '一带一路', '重型基建']:
            return '交通运输与物流'
        if plate_name in ['职业教育', 'K12教育', '民办高教', '内地教育股', '在线教育', '教育', '其他支援服务', '采购及供应链管理']:
            return '教育与服务'
        if plate_name in ['内银股', '银行', '保险', '保险股', '证券及经纪', '中资券商股', '投资及资产管理', '信贷', '其他金融', '高股息概念', '稳定币概念', '加密货币概念股', '香港本地银行股', '蚂蚁金服概念']:
            return '金融与投资'
        if plate_name in ['赌场及博彩', '博彩股', '影视娱乐', '影视股', '玩具及消闲用品', '旅游及观光', '酒店及度假村', '消闲及文娱设施']:
            return '娱乐与休闲'
        if plate_name in ['汽车零件', '汽车零售商', '汽车经销商', '新能源车企', '特斯拉概念股', '综合车企股', '商业用车及货车']:
            return '汽车与配件'
        if plate_name in ['MSCI中国大陆小型股', 'MSCI中国香港小型股', '红海危机概念', '双十一', '港股通(沪)', '红筹股', '蓝筹股']:
            return '其他概念股'
        if plate_name in ['云办公', '短视频概念股', '明星科网股', '抖音概念股', '腾讯概念', '阿里概念股', '云计算', '手游股', '游戏软件', 'SaaS概念']:
            return '互联网'
        return '其他'

    def _calculate_summary(self, df: pd.DataFrame, days_back: int, group_col: str, change_col: str, weight_col: str = 'market_cap') -> pd.DataFrame:
        """
        Generic method to calculate summary for plate or stock data.
        """
        if df.empty:
            return pd.DataFrame(columns=[group_col, change_col, 'total_volume', 'total_volume_str', 'plate_cluster'])

        df['time'] = pd.to_datetime(df['time'])
        df = df.sort_values(by=['ticker', 'time'])

        first_day = df.loc[df.groupby('ticker')['time'].idxmin()]
        last_day = df.loc[df.groupby('ticker')['time'].idxmax()]

        cols_to_merge = ['ticker', group_col, 'close', weight_col]
        if 'plate_cluster' in df.columns:
            cols_to_merge.append('plate_cluster')

        merged_df = pd.merge(
            first_day[cols_to_merge],
            last_day[['ticker', 'close']],
            on='ticker',
            suffixes=['_start', '_end']
        )

        merged_df['price_change'] = (merged_df['close_end'] - merged_df['close_start']) / merged_df['close_start']

        last_n_days_df = df.groupby('ticker').tail(days_back).copy()
        last_n_days_df['turnover'] = last_n_days_df['close'] * last_n_days_df['volume']
        total_turnover = last_n_days_df.groupby('ticker')['turnover'].sum().reset_index()

        final_df = pd.merge(merged_df, total_turnover, on='ticker')
        final_df.rename(columns={'turnover': 'total_volume'}, inplace=True)

        def weighted_avg(group):
            return (group['price_change'] * group[weight_col]).sum() / group[weight_col].sum()

        group_by_cols = [group_col]
        if 'plate_cluster' in final_df.columns:
            group_by_cols.append('plate_cluster')

        summary = final_df.groupby(group_by_cols).apply(lambda x: pd.Series({
            change_col: weighted_avg(x),
            'total_volume': x['total_volume'].sum()
        })).reset_index()

        summary = summary[summary['total_volume'] >= 1e8]
        summary = summary.sort_values(by='total_volume', ascending=False).head(200)

        summary['total_volume_str'] = (summary['total_volume'] / 1e8).round(2).astype(str) + '亿'

        return summary

    def calculate_plate_summary(self, df: pd.DataFrame, days_back: int) -> pd.DataFrame:
        """
        Calculates the plate summary using the generic method.
        """
        return self._calculate_summary(df, days_back, 'plate_name', 'avg_price_change', 'market_cap')

    def calculate_stock_summary(self, df: pd.DataFrame, days_back: int) -> pd.DataFrame:
        """
        Calculates the stock summary using the generic method.
        """
        return self._calculate_summary(df, days_back, 'stock_name', 'price_change', 'market_cap')

    def register_callbacks(self):
        """
        Registers all Dash callbacks for interactivity.
        """
        @self.app.callback(
            [Output('main-container', 'children'), Output('view-state-store', 'data')],
            [Input('primary-view-selector', 'value'),
             Input('secondary-view-selector', 'value'),
             Input('period-selector', 'value')]
        )
        def display_main_content(primary_view, secondary_view, days_back):
            if primary_view == 'plate':
                raw_data = self.data_loader.get_plate_summary(days_back=days_back)
                summary_data = self.calculate_plate_summary(raw_data, days_back)
                if secondary_view == 'heatmap':
                    children = dcc.Graph(id='plate-treemap', figure=self.create_treemap_figure(summary_data, 'plate_name', 'avg_price_change'), style={'height': '80vh'})
                elif secondary_view == 'list':
                    children = self.create_summary_datatable('plate-list-table', summary_data, "板块名称", "plate_name", "平均涨跌幅(%)", "avg_price_change")
            elif primary_view == 'stock':
                logger.info("Fetching data for stock view...")
                # 1. Get all stock-plate mappings
                all_mappings = self.data_loader.get_stock_plate_mappings()

                # 2. Calculate plate sizes
                plate_sizes = all_mappings.groupby('plate_name').size().reset_index(name='num_stocks')

                # 3. Find the smallest plate for each stock
                merged_mappings = pd.merge(all_mappings, plate_sizes, on='plate_name')
                smallest_plates = merged_mappings.loc[merged_mappings.groupby('ticker')['num_stocks'].idxmin()]

                # 4. Get stock summary data
                raw_stock_data = self.data_loader.get_stock_summary(days_back=days_back)

                # 5. Merge with smallest plate data
                raw_data = pd.merge(raw_stock_data, smallest_plates[['ticker', 'plate_name']], on='ticker', how='left')
                raw_data['plate_cluster'] = raw_data['plate_name'].apply(self.get_plate_cluster)
                logger.info(f"Merged data shape: {raw_data.shape}")

                summary_data = self.calculate_stock_summary(raw_data, days_back)
                
                if secondary_view == 'heatmap':
                    logger.info("Generating clustered stock heatmap...")
                    # Ensure there are no NaN parents and filter them out
                    summary_data = summary_data.dropna(subset=['plate_cluster'])
                    
                    # Create a hierarchical dataframe for the treemap
                    df_clusters = pd.DataFrame({
                        'id': summary_data['plate_cluster'].unique(),
                        'parent': '',
                        'label': summary_data['plate_cluster'].unique(),
                        'value': 0,
                        'color': 0,
                    })

                    df_stocks = pd.DataFrame({
                        'id': summary_data['stock_name'],
                        'parent': summary_data['plate_cluster'],
                        'label': summary_data['stock_name'],
                        'value': summary_data['total_volume'],
                        'color': summary_data['price_change'],
                    })

                    df_treemap = pd.concat([df_clusters, df_stocks], ignore_index=True)
                    
                    customdata = pd.concat([
                        pd.Series([[0, ''] for _ in df_clusters.index]), # Placeholder for clusters
                        summary_data.apply(lambda row: [row['price_change'], row['total_volume_str']], axis=1).reset_index(drop=True)
                    ], ignore_index=True)

                    fig = go.Figure(go.Treemap(
                        ids=df_treemap['id'],
                        labels=df_treemap['label'],
                        parents=df_treemap['parent'],
                        values=df_treemap['value'],
                        marker_colors=df_treemap['color'],
                        marker_colorscale=[[0, '#ff0000'], [0.4, '#8b0000'], [0.5, '#ffffff'], [0.6, '#006400'], [1, '#2ca02c']],
                        marker_cmin=-0.03,
                        marker_cmax=0.03,
                        texttemplate="%{label}<br>%{customdata[0]:.2%}",
                        hovertemplate='<b>%{label}</b><br>Change: %{customdata[0]:.2%}<br>Total Volume: %{customdata[1]}<extra></extra>',
                        root_color="lightgrey"
                    ))
                    fig.data[0].customdata = customdata
                    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
                    
                    children = dcc.Graph(id='stock-treemap', figure=fig, style={'height': '80vh'})

                elif secondary_view == 'list':
                    children = self.create_summary_datatable('stock-list-table', summary_data, "股票名称", "stock_name", "涨跌幅(%)", "price_change")

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
                if state['primary_view'] == 'plate':
                    raw_data = self.data_loader.get_plate_summary(days_back=state['days_back'])
                    summary_data = self.calculate_plate_summary(raw_data, state['days_back'])
                    if state['secondary_view'] == 'heatmap':
                        return dcc.Graph(id='plate-treemap', figure=self.create_treemap_figure(summary_data, 'plate_name', 'avg_price_change'), style={'height': '80vh'})
                    elif state['secondary_view'] == 'list':
                        return self.create_summary_datatable('plate-list-table', summary_data, "板块名称", "plate_name", "平均涨跌幅(%)", "avg_price_change")
                elif state['primary_view'] == 'stock':
                    logger.info("Fetching data for stock view...")
                    # 1. Get all stock-plate mappings
                    all_mappings = self.data_loader.get_stock_plate_mappings()

                    # 2. Calculate plate sizes
                    plate_sizes = all_mappings.groupby('plate_name').size().reset_index(name='num_stocks')

                    # 3. Find the smallest plate for each stock
                    merged_mappings = pd.merge(all_mappings, plate_sizes, on='plate_name')
                    smallest_plates = merged_mappings.loc[merged_mappings.groupby('ticker')['num_stocks'].idxmin()]

                    # 4. Get stock summary data
                    raw_stock_data = self.data_loader.get_stock_summary(days_back=state['days_back'])

                    # 5. Merge with smallest plate data
                    raw_data = pd.merge(raw_stock_data, smallest_plates[['ticker', 'plate_name']], on='ticker', how='left')
                    raw_data['plate_cluster'] = raw_data['plate_name'].apply(self.get_plate_cluster)
                    logger.info(f"Merged data shape: {raw_data.shape}")

                    summary_data = self.calculate_stock_summary(raw_data, state['days_back'])
                    
                    if state['secondary_view'] == 'heatmap':
                        logger.info("Generating clustered stock heatmap...")
                        # Ensure there are no NaN parents and filter them out
                        summary_data = summary_data.dropna(subset=['plate_cluster'])
                        
                        # Create a hierarchical dataframe for the treemap
                        df_clusters = pd.DataFrame({
                            'id': summary_data['plate_cluster'].unique(),
                            'parent': '',
                            'label': summary_data['plate_cluster'].unique(),
                            'value': 0,
                            'color': 0,
                        })

                        df_stocks = pd.DataFrame({
                            'id': summary_data['stock_name'],
                            'parent': summary_data['plate_cluster'],
                            'label': summary_data['stock_name'],
                            'value': summary_data['total_volume'],
                            'color': summary_data['price_change'],
                        })

                        df_treemap = pd.concat([df_clusters, df_stocks], ignore_index=True)
                        
                        customdata = pd.concat([
                            pd.Series([[0, ''] for _ in df_clusters.index]), # Placeholder for clusters
                            summary_data.apply(lambda row: [row['price_change'], row['total_volume_str']], axis=1).reset_index(drop=True)
                        ], ignore_index=True)

                        fig = go.Figure(go.Treemap(
                            ids=df_treemap['id'],
                            labels=df_treemap['label'],
                            parents=df_treemap['parent'],
                            values=df_treemap['value'],
                            marker_colors=df_treemap['color'],
                            marker_colorscale=[[0, '#2ca02c'], [0.4, '#006400'], [0.5, '#ffffff'], [0.6, '#8b0000'], [1, '#ff0000']],
                            marker_cmin=-0.03,
                            marker_cmax=0.03,
                            texttemplate="%{label}<br>%{customdata[0]:.2%}",
                            hovertemplate='<b>%{label}</b><br>Change: %{customdata[0]:.2%}<br>Total Volume: %{customdata[1]}<extra></extra>',
                            root_color="lightgrey"
                        ))
                        fig.data[0].customdata = customdata
                        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
                        
                        return dcc.Graph(id='stock-treemap', figure=fig, style={'height': '80vh'})

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
            marker_colorscale=[[0, '#2ca02c'], [0.4, '#006400'], [0.5, '#ffffff'], [0.6, '#8b0000'], [1, '#ff0000']],
        ))
        treemap_fig.update_traces(marker_cmin=fixed_cmin, marker_cmax=fixed_cmax)
        treemap_fig.update_layout(
            yaxis_showgrid=False, yaxis_zeroline=False, yaxis_ticks='', yaxis_showticklabels=False,
            xaxis_showgrid=False, xaxis_zeroline=False, xaxis_ticks='', xaxis_showticklabels=False,
            plot_bgcolor='#f8f9fa',
            margin=dict(l=0, r=0, t=0, b=0)
        )
        return treemap_fig

    def create_summary_datatable(self, table_id, df, name_col_label, name_col_id, change_col_label, change_col_id):
        """
        Creates a styled DataTable for summary data.
        """
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
                {'if': {'row_index': 'odd'}, 'backgroundColor': '#f8f9fa'},
                {'if': {'filter_query': f'{{{change_col_id}}} > 0', 'column_id': change_col_id}, 'color': '#dc3545', 'fontWeight': 'bold'},
                {'if': {'filter_query': f'{{{change_col_id}}} < 0', 'column_id': change_col_id}, 'color': '#28a745', 'fontWeight': 'bold'},
                {'if': {'column_id': name_col_id}, 'cursor': 'pointer', 'color': '#007bff', 'textDecoration': 'underline'}
            ],
            style_table={'border': '1px solid #dee2e6', 'borderRadius': '5px', 'overflow': 'hidden'}
        )

    def render_details_view(self, plate_name, days_back):
        """
        Renders the details view for a selected plate.
        """
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
            html.Button('Back to Main View', id='back-button', n_clicks=0, className="btn btn-primary mb-3"),
            html.H2(f"Details for {plate_name}", className="text-primary"),
            dash_table.DataTable(
                columns=columns,
                data=plate_details_df.to_dict('records'),
                sort_action="native",
                filter_action="native",
                style_header={'backgroundColor': '#343a40', 'color': 'white', 'fontWeight': 'bold', 'textAlign': 'center'},
                style_cell={'textAlign': 'left', 'padding': '10px', 'border': '1px solid #dee2e6', 'fontSize': '14px'},
                style_data_conditional=[
                    {'if': {'row_index': 'odd'}, 'backgroundColor': '#f8f9fa'},
                    {'if': {'filter_query': '{price_change_pct} > 0', 'column_id': 'price_change_pct'}, 'color': '#dc3545', 'fontWeight': 'bold'},
                    {'if': {'filter_query': '{price_change_pct} < 0', 'column_id': 'price_change_pct'}, 'color': '#28a745', 'fontWeight': 'bold'},
                    {'if': {'filter_query': '{pe_ttm} < 15 and {pe_ttm} > 0', 'column_id': 'pe_ttm'}, 'backgroundColor': 'rgba(255, 193, 7, 0.3)'},
                    {'if': {'filter_query': '{pb_mrq} < 1 and {pb_mrq} > 0', 'column_id': 'pb_mrq'}, 'backgroundColor': 'rgba(255, 193, 7, 0.3)'},
                    {'if': {'filter_query': '{revenue_cagr_3y} > 0.15', 'column_id': 'revenue_cagr_3y'}, 'backgroundColor': 'rgba(40, 167, 69, 0.3)'},
                    {'if': {'filter_query': '{net_income_cagr_3y} > 0.15', 'column_id': 'net_income_cagr_3y'}, 'backgroundColor': 'rgba(40, 167, 69, 0.3)'},
                    {'if': {'filter_query': '{gross_margin} > 0.40', 'column_id': 'gross_margin'}, 'backgroundColor': 'rgba(40, 167, 69, 0.3)'},
                    {'if': {'filter_query': '{net_margin} > 0.10', 'column_id': 'net_margin'}, 'backgroundColor': 'rgba(40, 167, 69, 0.3)'}
                ],
                style_table={'border': '1px solid #dee2e6', 'borderRadius': '5px', 'overflow': 'hidden'}
            )
        ], className="p-3 bg-white rounded shadow")

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