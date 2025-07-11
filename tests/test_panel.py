import pytest
import pandas as pd
from unittest.mock import MagicMock
from dash.dependencies import Output
from src.panel.panel import Panel

@pytest.fixture
def mock_db_api():
    """Fixture for a mock database API."""
    return MagicMock()

@pytest.fixture
def panel_instance(mock_db_api):
    """Fixture for a Panel instance with a mock DB API."""
    return Panel(mock_db_api)

@pytest.fixture
def sample_stock_data():
    """Creates a sample DataFrame for stock data."""
    data = {
        'ticker': ['AAPL', 'AAPL', 'GOOG', 'GOOG'],
        'time': pd.to_datetime(['2025-01-01', '2025-01-02', '2025-01-01', '2025-01-02']),
        'close': [100, 110, 2000, 2100],
        'volume': [1e8, 1.1e8, 0.5e8, 0.6e8],
        'market_cap': [2e12, 2.1e12, 1.5e12, 1.6e12],
        'stock_name': ['Apple', 'Apple', 'Google', 'Google']
    }
    return pd.DataFrame(data)

@pytest.fixture
def sample_plate_mappings_data():
    """Creates a sample DataFrame for plate mappings."""
    data = {
        'ticker': ['AAPL', 'GOOG', 'MSFT'],
        'plate_name': ['Tech', 'Tech', 'Tech'],
        'plate_cluster': ['Technology', 'Technology', 'Technology']
    }
    return pd.DataFrame(data)

@pytest.fixture
def sample_plate_data():
    """Creates a sample DataFrame for plate data."""
    data = {
        'ticker': ['AAPL', 'AAPL', 'MSFT', 'MSFT'],
        'time': pd.to_datetime(['2025-01-01', '2025-01-02', '2025-01-01', '2025-01-02']),
        'close': [100, 110, 300, 310],
        'volume': [1e8, 1.1e8, 0.8e8, 0.9e8],
        'market_cap': [2e12, 2.1e12, 1.8e12, 1.9e12],
        'plate_name': ['Tech', 'Tech', 'Tech', 'Tech']
    }
    return pd.DataFrame(data)

def test_get_plate_cluster(panel_instance):
    assert panel_instance.get_plate_cluster('医疗设备及用品') == '医疗与健康'
    assert panel_instance.get_plate_cluster('地产投资') == '地产与建筑'
    assert panel_instance.get_plate_cluster('some_other_plate') == '其他'

def test_calculate_stock_summary(panel_instance, sample_stock_data):
    summary = panel_instance.calculate_stock_summary(sample_stock_data, 1)
    assert not summary.empty
    assert 'stock_name' in summary.columns
    assert 'price_change' in summary.columns
    assert len(summary) == 2

def test_calculate_plate_summary(panel_instance, sample_plate_data):
    summary = panel_instance.calculate_plate_summary(sample_plate_data, 1)
    assert not summary.empty
    assert 'plate_name' in summary.columns
    assert 'avg_price_change' in summary.columns
    assert len(summary) == 1

def test_calculate_summary_empty(panel_instance):
    summary = panel_instance._calculate_summary(pd.DataFrame(), 1, 'plate_name', 'avg_price_change')
    assert summary.empty

def test_create_radio_items(panel_instance):
    radio_items = panel_instance._create_radio_items('test-id', [{'label': 'Test', 'value': 'test'}], 'test')
    assert radio_items.id == 'test-id'
    assert radio_items.value == 'test'

def test_context_manager(panel_instance):
    with panel_instance as p:
        p.db_api.connect.assert_called_with(read_only=True)
    p.db_api.close.assert_called_once()

def test_display_main_content_stock_heatmap(panel_instance, sample_stock_data, sample_plate_mappings_data):
    # Mock the data loader methods
    panel_instance.data_loader = MagicMock()
    panel_instance.data_loader.get_stock_summary.return_value = sample_stock_data
    panel_instance.data_loader.get_stock_plate_mappings.return_value = sample_plate_mappings_data

    # Call the function that contains the logic to be tested
    # In a real Dash app, this would be a callback. Here, we simulate the call.
    # This is a simplified test to check the data processing logic.
    # A full test would require a running Dash app.
    with panel_instance.app.server.test_request_context():
        callback_key = '..main-container.children...view-state-store.data..'
        outputs_list = [
            {'id': 'main-container', 'property': 'children'},
            {'id': 'view-state-store', 'property': 'data'}
        ]
        result = panel_instance.app.callback_map[callback_key]['callback'](
            'stock', 'heatmap', 1, outputs_list=outputs_list
        )
        import json
        response_data = json.loads(result)
        assert response_data['response']['main-container']['children'] is not None
        assert response_data['response']['view-state-store']['data']['primary_view'] == 'stock'

def test_display_main_content_plate_heatmap(panel_instance, sample_plate_data):
    # Mock the data loader methods
    panel_instance.data_loader = MagicMock()
    panel_instance.data_loader.get_plate_summary.return_value = sample_plate_data

    # Call the function that contains the logic to be tested
    with panel_instance.app.server.test_request_context():
        callback_key = '..main-container.children...view-state-store.data..'
        outputs_list = [
            {'id': 'main-container', 'property': 'children'},
            {'id': 'view-state-store', 'property': 'data'}
        ]
        result = panel_instance.app.callback_map[callback_key]['callback'](
            'plate', 'heatmap', 1, outputs_list=outputs_list
        )
        import json
        response_data = json.loads(result)
        assert response_data['response']['main-container']['children'] is not None
        assert response_data['response']['view-state-store']['data']['primary_view'] == 'plate'

def test_display_plate_details_from_heatmap(panel_instance):
    with panel_instance.app.server.test_request_context():
        callback_key = '..main-container.children@6ec63c7c0ae3d76937dbe15eab2099c8566b63a1f6b601337cc2dde5fd495302...view-state-store.data@6ec63c7c0ae3d76937dbe15eab2099c8566b63a1f6b601337cc2dde5fd495302..'
        outputs_list = [
            {'id': 'main-container', 'property': 'children'},
            {'id': 'view-state-store', 'property': 'data'}
        ]
        panel_instance.data_loader.get_plate_details.return_value = pd.DataFrame({'ticker': [], 'name': []})
        result = panel_instance.app.callback_map[callback_key]['callback'](
            {'points': [{'label': 'Test Plate'}]}, {'days_back': 1}, outputs_list=outputs_list
        )
        import json
        response_data = json.loads(result)
        assert response_data['response']['view-state-store']['data']['selected_plate'] == 'Test Plate'

def test_display_plate_details_from_list(panel_instance):
    with panel_instance.app.server.test_request_context():
        callback_key = '..main-container.children@621154554c8d9e3ce660b0573b0499e1f4ccf18c970d462cfdc59f292acd8766...view-state-store.data@621154554c8d9e3ce660b0573b0499e1f4ccf18c970d462cfdc59f292acd8766..'
        outputs_list = [
            {'id': 'main-container', 'property': 'children'},
            {'id': 'view-state-store', 'property': 'data'}
        ]
        result = panel_instance.app.callback_map[callback_key]['callback'](
            {'row': 0, 'column': 0}, {'days_back': 1}, [{'plate_name': 'Test Plate'}], outputs_list=outputs_list
        )
        import json
        response_data = json.loads(result)
        assert response_data['response']['view-state-store']['data']['selected_plate'] == 'Test Plate'

def test_go_back(panel_instance):
    with panel_instance.app.server.test_request_context():
        callback_key = 'view-state-store.data@0d977e805e4dcaeed789cb11509bb1ccada0db3052876479c48ef44784eedd50'
        outputs_list = [{'id': 'view-state-store', 'property': 'data'}]
        result = panel_instance.app.callback_map[callback_key]['callback'](1, {'view_mode': 'details'}, outputs_list=outputs_list)
        import json
        response_data = json.loads(result)
        assert response_data['response']['view-state-store']['data']['view_mode'] == 'main'

def test_render_details_view(panel_instance):
    panel_instance.data_loader = MagicMock()
    panel_instance.data_loader.get_plate_details.return_value = pd.DataFrame()
    details_view = panel_instance.render_details_view('Test Plate', 1)
    assert details_view.children[1].children == 'Details for Test Plate'

def test_create_treemap_figure_empty(panel_instance):
    fig = panel_instance.create_treemap_figure(pd.DataFrame(columns=['plate_name', 'avg_price_change', 'total_volume', 'total_volume_str']), 'plate_name', 'avg_price_change')
    assert fig is not None

def test_run(panel_instance):
    panel_instance.app.run = MagicMock()
    panel_instance.run(debug=False)
    panel_instance.app.run.assert_called_with(debug=False)
