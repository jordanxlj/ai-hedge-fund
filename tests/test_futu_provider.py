import pytest
from unittest.mock import MagicMock, patch
from src.data.provider.futu_provider import FutuDataProvider
from src.data.models import FinancialProfile
from src.data.db.base import DatabaseAPI

@pytest.fixture
def mock_db_api():
    """Fixture to create a mock DatabaseAPI."""
    mock_api = MagicMock(spec=DatabaseAPI)
    mock_api.table_exists.return_value = True
    return mock_api

@pytest.fixture
def futu_provider(mock_db_api):
    """Fixture to create a FutuDataProvider with a mock DB API."""
    with patch('src.data.provider.futu_provider.ft.OpenQuoteContext', MagicMock()):
        provider = FutuDataProvider(db_api=mock_db_api)
    return provider

def test_get_financial_metrics_success(futu_provider, mock_db_api):
    """Test get_financial_metrics successfully retrieves data."""
    # Arrange
    ticker = "US.AAPL"
    end_date = "2023-12-31"
    expected_profiles = [
        FinancialProfile(ticker=ticker, name="Apple Inc.", report_period="2023-12-31", period="annual", revenue=100.0)
    ]
    mock_db_api.query_to_models.return_value = expected_profiles

    # Act
    profiles = futu_provider.get_financial_metrics(ticker, end_date)

    # Assert
    assert profiles == expected_profiles
    mock_db_api.connect.assert_called_once()
    mock_db_api.table_exists.assert_called_once()
    mock_db_api.query_to_models.assert_called_once()
    mock_db_api.close.assert_called_once()

def test_get_financial_metrics_table_not_exists(futu_provider, mock_db_api):
    """Test get_financial_metrics when the table does not exist."""
    # Arrange
    mock_db_api.table_exists.return_value = False

    # Act
    profiles = futu_provider.get_financial_metrics("US.AAPL", "2023-12-31")

    # Assert
    assert profiles == []
    mock_db_api.connect.assert_called_once()
    mock_db_api.table_exists.assert_called_once()
    mock_db_api.query_to_models.assert_not_called()
    mock_db_api.close.assert_called_once()

def test_is_available(futu_provider, mock_db_api):
    """Test the is_available method."""
    # Arrange
    mock_db_api.connect.return_value = True

    # Act
    available = futu_provider.is_available()

    # Assert
    assert available is True
    mock_db_api.connect.assert_called_once()
    mock_db_api.close.assert_called_once()

def test_is_available_failure(futu_provider, mock_db_api):
    """Test is_available when connection fails."""
    # Arrange
    mock_db_api.connect.side_effect = Exception("Connection failed")

    # Act
    available = futu_provider.is_available()

    # Assert
    assert available is False
