import futu as ft
from typing import List, Dict, Any
import logging
from datetime import date, datetime, timedelta
from pydantic import ValidationError

from src.data.models import FinancialProfile

logger = logging.getLogger(__name__)

def get_db_path(db_name: str = "futu_financials.duckdb") -> str:
    """Constructs the full path to the DuckDB database file."""
    return f"data/{db_name}"

def get_report_period_date(query_date: date, quarter: str) -> date:
    """
    Calculates the standardized report period end date based on a query date and quarter.
    """
    current_year = query_date.year
    
    if quarter == 'annual':
        return date(current_year - 1, 12, 31)
    elif quarter == 'q1':
        year = current_year if query_date.month >= 4 else current_year - 1
        return date(year, 3, 31)
    elif quarter == 'interim':
        year = current_year if query_date.month >= 7 else current_year - 1
        return date(year, 6, 30)
    elif quarter == 'q3':
        year = current_year if query_date.month >= 10 else current_year - 1
        return date(year, 9, 30)
    else: # Fallback for unrecognized quarters
        return query_date

class FutuDummyStockData:
    """A helper class to create a stock data object from a dictionary."""
    def __init__(self, data_dict):
        self.__dict__.update(data_dict)
    
    def __getattr__(self, name):
        return self.__dict__.get(name)

# Mapping from Futu API field names to our FinancialProfile model field names
FUTU_FIELD_MAPPING = {
    'pe_ttm': 'price_to_earnings_ratio',
    'pb_rate': 'price_to_book_ratio',
    'ps_ttm': 'price_to_sales_ratio',
    'pcf_ttm': 'price_to_cashflow_ratio',
    'market_val': 'market_cap',
    'net_profit': 'net_income',
    'net_profit_rate': 'net_margin',
    'gross_profit_rate': 'gross_margin',
    'return_on_equity_rate': 'return_on_equity',
    'debt_asset_rate': 'debt_to_assets',
    'sum_of_business': 'revenue',
}

def futu_data_to_financial_profile(data: dict, report_date_str: str, quarter: str) -> List[FinancialProfile]:
    """Converts a dictionary of Futu data into a list of FinancialProfile Pydantic models."""
    profiles = []
    for stock_code, values in data.items():
        # Ensure the ticker includes the market prefix (e.g., 'US.MSFT')
        values['ticker'] = stock_code
        values['report_period'] = report_date_str
        values['period'] = quarter
        
        # Map the 'stock_name' from the raw data to the 'name' field in the model
        if 'stock_name' in values:
            values['name'] = values.pop('stock_name')

        try:
            profiles.append(FinancialProfile(**values))
        except ValidationError as e:
            logger.error(f"Pydantic validation error for stock {stock_code}: {e}")
    return profiles


def get_report_period_date(current_date: date, quarter: str) -> date:
    """
    Calculates the standardized report period end date based on a query date and quarter.
    """
    current_year = current_date.year
    
    if quarter == 'annual':
        return date(current_year - 1, 12, 31)
    elif quarter == 'q1':
        year = current_year if current_date.month >= 4 else current_year - 1
        return date(year, 3, 31)
    elif quarter == 'interim':
        year = current_year if current_date.month >= 7 else current_year - 1
        return date(year, 6, 30)
    elif quarter == 'q3':
        year = current_year if current_date.month >= 10 else current_year - 1
        return date(year, 9, 30)
    else: # Fallback for unrecognized quarters
        return current_date 