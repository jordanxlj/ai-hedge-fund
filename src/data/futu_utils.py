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
    'basic_eps' : 'earnings_per_share',
    'cash_and_cash_equivalents' : 'cash_and_equivalents',
    'current_asset_ratio' : 'current_assets_ratio',
    'current_debt_ratio' : 'current_liabilities_ratio',
    'debt_asset_rate' : 'debt_to_assets',
    'ebit_growth_rate' : 'ebit_growth',
    'ebit_ttm' : 'ebit',
    'eps_growth_rate' : 'earnings_per_share_growth',
    'financial_cost_rate' : 'financial_cost_rate',
    'fixed_asset_turnover' : 'fixed_asset_turnover',
    'float_share' : 'outstanding_shares',
    'gross_profit_rate' : 'gross_margin',
    'inventory_turnover' : 'inventory_turnover',
    'market_val' : 'market_cap',
    'net_profit' : 'net_income',
    'net_profit_cash_cover_ttm' : 'cash_conversion_ratio',
    'net_profit_rate' : 'net_margin',
    'net_profix_growth' : 'earnings_growth',
    'nocf_growth_rate' : 'operating_cash_flow_ratio',
    'nocf_per_share' : 'free_cash_flow_per_share',
    'nocf_per_share_growth_rate' : 'free_cash_flow_per_share_growth',
    'operating_cash_flow_ttm' : 'operating_cash_flow',
    'operating_margin_ttm' : 'operating_margin',
    'operating_profit_growth_rate' : 'operating_income_growth',
    'operating_profit_to_total_profit' : 'operating_income_to_total_income_ratio',
    'operating_profit_ttm' : 'operating_income',
    'operating_revenue_cash_cover' : 'cash_from_operations_to_revenue_ratio',
    'pb_rate' : 'price_to_book_ratio',
    'pcf_ttm' : 'price_to_cashflow_ratio',
    'pe_ttm' : 'price_to_earnings_ratio',
    'profit_before_tax_growth_rate' : 'pretax_income_growth',
    'profit_to_shareholders_growth_rate' : 'net_income_to_shareholders_growth',
    'ps_ttm' : 'price_to_sales_ratio',
    'return_on_equity_rate' : 'return_on_equity',
    'roa_ttm' : 'return_on_assets',
    'roe_growth_rate' : 'return_on_equity_growth',
    'roic' : 'return_on_invested_capital',
    'roic_growth_rate' : 'return_on_invested_capital_growth',
    'shareholder_net_profit_ttm' : 'net_income_to_shareholders',
    'sum_of_business' : 'revenue',
    'sum_of_business_growth' : 'revenue_growth',
    'total_asset_turnover' : 'asset_turnover',
    'total_assets_growth_rate' : 'total_assets_growth',
    'total_share' : 'total_shares_outstanding',
}

def futu_data_to_financial_profile(data: dict, report_date_str: str, quarter: str) -> List[FinancialProfile]:
    """Converts a dictionary of Futu data into a list of FinancialProfile Pydantic models."""
    profiles = []
    for stock_code, values in data.items():
        # Ensure the ticker includes the market prefix (e.g., 'US.MSFT')
        values['ticker'] = stock_code
        values['report_period'] = report_date_str
        values['period'] = quarter

        # Rename keys based on mapping
        for futu_key, model_key in FUTU_FIELD_MAPPING.items():
            if futu_key in values and values[futu_key] is not None:
                values[model_key] = values[futu_key]

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