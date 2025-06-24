import futu as ft
from typing import List
import logging
from datetime import date, datetime

from src.data.models import FinancialMetrics

logger = logging.getLogger(__name__)

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

def convert_to_financial_metrics(stock_data, ticker: str, name: str, end_date: str, period: str, market=None) -> List[FinancialMetrics]:
    """
    Converts a Futu stock filter result object to a FinancialMetrics object.
    """
    try:
        financial_data = {}
        
        # Determine currency from market
        currency = "USD" # Default
        if market == ft.Market.HK:
            currency = "HKD"
        elif market == ft.Market.US:
            currency = "USD" 
        elif ticker.upper().endswith('.HK'):
             currency = "HKD"
        elif ticker.upper().endswith('.US'):
             currency = "USD"

        # Dynamically extract all available financial data from the stock_data object
        stock_vars = vars(stock_data)
        for key, value in stock_vars.items():
            if isinstance(key, tuple) and len(key) == 2:
                # This is a financial metric, e.g., ('accounts_receivable', 'annual')
                attr_name, _ = key
                financial_data[attr_name] = value
            elif isinstance(key, str) and key not in ['stock_code', 'stock_name']:
                # This could be other data like market_val
                financial_data[key] = value

        # Mapping from Futu fields to FinancialMetrics fields
        # This is not a complete 1-to-1 mapping in names, so we need to be explicit
        # where names differ or logic is needed.
        
        # Valuation Ratios
        financial_data['price_to_earnings_ratio'] = financial_data.pop('pe_ttm', None)
        financial_data['price_to_book_ratio'] = financial_data.pop('pb_rate', None)
        financial_data['price_to_sales_ratio'] = financial_data.pop('ps_ttm', None)
        financial_data['pcf_ratio'] = financial_data.pop('pcf_ttm', None)

        # Market & Shares
        financial_data['market_cap'] = financial_data.pop('market_val', None)
        financial_data['total_shares_outstanding'] = financial_data.pop('total_share', None)
        financial_data['outstanding_shares'] = financial_data.pop('float_share', None)

        # Profitability
        financial_data['net_income'] = financial_data.pop('net_profit', None)
        financial_data['net_margin'] = financial_data.pop('net_profit_rate', None)
        financial_data['gross_margin'] = financial_data.pop('gross_profit_rate', None)
        financial_data['ebit'] = financial_data.pop('ebit_ttm', None)
        financial_data['net_income_to_shareholders'] = financial_data.pop('shareholder_net_profit_ttm', None)

        # Return Ratios
        financial_data['return_on_equity'] = financial_data.pop('return_on_equity_rate', None)
        financial_data['return_on_assets'] = financial_data.pop('roa_ttm', None)
        financial_data['return_on_invested_capital'] = financial_data.pop('roic', None)

        # Growth Rates
        financial_data['earnings_growth'] = financial_data.pop('net_profix_growth', None)
        financial_data['revenue_growth'] = financial_data.pop('sum_of_business_growth', None)
        financial_data['eps_growth'] = financial_data.pop('eps_growth_rate', None)
        financial_data['roe_growth'] = financial_data.pop('roe_growth_rate', None)
        financial_data['roic_growth'] = financial_data.pop('roic_growth_rate', None)
        financial_data['operating_cash_flow_growth'] = financial_data.pop('nocf_growth_rate', None)
        financial_data['free_cash_flow_per_share_growth'] = financial_data.pop('nocf_per_share_growth_rate', None)
        financial_data['operating_income_growth'] = financial_data.pop('operating_profit_growth_rate', None)
        financial_data['total_assets_growth'] = financial_data.pop('total_assets_growth_rate', None)
        financial_data['net_income_to_shareholders_growth'] = financial_data.pop('profit_to_shareholders_growth_rate', None)
        financial_data['pretax_income_growth'] = financial_data.pop('profit_before_tax_growth_rate', None)
        financial_data['ebit_growth'] = financial_data.pop('ebit_growth_rate', None)

        # Financial Health
        financial_data['debt_to_assets'] = financial_data.pop('debt_asset_rate', None)
        financial_data['debt_to_equity'] = financial_data.pop('property_ratio', None)
        financial_data['current_assets_ratio'] = financial_data.pop('current_asset_ratio', None)
        financial_data['current_liabilities_ratio'] = financial_data.pop('current_debt_ratio', None)
        
        # Margins
        financial_data['operating_margin'] = financial_data.pop('operating_margin_ttm', None)
        
        # Cash Flow
        financial_data['operating_cash_flow'] = financial_data.pop('operating_cash_flow_ttm', None)
        financial_data['cash_conversion_ratio'] = financial_data.pop('net_profit_cash_cover_ttm', None)
        financial_data['cash_from_operations_to_revenue_ratio'] = financial_data.pop('operating_revenue_cash_cover', None)
        
        # Per Share
        financial_data['earnings_per_share'] = financial_data.pop('basic_eps', None)
        financial_data['free_cash_flow_per_share'] = financial_data.pop('nocf_per_share', None)

        # Other Balance Sheet / Income Statement items
        financial_data['revenue'] = financial_data.pop('sum_of_business', None)
        financial_data['cash_and_equivalents'] = financial_data.pop('cash_and_cash_equivalents', None)
        financial_data['operating_income'] = financial_data.pop('operating_profit_ttm', None)
        financial_data['operating_income_to_total_income_ratio'] = financial_data.pop('operating_profit_to_total_profit', None)

        # Turnover Ratios
        financial_data['inventory_turnover_ratio'] = financial_data.pop('inventory_turnover', None)
        financial_data['asset_turnover_ratio'] = financial_data.pop('total_asset_turnover', None)
        financial_data['fixed_asset_turnover_ratio'] = financial_data.pop('fixed_asset_turnover', None)

        # Cost Ratios
        financial_data['financial_cost_to_revenue_ratio'] = financial_data.pop('financial_cost_rate', None)

        # Create FinancialMetrics object, filtering out any None values from the dict
        valid_financial_data = {k: v for k, v in financial_data.items() if v is not None}
        
        metrics = FinancialMetrics(
            ticker=ticker,
            name=name,
            report_period=end_date,
            period=period,
            currency=currency,
            **valid_financial_data
        )
        
        return [metrics]
        
    except Exception as e:
        logger.error(f"Failed to convert financial metric data for {ticker}: {e}")
        import traceback
        logger.error(f"Detailed error: {traceback.format_exc()}")
        return [] 