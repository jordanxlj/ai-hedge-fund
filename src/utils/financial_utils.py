from src.data.models import FinancialProfile
import logging

logger = logging.getLogger(__name__)

def reconstruct_financial_metrics(v: FinancialProfile) -> FinancialProfile:
    """
    重构财务指标，计算或重新计算一些复合指标
    
    Args:
        v: An FinancialProfile object.
        
    Returns:
        FinancialProfile: The updated FinancialProfile object.
    """
    logger.debug(f"reconstructing financial metrics for {v.ticker} at {v.report_period}")
    
    # --- Phase 1: Forward calculation (from fundamentals to ratios) ---
    # Goodwill and Intangible Assets
    if v.goodwill_and_intangible_assets is None and v.goodwill is not None and v.intangible_assets is not None:
        v.goodwill_and_intangible_assets = v.goodwill + v.intangible_assets

    # Current Ratio
    if v.current_ratio is None and v.current_assets is not None and v.current_liabilities is not None and v.current_liabilities > 0:
        v.current_ratio = v.current_assets / v.current_liabilities
    
    # Quick Ratio
    if v.quick_ratio is None and v.current_assets is not None and v.inventories is not None and v.current_liabilities is not None and v.current_liabilities > 0:
        v.quick_ratio = (v.current_assets - v.inventories) / v.current_liabilities
        
    # ROE
    if v.return_on_equity is None and v.net_income is not None and v.shareholders_equity is not None and v.shareholders_equity > 0:
        v.return_on_equity = v.net_income / v.shareholders_equity
        
    # ROA
    if v.return_on_assets is None and v.net_income is not None and v.total_assets is not None and v.total_assets > 0:
        v.return_on_assets = v.net_income / v.total_assets
        
    # Net Margin
    if v.net_margin is None and v.net_income is not None and v.revenue is not None and v.revenue > 0:
        v.net_margin = v.net_income / v.revenue
        
    # Working Capital
    if v.working_capital is None and v.current_assets is not None and v.current_liabilities is not None:
        v.working_capital = v.current_assets - v.current_liabilities
        
    # Debt to Equity
    if v.debt_to_equity is None and v.total_debt is not None and v.shareholders_equity is not None and v.shareholders_equity > 0:
        v.debt_to_equity = v.total_debt / v.shareholders_equity
        
    # Gross Margin
    if v.gross_margin is None and v.gross_profit is not None and v.revenue is not None and v.revenue > 0:
        v.gross_margin = v.gross_profit / v.revenue
        logger.debug(f"gross margin: {v.gross_margin}")
        
    # Operating Margin
    if v.operating_margin is None and v.operating_income is not None and v.revenue is not None and v.revenue > 0:
        v.operating_margin = v.operating_income / v.revenue
        logger.debug(f"operating margin: {v.operating_margin}")

    # Sales Expense Ratio
    if v.sales_expense_ratio is None and v.selling_expenses is not None and v.revenue is not None and v.revenue > 0:
        v.sales_expense_ratio = v.selling_expenses / v.revenue
        logger.debug(f"sales expense ratio: {v.sales_expense_ratio}")

    # Long-term Debt to Assets Ratio
    if v.long_term_debt_to_assets_ratio is None and v.long_term_debt is not None and v.total_assets is not None and v.total_assets > 0:
        v.long_term_debt_to_assets_ratio = v.long_term_debt / v.total_assets
        logger.debug(f"long term debt to assets: {v.long_term_debt_to_assets_ratio}")

    # Capex to Operating Cash Flow Ratio
    if v.capex_to_operating_cash_flow is None and v.capital_expenditure is not None and v.operating_cash_flow is not None and v.operating_cash_flow > 0:
        v.capex_to_operating_cash_flow = abs(v.capital_expenditure) / v.operating_cash_flow
        logger.debug(f"capex to operating cash flow : {v.capex_to_operating_cash_flow}")
        
    # --- ROIC Calculation ---
    # 1. Effective Tax Rate
    if v.tax_rate is None and v.income_tax_expense is not None and v.profit_before_tax is not None and v.profit_before_tax > 0:
        v.tax_rate = v.income_tax_expense / v.profit_before_tax
        logger.debug(f"tax rate: {v.tax_rate}")

    # 2. NOPAT
    if v.nopat is None and v.ebit is not None and v.tax_rate is not None:
        v.nopat = v.ebit * (1 - v.tax_rate)
        logger.debug(f"nopat: {v.nopat}")
        
    # 3. Invested Capital
    if v.invested_capital is None and v.total_debt is not None and v.shareholders_equity is not None:
        cash = v.cash_and_equivalents or 0
        v.invested_capital = v.total_debt + v.shareholders_equity - cash
        logger.debug(f"invested capital: {v.invested_capital}")
        
    # 4. ROIC
    if v.return_on_invested_capital is None and v.nopat is not None and v.invested_capital is not None and v.invested_capital > 0:
        v.return_on_invested_capital = v.nopat / v.invested_capital
        logger.debug(f"return on invested capital: {v.return_on_invested_capital}")

    # --- Phase 2: Backward calculation (from ratios to fundamentals) ---
    # This part should be carefully implemented to avoid overwriting existing data.
    # We only calculate if the target fundamental is None.

    # Working Capital
    if v.current_assets is None and v.working_capital is not None and v.current_liabilities is not None:
        v.current_assets = v.working_capital + v.current_liabilities
        logger.debug(f"Reconstructed current_assets from working_capital: {v.current_assets}")
    if v.current_liabilities is None and v.working_capital is not None and v.current_assets is not None:
        v.current_liabilities = v.current_assets - v.working_capital
        logger.debug(f"Reconstructed current_liabilities from working_capital: {v.current_liabilities}")

    # Current Ratio
    if v.current_assets is None and v.current_ratio is not None and v.current_liabilities is not None:
        v.current_assets = v.current_ratio * v.current_liabilities
        logger.debug(f"Reconstructed current_assets from current_ratio: {v.current_assets}")
    if v.current_liabilities is None and v.current_ratio is not None and v.current_assets is not None and v.current_ratio > 0:
        v.current_liabilities = v.current_assets / v.current_ratio
        logger.debug(f"Reconstructed current_liabilities from current_ratio: {v.current_liabilities}")

    # Quick Ratio
    if v.inventories is None and v.quick_ratio is not None and v.current_assets is not None and v.current_liabilities is not None:
        v.inventories = v.current_assets - (v.quick_ratio * v.current_liabilities)
        logger.debug(f"Reconstructed inventories from quick_ratio: {v.inventories}")
    
    # Net Margin
    if v.net_income is None and v.net_margin is not None and v.revenue is not None:
        v.net_income = v.net_margin * v.revenue
        logger.debug(f"Reconstructed net_income from net_margin: {v.net_income}")
    if v.revenue is None and v.net_margin is not None and v.net_income is not None and v.net_margin > 0:
        v.revenue = v.net_income / v.net_margin
        logger.debug(f"Reconstructed revenue from net_margin: {v.revenue}")
        
    # ROE
    if v.net_income is None and v.return_on_equity is not None and v.shareholders_equity is not None:
        v.net_income = v.return_on_equity * v.shareholders_equity
        logger.debug(f"Reconstructed net_income from ROE: {v.net_income}")
    if v.shareholders_equity is None and v.return_on_equity is not None and v.net_income is not None and v.return_on_equity > 0:
        v.shareholders_equity = v.net_income / v.return_on_equity
        logger.debug(f"Reconstructed shareholders_equity from ROE: {v.shareholders_equity}")

    # ROA
    if v.net_income is None and v.return_on_assets is not None and v.total_assets is not None:
        v.net_income = v.return_on_assets * v.total_assets
        logger.debug(f"Reconstructed net_income from ROA: {v.net_income}")
    if v.total_assets is None and v.return_on_assets is not None and v.net_income is not None and v.return_on_assets > 0:
        v.total_assets = v.net_income / v.return_on_assets
        logger.debug(f"Reconstructed total_assets from ROA: {v.total_assets}")

    return v 