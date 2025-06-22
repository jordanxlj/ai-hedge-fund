from pydantic import BaseModel
from enum import Enum


class TransactionType(str, Enum):
    """交易类型枚举"""
    BUY = "增持"
    SELL = "减持"
    PURCHASE = "购买"
    SALE = "出售"


class Price(BaseModel):
    open: float
    close: float
    high: float
    low: float
    volume: int
    time: str
    ticker: str | None = None


class PriceResponse(BaseModel):
    ticker: str
    prices: list[Price]


class FinancialMetrics(BaseModel):
    ticker: str
    report_period: str
    period: str
    currency: str | None = None
    market_cap: float | None = None
    enterprise_value: float | None = None
    price_to_earnings_ratio: float | None = None
    price_to_book_ratio: float | None = None
    price_to_sales_ratio: float | None = None
    enterprise_value_to_ebitda_ratio: float | None = None
    enterprise_value_to_revenue_ratio: float | None = None
    free_cash_flow_yield: float | None = None
    peg_ratio: float | None = None
    gross_margin: float | None = None
    operating_margin: float | None = None
    net_margin: float | None = None
    return_on_equity: float | None = None
    return_on_assets: float | None = None
    return_on_invested_capital: float | None = None
    asset_turnover: float | None = None
    inventory_turnover: float | None = None
    receivables_turnover: float | None = None
    days_sales_outstanding: float | None = None
    operating_cycle: float | None = None
    working_capital_turnover: float | None = None
    current_ratio: float | None = None
    quick_ratio: float | None = None
    cash_ratio: float | None = None
    operating_cash_flow_ratio: float | None = None
    debt_to_equity: float | None = None
    debt_to_assets: float | None = None
    interest_coverage: float | None = None
    revenue_growth: float | None = None
    earnings_growth: float | None = None
    book_value_growth: float | None = None
    earnings_per_share_growth: float | None = None
    free_cash_flow_growth: float | None = None
    operating_income_growth: float | None = None
    ebitda_growth: float | None = None
    payout_ratio: float | None = None
    earnings_per_share: float | None = None
    book_value_per_share: float | None = None
    free_cash_flow_per_share: float | None = None
    
    # Tushare specific fields (for compatibility)
    roe: float | None = None  # Return on equity
    roa: float | None = None  # Return on assets  
    eps: float | None = None  # Earnings per share
    pe_ratio: float | None = None  # PE ratio
    pb_ratio: float | None = None  # PB ratio
    
    # Allow additional fields dynamically
    model_config = {"extra": "allow"}


class FinancialMetricsResponse(BaseModel):
    financial_metrics: list[FinancialMetrics]


class LineItem(BaseModel):
    ticker: str
    report_period: str
    period: str
    currency: str | None = None

    # Allow additional fields dynamically
    model_config = {"extra": "allow"}


class LineItemResponse(BaseModel):
    search_results: list[LineItem]


class InsiderTrade(BaseModel):
    ticker: str
    issuer: str | None = None
    name: str | None = None
    title: str | None = None
    is_board_director: bool | None = None
    transaction_date: str | None = None
    transaction_shares: float | None = None
    transaction_price_per_share: float | None = None
    transaction_value: float | None = None
    shares_owned_before_transaction: float | None = None
    shares_owned_after_transaction: float | None = None
    security_title: str | None = None
    filing_date: str
    
    # Tushare specific fields (for compatibility)
    transaction_type: TransactionType | None = None  # 交易类型
    change_ratio: float | None = None  # 占流通比例(%)
    after_ratio: float | None = None  # 变动后占流通比例(%)
    
    # Allow additional fields dynamically
    model_config = {"extra": "allow"}


class InsiderTradeResponse(BaseModel):
    insider_trades: list[InsiderTrade]


class CompanyNews(BaseModel):
    ticker: str
    title: str
    author: str
    source: str
    date: str
    url: str
    sentiment: str | None = None


class CompanyNewsResponse(BaseModel):
    company_news: list[CompanyNews]


class CompanyFacts(BaseModel):
    ticker: str
    name: str
    cik: str | None = None
    industry: str | None = None
    sector: str | None = None
    category: str | None = None
    exchange: str | None = None
    is_active: bool | None = None
    listing_date: str | None = None
    location: str | None = None
    market_cap: float | None = None
    number_of_employees: int | None = None
    sec_filings_url: str | None = None
    sic_code: str | None = None
    sic_industry: str | None = None
    sic_sector: str | None = None
    website_url: str | None = None
    weighted_average_shares: int | None = None


class CompanyFactsResponse(BaseModel):
    company_facts: CompanyFacts


class Position(BaseModel):
    cash: float = 0.0
    shares: int = 0
    ticker: str


class Portfolio(BaseModel):
    positions: dict[str, Position]  # ticker -> Position mapping
    total_cash: float = 0.0


class AnalystSignal(BaseModel):
    signal: str | None = None
    confidence: float | None = None
    reasoning: dict | str | None = None
    max_position_size: float | None = None  # For risk management signals


class TickerAnalysis(BaseModel):
    ticker: str
    analyst_signals: dict[str, AnalystSignal]  # agent_name -> signal mapping


class AgentStateData(BaseModel):
    tickers: list[str]
    portfolio: Portfolio
    start_date: str
    end_date: str
    ticker_analyses: dict[str, TickerAnalysis]  # ticker -> analysis mapping


class AgentStateMetadata(BaseModel):
    show_reasoning: bool = False
    model_config = {"extra": "allow"}


class AggregatedFinancialInfo(FinancialMetrics):
    """
    聚合财务信息模型，继承自FinancialMetrics，
    并添加各个Agent查询的LineItem属性
    """
    
    # 收入相关
    revenue: float | None = None
    gross_profit: float | None = None
    operating_expense: float | None = None
    
    # 利润相关
    net_income: float | None = None
    operating_income: float | None = None
    ebit: float | None = None  # 息税前利润
    ebitda: float | None = None  # 息税折旧摊销前利润
    ev_to_ebit: float | None = None # 企业价值对息税前利润的比率
    
    # 现金流相关
    free_cash_flow: float | None = None
    operating_cash_flow: float | None = None
    capital_expenditure: float | None = None
    depreciation_and_amortization: float | None = None
    
    # 资产负债相关
    total_assets: float | None = None
    total_liabilities: float | None = None
    current_assets: float | None = None
    current_liabilities: float | None = None
    working_capital: float | None = None
    cash_and_equivalents: float | None = None
    short_term_debt: float | None = None
    long_term_debt: float | None = None
    total_debt: float | None = None
    shareholders_equity: float | None = None
    goodwill: float | None = None
    intangible_assets: float | None = None
    goodwill_and_intangible_assets: float | None = None
    inventories: float | None = None
    
    # 股份相关
    outstanding_shares: float | None = None
    #earnings_per_share: float | None = None  # already in the FinancialMetrics
    
    # 研发和其他
    research_and_development: float | None = None
    dividends_and_other_cash_distributions: float | None = None
    issuance_or_purchase_of_equity_shares: float | None = None
    
    # 比率和百分比（已在FinancialMetrics中定义的不重复）
    debt_to_equity_ratio: float | None = None
    
    # 允许动态添加字段
    model_config = {"extra": "allow"}
