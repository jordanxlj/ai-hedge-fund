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
    # 基本信息
    ticker: str                 # 股票代码
    name: str                   # 公司名称
    report_period: str          # 报告期间
    period: str                 # 数据周期
    currency: str | None = None   # 货币单位

    # 估值指标
    price_to_earnings_ratio: float | None = None          # 市盈率
    price_to_book_ratio: float | None = None              # 市净率
    price_to_sales_ratio: float | None = None             # 市销率
    price_to_cashflow_ratio: float | None = None          # 市现率
    enterprise_value_to_ebitda_ratio: float | None = None # 企业价值/EBITDA比率
    enterprise_value_to_revenue_ratio: float | None = None# 企业价值/收入比率
    peg_ratio: float | None = None                        # PEG比率（市盈率相对盈利增长比率）

    # 盈利能力指标
    gross_margin: float | None = None                     # 毛利率
    operating_margin: float | None = None                 # 营业利润率
    net_margin: float | None = None                       # 净利率
    return_on_equity: float | None = None                 # 净资产收益率
    return_on_assets: float | None = None                 # 资产收益率
    return_on_invested_capital: float | None = None       # 投入资本回报率

    # 运营效率指标
    asset_turnover: float | None = None                   # 资产周转率
    fixed_asset_turnover: float | None = None             # 固定资产周转率
    inventory_turnover: float | None = None               # 存货周转率
    receivables_turnover: float | None = None             # 应收账款周转率
    days_sales_outstanding: float | None = None           # 应收账款周转天数
    operating_cycle: float | None = None                  # 运营周期
    working_capital_turnover: float | None = None         # 营运资本周转率

    # 流动性指标
    current_ratio: float | None = None                    # 流动比率
    quick_ratio: float | None = None                      # 速动比率
    cash_ratio: float | None = None                       # 现金比率
    operating_cash_flow_ratio: float | None = None        # 经营现金流比率

    # 杠杆和偿债能力指标
    debt_to_equity: float | None = None                   # 债务权益比
    debt_to_assets: float | None = None                   # 债务资产比
    interest_coverage: float | None = None                # 利息保障倍数
    
    # 增长指标
    revenue_growth: float | None = None                   # 收入增长率
    earnings_growth: float | None = None                  # 盈利增长率
    book_value_growth: float | None = None                # 账面价值增长率
    free_cash_flow_growth: float | None = None            # 自由现金流增长率
    operating_income_growth: float | None = None          # 营业收入增长率
    ebitda_growth: float | None = None                    # EBITDA增长率
    total_assets_growth: float | None = None              # 总资产增长率
    return_on_equity_growth: float | None = None          # 净资产收益率增长率
    return_on_assets_growth: float | None = None          # 资产收益率增长率
    return_on_invested_capital_growth: float | None = None# 投入资本回报率增长率
    earnings_per_share_growth: float | None = None        # 每股收益增长率
    free_cash_flow_per_share_growth: float | None = None  # 每股自由现金流增长率

    # 每股指标
    earnings_per_share: float | None = None               # 每股收益
    book_value_per_share: float | None = None             # 每股账面价值
    free_cash_flow_per_share: float | None = None         # 每股自由现金流

    # 其他指标
    market_cap: float | None = None                       # 市值
    enterprise_value: float | None = None                 # 企业价值
    free_cash_flow_yield: float | None = None             # 自由现金流收益率
    payout_ratio: float | None = None                     # 股息支付比率
    financial_cost_rate: float | None = None              # 财务成本率
    total_shares_outstanding: float | None = None         # 总流通股数
    outstanding_shares: float | None = None               # 流通股数

    # Allow additional fields dynamically
    model_config = {"extra": "allow"}


class FinancialMetricsResponse(BaseModel):
    financial_metrics: list[FinancialMetrics]


class LineItem(BaseModel):
    """Represents a single line item from a financial statement."""
    ticker: str
    report_period: str
    period: str  # e.g., "annual", "quarterly"
    currency: str | None = None
    name: str  # The name of the financial metric (e.g., "revenue", "net_income")
    value: float | None = None
    statement_type: str | None = None  # "income_statement", "balance_sheet", "cash_flow_statement"

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


class FinancialProfile(FinancialMetrics):
    """全面的财务信息，包括从财务报表中提取的核心项目和计算得出的指标。"""

    # 收入相关
    revenue: float | None = None                          # 收入
    gross_profit: float | None = None                     # 毛利润
    operating_expense: float | None = None                # 经营费用
    selling_expenses: float | None = None                 # 销售费用
    sales_expense_ratio: float | None = None              # 销售费用率
    accounts_receivable: float | None = None              # 应收账款净额

    # 利润相关
    net_income: float | None = None                       # 净利润
    operating_income: float | None = None                 # 营业利润
    ebit: float | None = None                             # 息税前利润
    ebitda: float | None = None                           # 息税折旧摊销前利润
    profit_before_tax: float | None = None                # 税前利润
    income_tax_expense: float | None = None               # 所得税费用
    operating_profit_to_total_profit: float | None = None # 营业利润占总利润比

    # 现金流相关
    free_cash_flow: float | None = None                   # 自由现金流
    operating_cash_flow: float | None = None              # 经营现金流
    capital_expenditure: float | None = None              # 资本支出
    depreciation_and_amortization: float | None = None    # 折旧与摊销
    cash_conversion_ratio: float | None = None            # 现金转换率
    operating_revenue_cash_cover: float | None = None     # 经营现金收入比
    capex_to_operating_cash_flow: float | None = None     # 资本支出与经营现金流比

    # 资产负债相关
    total_assets: float | None = None                     # 总资产
    total_liabilities: float | None = None                # 总负债
    current_assets: float | None = None                   # 流动资产
    current_assets_ratio: float | None = None             # 流动资产比率
    current_liabilities: float | None = None              # 流动负债
    current_liabilities_ratio: float | None = None        # 流动负债比率
    working_capital: float | None = None                  # 营运资本
    cash_and_equivalents: float | None = None             # 现金及等价物
    short_term_debt: float | None = None                  # 短期债务
    long_term_debt: float | None = None                   # 长期债务
    total_debt: float | None = None                       # 总债务
    shareholders_equity: float | None = None              # 股东权益
    goodwill: float | None = None                         # 商誉
    intangible_assets: float | None = None                # 无形资产
    goodwill_and_intangible_assets: float | None = None   # 商誉与无形资产
    inventories: float | None = None                      # 存货

    # 股份相关
    issuance_or_purchase_of_equity_shares: float | None = None # 股权发行或回购

    # 股息相关
    dividends_and_other_cash_distributions: float | None = None # 股息和其他现金分配
    dividend_yield: float | None = None                   # 股息收益率

    # 研发相关
    research_and_development: float | None = None         # 研发费用

    # 比率和估值 (非继承自FinancialMetrics的附加字段)
    ev_to_ebit: float | None = None                       # 企业价值对息税前利润比率
    long_term_debt_to_assets_ratio: float | None = None   # 长期债务资产比

    # 投资资本回报率（ROIC）相关
    nopat: float | None = None                            # 税后经营利润
    invested_capital: float | None = None                 # 投资资本
    tax_rate: float | None = None                         # 税率

    # 增长指标 (非继承自FinancialMetrics的附加字段)
    pretax_income_growth: float | None = None             # 税前利润增长率

    model_config = {"extra": "allow"}
