-- Valuation Strategy Analysis SQL
--
-- This query synthesizes three distinct investment philosophies (Undervalued, High Growth, High Cash Return)
-- into a single, comprehensive analysis. It calculates a score for each strategy and a total score,
-- allowing for a multi-faceted view of each stock.
--
-- How to use:
-- 1. Replace '{{table_name}}' with the name of your financial profile table.
-- 2. The table {{table_name}} should contain historical data with 'ticker', 'report_period', 'revenue', and 'net_income'.
-- 3. Run this query using the updated Python script.

WITH
-- This placeholder will be replaced by the content of 'common_logic.sql'
{{common_logic}},

all_financial_data AS (
    SELECT * FROM {{table_name}}
),

-- Use the latest data for each ticker as the source for scoring
source_data AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY report_period DESC) as rn
        FROM all_financial_data
    )
    WHERE rn = 1
),

-- Calculate 3-year revenue CAGR
revenue_cagr AS (
    SELECT
        ticker,
        -- Using ABS on the denominator to avoid errors with negative or zero start-year revenue
        (POWER(
            (SELECT revenue FROM all_financial_data WHERE ticker = fd.ticker ORDER BY report_period DESC LIMIT 1) / 
            NULLIF(ABS((SELECT revenue FROM all_financial_data WHERE ticker = fd.ticker ORDER BY report_period ASC LIMIT 1)), 0),
            1.0/3
        ) - 1) * 100 AS revenue_cagr_3y
    FROM (SELECT DISTINCT ticker FROM all_financial_data) fd
),

-- Calculate 3-year net income CAGR
net_income_cagr AS (
    SELECT
        ticker,
        -- Using ABS on the denominator to avoid errors with negative or zero start-year net income
        (POWER(
            (SELECT net_income FROM all_financial_data WHERE ticker = fd.ticker ORDER BY report_period DESC LIMIT 1) / 
            NULLIF(ABS((SELECT net_income FROM all_financial_data WHERE ticker = fd.ticker ORDER BY report_period ASC LIMIT 1)), 0),
            1.0/3
        ) - 1) * 100 AS net_income_cagr_3y
    FROM (SELECT DISTINCT ticker FROM all_financial_data) fd
),

-- Score for Undervalued strategy (Ben Graham, Michael Burry)
undervalued_scores AS (
    SELECT
        ticker,
        (
            (CASE WHEN price_to_earnings_ratio < 15 AND price_to_earnings_ratio > 0 THEN 1 ELSE 0 END) +
            (CASE WHEN price_to_book_ratio < 1 AND price_to_book_ratio > 0 THEN 2 ELSE 0 END) +
            (CASE WHEN enterprise_value_to_ebitda_ratio < 6 AND enterprise_value_to_ebitda_ratio > 0 THEN 1 ELSE 0 END) +
            (CASE WHEN current_ratio > 1.5 THEN 1 ELSE 0 END) +
            (CASE WHEN operating_cash_flow > 0 THEN 1 ELSE 0 END) +
            (CASE WHEN net_income > 0 THEN 1 ELSE 0 END)
        ) AS undervalued_score
    FROM source_data
),

-- Score for High Growth strategy (Cathie Wood, Peter Lynch, Phil Fisher)
high_growth_scores AS (
    SELECT
        s.ticker,
        (
            (CASE WHEN s.revenue_growth > 0.15 THEN 1 ELSE 0 END) +
            (CASE WHEN rc.revenue_cagr_3y > 15 THEN 2 WHEN rc.revenue_cagr_3y > 10 THEN 1 ELSE 0 END) +
            (CASE WHEN s.earnings_per_share_growth > 15 THEN 1 ELSE 0 END) +
            (CASE WHEN nic.net_income_cagr_3y > 15 THEN 2 WHEN nic.net_income_cagr_3y > 10 THEN 1 ELSE 0 END) +
            (CASE WHEN s.revenue > 0 AND (s.research_and_development / s.revenue) > 0.05 THEN 1 ELSE 0 END) +
            (CASE WHEN p.plate_cluster IN ('Technology', 'Healthcare', 'Communication Services') THEN 1 ELSE 0 END) +
            (CASE WHEN s.gross_margin > 0.40 THEN 1 ELSE 0 END) +
            (CASE WHEN s.net_margin > 0.10 THEN 1 ELSE 0 END) +
            (CASE WHEN s.peg_ratio < 1.5 AND s.peg_ratio > 0 THEN 1 ELSE 0 END) +
            -- Profitability quality check
            (CASE WHEN s.net_income > 0 AND s.free_cash_flow / s.net_income > 1 THEN 1 ELSE 0 END)
        ) AS high_growth_score
    FROM source_data s
    LEFT JOIN clustered_plate_data p ON s.ticker = p.ticker
    LEFT JOIN revenue_cagr rc ON s.ticker = rc.ticker
    LEFT JOIN net_income_cagr nic ON s.ticker = nic.ticker
),

-- Score for High Cash Return strategy (Warren Buffett, Bill Ackman)
high_cash_return_scores AS (
    SELECT
        ticker,
        (
            (CASE WHEN return_on_equity > 0.15 THEN 1 ELSE 0 END) +
            (CASE WHEN net_margin > 0.15 THEN 1 ELSE 0 END) +
            (CASE WHEN debt_to_equity < 0.5 AND debt_to_equity > 0 THEN 1 ELSE 0 END) +
            (CASE WHEN return_on_invested_capital > 0.12 THEN 1 ELSE 0 END) +
            (CASE WHEN free_cash_flow_yield > 0.05 THEN 1 ELSE 0 END) +
            (CASE WHEN dividend_yield > 0 THEN 1 ELSE 0 END)
        ) AS high_cash_return_score
    FROM source_data
)

-- Final SELECT to join all scores and present the results
SELECT
    s.ticker,
    s.name,
    p.smallest_plate,
    p.plate_cluster,
    -- Format decimal columns to 3 decimal places
    ROUND(s.price_to_earnings_ratio, 3) AS price_to_earnings_ratio,
    ROUND(s.price_to_book_ratio, 3) AS price_to_book_ratio,
    ROUND(s.current_ratio, 3) AS current_ratio,
    ROUND(s.revenue_growth, 3) AS revenue_growth,
    ROUND(s.earnings_per_share_growth, 3) AS earnings_per_share_growth,
    ROUND(nic.net_income_cagr_3y, 2) AS net_income_cagr_3y,
    ROUND(rc.revenue_cagr_3y, 2) AS revenue_cagr_3y,
    ROUND(s.gross_margin, 3) AS gross_profit_margin,
    ROUND(s.net_margin, 3) AS net_profit_margin,
    ROUND(s.peg_ratio, 3) AS peg_ratio,
    ROUND(s.return_on_equity, 3) AS return_on_equity,
    ROUND(s.debt_to_equity, 3) AS debt_to_equity_ratio,
    ROUND(s.return_on_invested_capital, 3) AS return_on_invested_capital,
    ROUND(s.free_cash_flow_yield, 3) AS free_cash_flow_yield,
    ROUND(s.market_cap / 10^8, 2) as market_value,
    COALESCE(u.undervalued_score, 0) AS undervalued_score,
    COALESCE(g.high_growth_score, 0) AS high_growth_score,
    COALESCE(c.high_cash_return_score, 0) AS high_cash_return_score,
    (
        COALESCE(u.undervalued_score, 0) +
        COALESCE(g.high_growth_score, 0) +
        COALESCE(c.high_cash_return_score, 0)
    ) AS total_score
FROM source_data s
LEFT JOIN clustered_plate_data p ON s.ticker = p.ticker
LEFT JOIN undervalued_scores u ON s.ticker = u.ticker
LEFT JOIN high_growth_scores g ON s.ticker = g.ticker
LEFT JOIN high_cash_return_scores c ON s.ticker = c.ticker
LEFT JOIN revenue_cagr rc ON s.ticker = rc.ticker
LEFT JOIN net_income_cagr nic ON s.ticker = nic.ticker
WHERE
    COALESCE(u.undervalued_score, 0) +
    COALESCE(g.high_growth_score, 0) +
    COALESCE(c.high_cash_return_score, 0) > 0
ORDER BY
    total_score DESC,
    high_growth_score DESC,
    high_cash_return_score DESC,
    undervalued_score DESC; 