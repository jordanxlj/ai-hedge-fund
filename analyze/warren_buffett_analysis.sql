-- Warren Buffett Style Analysis SQL
--
-- This query analyzes stocks based on an interpretation of Warren Buffett's investment
-- philosophy, focusing on high-quality companies with a durable competitive advantage,
-- purchased at a fair price.
-- It uses a common CTE for plate clustering.
--
-- How to use:
-- 1. Replace '{{table_name}}' with the name of your financial profile table.
-- 2. Run this query using the updated Python script.

WITH
-- This placeholder will be replaced by the content of 'common_logic.sql'
{{common_logic}},

source_data AS (
    SELECT * FROM {{table_name}}
),

buffett_checks AS (
    SELECT
        s.ticker,
        s.name,
        p.smallest_plate,
        p.plate_cluster,
        s.return_on_equity,
        s.net_margin,
        s.debt_to_equity,
        s.price_to_earnings_ratio,
        s.operating_cash_flow,

        -- Criterion 1: Durable Competitive Advantage (ROE > 15%) - weighted 2 points
        CASE WHEN s.return_on_equity > 0.15 THEN 2 ELSE 0 END AS quality_roe_check,

        -- Criterion 2: Excellent Profitability (Net Margin > 10%) - weighted 2 points
        CASE WHEN s.net_margin > 10 THEN 2 ELSE 0 END AS profitability_check,

        -- Criterion 3: Conservative Debt (Debt-to-Equity < 0.5) - 1 point
        CASE WHEN s.debt_to_equity < 0.5 THEN 1 ELSE 0 END AS debt_check,

        -- Criterion 4: Fair Price (P/E < 25) - 1 point
        CASE WHEN s.price_to_earnings_ratio < 25 AND s.price_to_earnings_ratio > 0 THEN 1 ELSE 0 END AS valuation_check,
        
        -- Criterion 5: Positive Cash Flow - 1 point
        CASE WHEN s.operating_cash_flow > 0 THEN 1 ELSE 0 END AS ocf_check

    FROM source_data s
    LEFT JOIN clustered_plate_data p ON s.ticker = p.ticker
)

SELECT
    ticker,
    name,
    smallest_plate,
    plate_cluster,
    return_on_equity,
    net_margin,
    debt_to_equity,
    price_to_earnings_ratio,
    (
        quality_roe_check +
        profitability_check +
        debt_check +
        valuation_check +
        ocf_check
    ) AS warren_buffett_score
FROM buffett_checks
WHERE warren_buffett_score > 0 -- Only show stocks that meet at least one criterion
ORDER BY warren_buffett_score DESC, return_on_equity DESC; 