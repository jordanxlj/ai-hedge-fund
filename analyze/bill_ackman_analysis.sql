-- Bill Ackman (Pershing Square) Style Analysis SQL
--
-- This query analyzes stocks based on an interpretation of Bill Ackman's investment
-- philosophy, focusing on high-quality, predictable, free-cash-flow-generative businesses.
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

ackman_checks AS (
    SELECT
        s.ticker,
        s.name,
        p.smallest_plate,
        p.plate_cluster,
        s.return_on_invested_capital,
        s.free_cash_flow_yield,
        s.gross_margin,
        s.debt_to_equity,

        -- Criterion 1: High-quality business (ROIC > 15%) - weighted 2 points
        CASE WHEN s.return_on_invested_capital > 15 THEN 2 ELSE 0 END AS quality_check,

        -- Criterion 2: Strong FCF generation (FCF Yield > 5%) - weighted 2 points
        CASE WHEN s.free_cash_flow_yield > 0.05 THEN 2 ELSE 0 END AS fcf_check,

        -- Criterion 3: Durable moat (Gross Margin > 50%) - 1 point
        CASE WHEN s.gross_margin > 50 THEN 1 ELSE 0 END AS moat_check,

        -- Criterion 4: Strong balance sheet (Debt-to-Equity < 0.5) - 1 point
        CASE WHEN s.debt_to_equity < 0.5 THEN 1 ELSE 0 END AS balance_sheet_check

    FROM source_data s
    LEFT JOIN clustered_plate_data p ON s.ticker = p.ticker
)

SELECT
    ticker,
    name,
    smallest_plate,
    plate_cluster,
    return_on_invested_capital,
    free_cash_flow_yield,
    gross_margin,
    debt_to_equity,
    (
        quality_check +
        fcf_check +
        moat_check +
        balance_sheet_check
    ) AS bill_ackman_score
FROM ackman_checks
WHERE bill_ackman_score > 0 -- Only show stocks that meet at least one criterion
ORDER BY bill_ackman_score DESC, return_on_invested_capital DESC; 