-- Michael Burry Style Analysis SQL
--
-- This query analyzes stocks based on an interpretation of Michael Burry's investment
-- philosophy, focusing on deep value, a strong margin of safety, and overlooked assets.
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

burry_checks AS (
    SELECT
        s.ticker,
        s.name,
        p.smallest_plate,
        p.plate_cluster,
        s.price_to_book_ratio,
        s.enterprise_value_to_ebitda_ratio,
        s.debt_to_equity,
        s.operating_cash_flow,

        -- Criterion 1: Deep value (Price-to-Book < 0.75) - weighted 2 points
        CASE WHEN s.price_to_book_ratio < 0.75 AND s.price_to_book_ratio > 0 THEN 2 ELSE 0 END AS pb_check,

        -- Criterion 2: Low EV/EBITDA (< 5) - weighted 2 points
        CASE WHEN s.enterprise_value_to_ebitda_ratio < 5 AND s.enterprise_value_to_ebitda_ratio > 0 THEN 2 ELSE 0 END AS ev_ebitda_check,

        -- Criterion 3: Strong balance sheet (Debt-to-Equity < 0.5) - 1 point
        CASE WHEN s.debt_to_equity < 0.5 THEN 1 ELSE 0 END AS balance_sheet_check,

        -- Criterion 4: Positive operating cash flow - 1 point
        CASE WHEN s.operating_cash_flow > 0 THEN 1 ELSE 0 END AS ocf_check

    FROM source_data s
    LEFT JOIN clustered_plate_data p ON s.ticker = p.ticker
)

SELECT
    ticker,
    name,
    smallest_plate,
    plate_cluster,
    price_to_book_ratio,
    enterprise_value_to_ebitda_ratio,
    debt_to_equity,
    operating_cash_flow,
    (
        pb_check +
        ev_ebitda_check +
        balance_sheet_check +
        ocf_check
    ) AS michael_burry_score
FROM burry_checks
WHERE michael_burry_score > 0 -- Only show stocks that meet at least one criterion
ORDER BY michael_burry_score DESC, enterprise_value_to_ebitda_ratio ASC, price_to_book_ratio ASC; 