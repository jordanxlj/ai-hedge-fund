 -- Peter Lynch Style Analysis SQL
--
-- This query analyzes stocks based on an interpretation of Peter Lynch's investment
-- philosophy, focusing on "Growth at a Reasonable Price" (GARP).
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

lynch_checks AS (
    SELECT
        s.ticker,
        s.name,
        p.smallest_plate,
        p.plate_cluster,
        s.peg_ratio,
        s.earnings_growth,
        s.debt_to_equity,
        s.dividend_yield,
        s.price_to_earnings_ratio,

        -- Criterion 1: Favorable PEG ratio (< 1.0) - weighted 2 points
        CASE WHEN s.peg_ratio < 1 AND s.peg_ratio > 0 THEN 2 ELSE 0 END AS peg_check,

        -- Criterion 2: Strong earnings growth (15% < growth < 50%) - 1 point
        CASE WHEN s.earnings_growth > 15 AND s.earnings_growth < 50 THEN 1 ELSE 0 END AS growth_check,

        -- Criterion 3: Low debt (Debt-to-Equity < 0.5) - 1 point
        CASE WHEN s.debt_to_equity < 0.5 THEN 1 ELSE 0 END AS debt_check,

        -- Criterion 4: Pays a dividend - 1 point
        CASE WHEN s.dividend_yield > 0 THEN 1 ELSE 0 END AS dividend_check,
        
        -- Criterion 5: P/E is not excessively high (< 40) - 1 point
        CASE WHEN s.price_to_earnings_ratio < 40 AND s.price_to_earnings_ratio > 0 THEN 1 ELSE 0 END AS pe_check

    FROM source_data s
    LEFT JOIN clustered_plate_data p ON s.ticker = p.ticker
)

SELECT
    ticker,
    name,
    smallest_plate,
    plate_cluster,
    peg_ratio,
    earnings_growth,
    debt_to_equity,
    price_to_earnings_ratio,
    (
        peg_check +
        growth_check +
        debt_check +
        dividend_check +
        pe_check
    ) AS peter_lynch_score
FROM lynch_checks
WHERE peter_lynch_score > 0 -- Only show stocks that meet at least one criterion
ORDER BY peter_lynch_score DESC, peg_ratio ASC;
