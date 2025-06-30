-- Ben Graham Analysis SQL
--
-- This query analyzes stocks from a specified financial profile table
-- based on the investment principles of Benjamin Graham. It calculates a
-- "Graham Score" to identify potentially undervalued, financially sound companies.
--
-- How to use:
-- 1. Replace '{{table_name}}' with the actual name of your financial profile table.
--    For example: financial_profile_2023_12_31
-- 2. Run the query in your DuckDB environment.

WITH
-- This placeholder will be replaced by the content of 'common_plate_cluster_logic.sql'
{{common_logic}},

source_data AS (
    SELECT * FROM {{table_name}}
),

graham_checks AS (
    SELECT
        s.ticker,
        s.name,
        p.smallest_plate,
        p.plate_cluster,
        s.price_to_earnings_ratio,
        s.price_to_book_ratio,
        s.current_ratio,
        s.net_income,
        s.dividend_yield,
        s.market_cap,

        -- Criterion 1: P/E Ratio should be moderate (e.g., < 15)
        CASE WHEN s.price_to_earnings_ratio > 0 AND s.price_to_earnings_ratio < 15 THEN 1 ELSE 0 END AS pe_ratio_check,
        -- Criterion 2: P/B Ratio should be moderate (e.g., < 1.5)
        CASE WHEN s.price_to_book_ratio > 0 AND s.price_to_book_ratio < 1.5 THEN 1 ELSE 0 END AS pb_ratio_check,
        -- Criterion 3: Graham Number check (P/E * P/B < 22.5)
        CASE WHEN (s.price_to_earnings_ratio * s.price_to_book_ratio) < 22.5 THEN 1 ELSE 0 END AS graham_number_check,
        -- Criterion 4: Strong financial position (Current Ratio > 2)
        CASE WHEN s.current_ratio > 2.0 THEN 1 ELSE 0 END AS current_ratio_check,
        -- Criterion 5: Consistent profitability (Positive Net Income)
        CASE WHEN s.net_income > 0 THEN 1 ELSE 0 END AS profitability_check,
        -- Criterion 6: History of paying dividends
        CASE WHEN s.dividend_yield > 0 THEN 1 ELSE 0 END AS dividend_check

    FROM source_data s
    -- Use a LEFT JOIN to ensure all stocks are included, even if they have no plate mapping
    LEFT JOIN clustered_plate_data p ON s.ticker = p.ticker
)

SELECT
    ticker,
    name,
    smallest_plate,
    plate_cluster,
    price_to_earnings_ratio,
    price_to_book_ratio,
    current_ratio,
    net_income,
    dividend_yield,
    market_cap / 10^8 as market_value,
    (
        pe_ratio_check +
        pb_ratio_check +
        graham_number_check +
        current_ratio_check +
        profitability_check +
        dividend_check
    ) AS graham_score
FROM graham_checks
ORDER BY graham_score DESC,
         plate_cluster,
         price_to_earnings_ratio ASC, -- 相同分数下，低 P/E 优先
         price_to_book_ratio ASC,     -- 再按低 P/B 排序
         ticker ASC;                  -- 最后按股票代码排序以确保稳定性