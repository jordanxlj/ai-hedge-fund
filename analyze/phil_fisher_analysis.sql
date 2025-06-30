-- Philip Fisher Style Analysis SQL
--
-- This query analyzes stocks based on an interpretation of Philip Fisher's growth
-- investing philosophy, focusing on innovative companies with long-term potential.
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

fisher_checks AS (
    SELECT
        s.ticker,
        s.name,
        p.smallest_plate,
        p.plate_cluster,
        s.revenue_growth,
        s.research_and_development,
        s.revenue,
        s.net_margin,
        s.gross_margin,
        s.operating_cash_flow,

        -- Criterion 1: Sustained Revenue Growth (> 15%) - weighted 2 points
        CASE WHEN s.revenue_growth > 15 THEN 2 ELSE 0 END AS revenue_growth_check,

        -- Criterion 2: Commitment to Innovation (R&D is > 5% of Revenue) - weighted 2 points
        CASE WHEN s.revenue > 0 AND (s.research_and_development / s.revenue) > 0.05 THEN 2 ELSE 0 END AS innovation_check,

        -- Criterion 3: Superior Profitability (Net Margin > 10%) - weighted 2 points
        CASE WHEN s.net_margin > 10 THEN 2 ELSE 0 END AS profitability_check,

        -- Criterion 4: Strong Competitive Moat (Gross Margin > 50%) - 1 point
        CASE WHEN s.gross_margin > 50 THEN 1 ELSE 0 END AS moat_check,
        
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
    revenue_growth,
    (research_and_development / revenue) AS rnd_to_revenue_ratio,
    net_margin,
    gross_margin,
    (
        revenue_growth_check +
        innovation_check +
        profitability_check +
        moat_check +
        ocf_check
    ) AS phil_fisher_score
FROM fisher_checks
WHERE phil_fisher_score > 0 -- Only show stocks that meet at least one criterion
ORDER BY phil_fisher_score DESC, revenue_growth DESC; 