-- Cathie Wood (ARK Invest) Style Analysis SQL (Refactored to use Common Plate Logic)
--
-- This query analyzes stocks based on an interpretation of Cathie Wood's investment
-- philosophy, which emphasizes disruptive innovation, high growth, and future potential.
-- It uses a common CTE for plate clustering.
--
-- How to use:
-- 1. Replace '{{table_name}}' with the name of your financial profile table.
-- 2. Run this query using the updated Python script.

WITH
-- This placeholder will be replaced by the content of 'common_plate_cluster_logic.sql'
{{common_logic}},

source_data AS (
    SELECT * FROM {{table_name}}
),

ark_checks AS (
    SELECT
        s.ticker,
        s.name,
        p.smallest_plate,
        p.plate_cluster,
        s.revenue_growth,
        s.research_and_development,
        s.revenue,
        s.gross_margin,

        -- Criterion 1: High revenue growth (> 25%) - weighted 2 points
        CASE WHEN s.revenue_growth > 25 THEN 2 ELSE 0 END AS revenue_growth_check,

        -- Criterion 2: Significant R&D spend (>10% of revenue) - weighted 2 points
        CASE WHEN s.revenue > 0 AND (s.research_and_development / s.revenue) > 0.10 THEN 2 ELSE 0 END AS rnd_investment_check,

        -- Criterion 3: In an innovative sector - 1 point
        CASE WHEN p.plate_cluster IN ('科技与创新', '医疗与健康') THEN 1 ELSE 0 END AS innovation_sector_check,

        -- Criterion 4: High gross margin (> 50%) - 1 point
        CASE WHEN s.gross_margin > 50 THEN 1 ELSE 0 END AS gross_margin_check

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
    gross_margin,
    (
        revenue_growth_check +
        rnd_investment_check +
        innovation_sector_check +
        gross_margin_check
    ) AS cathie_wood_score
FROM ark_checks
WHERE cathie_wood_score > 0 -- Only show stocks that meet at least one criterion
ORDER BY cathie_wood_score DESC,
         revenue_growth DESC;