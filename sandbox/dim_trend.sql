WITH
    base AS (
        SELECT
            v.symbol,
            v.sector,
            v.fiscal_year,
            v.metric_name,
            v.metric_value AS current_value,
            LAG (v.metric_value) OVER (
                PARTITION BY
                    v.symbol,
                    v.metric_name
                ORDER BY
                    v.fiscal_year
            ) AS previous_value
        FROM
            gold_equity_derivedmetrics_allyears v
    ),
    delta_calc AS (
        SELECT
            b.*,
            CASE
            -- First available year OR invalid prior value
                WHEN b.previous_value IS NULL
                OR b.previous_value = 0 THEN NULL
                ELSE (b.current_value - b.previous_value) * 1.0 / ABS(b.previous_value)
            END AS trend_pct
        FROM
            base b
    )
SELECT
    d.symbol,
    d.sector,
    d.metric_name,
    d.fiscal_year,
    d.current_value,
    d.previous_value,
    d.trend_pct,
    md.higher_the_better,
    CASE
    -- First year: no trend by definition
        WHEN d.trend_pct IS NULL THEN 'stable'
        -- Higher is better metrics
        WHEN md.higher_the_better = 1
        AND d.trend_pct >= 0.02 THEN 'improving'
        WHEN md.higher_the_better = 1
        AND d.trend_pct <= -0.02 THEN 'deteriorating'
        -- Lower is better metrics
        WHEN md.higher_the_better = 0
        AND d.trend_pct <= -0.02 THEN 'improving'
        WHEN md.higher_the_better = 0
        AND d.trend_pct >= 0.02 THEN 'deteriorating'
        -- Noise band
        ELSE 'stable'
    END AS trend_signal
    -- ROW_NUMBER() OVER (
    --     PARTITION BY
    --         d.symbol,
    --         d.metric_name
    --     ORDER BY
    --         d.fiscal_year DESC
    -- ) AS rn
FROM
    delta_calc d
    JOIN dim_trend md ON d.metric_name = md.metric_name
    -- SELECT
    --     symbol,
    --     sector,
    --     CASE
    --         WHEN trend_signal = 'improving'
    --         AND macro_phase = 'Peak' THEN score * macro_weight * sector_weight
    --         ELSE 0
    --     END AS metric_trend_score
    -- FROM
    --     ranked_trend
    -- WHERE
    --     rn = 1