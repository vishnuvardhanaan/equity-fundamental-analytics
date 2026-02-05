WITH
    -- 1. Base metric history
    base AS (
        SELECT
            v.symbol,
            v.sector,
            v.metric_name,
            v.fiscal_year,
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
    -- 2. Trend calculation
    delta_calc AS (
        SELECT
            b.*,
            CASE
                WHEN b.previous_value IS NULL
                OR b.previous_value = 0 THEN NULL
                ELSE (b.current_value - b.previous_value) * 1.0 / ABS(b.previous_value)
            END AS trend_pct
        FROM
            base b
    ),
    -- 3. Trend signal + latest year only
    latest_trend AS (
        SELECT
            d.symbol,
            d.sector,
            d.metric_name,
            d.fiscal_year,
            CASE
                WHEN d.trend_pct IS NULL THEN 'stable'
                WHEN md.higher_the_better = 1
                AND d.trend_pct >= md.threshold THEN 'improving'
                WHEN md.higher_the_better = 1
                AND d.trend_pct <= - md.threshold THEN 'deteriorating'
                WHEN md.higher_the_better = 0
                AND d.trend_pct <= - md.threshold THEN 'improving'
                WHEN md.higher_the_better = 0
                AND d.trend_pct >= md.threshold THEN 'deteriorating'
                ELSE 'stable'
            END AS trend_signal,
            ROW_NUMBER() OVER (
                PARTITION BY
                    d.symbol,
                    d.metric_name
                ORDER BY
                    d.fiscal_year DESC
            ) AS rn
        FROM
            delta_calc d
            JOIN dim_trend md ON d.metric_name = md.metric_name
    ),
    -- 4. Freeze FACT grain
    final_trend_fact AS (
        SELECT
            symbol,
            sector,
            metric_name,
            trend_signal
        FROM
            latest_trend
        WHERE
            rn = 1
    ),
    -- 5. Filter POLICY rules (macro + sector)
    filtered_dim_trend AS (
        SELECT
            metric_name,
            sector_name,
            macro_phase,
            score,
            macro_weight,
            sector_weight
        FROM
            dim_trend
        WHERE
            macro_phase = 'Peak'
    ),
    -- 6. Metric-level scoring
    metric_scores AS (
        SELECT
            f.symbol,
            f.sector,
            d.macro_phase,
            f.metric_name,
            f.trend_signal,
            d.score,
            d.macro_weight,
            d.sector_weight,
            CASE
                WHEN f.trend_signal = 'improving' THEN d.score * d.macro_weight * d.sector_weight
                ELSE 0
            END AS metric_trend_score
        FROM
            final_trend_fact f
            LEFT JOIN filtered_dim_trend d ON f.metric_name = d.metric_name
            AND f.sector = d.sector_name
    )
    -- 7. Final aggregation
SELECT
    symbol,
    sector,
    macro_phase,
    metric_name,
    trend_signal,
    score,
    macro_weight,
    sector_weight,
    metric_trend_score AS trend_score
FROM
    metric_scores
GROUP BY
    symbol;