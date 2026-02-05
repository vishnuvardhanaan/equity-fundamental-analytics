CREATE VIEW IF NOT EXISTS vw_company_score_summary AS
SELECT
    symbol,
    sector,
    macro_phase,
    run_date,
    SUM(weighted_score) AS total_score,
    SUM(
        CASE
            WHEN rule_type = 'absolute' THEN weighted_score
            ELSE 0
        END
    ) AS absolute_score,
    SUM(
        CASE
            WHEN rule_type = 'relative' THEN weighted_score
            ELSE 0
        END
    ) AS relative_score,
    SUM(
        CASE
            WHEN rule_type = 'trend' THEN weighted_score
            ELSE 0
        END
    ) AS trend_score
FROM
    fact_rule_evaluations
GROUP BY
    symbol,
    sector,
    macro_phase,
    run_date;

CREATE VIEW IF NOT EXISTS vw_company_strengths AS
SELECT
    symbol,
    sector,
    macro_phase,
    run_date,
    metric_name,
    metric_value,
    comparator_value,
    weighted_score,
    '• ' || metric_name || ' of ' || ROUND(metric_value, 2) || ' exceeds benchmark (' || ROUND(comparator_value, 2) || ')' AS strength_text
FROM
    fact_rule_evaluations
WHERE
    weighted_score > 0
ORDER BY
    weighted_score DESC;

CREATE VIEW IF NOT EXISTS vw_company_weaknesses AS
SELECT
    symbol,
    sector,
    macro_phase,
    run_date,
    metric_name,
    metric_value,
    comparator_value,
    rule_weight,
    '• ' || metric_name || ' of ' || ROUND(metric_value, 2) || ' falls short of required level (' || ROUND(comparator_value, 2) || ')' AS weakness_text
FROM
    fact_rule_evaluations
WHERE
    rule_result = 'fail'
    AND rule_weight > 0
ORDER BY
    rule_weight DESC;

CREATE VIEW IF NOT EXISTS vw_company_improvement_levers AS
SELECT
    symbol,
    sector,
    macro_phase,
    run_date,
    metric_name,
    ROUND(comparator_value - metric_value, 2) AS required_improvement,
    rule_weight AS potential_score_gain,
    '• Improving ' || metric_name || ' by ' || ROUND(comparator_value - metric_value, 2) || ' would add +' || ROUND(rule_weight, 2) || ' to the score' AS improvement_text
FROM
    fact_rule_evaluations
WHERE
    rule_result = 'fail'
    AND operator IN ('>', '>=') -- only meaningful counterfactuals
ORDER BY
    rule_weight DESC;

CREATE VIEW IF NOT EXISTS vw_company_explanation AS
WITH
    summary AS (
        SELECT
            *
        FROM
            vw_company_score_summary
    ),
    strengths AS (
        SELECT
            symbol,
            macro_phase,
            run_date,
            GROUP_CONCAT (strength_text, CHAR(10)) AS strengths
        FROM
            (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            symbol,
                            macro_phase,
                            run_date
                        ORDER BY
                            weighted_score DESC
                    ) AS rn
                FROM
                    vw_company_strengths
            )
        WHERE
            rn <= 3
        GROUP BY
            symbol,
            macro_phase,
            run_date
    ),
    weaknesses AS (
        SELECT
            symbol,
            macro_phase,
            run_date,
            GROUP_CONCAT (weakness_text, CHAR(10)) AS weaknesses
        FROM
            (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            symbol,
                            macro_phase,
                            run_date
                        ORDER BY
                            rule_weight DESC
                    ) AS rn
                FROM
                    vw_company_weaknesses
            )
        WHERE
            rn <= 2
        GROUP BY
            symbol,
            macro_phase,
            run_date
    ),
    improvements AS (
        SELECT
            symbol,
            macro_phase,
            run_date,
            GROUP_CONCAT (improvement_text, CHAR(10)) AS improvement_levers
        FROM
            (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            symbol,
                            macro_phase,
                            run_date
                        ORDER BY
                            potential_score_gain DESC
                    ) AS rn
                FROM
                    vw_company_improvement_levers
            )
        WHERE
            rn = 1
        GROUP BY
            symbol,
            macro_phase,
            run_date
    )
SELECT
    s.symbol,
    s.sector,
    s.macro_phase,
    s.run_date,
    s.total_score,
    s.absolute_score,
    s.relative_score,
    s.trend_score,
    COALESCE(
        strengths.strengths,
        '• No major strengths identified'
    ) AS strengths,
    COALESCE(
        weaknesses.weaknesses,
        '• No major weaknesses identified'
    ) AS weaknesses,
    COALESCE(
        improvements.improvement_levers,
        '• No high-impact improvement levers identified'
    ) AS improvement_levers
FROM
    summary s
    LEFT JOIN strengths ON s.symbol = strengths.symbol
    AND s.macro_phase = strengths.macro_phase
    AND s.run_date = strengths.run_date
    LEFT JOIN weaknesses ON s.symbol = weaknesses.symbol
    AND s.macro_phase = weaknesses.macro_phase
    AND s.run_date = weaknesses.run_date
    LEFT JOIN improvements ON s.symbol = improvements.symbol
    AND s.macro_phase = improvements.macro_phase
    AND s.run_date = improvements.run_date;