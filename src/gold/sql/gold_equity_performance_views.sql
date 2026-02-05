-- ============================================
-- Financial Health View
-- ============================================
DROP VIEW IF EXISTS vw_financial_health;

CREATE VIEW vw_financial_health AS
SELECT
    symbol,
    company_name,
    sector,
    industry,
    latest_fiscal_year,
    -- Liquidity
    cash_ratio,
    current_ratio,
    working_capital,
    -- Leverage
    debt_to_equity,
    net_leverage,
    short_term_debt_share,
    total_liabilities_to_assets,
    -- Asset quality
    goodwill_share,
    tangible_equity_ratio,
    inventory_intensity,
    receivables_intensity,
    ppe_intensity,
    accumulated_depreciation_ratio
FROM
    gold_equity_derivedmetrics;

-- ============================================
-- Profitability & Growth View
-- ============================================
DROP VIEW IF EXISTS vw_profitability_growth;

CREATE VIEW vw_profitability_growth AS
SELECT
    d.symbol,
    d.company_name,
    d.sector,
    d.industry,
    d.latest_fiscal_year,
    -- Margins
    gross_margin,
    ebit_margin,
    ebitda_margin,
    net_margin,
    margin_stability,
    -- Growth
    d.revenue_cagr_4y,
    d.rent_growth_rate,
    operating_leverage,
    total_revenue_yoy_pct,
    operating_income_yoy_pct
FROM
    gold_equity_derivedmetrics d
    JOIN gold_equity_basemetrics b ON d.symbol = b.symbol;

-- ============================================
-- Cashflow Quality View
-- ============================================
DROP VIEW IF EXISTS vw_cashflow_quality;

CREATE VIEW vw_cashflow_quality AS
SELECT
    symbol,
    company_name,
    sector,
    industry,
    latest_fiscal_year,
    operating_cash_flow,
    ocf_margin,
    cash_conversion_ratio,
    free_cash_flow,
    fcf_margin,
    capex,
    capex_intensity,
    capex_coverage_ratio,
    net_financing_cash_flow,
    dividends_paid
FROM
    gold_equity_derivedmetrics;

-- ============================================
-- Stability & Risk View
-- ============================================
DROP VIEW IF EXISTS vw_stability_risk;

CREATE VIEW vw_stability_risk AS
SELECT
    symbol,
    company_name,
    sector,
    industry,
    latest_fiscal_year,
    revenue_volatility,
    ebit_volatility,
    ocf_volatility,
    fcf_volatility,
    net_income_volatility,
    margin_stability
FROM
    gold_equity_derivedmetrics;

-- ============================================
-- Scoring Explainable View
-- ============================================
DROP VIEW IF EXISTS vw_scoring_explainable;

CREATE VIEW vw_scoring_explainable AS
SELECT
    s.symbol,
    s.macro_phase,
    s.sector,
    s.absolute_score,
    s.relative_score,
    s.trend_score,
    s.total_score,
    r.rule_type,
    r.metric_name,
    r.metric_value,
    r.operator,
    r.comparator_value,
    r.rule_result,
    r.raw_score,
    r.rule_weight,
    r.weighted_score,
    r.run_date
FROM
    gold_scoring_engine s
    JOIN gold_rule_evaluations r ON s.symbol = r.symbol
    AND s.macro_phase = r.macro_phase;

-- ============================================
-- Time Series Trends View
-- ============================================
DROP VIEW IF EXISTS vw_timeseries_trends;

CREATE VIEW vw_timeseries_trends AS
SELECT
    symbol,
    sector,
    fiscal_year,
    metric_name,
    metric_value
FROM
    gold_equity_derivedmetrics_allyears;

-- ============================================
-- Sector Averages View
-- ============================================
DROP VIEW IF EXISTS vw_sector_averages;

CREATE VIEW vw_sector_averages AS
SELECT
    sector,
    AVG(cash_ratio) AS avg_cash_ratio,
    AVG(current_ratio) AS avg_current_ratio,
    AVG(debt_to_equity) AS avg_debt_to_equity,
    AVG(net_leverage) AS avg_net_leverage,
    AVG(total_liabilities_to_assets) AS avg_tl_to_assets,
    AVG(gross_margin) AS avg_gross_margin,
    AVG(ebit_margin) AS avg_ebit_margin,
    AVG(net_margin) AS avg_net_margin,
    AVG(margin_stability) AS avg_margin_stability,
    AVG(revenue_cagr_4y) AS avg_revenue_cagr,
    AVG(operating_leverage) AS avg_operating_leverage,
    AVG(ocf_margin) AS avg_ocf_margin,
    AVG(fcf_margin) AS avg_fcf_margin,
    AVG(cash_conversion_ratio) AS avg_cash_conversion,
    AVG(revenue_volatility) AS avg_revenue_volatility,
    AVG(ebit_volatility) AS avg_ebit_volatility,
    AVG(ocf_volatility) AS avg_ocf_volatility
FROM
    gold_equity_derivedmetrics
GROUP BY
    sector;

-- ============================================
-- Peer Relative Position View
-- ============================================
DROP VIEW IF EXISTS vw_peer_relative_position;

CREATE VIEW vw_peer_relative_position AS
WITH
    latest_year AS (
        SELECT
            symbol,
            sector,
            metric_name,
            metric_value,
            fiscal_year,
            ROW_NUMBER() OVER (
                PARTITION BY
                    symbol,
                    metric_name
                ORDER BY
                    fiscal_year DESC
            ) AS rn
        FROM
            gold_equity_derivedmetrics_allyears
    )
SELECT
    symbol,
    sector,
    metric_name,
    metric_value,
    fiscal_year,
    RANK() OVER (
        PARTITION BY
            sector,
            metric_name
        ORDER BY
            metric_value DESC
    ) AS sector_rank
FROM
    latest_year
WHERE
    rn = 1;