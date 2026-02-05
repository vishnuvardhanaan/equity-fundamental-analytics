SELECT
    -- Identifiers / dimensions
    symbol,
    company_name,
    city,
    sector,
    industry,
    latest_fiscal_year,
    /* ---------- Liquidity ---------- */
    cash_and_cash_equivalents AS cash_and_cash_equivalents,
    current_assets AS current_assets,
    cash_and_cash_equivalents / NULLIF(current_liabilities, 0) AS cash_ratio,
    current_assets / NULLIF(current_liabilities, 0) AS current_ratio,
    current_assets - current_liabilities AS working_capital,
    inventory / NULLIF(current_assets, 0) AS inventory_to_current_assets,
    /* ---------- Leverage & Capital Structure ---------- */
    long_term_debt AS long_term_debt,
    total_debt AS total_debt,
    current_debt AS current_debt,
    total_debt / NULLIF(stockholders_equity, 0) AS debt_to_equity,
    total_debt - cash_and_cash_equivalents AS net_debt,
    (total_debt - cash_and_cash_equivalents) / NULLIF(ebitda, 0) AS net_leverage,
    current_debt / NULLIF(total_debt, 0) AS short_term_debt_share,
    current_liabilities AS current_liabilities,
    total_liabilities AS total_liabilities,
    total_liabilities / NULLIF(total_assets, 0) AS total_liabilities_to_assets,
    /* ---------- Asset Quality ---------- */
    goodwill / NULLIF(total_assets, 0) AS goodwill_share,
    net_tangible_assets AS net_tangible_assets,
    net_tangible_assets / NULLIF(stockholders_equity, 0) AS tangible_equity_ratio,
    inventory AS inventory,
    inventory / NULLIF(total_assets, 0) AS inventory_intensity,
    accounts_receivable / NULLIF(total_assets, 0) AS receivables_intensity,
    net_ppe AS net_ppe,
    net_ppe / NULLIF(total_assets, 0) AS ppe_intensity,
    abs(accumulated_depreciation) / NULLIF(gross_ppe, 0) AS accumulated_depreciation_ratio,
    accounts_payable AS accounts_payable,
    accounts_payable / NULLIF(total_liabilities, 0) AS payables_intensity,
    /* ---------- Profitability ---------- */
    operating_income / NULLIF(total_revenue, 0) AS operating_margin,
    gross_profit / NULLIF(total_revenue, 0) AS gross_margin,
    ebitda / NULLIF(total_revenue, 0) AS ebitda_margin,
    ebit / NULLIF(total_revenue, 0) AS ebit_margin,
    net_income / NULLIF(total_revenue, 0) AS net_margin,
    net_income AS net_income,
    /* ---------- Growth ---------- */
    revenue_cagr_4y AS revenue_cagr_4y,
    rent_growth_rate AS rent_growth_rate,
    /* ---------- Stability & Volatility ---------- */
    std_total_revenue_ny / NULLIF(total_revenue_mean_ny, 0) AS revenue_volatility,
    1.0 / NULLIF(std_operating_margin_ny, 0) AS margin_stability,
    std_ebit_ny / NULLIF(ebit_mean_ny, 0) AS ebit_volatility,
    std_ocf_ny / NULLIF(ocf_mean_ny, 0) AS ocf_volatility,
    std_fcf_ny / NULLIF(fcf_mean_ny, 0) AS fcf_volatility,
    std_net_income_ny / NULLIF(net_income_mean_ny, 0) AS net_income_volatility,
    /* ---------- Operating Dynamics ---------- */
    operating_income_yoy_pct / NULLIF(total_revenue_yoy_pct, 0) AS operating_leverage,
    /* ---------- Cost Structure ---------- */
    selling_general_and_administration / NULLIF(total_revenue, 0) AS sga_to_revenue,
    cost_of_revenue / NULLIF(total_revenue, 0) AS cost_to_revenue,
    total_expenses / NULLIF(total_revenue, 0) AS cost_to_income,
    research_and_development / NULLIF(total_revenue, 0) AS rd_intensity,
    /* ---------- Cash Flow ---------- */
    operating_cash_flow AS operating_cash_flow,
    operating_cash_flow / NULLIF(net_income, 0) AS cash_conversion_ratio,
    operating_cash_flow / NULLIF(total_revenue, 0) AS ocf_margin,
    free_cash_flow AS free_cash_flow,
    free_cash_flow / NULLIF(total_revenue, 0) AS fcf_margin,
    operating_cash_flow / NULLIF(abs(capital_expenditure), 0) AS capex_coverage_ratio,
    abs(capital_expenditure) / NULLIF(total_revenue, 0) AS capex_intensity,
    abs(capital_expenditure) AS capex,
    /* ---------- Capital Allocation ---------- */
    financing_cash_flow AS net_financing_cash_flow,
    cash_dividends_paid AS dividends_paid
FROM
    gold_equity_basemetrics;