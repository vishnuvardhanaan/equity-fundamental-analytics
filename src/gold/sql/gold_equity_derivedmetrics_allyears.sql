WITH
    company_base AS (
        SELECT
            u.symbol,
            s.sector
        FROM
            clean_equity_universe u
            LEFT JOIN clean_equity_staticinfo s USING (symbol)
    ),
    fiscal_spine AS (
        SELECT
            symbol,
            fiscal_year
        FROM
            clean_stock_balancesheet
        UNION
        SELECT
            symbol,
            fiscal_year
        FROM
            clean_stock_incomestmt
        UNION
        SELECT
            symbol,
            fiscal_year
        FROM
            clean_stock_cashflowstmt
    ),
    balancesheet_required AS (
        SELECT
            *
        FROM
            clean_stock_balancesheet
    ),
    incomestmt_required AS (
        SELECT
            i.*,
            (
                operating_income - LAG (operating_income) OVER (
                    PARTITION BY
                        symbol
                    ORDER BY
                        fiscal_year
                )
            ) / NULLIF(
                LAG (operating_income) OVER (
                    PARTITION BY
                        symbol
                    ORDER BY
                        fiscal_year
                ),
                0
            ) AS operating_income_yoy_pct,
            (
                total_revenue - LAG (total_revenue) OVER (
                    PARTITION BY
                        symbol
                    ORDER BY
                        fiscal_year
                )
            ) / NULLIF(
                LAG (total_revenue) OVER (
                    PARTITION BY
                        symbol
                    ORDER BY
                        fiscal_year
                ),
                0
            ) AS total_revenue_yoy_pct,
            (
                rent_expense_supplemental - LAG (rent_expense_supplemental) OVER (
                    PARTITION BY
                        symbol
                    ORDER BY
                        fiscal_year
                )
            ) / NULLIF(
                LAG (rent_expense_supplemental) OVER (
                    PARTITION BY
                        symbol
                    ORDER BY
                        fiscal_year
                ),
                0
            ) AS rent_growth_rate,
            AVG(ebit) OVER (
                PARTITION BY
                    symbol
                ORDER BY
                    fiscal_year ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            ) AS ebit_mean_ny,
            SQRT(
                AVG(ebit * ebit) OVER (
                    PARTITION BY
                        symbol
                    ORDER BY
                        fiscal_year ROWS BETWEEN UNBOUNDED PRECEDING
                        AND CURRENT ROW
                ) - POWER(
                    AVG(ebit) OVER (
                        PARTITION BY
                            symbol
                        ORDER BY
                            fiscal_year ROWS BETWEEN UNBOUNDED PRECEDING
                            AND CURRENT ROW
                    ),
                    2
                )
            ) AS std_ebit_ny,
            AVG(net_income) OVER (
                PARTITION BY
                    symbol
                ORDER BY
                    fiscal_year ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            ) AS net_income_mean_ny,
            SQRT(
                AVG(net_income * net_income) OVER (
                    PARTITION BY
                        symbol
                    ORDER BY
                        fiscal_year ROWS BETWEEN UNBOUNDED PRECEDING
                        AND CURRENT ROW
                ) - POWER(
                    AVG(net_income) OVER (
                        PARTITION BY
                            symbol
                        ORDER BY
                            fiscal_year ROWS BETWEEN UNBOUNDED PRECEDING
                            AND CURRENT ROW
                    ),
                    2
                )
            ) AS std_net_income_ny,
            AVG(total_revenue) OVER (
                PARTITION BY
                    symbol
                ORDER BY
                    fiscal_year ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            ) AS total_revenue_mean_ny,
            SQRT(
                AVG(total_revenue * total_revenue) OVER (
                    PARTITION BY
                        symbol
                    ORDER BY
                        fiscal_year ROWS BETWEEN UNBOUNDED PRECEDING
                        AND CURRENT ROW
                ) - POWER(
                    AVG(total_revenue) OVER (
                        PARTITION BY
                            symbol
                        ORDER BY
                            fiscal_year ROWS BETWEEN UNBOUNDED PRECEDING
                            AND CURRENT ROW
                    ),
                    2
                )
            ) AS std_total_revenue_ny,
            AVG(operating_margin) OVER (
                PARTITION BY
                    symbol
            ) AS operating_margin_mean_ny,
            SQRT(
                AVG(operating_margin_square) OVER (
                    PARTITION BY
                        symbol
                ) - POWER(
                    AVG(operating_margin) OVER (
                        PARTITION BY
                            symbol
                    ),
                    2
                )
            ) AS std_operating_margin_ny,
            POWER(
                LAST_VALUE (total_revenue) OVER (
                    PARTITION BY
                        symbol
                    ORDER BY
                        fiscal_year ROWS BETWEEN UNBOUNDED PRECEDING
                        AND UNBOUNDED FOLLOWING
                ) / FIRST_VALUE (total_revenue) OVER (
                    PARTITION BY
                        symbol
                    ORDER BY
                        fiscal_year
                ),
                1.0 / 4
            ) - 1 AS revenue_cagr_4y
        FROM
            clean_stock_incomestmt i
    ),
    cashflowstmt_required AS (
        SELECT
            c.*,
            AVG(free_cash_flow) OVER (
                PARTITION BY
                    symbol
                ORDER BY
                    fiscal_year ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            ) AS fcf_mean_ny,
            SQRT(
                AVG(free_cash_flow * free_cash_flow) OVER (
                    PARTITION BY
                        symbol
                    ORDER BY
                        fiscal_year ROWS BETWEEN UNBOUNDED PRECEDING
                        AND CURRENT ROW
                ) - POWER(
                    AVG(free_cash_flow) OVER (
                        PARTITION BY
                            symbol
                        ORDER BY
                            fiscal_year ROWS BETWEEN UNBOUNDED PRECEDING
                            AND CURRENT ROW
                    ),
                    2
                )
            ) AS std_fcf_ny,
            AVG(operating_cash_flow) OVER (
                PARTITION BY
                    symbol
                ORDER BY
                    fiscal_year ROWS BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            ) AS ocf_mean_ny,
            SQRT(
                AVG(operating_cash_flow * operating_cash_flow) OVER (
                    PARTITION BY
                        symbol
                    ORDER BY
                        fiscal_year ROWS BETWEEN UNBOUNDED PRECEDING
                        AND CURRENT ROW
                ) - POWER(
                    AVG(operating_cash_flow) OVER (
                        PARTITION BY
                            symbol
                        ORDER BY
                            fiscal_year ROWS BETWEEN UNBOUNDED PRECEDING
                            AND CURRENT ROW
                    ),
                    2
                )
            ) AS std_ocf_ny
        FROM
            clean_stock_cashflowstmt c
    ),
    gold_equity_basemetrics AS (
        SELECT
            f.symbol,
            c.sector,
            f.fiscal_year,
            b.*,
            i.*,
            cf.*
        FROM
            fiscal_spine f
            JOIN company_base c ON f.symbol = c.symbol
            LEFT JOIN balancesheet_required b ON f.symbol = b.symbol
            AND f.fiscal_year = b.fiscal_year
            LEFT JOIN incomestmt_required i ON f.symbol = i.symbol
            AND f.fiscal_year = i.fiscal_year
            LEFT JOIN cashflowstmt_required cf ON f.symbol = cf.symbol
            AND f.fiscal_year = cf.fiscal_year
        WHERE
            c.sector IS NOT NULL
    ),
    base AS (
        SELECT
            *,
            cash_and_cash_equivalents AS cash_and_cash_equivalents,
            current_assets AS current_assets,
            cash_and_cash_equivalents / NULLIF(current_liabilities, 0) AS cash_ratio,
            current_assets / NULLIF(current_liabilities, 0) AS current_ratio,
            current_assets - current_liabilities AS working_capital,
            inventory / NULLIF(current_assets, 0) AS inventory_to_current_assets,
            total_assets as total_assets,
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
            total_revenue AS revenue,
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
            gold_equity_basemetrics
    )
SELECT
    symbol,
    sector,
    fiscal_year,
    'cash_and_cash_equivalents' AS metric_name,
    cash_and_cash_equivalents AS metric_value
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'current_assets',
    current_assets
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'cash_ratio',
    cash_ratio
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'current_ratio',
    current_ratio
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'working_capital',
    working_capital
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'accounts_payable',
    accounts_payable
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'inventory',
    inventory
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'inventory_to_current_assets',
    inventory_to_current_assets
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'total_assets',
    total_assets
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'long_term_debt',
    long_term_debt
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'total_debt',
    total_debt
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'current_debt',
    current_debt
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'debt_to_equity',
    debt_to_equity
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'net_debt',
    net_debt
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'net_leverage',
    net_leverage
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'short_term_debt_share',
    short_term_debt_share
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'current_liabilities',
    current_liabilities
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'total_liabilities',
    total_liabilities
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'total_liabilities_to_assets',
    total_liabilities_to_assets
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'goodwill_share',
    goodwill_share
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'net_tangible_assets',
    net_tangible_assets
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'tangible_equity_ratio',
    tangible_equity_ratio
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'inventory_intensity',
    inventory_intensity
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'receivables_intensity',
    receivables_intensity
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'ppe_intensity',
    ppe_intensity
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'net_ppe',
    net_ppe
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'accumulated_depreciation_ratio',
    accumulated_depreciation_ratio
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'payables_intensity',
    payables_intensity
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'operating_margin',
    operating_margin
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'gross_margin',
    gross_margin
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'ebitda_margin',
    ebitda_margin
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'ebit_margin',
    ebit_margin
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'net_margin',
    net_margin
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'net_income',
    net_income
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'revenue',
    revenue
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'revenue_cagr_4y',
    revenue_cagr_4y
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'rent_growth_rate',
    rent_growth_rate
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'revenue_volatility',
    revenue_volatility
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'margin_stability',
    margin_stability
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'ebit_volatility',
    ebit_volatility
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'ocf_volatility',
    ocf_volatility
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'fcf_volatility',
    fcf_volatility
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'net_income_volatility',
    net_income_volatility
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'operating_leverage',
    operating_leverage
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'sga_to_revenue',
    sga_to_revenue
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'cost_to_revenue',
    cost_to_revenue
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'cost_to_income',
    cost_to_income
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'rd_intensity',
    rd_intensity
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'operating_cash_flow',
    operating_cash_flow
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'cash_conversion_ratio',
    cash_conversion_ratio
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'ocf_margin',
    ocf_margin
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'free_cash_flow',
    free_cash_flow
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'fcf_margin',
    fcf_margin
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'capex_coverage_ratio',
    capex_coverage_ratio
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'capex_intensity',
    capex_intensity
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'capex',
    capex
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'net_financing_cash_flow',
    net_financing_cash_flow
FROM
    base
UNION ALL
SELECT
    symbol,
    sector,
    fiscal_year,
    'dividends_paid',
    dividends_paid
FROM
    base;