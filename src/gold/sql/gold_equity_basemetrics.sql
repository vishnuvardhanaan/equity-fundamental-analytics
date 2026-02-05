WITH
    -- 1) Latest dynamic (weekly) market data
    dynamicinfo_required AS (
        SELECT
            symbol,
            current_price,
            shares_outstanding,
            float_shares,
            held_percent_insiders,
            held_percent_institutions,
            beta
        FROM
            (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            symbol
                        ORDER BY
                            as_of_date DESC
                    ) AS rn
                FROM
                    clean_equity_dynamicinfo
            )
        WHERE
            rn = 1
    ),
    -- 2) Company identity + static + dynamic
    company_base AS (
        SELECT
            u.symbol,
            u.company_name,
            s.city,
            s.sector,
            s.industry,
            s.long_business_summary,
            d.current_price,
            d.shares_outstanding,
            d.float_shares,
            d.held_percent_insiders,
            d.held_percent_institutions,
            d.beta
        FROM
            clean_equity_universe u
            LEFT JOIN clean_equity_staticinfo s USING (symbol)
            LEFT JOIN dynamicinfo_required d USING (symbol)
    ),
    -- 3) Latest balance sheet
    balancesheet_required AS (
        SELECT
            symbol,
            fiscal_year,
            accounts_payable,
            accounts_receivable,
            accumulated_depreciation,
            cash_and_cash_equivalents,
            cash_cash_equivalents_and_short_term_investments,
            common_stock_equity,
            current_assets,
            current_debt,
            current_liabilities,
            goodwill,
            gross_ppe,
            inventory,
            long_term_debt,
            net_ppe,
            net_tangible_assets,
            other_intangible_assets,
            stockholders_equity,
            total_assets,
            total_debt,
            total_liabilities_net_minority_interest,
            receivables,
            total_liabilities,
            ROW_NUMBER() OVER (
                PARTITION BY
                    symbol
                ORDER BY
                    fiscal_year DESC
            ) AS rn
        FROM
            clean_stock_balancesheet
    ),
    -- 4) Income statement with derived metrics
    incomestmt_required AS (
        SELECT
            symbol,
            fiscal_year,
            cost_of_revenue,
            ebit,
            ebit_square,
            ebitda,
            gross_profit,
            net_income,
            net_income_square,
            net_income_common_stockholders,
            net_interest_income,
            operating_income,
            rent_expense_supplemental,
            selling_general_and_administration,
            total_expenses,
            total_revenue,
            total_revenue_square,
            research_and_development,
            total_operating_income_as_reported,
            operating_margin,
            operating_margin_square,
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
            ) AS ebit_mean_ny,
            SQRT(
                AVG(ebit_square) OVER (
                    PARTITION BY
                        symbol
                ) - POWER(
                    AVG(ebit) OVER (
                        PARTITION BY
                            symbol
                    ),
                    2
                )
            ) AS std_ebit_ny,
            AVG(net_income) OVER (
                PARTITION BY
                    symbol
            ) AS net_income_mean_ny,
            SQRT(
                AVG(net_income_square) OVER (
                    PARTITION BY
                        symbol
                ) - POWER(
                    AVG(net_income) OVER (
                        PARTITION BY
                            symbol
                    ),
                    2
                )
            ) AS std_net_income_ny,
            AVG(total_revenue) OVER (
                PARTITION BY
                    symbol
            ) AS total_revenue_mean_ny,
            SQRT(
                AVG(total_revenue_square) OVER (
                    PARTITION BY
                        symbol
                ) - POWER(
                    AVG(total_revenue) OVER (
                        PARTITION BY
                            symbol
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
            ) - 1 AS revenue_cagr_4y,
            ROW_NUMBER() OVER (
                PARTITION BY
                    symbol
                ORDER BY
                    fiscal_year DESC
            ) AS rn
        FROM
            clean_stock_incomestmt
    ),
    -- 5) Cash flow with means
    cashflowstmt_required AS (
        SELECT
            symbol,
            fiscal_year,
            capital_expenditure,
            capital_expenditure_reported,
            financing_cash_flow,
            free_cash_flow,
            free_cash_flow_square,
            issuance_of_debt,
            operating_cash_flow,
            operating_cash_flow_square,
            purchase_of_ppe,
            repayment_of_debt,
            sale_of_ppe,
            cash_dividends_paid,
            common_stock_dividend_paid,
            issuance_of_capital_stock,
            repurchase_of_capital_stock,
            preferred_stock_dividend_paid,
            cash_flow_from_continuing_operating_activities,
            AVG(free_cash_flow) OVER (
                PARTITION BY
                    symbol
            ) AS fcf_mean_ny,
            SQRT(
                AVG(free_cash_flow_square) OVER (
                    PARTITION BY
                        symbol
                ) - POWER(
                    AVG(free_cash_flow) OVER (
                        PARTITION BY
                            symbol
                    ),
                    2
                )
            ) AS std_fcf_ny,
            AVG(operating_cash_flow) OVER (
                PARTITION BY
                    symbol
            ) AS ocf_mean_ny,
            SQRT(
                AVG(operating_cash_flow_square) OVER (
                    PARTITION BY
                        symbol
                ) - POWER(
                    AVG(operating_cash_flow) OVER (
                        PARTITION BY
                            symbol
                    ),
                    2
                )
            ) AS std_ocf_ny,
            ROW_NUMBER() OVER (
                PARTITION BY
                    symbol
                ORDER BY
                    fiscal_year DESC
            ) AS rn
        FROM
            clean_stock_cashflowstmt
    )
    -- 6) Final projection
SELECT
    c.symbol,
    c.company_name,
    c.city,
    c.sector,
    c.industry,
    c.long_business_summary,
    b.fiscal_year AS latest_fiscal_year,
    c.current_price,
    c.shares_outstanding,
    c.float_shares,
    c.held_percent_insiders,
    c.held_percent_institutions,
    c.beta,
    b.accounts_payable,
    b.accounts_receivable,
    b.accumulated_depreciation,
    b.cash_and_cash_equivalents,
    b.cash_cash_equivalents_and_short_term_investments,
    b.common_stock_equity,
    b.current_assets,
    b.current_debt,
    b.current_liabilities,
    b.goodwill,
    b.gross_ppe,
    b.inventory,
    b.long_term_debt,
    b.net_ppe,
    b.net_tangible_assets,
    b.other_intangible_assets,
    b.stockholders_equity,
    b.total_assets,
    b.total_debt,
    b.total_liabilities_net_minority_interest,
    b.total_liabilities,
    i.cost_of_revenue,
    i.ebit,
    i.ebit_mean_ny,
    i.std_ebit_ny,
    i.ebitda,
    i.gross_profit,
    i.net_income,
    i.net_income_mean_ny,
    i.std_net_income_ny,
    i.net_income_common_stockholders,
    i.net_interest_income,
    i.operating_income,
    i.operating_income_yoy_pct,
    i.operating_margin,
    i.operating_margin_mean_ny,
    i.std_operating_margin_ny,
    i.rent_growth_rate,
    i.total_expenses,
    i.total_revenue,
    i.total_revenue_yoy_pct,
    i.total_revenue_mean_ny,
    i.std_total_revenue_ny,
    i.revenue_cagr_4y,
    i.research_and_development,
    i.selling_general_and_administration,
    cf.capital_expenditure,
    cf.financing_cash_flow,
    cf.free_cash_flow,
    cf.fcf_mean_ny,
    cf.std_fcf_ny,
    cf.operating_cash_flow,
    cf.ocf_mean_ny,
    cf.std_ocf_ny,
    cf.purchase_of_ppe,
    cf.repayment_of_debt,
    cf.sale_of_ppe,
    cf.cash_dividends_paid
FROM
    company_base c
    LEFT JOIN balancesheet_required b ON c.symbol = b.symbol
    AND b.rn = 1
    LEFT JOIN incomestmt_required i ON c.symbol = i.symbol
    AND i.rn = 1
    LEFT JOIN cashflowstmt_required cf ON c.symbol = cf.symbol
    AND cf.rn = 1
WHERE
    latest_fiscal_year IS NOT NULL
    AND sector IS NOT NULL;