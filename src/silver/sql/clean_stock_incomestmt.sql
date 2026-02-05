SELECT
    symbol,
    fiscal_year,
    fiscal_date,
    ingested_ts AS raw_ingested_ts,
    last_observed_ts AS raw_lastobserved_ts,
    MAX(
        CASE
            WHEN metric_name = 'CostOfRevenue' THEN value
        END
    ) AS 'cost_of_revenue',
    MAX(
        CASE
            WHEN metric_name = 'EBIT' THEN value
        END
    ) AS 'ebit',
    MAX(
        CASE
            WHEN metric_name = 'EBITDA' THEN value
        END
    ) AS 'ebitda',
    MAX(
        CASE
            WHEN metric_name = 'GrossProfit' THEN value
        END
    ) AS 'gross_profit',
    MAX(
        CASE
            WHEN metric_name = 'NetIncome' THEN value
        END
    ) AS 'net_income',
    MAX(
        CASE
            WHEN metric_name = 'NetIncomeCommonStockholders' THEN value
        END
    ) AS 'net_income_common_stockholders',
    MAX(
        CASE
            WHEN metric_name = 'NetInterestIncome' THEN value
        END
    ) AS 'net_interest_income',
    MAX(
        CASE
            WHEN metric_name = 'OperatingIncome' THEN value
        END
    ) AS 'operating_income',
    MAX(
        CASE
            WHEN metric_name = 'RentExpenseSupplemental' THEN value
        END
    ) AS 'rent_expense_supplemental',
    MAX(
        CASE
            WHEN metric_name = 'SellingGeneralAndAdministration' THEN value
        END
    ) AS 'selling_general_and_administration',
    MAX(
        CASE
            WHEN metric_name = 'TotalExpenses' THEN value
        END
    ) AS 'total_expenses',
    MAX(
        CASE
            WHEN metric_name = 'TotalRevenue' THEN value
        END
    ) AS 'total_revenue',
    MAX(
        CASE
            WHEN metric_name = 'ResearchAndDevelopment' THEN value
        END
    ) AS 'research_and_development',
    MAX(
        CASE
            WHEN metric_name = 'TotalOperatingIncomeAsReported' THEN value
        END
    ) AS 'total_operating_income_as_reported'
FROM
    raw_stock_incomestmt
GROUP BY
    symbol,
    fiscal_year,
    fiscal_date;