SELECT
    symbol,
    fiscal_year,
    fiscal_date,
    ingested_ts AS raw_ingested_ts,
    last_observed_ts AS raw_lastobserved_ts,
    MAX(
        CASE
            WHEN metric_name = 'CapitalExpenditure' THEN value
        END
    ) AS 'capital_expenditure',
    MAX(
        CASE
            WHEN metric_name = 'CapitalExpenditureReported' THEN value
        END
    ) AS 'capital_expenditure_reported',
    MAX(
        CASE
            WHEN metric_name = 'FinancingCashFlow' THEN value
        END
    ) AS 'financing_cash_flow',
    MAX(
        CASE
            WHEN metric_name = 'FreeCashFlow' THEN value
        END
    ) AS 'free_cash_flow',
    MAX(
        CASE
            WHEN metric_name = 'IssuanceOfDebt' THEN value
        END
    ) AS 'issuance_of_debt',
    MAX(
        CASE
            WHEN metric_name = 'OperatingCashFlow' THEN value
        END
    ) AS 'operating_cash_flow',
    MAX(
        CASE
            WHEN metric_name = 'PurchaseOfPPE' THEN value
        END
    ) AS 'purchase_of_ppe',
    MAX(
        CASE
            WHEN metric_name = 'RepaymentOfDebt' THEN value
        END
    ) AS 'repayment_of_debt',
    MAX(
        CASE
            WHEN metric_name = 'SaleOfPPE' THEN value
        END
    ) AS 'sale_of_ppe',
    MAX(
        CASE
            WHEN metric_name = 'CashDividendsPaid' THEN value
        END
    ) AS 'cash_dividends_paid',
    MAX(
        CASE
            WHEN metric_name = 'CommonStockDividendPaid' THEN value
        END
    ) AS 'common_stock_dividend_paid',
    MAX(
        CASE
            WHEN metric_name = 'IssuanceOfCapitalStock' THEN value
        END
    ) AS 'issuance_of_capital_stock',
    MAX(
        CASE
            WHEN metric_name = 'RepurchaseOfCapitalStock' THEN value
        END
    ) AS 'repurchase_of_capital_stock',
    MAX(
        CASE
            WHEN metric_name = 'PreferredStockDividendPaid' THEN value
        END
    ) AS 'preferred_stock_dividend_paid',
    MAX(
        CASE
            WHEN metric_name = 'CashFlowFromContinuingOperatingActivities' THEN value
        END
    ) AS 'cash_flow_from_continuing_operating_activities'
FROM
    raw_stock_cashflowstmt
GROUP BY
    symbol,
    fiscal_year,
    fiscal_date;