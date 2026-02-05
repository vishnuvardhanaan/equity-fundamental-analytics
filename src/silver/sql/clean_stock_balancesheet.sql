SELECT
    symbol,
    fiscal_year,
    fiscal_date,
    ingested_ts AS raw_ingested_ts,
    last_observed_ts AS raw_lastobserved_ts,
    MAX(
        CASE
            WHEN metric_name = 'AccountsPayable' THEN value
        END
    ) AS 'accounts_payable',
    MAX(
        CASE
            WHEN metric_name = 'AccountsReceivable' THEN value
        END
    ) AS 'accounts_receivable',
    MAX(
        CASE
            WHEN metric_name = 'AccumulatedDepreciation' THEN value
        END
    ) AS 'accumulated_depreciation',
    MAX(
        CASE
            WHEN metric_name = 'CashAndCashEquivalents' THEN value
        END
    ) AS 'cash_and_cash_equivalents',
    MAX(
        CASE
            WHEN metric_name = 'CashCashEquivalentsAndShortTermInvestments' THEN value
        END
    ) AS 'cash_cash_equivalents_and_short_term_investments',
    MAX(
        CASE
            WHEN metric_name = 'CommonStockEquity' THEN value
        END
    ) AS 'common_stock_equity',
    MAX(
        CASE
            WHEN metric_name = 'CurrentAssets' THEN value
        END
    ) AS 'current_assets',
    MAX(
        CASE
            WHEN metric_name = 'CurrentDebt' THEN value
        END
    ) AS 'current_debt',
    MAX(
        CASE
            WHEN metric_name = 'CurrentLiabilities' THEN value
        END
    ) AS 'current_liabilities',
    MAX(
        CASE
            WHEN metric_name = 'Goodwill' THEN value
        END
    ) AS 'goodwill',
    MAX(
        CASE
            WHEN metric_name = 'GrossPPE' THEN value
        END
    ) AS 'gross_ppe',
    MAX(
        CASE
            WHEN metric_name = 'Inventory' THEN value
        END
    ) AS 'inventory',
    MAX(
        CASE
            WHEN metric_name = 'LongTermDebt' THEN value
        END
    ) AS 'long_term_debt',
    MAX(
        CASE
            WHEN metric_name = 'NetPPE' THEN value
        END
    ) AS 'net_ppe',
    MAX(
        CASE
            WHEN metric_name = 'NetTangibleAssets' THEN value
        END
    ) AS 'net_tangible_assets',
    MAX(
        CASE
            WHEN metric_name = 'OtherIntangibleAssets' THEN value
        END
    ) AS 'other_intangible_assets',
    MAX(
        CASE
            WHEN metric_name = 'StockholdersEquity' THEN value
        END
    ) AS 'stockholders_equity',
    MAX(
        CASE
            WHEN metric_name = 'TotalAssets' THEN value
        END
    ) AS 'total_assets',
    MAX(
        CASE
            WHEN metric_name = 'TotalDebt' THEN value
        END
    ) AS 'total_debt',
    MAX(
        CASE
            WHEN metric_name = 'TotalLiabilitiesNetMinorityInterest' THEN value
        END
    ) AS 'total_liabilities_net_minority_interest',
    MAX(
        CASE
            WHEN metric_name = 'Receivables' THEN value
        END
    ) AS 'receivables'
FROM
    raw_stock_balancesheet
GROUP BY
    symbol,
    fiscal_year,
    fiscal_date;