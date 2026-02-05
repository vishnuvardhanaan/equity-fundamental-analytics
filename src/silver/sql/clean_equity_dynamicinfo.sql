SELECT
    symbol,
    as_of_date,
    --Market snapshot
    fulltimeemployees AS fulltime_employees,
    currentprice AS current_price,
    marketcap AS market_capitalization,
    enterprisevalue AS enterprise_value,
    --Risk & behavior
    sharesoutstanding AS shares_outstanding,
    impliedsharesoutstanding AS implied_shares_outstanding,
    floatshares AS float_shares,
    heldpercentinsiders AS held_percent_insiders,
    heldpercentinstitutions AS held_percent_institutions,
    beta,
    --Valuation ratios
    trailingpe AS trailing_pe,
    forwardpe AS forward_pe,
    pricetobook AS price_to_book,
    enterprisetorevenue AS enterprise_to_revenue,
    enterprisetoebitda AS enterprise_to_ebitda,
    --Capital structure & scale
    totalcash AS total_cash,
    totaldebt AS total_debt,
    totalrevenue AS total_revenue,
    ebitda,
    netincometocommon AS netincome_to_common,
    --Profitability & return metrics
    profitmargins AS profit_margins,
    grossmargins AS gross_margins,
    operatingmargins AS operating_margins,
    ebitdamargins AS ebitda_margins,
    returnonequity AS return_on_equity,
    returnonassets AS return_on_assets,
    debttoequity AS debt_to_equity,
    freecashflow AS free_cash_flow,
    operatingcashflow AS operating_cash_flow,
    --Liquidity ratios
    currentratio AS current_ratio,
    quickratio AS quick_ratio,
    --Dividends
    dividendrate AS dividend_rate,
    dividendyield AS dividend_yield,
    payoutratio AS payout_ratio,
    lastdividendvalue AS last_dividend_value,
    date(lastdividenddate, 'unixepoch') AS last_dividend_date,
    ingested_at as raw_ingested_ts
FROM
    raw_equity_dynamicinfo