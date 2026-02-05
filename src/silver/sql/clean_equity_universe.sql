SELECT
    SYMBOL AS symbol,
    ISIN_NUMBER AS isin,
    NAME_OF_COMPANY AS company_name,
    INGESTED_TS AS raw_ingested_ts
FROM
    raw_equity_universe
WHERE
    SERIES = 'EQ';