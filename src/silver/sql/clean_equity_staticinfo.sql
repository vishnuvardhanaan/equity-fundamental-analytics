SELECT
    CASE
        WHEN symbol LIKE '%.NS' THEN substr(symbol, 1, length(symbol) - 3)
        ELSE symbol
    END AS symbol,
    city,
    industry,
    sector,
    longname AS long_name,
    longbusinesssummary AS long_business_summary,
    date(lastfiscalyearend, 'unixepoch') AS last_fiscalyear_end,
    date(mostrecentquarter, 'unixepoch') AS most_recent_quarter,
    ingested_at AS raw_ingested_ts
FROM
    raw_equity_staticinfo;