import logging
import sqlite3

import pandas as pd

from src.common.paths import BRONZE_DB, SILVER_DB, SILVER_SQL_DIR


def run():
    logging.info("Cleaning Equity Dynamic Information Completed")

    conn1 = sqlite3.connect(BRONZE_DB)
    conn2 = sqlite3.connect(SILVER_DB)

    sql_file = SILVER_SQL_DIR / "clean_equity_dynamicinfo.sql"
    query = sql_file.read_text()

    df_red = pd.read_sql(query, conn1)

    df_red["as_of_date"] = pd.to_datetime(df_red["as_of_date"])
    df_red["last_dividend_date"] = pd.to_datetime(df_red["last_dividend_date"])
    df_red["raw_ingested_ts"] = pd.to_datetime(df_red["raw_ingested_ts"])
    df_red["clean_processed_ts"] = pd.Timestamp.now(tz="UTC").tz_localize(None)
    df_red["symbol"] = df_red["symbol"].astype("string")

    for col in df_red.select_dtypes(include="object").columns:
        df_red[col] = pd.to_numeric(df_red[col], errors="coerce")

    df_red.to_sql("clean_equity_dynamicinfo", conn2, if_exists="replace", index=False)

    conn1.close()
    conn2.close()

    logging.info("Cleaning Equity Dynamic Information Completed")
