import logging
import sqlite3

import pandas as pd

from src.common.paths import BRONZE_DB, SILVER_DB, SILVER_SQL_DIR


def run():
    logging.info("Cleaning Stock Income Statement Started")

    conn1 = sqlite3.connect(BRONZE_DB)
    conn2 = sqlite3.connect(SILVER_DB)

    sql_file = SILVER_SQL_DIR / "clean_stock_incomestmt.sql"
    query = sql_file.read_text()

    df_rsi = pd.read_sql(query, conn1)

    df_rsi = df_rsi[df_rsi["fiscal_year"] != 2020]
    df_rsi["symbol"] = df_rsi["symbol"].astype("string")
    df_rsi["fiscal_date"] = pd.to_datetime(df_rsi["fiscal_date"])
    df_rsi["raw_ingested_ts"] = pd.to_datetime(df_rsi["raw_ingested_ts"])
    df_rsi["raw_lastobserved_ts"] = pd.to_datetime(df_rsi["raw_lastobserved_ts"])
    df_rsi["ebit_square"] = df_rsi["ebit"] ** 2
    df_rsi["net_income_square"] = df_rsi["net_income"] ** 2
    df_rsi["total_revenue_square"] = df_rsi["total_revenue"] ** 2
    df_rsi["operating_margin"] = df_rsi["operating_income"] / df_rsi["total_revenue"]
    df_rsi["operating_margin_square"] = (
        df_rsi["operating_income"] / df_rsi["total_revenue"]
    ) ** 2
    df_rsi["clean_processed_ts"] = pd.Timestamp.now(tz="UTC").tz_localize(None)

    df_rsi.to_sql("clean_stock_incomestmt", conn2, if_exists="replace", index=False)

    conn1.close()
    conn2.close()

    logging.info("Cleaning Stock Income Statement Completed")
