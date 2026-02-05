import logging
import sqlite3

import pandas as pd

from src.common.paths import BRONZE_DB, SILVER_DB, SILVER_SQL_DIR


def run():
    logging.info("Cleaning Stock Balance Sheet Started")

    conn1 = sqlite3.connect(BRONZE_DB)
    conn2 = sqlite3.connect(SILVER_DB)

    sql_file = SILVER_SQL_DIR / "clean_stock_balancesheet.sql"
    query = sql_file.read_text()

    df_rsb = pd.read_sql(query, conn1)

    df_rsb = df_rsb[df_rsb["fiscal_year"] != 2020]
    df_rsb["symbol"] = df_rsb["symbol"].astype("string")
    df_rsb["fiscal_date"] = pd.to_datetime(df_rsb["fiscal_date"])
    df_rsb["raw_ingested_ts"] = pd.to_datetime(df_rsb["raw_ingested_ts"])
    df_rsb["raw_lastobserved_ts"] = pd.to_datetime(df_rsb["raw_lastobserved_ts"])
    df_rsb["total_liabilities"] = df_rsb["total_assets"] - df_rsb["stockholders_equity"]
    df_rsb["clean_processed_ts"] = pd.Timestamp.now(tz="UTC").tz_localize(None)

    df_rsb.to_sql("clean_stock_balancesheet", conn2, if_exists="replace", index=False)

    conn1.close()
    conn2.close()

    logging.info("Cleaning Stock Balance Sheet Completed")
