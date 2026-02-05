import logging
import sqlite3

import pandas as pd

from src.common.paths import BRONZE_DB, SILVER_DB, SILVER_SQL_DIR


def run():
    logging.info("Cleaning Cash Flow Statement Started")

    conn1 = sqlite3.connect(BRONZE_DB)
    conn2 = sqlite3.connect(SILVER_DB)

    sql_file = SILVER_SQL_DIR / "clean_stock_cashflowstmt.sql"
    query = sql_file.read_text()

    df_rsc = pd.read_sql(query, conn1)

    df_rsc = df_rsc[df_rsc["fiscal_year"] != 2020]
    df_rsc["symbol"] = df_rsc["symbol"].astype("string")
    df_rsc["fiscal_date"] = pd.to_datetime(df_rsc["fiscal_date"])
    df_rsc["raw_ingested_ts"] = pd.to_datetime(df_rsc["raw_ingested_ts"])
    df_rsc["raw_lastobserved_ts"] = pd.to_datetime(df_rsc["raw_lastobserved_ts"])
    df_rsc["operating_cash_flow_square"] = df_rsc["operating_cash_flow"] ** 2
    df_rsc["free_cash_flow_square"] = df_rsc["free_cash_flow"] ** 2
    df_rsc["clean_processed_ts"] = pd.Timestamp.now(tz="UTC").tz_localize(None)

    df_rsc.to_sql("clean_stock_cashflowstmt", conn2, if_exists="replace", index=False)

    conn1.close()
    conn2.close()

    logging.info("Cleaning Cash Flow Statement Completed")
