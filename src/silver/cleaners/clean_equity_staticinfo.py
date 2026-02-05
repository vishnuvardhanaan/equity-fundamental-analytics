import logging
import sqlite3

import pandas as pd

from src.common.paths import BRONZE_DB, SILVER_DB, SILVER_SQL_DIR


def run():
    logging.info("Cleaning Equity Static Information Started")

    conn1 = sqlite3.connect(BRONZE_DB)
    conn2 = sqlite3.connect(SILVER_DB)

    sql_file = SILVER_SQL_DIR / "clean_equity_staticinfo.sql"
    query = sql_file.read_text()

    df_res = pd.read_sql(query, conn1)

    df_res["last_fiscalyear_end"] = pd.to_datetime(df_res["last_fiscalyear_end"])
    df_res["most_recent_quarter"] = pd.to_datetime(df_res["most_recent_quarter"])
    df_res["raw_ingested_ts"] = pd.to_datetime(df_res["raw_ingested_ts"])
    df_res["clean_processed_ts"] = pd.Timestamp.now(tz="UTC").tz_localize(None)

    for col in df_res.select_dtypes(include="object").columns:
        df_res[col] = df_res[col].astype("string")

    df_res.to_sql("clean_equity_staticinfo", conn2, if_exists="replace", index=False)

    conn1.close()
    conn2.close()

    logging.info("Cleaning Equity Static Information Completed")
